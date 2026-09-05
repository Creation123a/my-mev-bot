package main

import (
	"context"
	"fmt"
	"log"
	"math"
	"math/big"
	"os"
	"os/signal"
	"runtime"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	gethTypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
	"golang.org/x/sys/unix"
	
	"my-mev-bot/Bot/Bonding"
	"my-mev-bot/Bot/Config"
	"my-mev-bot/Bot/Dashboard"
	"my-mev-bot/Bot/Execution"
	gatekeeperpkg "my-mev-bot/Bot/Gatekeeper"
	"my-mev-bot/Bot/Ingestion"
	"my-mev-bot/Bot/Predictive"
	"my-mev-bot/Bot/Solver"
	"my-mev-bot/Bot/State"
	"my-mev-bot/Bot/Types"
	//"my-mev-bot/Bot/Liquidation"
)

const (
	eventChanSize         = 2048
	candidateChanSize     = 128
	executionChanSize     = 64
	defaultGasLimit       = 500000
	maxCandidatesPerEvent = 8
	numSimWorkers         = 4
)

var (
	dashServer   *dashboard.DashboardServer
	profitMu     sync.Mutex
	totalProfit  float64
	lastLogMu    sync.Mutex
	lastSwapLog  *types.SwapLog
	activeTrades uint32 // used by memoryGuardian (kept for compatibility, but GC removed)
	dryRun       bool
)
type blockInfo struct {
    Number uint64
    Hash   common.Hash
}
var latestBlock atomic.Value // stores *blockInfo

// Global price cache used only in the fallback path of worker2.
var globalPriceCache sync.Map

var payloadPool = sync.Pool{
	New: func() interface{} {
		return &types.ExecutionPayload{}
	},
}

// Event signature for ArbitrageExecuted
var arbitrageExecutedEvent abi.Event
var executorABI *abi.ABI

func init() {
	parsed, err := abi.JSON(strings.NewReader(`[{"anonymous":false,"inputs":[{"indexed":true,"name":"loanToken","type":"address"},{"indexed":false,"name":"loanAmount","type":"uint256"},{"indexed":false,"name":"profit","type":"uint256"},{"indexed":false,"name":"minProfit","type":"uint256"},{"indexed":false,"name":"success","type":"bool"}],"name":"ArbitrageExecuted","type":"event"}]`))
	if err != nil {
		panic(err)
	}
	executorABI = &parsed
	arbitrageExecutedEvent = parsed.Events["ArbitrageExecuted"]
	predictive.SetPayloadPool(&payloadPool)
}

func getPayload() *types.ExecutionPayload {
	return payloadPool.Get().(*types.ExecutionPayload)
}

func putPayload(p *types.ExecutionPayload) {
	p.Reset()
	payloadPool.Put(p)
}

func pinToCore(core int) {
	runtime.LockOSThread()
	var mask unix.CPUSet
	mask.Zero()
	mask.Set(core)
	if err := unix.SchedSetaffinity(0, &mask); err != nil {
		log.Printf("Failed to pin to core %d: %v", core, err)
	} else {
		log.Printf("[Kernel] Pinned worker to CPU Core %d", core)
	}
}

func main() {
	dryRun = os.Getenv("DRY_RUN") == "true"
	// ===== 1. Set incremental GC (removes STW pauses) =====
	debug.SetGCPercent(50)

	// ===== 2. Lock main thread to prevent migration =====
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	cfg, err := config.LoadConfig()
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	if err := solver.InitExecutorABI(); err != nil {
		log.Fatalf("failed to init executor ABI: %v", err)
	}

	dashServer = dashboard.NewDashboardServer()
	go func() {
		defer recoverPanic("Dashboard server")
		if err := dashServer.Start(":8080"); err != nil {
			log.Printf("Dashboard server error: %v", err)
		}
	}()

	printStartupBanner(cfg)

	if cfg.ReplaceBumpPercent < 10 {
		log.Printf("Warning: REPLACE_BUMP_PERCENT=%d is below 10; clamping to 10.", cfg.ReplaceBumpPercent)
		cfg.ReplaceBumpPercent = 10
	}

	privateKey, err := crypto.HexToECDSA(cfg.PrivateKey)
	if err != nil {
		log.Fatalf("Failed to parse private key: %v", err)
	}
	ownerAddress := crypto.PubkeyToAddress(privateKey.PublicKey)

	matrix := state.NewMatrix()
	blacklist := state.NewBlacklist()
	lru := state.NewMemeTokenCache()

	anchors := config.AnchorAssets()
	anchorSet := make(map[common.Address]bool, len(anchors))
	for _, a := range anchors {
		anchorSet[a] = true
	}
	blacklist.SetProtectedAddresses(anchors[:])

	ethClient, err := ethclient.Dial(cfg.BaseHTTPRPC)
	if err != nil {
		log.Fatalf("Failed to create eth client: %v", err)
	}
nonceTracker := state.NewNonceTracker()
if err := nonceTracker.SyncFromNode(ethClient, ownerAddress); err != nil {
    log.Fatalf("Failed to sync nonce from node: %v", err)
}


	if err := solver.FetchCurrentFees(ethClient); err != nil {
		log.Printf("Warning: failed to fetch initial fees: %v", err)
	}
	matrix.SetEthClient(ethClient)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
// ---- Periodic nonce refresh ----
go func() {
    ticker := time.NewTicker(30 * time.Second)
    defer ticker.Stop()
    for {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
            if err := nonceTracker.SyncFromNode(ethClient, ownerAddress); err != nil {
                log.Printf("[Nonce] Sync failed: %v", err)
            }
        }
    }
}()
	// ---- Predictive Flashblock PLL ----
	pll := predictive.NewFlashblockPLL()
	wsClient, err := ethclient.Dial(cfg.BaseWSRPC)
	if err != nil {
		log.Fatalf("Failed to create WebSocket client for PLL: %v", err)
	}
	defer wsClient.Close()

go func() {
    defer recoverPanic("PLL subscriber")
    headers := make(chan *gethTypes.Header, 10)
    sub, err := wsClient.SubscribeNewHead(ctx, headers)
    if err != nil {
        log.Printf("[PLL] Failed to subscribe to new heads: %v", err)
        return
    }
    defer sub.Unsubscribe()
    for {
        select {
        case <-ctx.Done():
            return
        case err := <-sub.Err():
            log.Printf("[PLL] Subscription error: %v", err)
            return
        case head := <-headers:
            pll.RecordBlockTick(time.Now())
            // Cache the latest block info (atomic store)
            latestBlock.Store(&blockInfo{
                Number: head.Number.Uint64(),
                Hash:   head.Hash(),
            })
        }
    }
}()
	// ===== 3. Create StateCache and warm it up =====
	stateCache := execution.NewStateCache()

	loanProviders := []common.Address{config.BalancerVault}
	if cfg.DODOPoolAddress != "" {
		dodoAddr := common.HexToAddress(cfg.DODOPoolAddress)
		if dodoAddr != (common.Address{}) {
			loanProviders = append(loanProviders, dodoAddr)
		}
	}
	
	allAddrs := append(loanProviders, anchors[:]...)
	for _, pool := range matrix.GetPools() {
		allAddrs = append(allAddrs, pool.Token0, pool.Token1)
	}
	// In main.go, after loanProviders and before WarmUpAddresses:
bondingFactories := []common.Address{
    common.HexToAddress("0x1A540088125d00dD3990f9dA45CA0859af4d3B01"), // Virtuals
    common.HexToAddress("0xC68007C16088d228EF0DF92dB6A9FA19F57b9A23"), // MoltMoon
    common.HexToAddress("0x7706d3389A197D667793Fe4991A5406085FFdfD6"), // Base.meme
    common.HexToAddress("0x5C0Ce7E1df7bE75E4De827E6A94EFE6F0764D00b"), // ClawLaunch
    common.HexToAddress("0x3c267B8053683A3FeE9dbDEAA65e06a3e6A6133B"), // Pump.fun
    common.HexToAddress("0x8FA4b802779BBe63ffE72b947f9FBE676A3D801a"), // Thryx
}
allAddrs = append(allAddrs, bondingFactories...)
allAddrs = append(allAddrs, common.HexToAddress(cfg.BondingExecutorAddress))
	stateCache.WarmUpAddresses(ethClient, allAddrs)

	// ---- Create GEVMSimulator ----
	gevm := execution.NewGEVMSimulator(cfg.BaseHTTPRPC, cfg.BaseWSRPC, ownerAddress)
	gevm.SetMatrix(matrix)

	executorCode, err := ethClient.CodeAt(ctx, cfg.ExecutorAddress, nil)
	if err != nil {
		log.Printf("Warning: failed to fetch executor code: %v; local EVM may not work.", err)
	} else {
		stateCache.SetCode(cfg.ExecutorAddress, executorCode)
	}
	poolsMap := matrix.GetPools()
	for _, pool := range poolsMap {
		code, err := ethClient.CodeAt(ctx, pool.PoolAddress, nil)
		if err != nil {
			log.Printf("Warning: failed to fetch code for pool %s: %v", pool.PoolAddress.Hex(), err)
			continue
		}
		stateCache.SetCode(pool.PoolAddress, code)
	}
	gevm.SetStateCache(stateCache)

	memeCache := lru
	dexCache := state.NewDEXFactoryCache()
	pairCache := state.NewBasePairCache()

	var gk *gatekeeperpkg.Gatekeeper
if !dryRun {
    gk = gatekeeperpkg.New(
        ethClient,
        gevm,
        memeCache,
        dexCache,
        pairCache,
        blacklist,
        matrix,
        ownerAddress,
    )
}

	gevm.StartWebSocketContextUpdater(ctx)

	// ---- Sender ----
	sender, err := execution.NewSender(cfg.BaseExecRPC, cfg.PrivateKey)
	if err != nil {
		log.Fatalf("Failed to initialize sender: %v", err)
	}

	sender.SetReplaceConfig(
		time.Duration(cfg.ReplaceTimeoutMs)*time.Millisecond,
		float64(100+cfg.ReplaceBumpPercent)/100.0,
		cfg.MaxReplaceAttempts,
	)
	sender.SetEthPrice(solver.GetEthPrice())
	sender.SetReleasePayloadFunc(putPayload)

	// Nonce rollback callback
	sender.SetRollbackNonceFunc(func() {
		nonceTracker.Rollback()
	})

	sender.SetConfirmationCallback(func(nonce uint64, txHash common.Hash, receipt *gethTypes.Receipt, payload *types.ExecutionPayload) {
		defer func() {
			if payload != nil {
				putPayload(payload)
			}
		}()
		var profitWei *big.Int
		for _, vLog := range receipt.Logs {
			if vLog.Address == cfg.ExecutorAddress && len(vLog.Topics) > 0 && vLog.Topics[0] == arbitrageExecutedEvent.ID {
				var event struct {
					LoanToken  common.Address
					LoanAmount *big.Int
					Profit     *big.Int
					MinProfit  *big.Int
					Success    bool
				}
				err := executorABI.UnpackIntoInterface(&event, "ArbitrageExecuted", vLog.Data)
				if err == nil && event.Success {
					profitWei = event.Profit
					break
				}
			}
		}

		var profitUSD float64
		if profitWei != nil && profitWei.Sign() > 0 {
			tokenPrice, ok := solver.GetTokenPrice(payload.BorrowedToken, matrix, &globalPriceCache)
if !ok || tokenPrice <= 0 {
    tokenPrice = 1.0
}
			decimals := solver.GetTokenDecimals(payload.BorrowedToken)
			profitUSD = (float64FromBig(profitWei) / math.Pow10(decimals)) * tokenPrice
			ethPrice := solver.GetEthPrice()
			l2BaseFeeUSD := solver.GetCurrentL2BaseFeeUSD()
			priorityFeeUSD := float64(payload.PriorityFeeWei) * ethPrice / 1e18
			gasCostUSD := float64(receipt.GasUsed) * (l2BaseFeeUSD + priorityFeeUSD)
			profitUSD -= gasCostUSD
		} else {
			profitUSD = payload.MinProfitUSD
		}

		profitMu.Lock()
		totalProfit += profitUSD
		profitMu.Unlock()

		if dashServer != nil {
			dashServer.AddProfit(profitUSD)
			dashServer.SetTradeStatus("SUCCESS", "")
			msg := fmt.Sprintf("[+] WIN | Tx: %s | Realised Profit: $%.2f | Route: %s | Block: %d",
				txHash.Hex(), profitUSD, payload.RouteDesc, receipt.BlockNumber.Uint64())
			dashServer.Log(msg)
		}
		log.Printf("[+] WIN | Tx: %s | Realised Profit: $%.2f | Route: %s | Block: %d",
			txHash.Hex(), profitUSD, payload.RouteDesc, receipt.BlockNumber.Uint64())
	})

	sender.SetSolverCallback(func(retryCtx execution.RetryContext) []*types.RouteCandidate {
		lastLogMu.Lock()
		logEntry := lastSwapLog
		lastLogMu.Unlock()
		if logEntry == nil {
			return nil
		}
		allCands := solver.EvaluateEvent(logEntry, matrix, cfg)
		if len(allCands) == 0 {
			return nil
		}
		avoidPools := make(map[common.Address]bool)
		for _, pool := range retryCtx.Pools {
			if pool != (common.Address{}) {
				avoidPools[pool] = true
			}
		}
		type scoredCand struct {
			cand  *types.RouteCandidate
			score float64
		}
		var scored []scoredCand
		for _, cand := range allCands {
			usesFailedPool := false
			for i := 0; i < int(cand.Hops); i++ {
				if avoidPools[cand.Pools[i]] {
					usesFailedPool = true
					break
				}
			}
			if usesFailedPool {
				continue
			}
			score := cand.ExpectedProfitUSD
			if retryCtx.OriginalCandidate != nil && cand.Hops != retryCtx.OriginalCandidate.Hops {
				score *= 1.15
			}
			if retryCtx.OriginalCandidate != nil {
				shared := 0
				total := int(cand.Hops) + 1
				origSet := make(map[common.Address]bool)
				for _, t := range retryCtx.OriginalCandidate.Tokens[:retryCtx.OriginalCandidate.Hops+1] {
					if t != (common.Address{}) {
						origSet[t] = true
					}
				}
				for i := 0; i < total; i++ {
					if origSet[cand.Tokens[i]] {
						shared++
					}
				}
				if total > 0 {
					overlap := float64(shared) / float64(total)
					score *= (1.0 - 0.4*overlap)
				}
			}
			scored = append(scored, scoredCand{cand: cand, score: score})
		}
		if len(scored) == 0 {
			return nil
		}
		sort.Slice(scored, func(i, j int) bool {
			return scored[i].score > scored[j].score
		})
		maxReturn := 3
		if len(scored) < maxReturn {
			maxReturn = len(scored)
		}
		filtered := make([]*types.RouteCandidate, maxReturn)
		for i := 0; i < maxReturn; i++ {
			filtered[i] = scored[i].cand
		}
		return filtered
	})

	// ---- Set the rebuild function for retries ----
	sender.SetRebuildFunc(func(payload *types.ExecutionPayload, cand *types.RouteCandidate) error {
		loanProvider := payload.LoanProvider
		loanPool := payload.LoanPool
		loanToken := cand.Tokens[0]
		loanAmount := cand.AmountIn

		// Compute minProfitWei from cand's NetProfitWei or from ExpectedProfitUSD.
		minProfitWei := cand.NetProfitWei
		if minProfitWei == nil || minProfitWei.Sign() <= 0 {
			tokenPrice, ok := solver.GetTokenPrice(loanToken, matrix, &globalPriceCache)
if !ok || tokenPrice <= 0 {
    tokenPrice = 1.0
}
			decimals := solver.GetTokenDecimals(loanToken)
			profitWei := new(big.Float).Mul(
				big.NewFloat(cand.ExpectedProfitUSD/tokenPrice),
				big.NewFloat(math.Pow10(decimals)),
			)
			minProfitWei = new(big.Int)
			profitWei.Int(minProfitWei)
		}

		deadline := uint64(time.Now().Unix()) + 120

		// Build new calldata.
		calldata, err := solver.BuildCalldata(
			cand,
			loanProvider,
			loanPool,
			loanToken,
			loanAmount,
			minProfitWei,
			deadline,
		)
		if err != nil {
			return fmt.Errorf("rebuild calldata: %w", err)
		}

		// Update payload.
		payload.Calldata = make([]byte, len(calldata))
		copy(payload.Calldata, calldata)
		payload.BorrowedToken = loanToken
		payload.BorrowedAmount = loanAmount
		payload.MinProfitWei = new(big.Int).Set(minProfitWei)
		payload.MinProfitUSD = cand.ExpectedProfitUSD
		payload.RouteDesc = formatCandidateRoute(cand)
		payload.RoutePools = make([]common.Address, int(cand.Hops))
		copy(payload.RoutePools, cand.Pools[:cand.Hops])
		payload.OriginalCandidate = cand

		// Keep the same nonce (replacement transaction) – already set.
		rawTx, txHash, err := sender.PrepareAndSignTransaction(payload)
		if err != nil {
			return fmt.Errorf("re-sign failed: %w", err)
		}
		payload.SignedRawTx = rawTx
		payload.TxHash = txHash
		return nil
	})

	// ---- BOOTSTRAP: seed matrix with recent blocks ----
	if !dryRun && gk != nil {
    seedMatrixFromRecentBlocks(ethClient, gk)
}

	// ---- Channels ----
	eventChan := make(chan *types.SwapLog, eventChanSize)
	/*candidateChan := make(chan *types.RouteCandidate, candidateChanSize)*/
	executionChan := make(chan *types.ExecutionPayload, executionChanSize)

	solver.StartL1FeeUpdater(ethClient, ctx)

// ---- ETH price updater ----
go func() {
    ticker := time.NewTicker(10 * time.Second)
    defer ticker.Stop()
    for {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
            // Fetch ETH price from matrix (WETH/USDC)
            price, ok := solver.GetTokenPrice(config.WETHAddress, matrix, &globalPriceCache)
            if ok && price > 0 {
                solver.SetEthPrice(price)
                sender.SetEthPrice(price)
            }
        }
    }
}()
// ---- Background cache cleaner for globalPriceCache ----
go func() {
    ticker := time.NewTicker(5 * time.Minute)
    defer ticker.Stop()
    for {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
            now := time.Now().UnixNano()
            globalPriceCache.Range(func(key, value interface{}) bool {
                entry, ok := value.(*solver.PriceEntry)
                if ok && now-entry.Timestamp > int64(60*time.Second) {
                    globalPriceCache.Delete(key)
                }
                return true
            })
        }
    }
}()
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	statusChan := make(chan string, 1)

	log.Println("[Main] Subscribing to ALL swap events (dynamic discovery enabled)")
	if !dryRun {
    decoder := ingestion.NewDecoder()
    go func() {
        defer recoverPanic("WebSocket reader")
        ingestion.StartWebSocketReader(ctx, cfg.BaseWSRPC, eventChan, decoder, statusChan, nil)
    }()
}

	go func() {
		defer recoverPanic("WebSocket status handler")
		for status := range statusChan {
			switch status {
			case "connected":
				if dashServer != nil {
					dashServer.SetConnectionStatus("🟢 Connected")
				}
			case "disconnected":
				if dashServer != nil {
					dashServer.SetConnectionStatus("🔴 Disconnected")
			
				}
			}
		}
	}()

	var wg sync.WaitGroup

	cores := cfg.WorkerCoreIDs
	if len(cores) < 3 {
		cores = []int{2, 3, 4}
	}

	// ===== 4. Start Memory Guardian (removed – no longer needed) =====

	// ===== 5. Start Speculative Multiverse Seeder (background) =====
	/*go func() {
		defer recoverPanic("Speculative seeder")
		speculativeSeeder(ctx, matrix, lru, cfg)
	}()*/

	// ===== 6. Start Bonding Tracker =====
	bondingExecutor := common.HexToAddress(cfg.BondingExecutorAddress)
bondingTracker := bonding.NewTracker(
    ethClient,
    gevm,
    matrix,
    executionChan,
    &payloadPool,
    bondingExecutor,
    uint64(cfg.MaxPriorityFeeGwei*1e9),
)
bondingTracker.SetWSURL(cfg.BaseWSRPC)
bondingTracker.SetGEVM(gevm)

	wg.Add(1)
	go func() {
		defer recoverPanic("Bonding tracker")
		if cfg.EnableCPUPinning {
			core := cfg.BondingCoreID
			if core < 0 {
				if len(cores) > 0 {
					core = cores[len(cores)-1] + 1
				} else {
					core = 2
				}
			}
			pinToCore(core)
		}
		defer wg.Done()
		bondingTracker.Run(ctx)
	}()
// ===== 7. Start Liquidation Tracker =====
/*if cfg.LiquidationEnabled {
    liquidationTracker := liquidation.NewTracker(
        ethClient,
        gevm,             // pass GEVM for simulation
        matrix,
        executionChan,
        &payloadPool,
        uint64(cfg.MaxPriorityFeeGwei*1e9),
        cfg,
        0.05,
    )
liquidationTracker.SetWSURL(cfg.BaseWSRPC)
    wg.Add(1)
    go func() {
        defer recoverPanic("Liquidation tracker")
        if cfg.EnableCPUPinning {
            core := cfg.LiquidationCoreID
            if core < 0 {
                if len(cores) > 0 {
                    core = cores[len(cores)-1] + 2 // use a core not used by others
                } else {
                    core = 5
                }
            }
            pinToCore(core)
        }
        defer wg.Done()
        liquidationTracker.Run(ctx)
    }()
}*/
	// ---- Worker1 ----
	wg.Add(1)
	go func() {
		defer recoverPanic("Worker1")
		if cfg.EnableCPUPinning {
			pinToCore(cores[0])
		}
		defer wg.Done()
		worker1(ctx, eventChan, executionChan, matrix, blacklist, lru, anchorSet, cfg, gk,bondingTracker, ethClient)
	}()

	// ---- Worker2 ----
	/*for i := 0; i < numSimWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer recoverPanic(fmt.Sprintf("Worker2-%d", workerID))
			if cfg.EnableCPUPinning && len(cores) > 2+workerID {
				pinToCore(cores[2+workerID])
			}
			defer wg.Done()
			worker2(ctx, candidateChan, executionChan, gevm, matrix, blacklist, anchorSet, cfg, sender)
		}(i)
	}
	Only for bonding curve arb */

	// ---- Worker3 ----
	broadcastWg := &sync.WaitGroup{}
broadcastSem := make(chan struct{}, 8) // limit concurrent broadcasts
	wg.Add(1)
	go func() {
		defer recoverPanic("Worker3")
		if cfg.EnableCPUPinning {
			pinToCore(cores[2])
		}
		defer wg.Done()
		worker3(ctx, executionChan, sender, nonceTracker, pll, broadcastWg, broadcastSem)
	}()

	<-sigChan
	log.Println("\nShutting down gracefully...")
	cancel()

	if dashServer != nil {
    dashServer.SetConnectionStatus("🔴 Disconnected")
}

// Wait for broadcast goroutines
broadcastWg.Wait()
log.Println("All broadcast goroutines finished.")

done := make(chan struct{})
go func() {
    wg.Wait()
    close(done)
}()
	select {
	case <-done:
		log.Println("All workers finished.")
	case <-time.After(5 * time.Second):
		log.Println("Timeout waiting for workers, forcing exit.")
	}
	log.Println("Bot stopped.")
}

// ===== Additional Functions =====

func recoverPanic(name string) {
	if r := recover(); r != nil {
		log.Printf("[PANIC] %s recovered: %v", name, r)
	}
}

// memoryGuardian removed – no longer used.

// speculativeSeeder – seeds multiverse cache without reserving nonces.
func speculativeSeeder(ctx context.Context, matrix *state.Matrix, lru *state.LRUCache, cfg *config.Config) {
	ticker := time.NewTicker(180 * time.Millisecond)
	defer ticker.Stop()

	// Pre‑allocate weighted slice to reuse.
	weighted := make([]weightedPool, 0, 32)

	// Build anchor set for filtering.
	anchors := config.AnchorAssets()
	anchorSet := make(map[common.Address]bool, len(anchors))
	for _, a := range anchors {
		anchorSet[a] = true
	}

	// Determine loan provider and loan pool (same logic as worker2).
	var loanProvider uint8
	var loanPool common.Address
	switch cfg.LoanProvider {
	case "BALANCER":
		loanProvider = 0
		loanPool = config.BalancerVault
	case "DODO":
		dodoAddr := common.HexToAddress(cfg.DODOPoolAddress)
		if dodoAddr == (common.Address{}) {
			loanProvider = 0
			loanPool = config.BalancerVault
		} else {
			loanProvider = 1
			loanPool = dodoAddr
		}
	case "AUTO":
		if cfg.DODOPoolAddress != "" {
			dodoAddr := common.HexToAddress(cfg.DODOPoolAddress)
			if dodoAddr != (common.Address{}) {
				loanProvider = 1
				loanPool = dodoAddr
				break
			}
		}
		loanProvider = 0
		loanPool = config.BalancerVault
	default:
		loanProvider = 0
		loanPool = config.BalancerVault
	}

	priorityFee := uint64(cfg.MaxPriorityFeeGwei * 1e9)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// If few meme pools, sleep longer.
			if matrix.PoolCount() < 3 {
				time.Sleep(5 * time.Second)
				continue
			}

			weighted = weighted[:0]
			matrix.RangePools(func(poolAddr common.Address, pool *types.PoolState) bool {
				if !anchorSet[pool.Token0] || !anchorSet[pool.Token1] {
					stats := matrix.GetPoolStats(poolAddr)
					score := 0.0
					if stats != nil {
						score = float64(stats.SwapsWindow1m)*0.7 + stats.LastLiquidityUSD*0.3
					}
					var addr [20]byte
					copy(addr[:], poolAddr.Bytes())
					weighted = append(weighted, weightedPool{addr, score})
				}
				return true
			})

			if len(weighted) == 0 {
				continue
			}
			sort.Slice(weighted, func(i, j int) bool {
				return weighted[i].score > weighted[j].score
			})
			memePools := make([][20]byte, 0, 5)
			for i := 0; i < len(weighted) && i < 5; i++ {
				memePools = append(memePools, weighted[i].addr)
			}

			if len(memePools) > 0 {
				// Call with all required parameters.
				predictive.SeedNextSubBlockBranches(memePools, matrix, cfg, loanProvider, loanPool, priorityFee)
			}
		}
	}
}

type weightedPool struct {
	addr  [20]byte
	score float64
}

// seedMatrixFromRecentBlocks (unchanged)
func seedMatrixFromRecentBlocks(client *ethclient.Client, gk *gatekeeper.Gatekeeper) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	header, err := client.HeaderByNumber(ctx, nil)
	if err != nil {
		log.Printf("[Bootstrap] Failed to get latest block: %v", err)
		return
	}
	currentBlock := header.Number.Uint64()
	if currentBlock < 8 {
		log.Printf("[Bootstrap] Chain height < 8; skipping seed")
		return
	}
	fromBlock := currentBlock - 8

	v2Topic := common.HexToHash("0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822")
	v3Topic := common.HexToHash("0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67")

	query := ethereum.FilterQuery{
		FromBlock: big.NewInt(int64(fromBlock)),
		ToBlock:   header.Number,
		Topics:    [][]common.Hash{{v2Topic, v3Topic}},
	}

	logs, err := client.FilterLogs(ctx, query)
	if err != nil {
		log.Printf("[Bootstrap] FilterLogs failed: %v", err)
		return
	}

	seen := make(map[common.Address]bool)
	var poolAddrs []common.Address
	for _, vLog := range logs {
		if !seen[vLog.Address] {
			seen[vLog.Address] = true
			poolAddrs = append(poolAddrs, vLog.Address)
		}
	}

	log.Printf("[Bootstrap] Found %d unique pools in last 50 blocks", len(poolAddrs))

	for _, addr := range poolAddrs {
		dummyLog := &types.SwapLog{
			Address:     addr,
			BlockNumber: currentBlock,
		}
		gk.ProcessLog(dummyLog)
	}
}

// ---------------------------------------------------------------------
// Worker functions
// ---------------------------------------------------------------------

func worker1(
	ctx context.Context,
	eventChan <-chan *types.SwapLog,
	/*candidateChan chan<- *types.RouteCandidate,*/
	executionChan chan<- *types.ExecutionPayload,
	matrix *state.Matrix,
	blacklist *state.Blacklist,
	lru *state.LRUCache,
	anchorSet map[common.Address]bool,
	cfg *config.Config,
	gatekeeper *gatekeeperpkg.Gatekeeper,
	bondingTracker *bonding.Tracker,
	ethClient *ethclient.Client,
) {
	/*var localCandidates [maxCandidatesPerEvent]*types.RouteCandidate
	candidateCount := 0*/

	for {
		select {
		case <-ctx.Done():
			return
		case swapLog, ok := <-eventChan:
			if !ok {
				return
			}

		
// ---- Re‑org detection ----
var blockHash common.Hash
// Try to get from cache first (most logs are from the latest block)
if info := latestBlock.Load(); info != nil {
    bi := info.(*blockInfo)
    if bi.Number == swapLog.BlockNumber {
        blockHash = bi.Hash
    }
}
// If cache miss (e.g., lag), fallback to RPC
if blockHash == (common.Hash{}) {
    header, err := ethClient.HeaderByNumber(ctx, new(big.Int).SetUint64(swapLog.BlockNumber))
    if err != nil {
        log.Printf("[Worker1] Failed to fetch header for block %d: %v", swapLog.BlockNumber, err)
        ingestion.PutSwapLog(swapLog)
        continue
    }
    blockHash = header.Hash()
}

if matrix.IsReorg(swapLog.BlockNumber, blockHash) {
    log.Printf("[Worker1] Re‑org detected at block %d (hash %s), clearing matrix and reseeding...",
        swapLog.BlockNumber, blockHash.Hex())
    matrix.Clear()
    if bondingTracker != nil {
        bondingTracker.Clear()
    }
    if !dryRun && gk != nil {
    seedMatrixFromRecentBlocks(ethClient, gk)
}
    matrix.UpdateLastBlock(swapLog.BlockNumber, blockHash)
    ingestion.PutSwapLog(swapLog)
    continue
}
			// ---- Pause check ----
			if atomic.LoadInt32(&dashboard.BotRunning) == 0 {
				ingestion.PutSwapLog(swapLog)
				select {
				case <-ctx.Done():
					return
				case <-time.After(50 * time.Millisecond):
				}
				continue
			}

			// ===== SHORT‑CIRCUIT =====
			/*Only for bonding curve arb
			if payload, matched := predictive.ShortCircuitEvaluate(swapLog.RawJSON); matched {
				select {
				case executionChan <- payload:
				default:
					log.Printf("[Worker1] Execution channel full, dropping short‑circuit payload for %s", swapLog.Address.Hex())
				}
				ingestion.PutSwapLog(swapLog)
				continue
			}*/

			// ===== Normal processing =====
			pool := matrix.GetPool(swapLog.Address)
			if pool == nil {
				// Unknown pool – offload to gatekeeper (non‑blocking)
				/* Only for bonding curve arb
				gatekeeper.ProcessLog(swapLog) */
				
				ingestion.PutSwapLog(swapLog)
				continue
			}
			matrix.UpdateFromLog(swapLog)

			if swapLog.TokenIn == (common.Address{}) || swapLog.TokenOut == (common.Address{}) {
				if pool != nil {
					swapLog.TokenIn = pool.Token0
					swapLog.TokenOut = pool.Token1
				}
			}

			// Copy log for retry context
			lastLogMu.Lock()
			copyLog := &types.SwapLog{
				Address:           swapLog.Address,
				Topics:            append([]common.Hash{}, swapLog.Topics...),
				Data:              append([]byte{}, swapLog.Data...),
				BlockNumber:       swapLog.BlockNumber,
				TxIndex:           swapLog.TxIndex,
				TxHash:            swapLog.TxHash,
				Timestamp:         swapLog.Timestamp,
				TokenIn:           swapLog.TokenIn,
				TokenOut:          swapLog.TokenOut,
				AmountIn:          new(big.Int).Set(swapLog.AmountIn),
				AmountOut:         new(big.Int).Set(swapLog.AmountOut),
				AmountInFloat:     swapLog.AmountInFloat,
				AmountOutFloat:    swapLog.AmountOutFloat,
				SqrtPriceX96:      new(big.Int).Set(swapLog.SqrtPriceX96),
				Liquidity:         new(big.Int).Set(swapLog.Liquidity),
				Tick:              swapLog.Tick,
				SqrtPriceX96Float: swapLog.SqrtPriceX96Float,
				LiquidityFloat:    swapLog.LiquidityFloat,
			}
			lastSwapLog = copyLog
			lastLogMu.Unlock()
// Only for bonding curve arb 
			/*candidates := solver.EvaluateEvent(swapLog, matrix, cfg)

			candidateCount = 0
			for _, cand := range candidates {
				blacklisted := false
				for i := 0; i < int(cand.Hops)+1; i++ {
					if blacklist.IsBlacklisted(cand.Tokens[i]) {
						blacklisted = true
						break
					}
				}
				if blacklisted {
					continue
				}
				if cand.ExpectedProfitUSD < cfg.MinProfitUSD || cand.ExecutionSlippage > 3.0 {
					continue
				}
				if candidateCount < maxCandidatesPerEvent {
					localCandidates[candidateCount] = cand
					candidateCount++
				} else {
					break
				}
			}
			for i := 0; i < candidateCount; i++ {
				select {
				case candidateChan <- localCandidates[i]:
				default:
				}
			}*/
/* Only for bonding curve arb */
			// Update LRU cache with new tokens
tokensToTrack := []common.Address{swapLog.TokenIn, swapLog.TokenOut}
// We don't process arbitrage candidates during bonding-only phase
for _, tok := range tokensToTrack {
    if tok != (common.Address{}) && !anchorSet[tok] {
        lru.Put(tok)
    }
}
			matrix.UpdateLastBlock(swapLog.BlockNumber, blockHash)
			ingestion.PutSwapLog(swapLog)
		}
	}
}

// worker2 – simulates and submits payloads. Does not reserve nonces.
func worker2(
	ctx context.Context,
	candidateChan <-chan *types.RouteCandidate,
	executionChan chan<- *types.ExecutionPayload,
	gevm *execution.GEVMSimulator,
	matrix *state.Matrix,
	blacklist *state.Blacklist,
	anchorSet map[common.Address]bool,
	cfg *config.Config,
	sender *execution.Sender,
) {
	// Priority fee refresh
	var priorityFeeWei uint64
	if dynFee, err := sender.GetDynamicPriorityFee(ctx); err == nil && dynFee > 0 {
		priorityFeeWei = dynFee
	} else {
		priorityFeeWei = uint64(cfg.MaxPriorityFeeGwei * 1e9)
	}
	var priorityFeeAtomic atomic.Uint64
	priorityFeeAtomic.Store(priorityFeeWei)

	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if dynFee, err := sender.GetDynamicPriorityFee(ctx); err == nil && dynFee > 0 {
					priorityFeeAtomic.Store(dynFee)
				}
			}
		}
	}()

	var loanProvider uint8
	var loanPool common.Address
	switch cfg.LoanProvider {
	case "BALANCER":
		loanProvider = 0
		loanPool = config.BalancerVault
	case "DODO":
		dodoAddr := common.HexToAddress(cfg.DODOPoolAddress)
		if dodoAddr == (common.Address{}) {
			log.Printf("[Worker2] DODO pool address is zero; falling back to Balancer")
			loanProvider = 0
			loanPool = config.BalancerVault
		} else {
			loanProvider = 1
			loanPool = dodoAddr
		}
	case "AUTO":
		if cfg.DODOPoolAddress != "" {
			dodoAddr := common.HexToAddress(cfg.DODOPoolAddress)
			if dodoAddr != (common.Address{}) {
				loanProvider = 1
				loanPool = dodoAddr
				break
			}
		}
		loanProvider = 0
		loanPool = config.BalancerVault
	default:
		loanProvider = 0
		loanPool = config.BalancerVault
	}

	for {
		select {
		case <-ctx.Done():
			return
		case cand, ok := <-candidateChan:
			if !ok {
				return
			}
			if atomic.LoadInt32(&dashboard.BotRunning) == 0 {
				select {
				case <-ctx.Done():
					return
				case <-time.After(50 * time.Millisecond):
				}
				continue
			}

			// ---- ETH price check (fail‑closed) ----
			ethPrice := solver.GetEthPrice()
			if ethPrice <= 1.0 || ethPrice == 3000.0 { // 3000 is the uninitialised default
				log.Printf("[Worker2] Skipping trade: ETH price not valid (%.2f)", ethPrice)
				// drop candidate (no payload to release)
				continue
			}

			start := time.Now()
			loanToken := cand.Tokens[0]
			loanAmount := cand.AmountIn

			minProfitWei := cand.NetProfitWei
			if minProfitWei == nil || minProfitWei.Sign() <= 0 {
				tokenPrice, ok := solver.GetTokenPrice(loanToken, matrix, &globalPriceCache)
if !ok || tokenPrice <= 0 {
    // skip trade or use fallback
    log.Printf("Skipping trade: unknown price for token %s", loanToken.Hex())
continue
}
				decimals := solver.GetTokenDecimals(loanToken)
				profitWei := new(big.Float).Mul(
					big.NewFloat(cand.ExpectedProfitUSD/tokenPrice),
					big.NewFloat(math.Pow10(decimals)),
				)
				minProfitWei = new(big.Int)
				profitWei.Int(minProfitWei)
			}

			deadline := uint64(time.Now().Unix()) + 120

			calldata, err := solver.BuildCalldata(
				cand,
				loanProvider,
				loanPool,
				loanToken,
				loanAmount,
				minProfitWei,
				deadline,
			)
			if err != nil {
				logDrop(cand, err)
				continue
			}

			payload := getPayload()
			payload.Reset()
			payload.TargetExecutor = cfg.ExecutorAddress
			payload.LoanProvider = loanProvider
			payload.LoanPool = loanPool
			payload.BorrowedToken = loanToken
			payload.BorrowedAmount = loanAmount
			payload.Calldata = calldata
			payload.GasLimit = defaultGasLimit
			payload.PriorityFeeWei = priorityFeeAtomic.Load()
			payload.MinProfitWei = new(big.Int).Set(minProfitWei)
			payload.DetectionTime = start
			payload.RouteDesc = formatCandidateRoute(cand)
			payload.RoutePools = make([]common.Address, int(cand.Hops))
			copy(payload.RoutePools, cand.Pools[:cand.Hops])
			payload.OriginalCandidate = cand
			payload.Nonce = 0 // will be set in worker3

			// Simulation
			backend := gevm.ChooseBackend(cand)
			var success bool
			var gasUsed uint64

			if backend == execution.SimBackendLocal {
				success, gasUsed, err = gevm.SimulateNative(payload)
				if err != nil || !success {
					success, gasUsed, err = gevm.SimulateWithBackend(cand, payload, execution.SimBackendRemote)
				}
			} else {
				success, gasUsed, err = gevm.SimulateWithBackend(cand, payload, backend)
			}

			if err != nil || !success {
				reverted := (err != nil && strings.Contains(err.Error(), "execution reverted"))
				if reverted {
					for i := 0; i < int(cand.Hops)+1; i++ {
						tok := cand.Tokens[i]
						if !anchorSet[tok] {
							blacklist.Add(tok)
						}
					}
				}
				logDrop(cand, err)
				putPayload(payload)
				continue
			}

			gasLimit := uint64(float64(gasUsed) * 1.05)
			if gasLimit < defaultGasLimit {
				gasLimit = defaultGasLimit
			}
			payload.GasLimit = gasLimit

			// Profit check (with valid ETH price)
			l1BaseFeeUSD := solver.GetCurrentL1BaseFeeUSD()
			l2BaseFeeUSD := solver.GetCurrentL2BaseFeeUSD()
			priorityFeeUSD := float64(payload.PriorityFeeWei) * ethPrice / 1e18
			totalGasCostUSD := float64(gasUsed) * (l1BaseFeeUSD + l2BaseFeeUSD + priorityFeeUSD)
			adjustedProfitUSD := cand.ExpectedProfitUSD - totalGasCostUSD
			if adjustedProfitUSD < cfg.MinProfitUSD {
				logDrop(cand, fmt.Errorf("gas-adjusted profit $%.2f below minimum $%.2f",
					adjustedProfitUSD, cfg.MinProfitUSD))
				putPayload(payload)
				continue
			}
			payload.MinProfitUSD = adjustedProfitUSD

			select {
			case executionChan <- payload:
			default:
				log.Println("[Worker2] Execution channel full, dropping payload")
				putPayload(payload)
			}
		}
	}
}

// worker3 – assigns nonce, signs, and broadcasts via BroadcastWithRetry.
// Now limits concurrency and tracks goroutines.
func worker3(
	ctx context.Context,
	executionChan <-chan *types.ExecutionPayload,
	sender *execution.Sender,
	nonceTracker *state.NonceTracker,
	pll *predictive.FlashblockPLL,
	broadcastWg *sync.WaitGroup,
	broadcastSem chan struct{},
) {
	for {
		select {
		case <-ctx.Done():
			return
		case payload, ok := <-executionChan:
			if !ok {
				return
			}
			if atomic.LoadInt32(&dashboard.BotRunning) == 0 {
				putPayload(payload)
				select {
				case <-ctx.Done():
					return
				case <-time.After(50 * time.Millisecond):
				}
				continue
			}

			// Assign nonce if not already set.
			if payload.Nonce == 0 {
				payload.Nonce = nonceTracker.NextNonce()
			}
			nonce := payload.Nonce

			log.Printf("[+] PENDING | Nonce: %d | Profit: $%.2f | GasTip: %.3f Gwei | Route: %s",
				nonce,
				payload.MinProfitUSD,
				float64(payload.PriorityFeeWei)/1e9,
				payload.RouteDesc,
			)

			// Acquire semaphore (blocks if too many concurrent broadcasts)
			select {
			case broadcastSem <- struct{}{}:
				// acquired
			case <-ctx.Done():
				putPayload(payload)
				return
			}

			broadcastWg.Add(1)
			go func(p *types.ExecutionPayload, n uint64) {
				defer func() {
					if r := recover(); r != nil {
						log.Printf("[PANIC] Broadcast goroutine: %v", r)
						if p.Nonce > 0 {
							nonceTracker.Rollback()
						}
						putPayload(p)
					}
					// Release semaphore and mark done
					<-broadcastSem
					broadcastWg.Done()
				}()

				// Sign if not already signed
				if len(p.SignedRawTx) == 0 {
					rawTx, txHash, err := sender.PrepareAndSignTransaction(p)
					if err != nil {
						log.Printf("[Worker3] Signing failed for %s: %v", p.RouteDesc, err)
						if p.Nonce > 0 {
							nonceTracker.Rollback()
						}
						putPayload(p)
						return
					}
					p.SignedRawTx = rawTx
					p.TxHash = txHash
				}

				// ---- Predictive scheduling ----
				dispatchTime := pll.OptimalDispatchTime()
				if dispatchTime.After(time.Now()) {
					sleepDuration := dispatchTime.Sub(time.Now())
					if sleepDuration > 0 && sleepDuration < 200*time.Millisecond {
						time.Sleep(sleepDuration)
					}
				}

				retryCtx := execution.RetryContext{
					Pools:             p.RoutePools,
					OriginalCandidate: p.OriginalCandidate,
					LoanToken:         p.BorrowedToken,
				}
				if p.OriginalCandidate != nil {
					retryCtx.Tokens = p.OriginalCandidate.Tokens[:int(p.OriginalCandidate.Hops)+1]
				} else {
					retryCtx.Tokens = []common.Address{}
				}

				err := sender.BroadcastWithRetry(p, retryCtx)
				if err != nil {
					msg := fmt.Sprintf("[-] DROP | Tx broadcast failed after retries | Reason: %v", err)
					log.Println(msg)
					if dashServer != nil {
						dashServer.Log(msg)
						dashServer.SetTradeStatus("FAILED", err.Error())
					}
					// BroadcastWithRetry already does rollback + putPayload on failure.
					// No extra cleanup needed.
				}
				// On success, BroadcastWithRetry calls confCallback -> putPayload.
			}(payload, nonce)
		}
	}
}
// printStartupBanner
func printStartupBanner(cfg *config.Config) {
	fmt.Println("=== Base Flash Arbitrage Bot ===")
	fmt.Printf("WS RPC:        %s\n", cfg.BaseWSRPC)
	fmt.Printf("HTTP RPC:      %s\n", cfg.BaseHTTPRPC)
	fmt.Printf("Exec RPC:      %s\n", cfg.BaseExecRPC)
	fmt.Printf("Executor:      %s\n", cfg.ExecutorAddress.Hex())
	fmt.Printf("Min Profit:    $%.2f\n", cfg.MinProfitUSD)
	fmt.Printf("CPU Pinning:   %v\n", cfg.EnableCPUPinning)
	fmt.Println("=================================")
}

// logDrop
func logDrop(cand *types.RouteCandidate, err error) {
	var msg string
	if cand == nil {
		msg = fmt.Sprintf("[-] DROP | Reason: %v", err)
	} else {
		reason := "unknown"
		if err != nil {
			reason = err.Error()
		}
		msg = fmt.Sprintf("[-] DROP | Route: %s | Reason: %s | Expected Profit: $%.2f",
			formatCandidateRoute(cand),
			reason,
			cand.ExpectedProfitUSD,
		)
	}
	log.Println(msg)
	if dashServer != nil {
		dashServer.Log(msg)
		if err != nil {
			dashServer.SetTradeStatus("FAILED", err.Error())
		} else {
			dashServer.SetTradeStatus("FAILED", "unknown error")
		}
	}
}

// formatCandidateRoute
func formatCandidateRoute(cand *types.RouteCandidate) string {
	if cand == nil || cand.Hops == 0 {
		return "unknown"
	}
	tokens := cand.Tokens[:int(cand.Hops)+1]
	dexes := cand.DexTypes[:int(cand.Hops)]
	route := ""
	for i := 0; i < int(cand.Hops); i++ {
		if i > 0 {
			route += " -> "
		}
		route += fmt.Sprintf("%s[%s]", tokens[i].Hex()[:8], dexName(dexes[i]))
	}
	route += " -> " + tokens[cand.Hops].Hex()[:8]
	return route
}

// dexName
func dexName(t types.DexType) string {
	switch t {
	case types.DexUniswapV3:
		return "UniV3"
	case types.DexPancakeV3:
		return "PanV3"
	case types.DexAerodromeV2:
		return "AeroV2"
	case types.DexAlienBaseV2:
		return "AlienV2"
	default:
		return "Unknown"
	}
}

// float64FromBig
func float64FromBig(v *big.Int) float64 {
	if v == nil {
		return 0
	}
	f := new(big.Float).SetInt(v)
	val, _ := f.Float64()
	return val
}
