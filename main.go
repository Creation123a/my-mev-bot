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
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/ethereum/go-ethereum/common"
	gethTypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
	"golang.org/x/sys/unix"


	"my-mev-bot/Bot/Config"
	"my-mev-bot/Bot/Dashboard"
	"my-mev-bot/Bot/Execution"
	"my-mev-bot/Bot/Ingestion"
	"my-mev-bot/Bot/Solver"
	"my-mev-bot/Bot/State"
	"my-mev-bot/Bot/Types"
	"my-mev-bot/Bot/Gatekeeper"
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
	dashServer *dashboard.DashboardServer
)

var (
	profitMu    sync.Mutex
	totalProfit float64
)

var (
	lastLogMu  sync.Mutex
	lastSwapLog *types.SwapLog
)

// Global price cache used only in the fallback path of worker2.
var globalPriceCache sync.Map

var payloadPool = sync.Pool{
	New: func() interface{} {
		return &types.ExecutionPayload{}
	},
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
	cfg, err := config.LoadConfig()
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	if err := solver.InitExecutorABI(); err != nil {
		log.Fatalf("failed to init executor ABI: %v", err)
	}

	dashServer = dashboard.NewDashboardServer()
	go func() {
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

	matrix.SetEthClient(ethClient)
ctx, cancel := context.WithCancel(context.Background())
defer cancel()
// ================================================================

	// ---- Create GEVMSimulator with WebSocket URL ----
	gevm := execution.NewGEVMSimulator(cfg.BaseHTTPRPC, cfg.BaseWSRPC, ownerAddress)

	gevm.SetMatrix(matrix)

	stateCache := execution.NewStateCache()
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
	// Create the three LRU caches
memeCache := lru // lru is already created as state.NewMemeTokenCache()
dexCache := state.NewDEXFactoryCache()
pairCache := state.NewBasePairCache()

// Create the gatekeeper with background qualification workers
gatekeeper := gatekeeper.New(
    ethClient,
    gevm,
    memeCache, // same as lru
    dexCache,
    pairCache,
    blacklist,
    matrix,
)

	// ---- Start WebSocket‑based block context updater ----
	gevm.StartWebSocketContextUpdater(ctx)

	// ---- Removed Anvil health checks and background ticker ----

	sender, err := execution.NewSender(cfg.BaseHTTPRPC, cfg.PrivateKey)
	if err != nil {
		log.Fatalf("Failed to initialize sender: %v", err)
	}

	sender.SetReplaceConfig(
		time.Duration(cfg.ReplaceTimeoutMs)*time.Millisecond,
		float64(100+cfg.ReplaceBumpPercent)/100.0,
		cfg.MaxReplaceAttempts,
	)

	sender.SetConfirmationCallback(func(nonce uint64, txHash common.Hash, receipt *gethTypes.Receipt, payload *types.ExecutionPayload) {
		profitUSD := payload.MinProfitUSD
		profitMu.Lock()
		totalProfit += profitUSD
		profitMu.Unlock()
		if dashServer != nil {
			dashServer.AddProfit(profitUSD)
			dashServer.SetTradeStatus("SUCCESS", "")
			msg := fmt.Sprintf("[+] WIN | Tx: %s | Profit: $%.2f | Route: %s | Confirmed at block %d",
				txHash.Hex(), profitUSD, payload.RouteDesc, receipt.BlockNumber.Uint64())
			dashServer.Log(msg)
		}
		log.Printf("[+] WIN | Tx: %s | Profit: $%.2f | Route: %s | Block: %d",
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

	sender.SetSimulateFunc(func(cand *types.RouteCandidate, payload *types.ExecutionPayload) (bool, uint64, error) {
		return gevm.SimulateNative(payload)
	})

	decoder := ingestion.NewDecoder()

	eventChan := make(chan *types.SwapLog, eventChanSize)
	candidateChan := make(chan *types.RouteCandidate, candidateChanSize)
	executionChan := make(chan *types.ExecutionPayload, executionChanSize)

	solver.StartL1FeeUpdater(ethClient, ctx)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	statusChan := make(chan string, 1)

    log.Println("[Main] Subscribing to ALL swap events (dynamic discovery enabled)")
  go ingestion.StartWebSocketReader(ctx, cfg.BaseWSRPC, eventChan, decoder, statusChan, nil)

	go func() {
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

	wg.Add(1)
go func() {
    if cfg.EnableCPUPinning {
        pinToCore(cores[0])
    }
    defer wg.Done()
    worker1(ctx, eventChan, candidateChan, matrix, blacklist, lru, anchorSet, cfg, gatekeeper)
}()

	for i := 0; i < numSimWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			if cfg.EnableCPUPinning && len(cores) > 2+workerID {
				pinToCore(cores[2+workerID])
			}
			defer wg.Done()
			worker2(ctx, candidateChan, executionChan, gevm, matrix, blacklist, anchorSet, cfg)
		}(i)
	}

	wg.Add(1)
	go func() {
		if cfg.EnableCPUPinning {
			pinToCore(cores[2])
		}
		defer wg.Done()
		worker3(ctx, executionChan, sender, nonceTracker)
	}()

	<-sigChan
	log.Println("\nShutting down gracefully...")
	cancel()

	if dashServer != nil {
		dashServer.SetConnectionStatus("🔴 Disconnected")
	}

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


// ---------------------------------------------------------------------
// Worker functions
// ---------------------------------------------------------------------

func worker1(
	ctx context.Context,
	eventChan <-chan *types.SwapLog,
	candidateChan chan<- *types.RouteCandidate,
	matrix *state.Matrix,
	blacklist *state.Blacklist,
	lru *state.LRUCache,
	anchorSet map[common.Address]bool,
	cfg *config.Config,
	gatekeeper *gatekeeper.Gatekeeper,
) {
	var localCandidates [maxCandidatesPerEvent]*types.RouteCandidate
	candidateCount := 0

	for {
		select {
		case <-ctx.Done():
			return
		case swapLog, ok := <-eventChan:
			if !ok {
				return
			}

			// ---- NEW: Gatekeeper Integration ----
			// Check if pool exists in matrix
			pool := matrix.GetPool(swapLog.Address)
			if pool == nil {
				// Unknown pool – offload discovery to gatekeeper (non‑blocking)
				gatekeeper.ProcessLog(swapLog)
				ingestion.PutSwapLog(swapLog)
				continue
			}
			// ---- End Gatekeeper Integration ----

			// Known pool – update state
			matrix.UpdateFromLog(swapLog)

			// Ensure TokenIn/TokenOut are set
			if swapLog.TokenIn == (common.Address{}) || swapLog.TokenOut == (common.Address{}) {
				if pool != nil {
					swapLog.TokenIn = pool.Token0
					swapLog.TokenOut = pool.Token1
				}
			}

			// Copy log for retry context
			lastLogMu.Lock()
			copyLog := &types.SwapLog{
				Address:          swapLog.Address,
				Topics:           append([]common.Hash{}, swapLog.Topics...),
				Data:             append([]byte{}, swapLog.Data...),
				BlockNumber:      swapLog.BlockNumber,
				TxIndex:          swapLog.TxIndex,
				TxHash:           swapLog.TxHash,
				Timestamp:        swapLog.Timestamp,
				TokenIn:          swapLog.TokenIn,
				TokenOut:         swapLog.TokenOut,
				AmountIn:         new(big.Int).Set(swapLog.AmountIn),
				AmountOut:        new(big.Int).Set(swapLog.AmountOut),
				AmountInFloat:    swapLog.AmountInFloat,
				AmountOutFloat:   swapLog.AmountOutFloat,
				SqrtPriceX96:     new(big.Int).Set(swapLog.SqrtPriceX96),
				Liquidity:        new(big.Int).Set(swapLog.Liquidity),
				Tick:             swapLog.Tick,
				SqrtPriceX96Float: swapLog.SqrtPriceX96Float,
				LiquidityFloat:    swapLog.LiquidityFloat,
			}
			lastSwapLog = copyLog
			lastLogMu.Unlock()

			// Evaluate candidates
			candidates := solver.EvaluateEvent(swapLog, matrix, cfg)
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

			// Send candidates to channel
			for i := 0; i < candidateCount; i++ {
				select {
				case candidateChan <- localCandidates[i]:
				default:
				}
			}

			// Update LRU cache with new tokens
			tokensToTrack := []common.Address{swapLog.TokenIn, swapLog.TokenOut}
			for _, cand := range candidates {
				for _, tok := range cand.Tokens {
					tokensToTrack = append(tokensToTrack, tok)
				}
			}
			for _, tok := range tokensToTrack {
				if tok != (common.Address{}) && !anchorSet[tok] {
					lru.Put(tok)
				}
			}

			// Return log to pool
			ingestion.PutSwapLog(swapLog)
		}
	}
}

// getTokenDecimals (unchanged)
func getTokenDecimals(token common.Address) int {
	if token == config.USDCAddress || token == config.USDBCAddress {
		return 6
	}
	if token == config.CBBTCAddress {
		return 8
	}
	if token == config.WETHAddress {
		return 18
	}
	return 18
}

// worker2 – fixed context leak and remote fallback
func worker2(
	ctx context.Context,
	candidateChan <-chan *types.RouteCandidate,
	executionChan chan<- *types.ExecutionPayload,
	gevm *execution.GEVMSimulator,
	matrix *state.Matrix,
	blacklist *state.Blacklist,
	anchorSet map[common.Address]bool,
	cfg *config.Config,
) {
	priorityFeeWei := uint64(cfg.MaxPriorityFeeGwei * 1e9)

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

			start := time.Now()

			loanToken := cand.Tokens[0]
			loanAmount := cand.AmountIn

			minProfitWei := cand.NetProfitWei
			if minProfitWei == nil || minProfitWei.Sign() <= 0 {
				tokenPrice := solver.GetTokenPrice(loanToken, matrix, &globalPriceCache)
				if tokenPrice <= 0 {
					tokenPrice = 1.0
				}
				decimals := getTokenDecimals(loanToken)
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

			backend := gevm.ChooseBackend(cand)
			simPayload := &types.ExecutionPayload{
				TargetExecutor: cfg.ExecutorAddress,
				Calldata:       calldata,
				GasLimit:       defaultGasLimit,
			}

			var success bool
			var gasUsed uint64

			if backend == execution.SimBackendLocal {
				// No context needed for local simulation – it's synchronous.
				success, gasUsed, err = gevm.SimulateNative(simPayload)
				if err != nil || !success {
					// Fall back to remote if local fails.
					success, gasUsed, err = gevm.SimulateWithBackend(cand, simPayload, execution.SimBackendRemote)
				}
			} else {
				success, gasUsed, err = gevm.SimulateWithBackend(cand, simPayload, backend)
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
				continue
			}

			gasLimit := gasUsed + gasUsed/5
			if gasLimit < defaultGasLimit {
				gasLimit = defaultGasLimit
			}

			ethPrice := solver.GetEthPrice()
			l1BaseFeeUSD := solver.GetCurrentL1BaseFeeUSD()
			l2TipUSD := float64(priorityFeeWei) * ethPrice / 1e18
			totalGasCostUSD := float64(gasUsed) * (l1BaseFeeUSD + l2TipUSD)

			adjustedProfitUSD := cand.ExpectedProfitUSD - totalGasCostUSD

			if adjustedProfitUSD < cfg.MinProfitUSD {
				logDrop(cand, fmt.Errorf("gas-adjusted profit $%.2f below minimum $%.2f",
					adjustedProfitUSD, cfg.MinProfitUSD))
				continue
			}

			payload := getPayload()
			payload.Reset()
			payload.TargetExecutor = cfg.ExecutorAddress
			payload.LoanProvider = loanProvider
			payload.LoanPool = loanPool
			payload.BorrowedToken = loanToken
			payload.BorrowedAmount = loanAmount
			payload.Calldata = make([]byte, len(calldata))
			copy(payload.Calldata, calldata)
			payload.GasLimit = gasLimit
			payload.PriorityFeeWei = priorityFeeWei
			payload.MinProfitUSD = adjustedProfitUSD
			payload.MinProfitWei = new(big.Int).Set(minProfitWei)
			payload.DetectionTime = start
			payload.Nonce = 0
			payload.RouteDesc = formatCandidateRoute(cand)
			payload.RoutePools = make([]common.Address, int(cand.Hops))
			payload.OriginalCandidate = cand
			copy(payload.RoutePools, cand.Pools[:cand.Hops])

			select {
			case executionChan <- payload:
			default:
				putPayload(payload)
				log.Println("[Worker2] Execution channel full, dropping payload")
			}
		}
	}
}

// worker3 – fixed nonce race: sign synchronously, broadcast asynchronously
func worker3(
	ctx context.Context,
	executionChan <-chan *types.ExecutionPayload,
	sender *execution.Sender,
	nonceTracker *state.NonceTracker,
) {
	for {
		select {
		case <-ctx.Done():
			return
		case payload, ok := <-executionChan:
			if !ok {
				return
			}
			nonce := nonceTracker.NextNonce()
			payload.Nonce = nonce

			// Sign synchronously to guarantee nonce order.
			rawTx, txHash, err := sender.PrepareAndSignTransaction(payload)
			if err != nil {
				nonceTracker.Rollback()
				msg := fmt.Sprintf("[-] DROP | Tx signing failed | Reason: %v", err)
				log.Println(msg)
				if dashServer != nil {
					dashServer.Log(msg)
					dashServer.SetTradeStatus("FAILED", err.Error())
				}
				putPayload(payload)
				continue
			}

			log.Printf("[+] PENDING | Tx: %s | Profit: $%.2f | GasTip: %.3f Gwei | Route: %s",
				txHash.Hex(),
				payload.MinProfitUSD,
				float64(payload.PriorityFeeWei)/1e9,
				payload.RouteDesc,
			)

			// Broadcast asynchronously to avoid blocking the loop.
			go func(raw []byte, p *types.ExecutionPayload, n uint64) {
				err := sender.BroadcastRawTransactionBytes(raw)
				if err != nil {
					// Do NOT rollback nonce – transaction may have been partially sent.
					msg := fmt.Sprintf("[-] DROP | Tx broadcast failed | Reason: %v", err)
					log.Println(msg)
					if dashServer != nil {
						dashServer.Log(msg)
						dashServer.SetTradeStatus("FAILED", err.Error())
					}
					putPayload(p)
					return
				}
				sender.RegisterPendingNonce(n, p)
			}(rawTx, payload, nonce)
		}
	}
}

// printStartupBanner (unchanged)
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

// logDrop (unchanged)
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

// formatCandidateRoute (unchanged)
func formatCandidateRoute(cand *types.RouteCandidate) string {
	if cand == nil || cand.Hops == 0 {
		return "unknown"
	}
	tokens := cand.Tokens[:cand.Hops+1]
	dexes := cand.DexTypes[:cand.Hops]
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

// dexName (unchanged)
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
