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
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/ethereum/go-ethereum/common"
	gethTypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
	"golang.org/x/sys/unix"

	"my-mev-bot/config"
	"my-mev-bot/dashboard"
	"my-mev-bot/execution"
	"my-mev-bot/ingestion"
	"my-mev-bot/solver"
	"my-mev-bot/state"
	"my-mev-bot/types"
)

const (
	eventChanSize         = 2048
	candidateChanSize     = 128
	executionChanSize     = 64
	defaultGasLimit       = 500000
	maxCandidatesPerEvent = 8
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

	// Validate factory addresses before starting.
	if err := config.ValidateFactories(); err != nil {
		log.Fatalf("Factory validation failed: %v", err)
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
	lru := state.NewDynamicTokenCache()

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
	// Sync nonce; fail hard if it doesn't succeed.
	if err := nonceTracker.SyncFromNode(ethClient, ownerAddress); err != nil {
		log.Fatalf("Failed to sync nonce from node: %v", err)
	}

	matrix.SetEthClient(ethClient)
	preloadPools(matrix)

	anvilRPC := os.Getenv("ANVIL_RPC")
	if anvilRPC == "" {
		anvilRPC = cfg.AnvilRPC
	}

	gevm := execution.NewGEVMSimulator(cfg.BaseHTTPRPC, ownerAddress, anvilRPC)
	if anvilRPC != "" {
		gevm.HealthCheckAnvil()
		go func() {
			ticker := time.NewTicker(30 * time.Second)
			defer ticker.Stop()
			for range ticker.C {
				gevm.HealthCheckAnvil()
			}
		}()
	}

	execEndpoint := cfg.BaseExecRPC
	if execEndpoint == "" {
		execEndpoint = cfg.BaseHTTPRPC
	}

	sender, err := execution.NewSender(cfg.BaseHTTPRPC, execEndpoint, cfg.PrivateKey)
	if err != nil {
		log.Fatalf("Failed to initialize sender: %v", err)
	}

	sender.SetReplaceConfig(
		time.Duration(cfg.ReplaceTimeoutMs)*time.Millisecond,
		float64(100+cfg.ReplaceBumpPercent)/100.0,
		cfg.MaxReplaceAttempts,
	)

	// Confirmation callback – currently reports estimated profit.
	// TODO: derive realized profit from receipt.GasUsed and EffectiveGasPrice.
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

	sender.SetSolverCallback(func(failedPool common.Address) []*types.RouteCandidate {
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
		var filtered []*types.RouteCandidate
		for _, cand := range allCands {
			usesFailed := false
			for i := 0; i < int(cand.Hops); i++ {
				if cand.Pools[i] == failedPool {
					usesFailed = true
					break
				}
			}
			if !usesFailed {
				filtered = append(filtered, cand)
			}
		}
		return filtered
	})

	decoder := ingestion.NewDecoder()

	eventChan := make(chan *types.SwapLog, eventChanSize)
	candidateChan := make(chan *types.RouteCandidate, candidateChanSize)
	executionChan := make(chan *types.ExecutionPayload, executionChanSize)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	solver.StartL1FeeUpdater(ethClient, ctx)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go ingestion.StartWebSocketReader(ctx, cfg.BaseWSRPC, eventChan, decoder)

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
		worker1(ctx, eventChan, candidateChan, matrix, blacklist, lru, anchorSet, cfg)
	}()

	wg.Add(1)
	go func() {
		if cfg.EnableCPUPinning {
			pinToCore(cores[1])
		}
		defer wg.Done()
		worker2(ctx, candidateChan, executionChan, gevm, matrix, blacklist, anchorSet, cfg)
	}()

	wg.Add(1)
	go func() {
		if cfg.EnableCPUPinning {
			pinToCore(cores[2])
		}
		defer wg.Done()
		worker3(ctx, executionChan, sender, nonceTracker)
	}()

	// Removed the fixed‑timer connection status goroutine – dashboard status must be driven by WebSocket reader callbacks.
	// TODO: Implement real WebSocket status updates.

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

func preloadPools(matrix *state.Matrix) {
	anchors := config.AnchorAssets()
	for i, addr := range anchors {
		matrix.RegisterToken(addr, uint8(i))
	}
	matrix.PreloadKnownPools()
}

func worker1(
	ctx context.Context,
	eventChan <-chan *types.SwapLog,
	candidateChan chan<- *types.RouteCandidate,
	matrix *state.Matrix,
	blacklist *state.Blacklist,
	lru *state.DynamicTokenCache,
	anchorSet map[common.Address]bool,
	cfg *config.Config,
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

			matrix.UpdateFromLog(swapLog)

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

			for i := 0; i < candidateCount; i++ {
				select {
				case candidateChan <- localCandidates[i]:
				default:
				}
			}

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

			ingestion.PutSwapLog(swapLog)
		}
	}
}

// getTokenDecimals returns the correct decimals for a token.
// This overrides the solver package's version for cbBTC (8 decimals).
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
				tokenPrice := solver.GetTokenPrice(loanToken, matrix)
				if tokenPrice <= 0 {
					tokenPrice = 1.0
				}
				decimals := getTokenDecimals(loanToken) // use local fixed function
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

			success, gasUsed, err := gevm.SimulateWithBackend(cand, simPayload, backend)
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

			// Reject candidate if gas‑adjusted profit is below minimum.
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

			txHash, err := sender.SendRawTransaction(payload)
			if err != nil {
				nonceTracker.Rollback()
				msg := fmt.Sprintf("[-] DROP | Tx broadcast failed | Reason: %v", err)
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

			sender.RegisterPendingNonce(nonce, payload)
		}
	}
}

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
