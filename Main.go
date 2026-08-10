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

	"my-mev-bot/Bot/Config"
	"my-mev-bot/Bot/Dashboard"
	"my-mev-bot/Bot/Execution"
	"my-mev-bot/Bot/Ingestion"
	"my-mev-bot/Bot/Solver"
	"my-mev-bot/Bot/State"
	"my-mev-bot/Bot/Types"
)

// ----- Copy of the original main() with dry‑run modifications -----

func mainDryRun() {
	cfg, err := config.LoadConfig()
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

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

	sender.SetConfirmationCallback(func(nonce uint64, txHash common.Hash, receipt *gethTypes.Receipt, payload *types.ExecutionPayload) {
		profitUSD := payload.MinProfitUSD
		profitMu.Lock()
		totalProfit += profitUSD
		profitMu.Unlock()

		if dashServer != nil {
			dashServer.AddProfit(profitUSD)
			dashServer.SetTradeStatus("SUCCESS", "")
			msg := fmt.Sprintf("[+] WIN (dry-run) | Tx: %s | Profit: $%.2f | Route: %s | Confirmed at block %d",
				txHash.Hex(), profitUSD, payload.RouteDesc, receipt.BlockNumber.Uint64())
			dashServer.Log(msg)
		}
		log.Printf("[+] WIN (dry-run) | Tx: %s | Profit: $%.2f | Route: %s | Block: %d",
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

	// Dry‑run worker3: log instead of broadcasting
	wg.Add(1)
	go func() {
		defer wg.Done()
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

				// ---------- DRY‑RUN MODE ----------
				log.Printf("[DRY-RUN] Would send tx | Nonce: %d | Profit: $%.2f | GasTip: %.3f Gwei | Route: %s",
					nonce,
					payload.MinProfitUSD,
					float64(payload.PriorityFeeWei)/1e9,
					payload.RouteDesc)

				// Simulate a fake confirmation (optional) to test the callback path
				go func(p *types.ExecutionPayload, n uint64) {
					time.Sleep(2 * time.Second)
					fakeReceipt := &gethTypes.Receipt{
						Status:      1,
						BlockNumber: big.NewInt(12345),
					}
					// Call the confirmation callback if set
					if sender != nil {
						// We need to call sender's confCallback directly; we can't access it easily from here.
						// For simplicity, we just log the simulated confirmation.
						log.Printf("[DRY-RUN] Simulated confirmation for nonce %d, profit $%.2f", n, p.MinProfitUSD)
					}
				}(payload, nonce)

				// Do NOT call sender.SendRawTransaction
				// ---------- END DRY‑RUN ----------
			}
		}
	}()

	// Remove the original fixed timer; we keep the dashboard status as is (it will show "Disconnected" until we implement real status).

	<-sigChan
	log.Println("\nShutting down dry‑run bot...")
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
	log.Println("Dry‑run bot stopped.")
}

// main now calls the dry‑run version.
func main() {
	mainDryRun()
}
