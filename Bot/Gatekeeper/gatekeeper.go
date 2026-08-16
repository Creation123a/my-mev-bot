// Package gatekeeper provides an asynchronous background qualification pipeline
// for discovering and promoting DEXs, base pairs, and meme tokens into the
// active 6,6,60 execution matrix.
package gatekeeper

import (
	"context"
	"fmt"
	"log"
	"math"
	"math/big"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"

	"my-mev-bot/Bot/Config"
	"my-mev-bot/Bot/Execution"
	"my-mev-bot/Bot/Solver"
	"my-mev-bot/Bot/State"
	"my-mev-bot/Bot/Types"
)

// DiscoveryCandidate holds minimal data for a new pool/token to be qualified.
type DiscoveryCandidate struct {
	PoolAddress  common.Address
	Token0       common.Address
	Token1       common.Address
	CurrentBlock uint64
}

// Gatekeeper manages the background qualification pipeline.
type Gatekeeper struct {
	client         *ethclient.Client
	gevm           *execution.GEVMSimulator
	candidateQueue chan DiscoveryCandidate
	factoryBlocks  map[common.Address]uint64 // last block seen per factory
	factoryCounts  map[common.Address]int    // swaps in 3‑block window
	mu             sync.Mutex

	// Caches passed from main (thread‑safe)
	memeCache   *state.LRUCache
	dexCache    *state.LRUCache
	pairCache   *state.LRUCache
	blacklist   *state.Blacklist
	matrix      *state.Matrix
}

// New creates a Gatekeeper with a background worker pool.
func New(
	client *ethclient.Client,
	gevm *execution.GEVMSimulator,
	memeCache *state.LRUCache,
	dexCache *state.LRUCache,
	pairCache *state.LRUCache,
	blacklist *state.Blacklist,
	matrix *state.Matrix,
) *Gatekeeper {
	gk := &Gatekeeper{
		client:         client,
		gevm:           gevm,
		candidateQueue: make(chan DiscoveryCandidate, 1024),
		factoryBlocks:  make(map[common.Address]uint64),
		factoryCounts:  make(map[common.Address]int),
		memeCache:      memeCache,
		dexCache:       dexCache,
		pairCache:      pairCache,
		blacklist:      blacklist,
		matrix:         matrix,
	}
	// Start background workers
	gk.startWorkers()
	return gk
}

// startWorkers launches the background qualification workers.
func (gk *Gatekeeper) startWorkers() {
	for i := 0; i < 2; i++ {
		go gk.worker()
	}
	log.Printf("[Gatekeeper] Started %d background workers", 2)
}

// ProcessLog is the hot‑path entry point. It enqueues the candidate non‑blocking.
func (gk *Gatekeeper) ProcessLog(log *types.SwapLog) {
	// Fast‑path: if pool already blacklisted, skip.
	if gk.blacklist.IsBlacklisted(log.Address) {
		return
	}
	// Fast‑path: if both tokens are already in the active caches, skip enqueue.
	// (The solver will handle it directly; we only need to enqueue unknown combos.)
	if gk.memeCache.Get(log.TokenIn) && gk.memeCache.Get(log.TokenOut) {
		return
	}
	if gk.matrix.GetPool(log.Address) != nil {
		// Pool is already in the matrix, but maybe the tokens aren't in caches.
		// We still enqueue to qualify the tokens.
	}
	select {
	case gk.candidateQueue <- DiscoveryCandidate{
		PoolAddress:  log.Address,
		Token0:       log.TokenIn,
		Token1:       log.TokenOut,
		CurrentBlock: log.BlockNumber,
	}:
	default:
		// Queue full – drop candidate to avoid blocking the hot path.
	}
}

// worker processes candidates from the queue.
func (gk *Gatekeeper) worker() {
	ctx := context.Background()
	for cand := range gk.candidateQueue {
		// 1. Skip if pool is blacklisted (re‑check in case it was added after enqueue).
		if gk.blacklist.IsBlacklisted(cand.PoolAddress) {
			continue
		}
		// 2. Fetch factory address (cached per pool).
		factoryAddr, err := gk.getFactory(ctx, cand.PoolAddress)
		if err != nil {
			continue
		}
		// 3. Track factory volume within 3‑block window.
		gk.mu.Lock()
		lastBlock := gk.factoryBlocks[factoryAddr]
		if cand.CurrentBlock > lastBlock+3 {
			gk.factoryCounts[factoryAddr] = 0 // reset if window expired
		}
		gk.factoryBlocks[factoryAddr] = cand.CurrentBlock
		gk.factoryCounts[factoryAddr]++
		count := gk.factoryCounts[factoryAddr]
		gk.mu.Unlock()

		// 4. Promote DEX if >=2 swaps in 3‑block window.
		if count >= 2 && !gk.dexCache.Get(factoryAddr) {
			gk.dexCache.Put(factoryAddr)
			log.Printf("[Gatekeeper] DEX factory promoted: %s", factoryAddr.Hex())
		}

		// 5. Qualify each token.
		gk.qualifyTokenLayer(cand.Token0, cand.Token1, cand.PoolAddress)
		gk.qualifyTokenLayer(cand.Token1, cand.Token0, cand.PoolAddress)
	}
}

// qualifyTokenLayer promotes a token based on its relationship to the paired token.
func (gk *Gatekeeper) qualifyTokenLayer(target, paired common.Address, pool common.Address) {
	if gk.blacklist.IsBlacklisted(target) {
		return
	}
	// Determine if target is a known base asset (anchor).
	isBase := gk.isBaseAsset(target)

	if isBase {
		// Base pair synergy: promote if paired token is in meme cache.
		if gk.memeCache.Get(paired) && !gk.pairCache.Get(target) {
			gk.pairCache.Put(target)
			log.Printf("[Gatekeeper] Base pair promoted: %s", target.Hex())
		}
	} else {
		// Meme coin candidate: run two‑way simulation + profit check.
		if gk.memeCache.Get(target) {
			// Already in cache, skip.
			return
		}
		// Check if the paired token is a base asset (required for two‑way simulation).
		if !gk.isBaseAsset(paired) {
			return // cannot simulate without a base asset
		}
		// Run two‑way simulation.
		profit, err := gk.simulateTwoWaySwap(pool, target, paired)
		if err != nil {
			// If simulation fails, blacklist the token (honeypot).
			gk.blacklist.Add(target)
			log.Printf("[Gatekeeper] Honeypot detected, blacklisted: %s", target.Hex())
			return
		}
		// Profit floor: $0.50 gross.
		if profit > solver.MemeAdmissionProfit {
			gk.memeCache.Put(target)
			log.Printf("[Gatekeeper] Meme coin promoted: %s (profit $%.2f)", target.Hex(), profit)
		}
	}
}

// isBaseAsset checks if the token is one of the anchor assets.
func (gk *Gatekeeper) isBaseAsset(token common.Address) bool {
	anchors := config.AnchorAssets()
	for _, a := range anchors {
		if a == token {
			return true
		}
	}
	return false
}

// getFactory fetches the factory address from the pool (cached per pool).
func (gk *Gatekeeper) getFactory(ctx context.Context, pool common.Address) (common.Address, error) {
	return gk.callFactory(ctx, pool)
}

// callFactory performs the actual RPC to get the factory.
func (gk *Gatekeeper) callFactory(ctx context.Context, pool common.Address) (common.Address, error) {
	return gk.matrix.CallFactory(ctx, pool)
}

// simulateTwoWaySwap runs a buy and sell simulation and returns net profit in USD.
func (gk *Gatekeeper) simulateTwoWaySwap(pool, memeToken, baseToken common.Address) (float64, error) {
	poolState := gk.matrix.GetPool(pool)
	if poolState == nil {
		return 0, fmt.Errorf("pool not found in matrix")
	}

	// Get base token price
	basePrice := solver.GetTokenPrice(baseToken, gk.matrix, nil)
	if basePrice <= 0 {
		return 0, fmt.Errorf("base token price unknown")
	}

	// Compute a small amount (e.g., $0.50 worth)
	usdAmount := 0.50
	baseDecimals := getTokenDecimals(baseToken)
	amountInFloat := (usdAmount / basePrice) * math.Pow10(baseDecimals)
	amountIn := new(big.Int).SetInt64(int64(amountInFloat))
	if amountIn.Sign() <= 0 {
		return 0, fmt.Errorf("amount too small")
	}

	// Simulate first swap: baseToken -> memeToken
	out1 := new(big.Int)
	if err := solver.ComputeSwap(poolState, baseToken, memeToken, amountIn, out1); err != nil {
		return 0, fmt.Errorf("first swap compute: %w", err)
	}
	if out1.Sign() <= 0 {
		return 0, fmt.Errorf("first swap output zero")
	}

	// Simulate second swap: memeToken -> baseToken
	out2 := new(big.Int)
	if err := solver.ComputeSwap(poolState, memeToken, baseToken, out1, out2); err != nil {
		return 0, fmt.Errorf("second swap compute: %w", err)
	}
	if out2.Sign() <= 0 {
		return 0, fmt.Errorf("second swap output zero")
	}

	// Compute net profit in wei of base token
	netWei := new(big.Int).Sub(out2, amountIn)
	if netWei.Sign() <= 0 {
		return 0, fmt.Errorf("no profit")
	}

	// Convert to USD
	netFloat := float64FromBig(netWei)
	profitUSD := (netFloat / math.Pow10(baseDecimals)) * basePrice

	return profitUSD, nil
}

// Helper to get token decimals (reuse from solver)
func getTokenDecimals(token common.Address) int {
	if token == config.USDCAddress || token == config.USDBCAddress {
		return 6
	}
	if token == config.WETHAddress {
		return 18
	}
	if token == config.CBBTCAddress {
		return 8
	}
	return 18
}

// Helper to convert big.Int to float64 (reuse from solver)
func float64FromBig(v *big.Int) float64 {
	if v == nil {
		return 0
	}
	f, _ := new(big.Float).SetInt(v).Float64()
	return f
}
