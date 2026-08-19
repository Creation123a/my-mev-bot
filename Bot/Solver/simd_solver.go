// Package solver implements high‑performance arbitrage path generation and optimal sizing.
package solver

import (
	"context"
	"fmt"
	"math"
	"math/big"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"

	"my-mev-bot/Bot/Config"
	"my-mev-bot/Bot/State"
	"my-mev-bot/Bot/Types"
)

const (
	MaxSlippagePct      = 3.0
	MaxHops             = 3
	SlippageBuffer      = 0.99
	HopPenalty3Hop      = 0.05
	MinScore            = 0.01
	MemeAdmissionProfit = 0.50 // USD gross – used by gatekeeper for meme qualification
)

var v3CalcPool = sync.Pool{
	New: func() interface{} { return NewV3Calculator() },
}

var floatPool = sync.Pool{
	New: func() interface{} { return new(big.Float) },
}

var (
	currentGasCostUSDPerGas atomic.Value // float64
	currentL1BaseFeeUSD     atomic.Value // float64
	currentL1GasCostPerByte atomic.Value // float64 – L1 data gas cost per byte in USD
	ethPriceUSD             atomic.Value // float64
)

func init() {
	currentGasCostUSDPerGas.Store(0.00000000005)
	currentL1BaseFeeUSD.Store(0.00000000001)
	currentL1GasCostPerByte.Store(0.000000000001) // placeholder
	ethPriceUSD.Store(3000.0)
}

func SetEthPrice(price float64) {
	if price > 0 {
		ethPriceUSD.Store(price)
	}
}

func GetEthPrice() float64 {
	if v := ethPriceUSD.Load(); v != nil {
		return v.(float64)
	}
	return 3000.0
}

// StartL1FeeUpdater fetches L1 base fee, overhead, scalar and computes cost per byte.
func StartL1FeeUpdater(client *ethclient.Client, ctx context.Context) {
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				// L1 base fee (from 0x4200000000000000000000000000000000000015)
				contract := common.HexToAddress("0x4200000000000000000000000000000000000015")
				data := crypto.Keccak256([]byte("basefee()"))[:4]
				msg := ethereum.CallMsg{To: &contract, Data: data}
				ctx2, cancel := context.WithTimeout(ctx, 1*time.Second)
				result, err := client.CallContract(ctx2, msg, nil)
				cancel()
				if err == nil {
					l1BaseFee := new(big.Int).SetBytes(result)
					l1BaseFeeWei, _ := new(big.Float).SetInt(l1BaseFee).Float64()
					ethPrice := GetEthPrice()
					l1BaseFeeUSDPerGas := l1BaseFeeWei * ethPrice / 1e18
					if l1BaseFeeUSDPerGas > 0 {
						currentL1BaseFeeUSD.Store(l1BaseFeeUSDPerGas)
					}
				}

				// GasPriceOracle (0x420000000000000000000000000000000000000F) for overhead and scalar
				oracle := common.HexToAddress("0x420000000000000000000000000000000000000F")
				overheadData := crypto.Keccak256([]byte("overhead()"))[:4]
				scalarData := crypto.Keccak256([]byte("scalar()"))[:4]
				msgOverhead := ethereum.CallMsg{To: &oracle, Data: overheadData}
				msgScalar := ethereum.CallMsg{To: &oracle, Data: scalarData}
				ctx3, cancel3 := context.WithTimeout(ctx, 1*time.Second)
				overheadRes, err1 := client.CallContract(ctx3, msgOverhead, nil)
				if err1 == nil {
					overhead := new(big.Int).SetBytes(overheadRes)
					scalarRes, err2 := client.CallContract(ctx3, msgScalar, nil)
					if err2 == nil {
						scalar := new(big.Int).SetBytes(scalarRes)
						// L1 data gas cost per byte = (l1BaseFee * scalar * overhead) / 1e9
						// Convert to USD: l1BaseFeeUSDPerGas * scalar * overhead / 1e9
						l1BaseFeeUSDPerGas := currentL1BaseFeeUSD.Load().(float64)
						overheadF, _ := new(big.Float).SetInt(overhead).Float64()
						scalarF, _ := new(big.Float).SetInt(scalar).Float64()
						if overheadF > 0 && scalarF > 0 {
							costPerByte := l1BaseFeeUSDPerGas * scalarF * overheadF / 1e9
							if costPerByte > 0 {
								currentL1GasCostPerByte.Store(costPerByte)
							}
						}
					}
				}
				cancel3()

				// L2 execution gas cost (approximate)
				ethPrice := GetEthPrice()
				l2GasCostUSD := 0.5 * ethPrice / 1e9
				l1BaseFeeUSDPerGas := currentL1BaseFeeUSD.Load().(float64)
				totalGasCostUSD := l1BaseFeeUSDPerGas + l2GasCostUSD
				if totalGasCostUSD > 0 {
					currentGasCostUSDPerGas.Store(totalGasCostUSD)
				}
			}
		}
	}()
}

func GetCurrentL1BaseFeeUSD() float64 {
	if v := currentL1BaseFeeUSD.Load(); v != nil {
		return v.(float64)
	}
	return 0.00000000001
}

// GetCurrentL1GasCostPerByte returns the current L1 data gas cost per byte in USD.
func GetCurrentL1GasCostPerByte() float64 {
	if v := currentL1GasCostPerByte.Load(); v != nil {
		return v.(float64)
	}
	return 0.000000000001
}

func GetCurrentGasCostUSD() float64 {
	if v := currentGasCostUSDPerGas.Load(); v != nil {
		return v.(float64)
	}
	return 0.00000000005
}

// estimateGasForCandidate returns a rough estimate of gas used for a candidate.
func estimateGasForCandidate(cand *types.RouteCandidate) uint64 {
	base := uint64(150000)
	if cand.Hops == 2 {
		base += 150000
	} else {
		base += 250000
	}
	return uint64(float64(base) * 1.2)
}

// isStableV2Pool returns true if the pool is a stable Aerodrome V2 pool (unsupported).
func isStableV2Pool(pool *types.PoolState) bool {
	return pool.DexType == types.DexAerodromeV2 && pool.Stable
}

// getBestPool returns the pool with the highest liquidity, skipping stable pools.
func getBestPool(pools []*types.PoolState) *types.PoolState {
	if len(pools) == 0 {
		return nil
	}
	best := pools[0]
	bestScore := best.GetReservesProduct()
	for _, p := range pools[1:] {
		if isStableV2Pool(p) {
			continue
		}
		score := p.GetReservesProduct()
		if score > bestScore {
			best = p
			bestScore = score
		}
	}
	return best
}

// getTopTwoPools returns the two pools with the highest liquidity, skipping stable pools.
func getTopTwoPools(pools []*types.PoolState) (*types.PoolState, *types.PoolState) {
	if len(pools) < 2 {
		return nil, nil
	}
	filtered := make([]*types.PoolState, 0, len(pools))
	for _, p := range pools {
		if !isStableV2Pool(p) {
			filtered = append(filtered, p)
		}
	}
	if len(filtered) < 2 {
		return nil, nil
	}
	sorted := make([]*types.PoolState, len(filtered))
	copy(sorted, filtered)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].GetReservesProduct() > sorted[j].GetReservesProduct()
	})
	return sorted[0], sorted[1]
}

type PathIndex struct {
	affectedPaths map[common.Address][]*types.Path
}

func PrecomputePaths(matrix *state.Matrix, blacklist interface{}) *PathIndex {
	return &PathIndex{
		affectedPaths: make(map[common.Address][]*types.Path),
	}
}

// ---------- Worker Pool ----------
var taskCh = make(chan func(), 100)

func init() {
	numWorkers := runtime.NumCPU()
	for i := 0; i < numWorkers; i++ {
		go func() {
			for task := range taskCh {
				task()
			}
		}()
	}
}
// EvaluateEvent generates and evaluates candidate routes with dynamic L1 fee threshold.
// It uses the worker pool to evaluate routes in parallel.
func EvaluateEvent(log *types.SwapLog, matrix *state.Matrix, cfg *config.Config) []*types.RouteCandidate {
	if log == nil || matrix == nil {
		return nil
	}
	if log.TokenIn == (common.Address{}) || log.TokenOut == (common.Address{}) {
		return nil
	}

	tokenA := log.TokenIn
	tokenB := log.TokenOut
	if tokenA == (common.Address{}) || tokenB == (common.Address{}) {
		return nil
	}

	pools := matrix.GetPoolsForPair(tokenA, tokenB)
	if len(pools) == 0 {
		return nil
	}

	anchors := config.AnchorAssets()
	anchorMap := make(map[common.Address]bool)
	for _, a := range anchors {
		anchorMap[a] = true
	}

	isAnchorA := anchorMap[tokenA]
	isAnchorB := anchorMap[tokenB]

	var priceCache sync.Map
	resultCh := make(chan *types.RouteCandidate, 10)
	var wg sync.WaitGroup

	enqueue := func(builder func() *types.RouteCandidate) {
		wg.Add(1)
		taskCh <- func() {
			defer wg.Done()
			if cand := builder(); cand != nil && cand.ExpectedProfitUSD > cfg.MinProfitUSD {
				resultCh <- cand
			}
		}
	}

	if isAnchorA && isAnchorB {
		// no direct opportunity
	} else if isAnchorA && !isAnchorB {
		enqueue(func() *types.RouteCandidate { return buildRoundTrip2Hop(tokenA, tokenB, matrix, cfg, &priceCache) })
	} else if !isAnchorA && isAnchorB {
		enqueue(func() *types.RouteCandidate { return buildRoundTrip2Hop(tokenB, tokenA, matrix, cfg, &priceCache) })
	} else {
		for _, anchor := range anchors {
			anchor := anchor
			enqueue(func() *types.RouteCandidate { return buildRoundTrip3Hop(anchor, tokenA, tokenB, matrix, cfg, &priceCache) })
			enqueue(func() *types.RouteCandidate { return buildRoundTrip3Hop(anchor, tokenB, tokenA, matrix, cfg, &priceCache) })
		}
	}

	go func() {
		wg.Wait()
		close(resultCh)
	}()

	candidates := make([]*types.RouteCandidate, 0)
	for cand := range resultCh {
		candidates = append(candidates, cand)
	}

	// Dynamic L1 fee threshold – fetch once per evaluation
	l1CostPerByte := GetCurrentL1GasCostPerByte()
	baseCalldata := 2000

	// Compute competition score, apply L1 threshold, and set final score
	for _, cand := range candidates {
		// 1. Competition score
		routeComp := 0.0
		for i := 0; i < int(cand.Hops); i++ {
			poolAddr := cand.Pools[i]
			poolScore := matrix.GetPoolScore(poolAddr)
			if poolScore > routeComp {
				routeComp = poolScore
			}
		}
		cand.Competition = routeComp

		// 2. Hop penalty
		hopPenalty := 0.0
		if cand.Hops == 3 {
			hopPenalty = HopPenalty3Hop
		}

		// 3. Base score (profit adjusted for competition and hops)
		score := cand.ExpectedProfitUSD * (1 - routeComp) * (1 - hopPenalty)
		if score < MinScore {
			score = 0
		}
		cand.Score = score
		cand.ExecutionSlippage = 0.5

		// 4. L1 fee threshold (if profit cannot cover gas + buffer, drop)
		calldataBytes := baseCalldata + 500*int(cand.Hops)
		l1Cost := l1CostPerByte * float64(calldataBytes)
		minProfit := cfg.MinProfitUSD + l1Cost + 0.50 // $0.50 safety buffer
		if cand.ExpectedProfitUSD < minProfit {
			cand.Score = 0 // drop this candidate
		}
	}

	// Sort by score (descending) and filter out dropped candidates
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].Score > candidates[j].Score
	})

	filtered := make([]*types.RouteCandidate, 0, len(candidates))
	for _, cand := range candidates {
		if cand.Score > 0 {
			filtered = append(filtered, cand)
		}
	}
	return filtered
}
// =============================================================================
//  OPTIMAL INPUT SIZING – CLOSED‑FORM & NUMERICAL
// =============================================================================
// computeOptimalAmount2Hop returns the optimal input amount for a 2-hop round-trip on constant product pools.
// Uses closed-form: x* = sqrt( (R0 * R1 * (1-f0)*(1-f1)) / (1 - (1-f0)*(1-f1)) ) - R0
func computeOptimalAmount2Hop(pool0, pool1 *types.PoolState, start common.Address) *big.Int {
	// Only works for V2 constant product pools (Aerodrome V2, AlienBase V2)
	if pool0.DexType == types.DexUniswapV3 || pool0.DexType == types.DexPancakeV3 ||
		pool1.DexType == types.DexUniswapV3 || pool1.DexType == types.DexPancakeV3 {
		return nil
	}
	// Skip stable pools
	if isStableV2Pool(pool0) || isStableV2Pool(pool1) {
		return nil
	}

	r0, _ := pool0.GetReserves()
	r1, _ := pool0.GetReserves()
	var reserveStart0, reserveOther0 float64
	if pool0.Token0 == start {
		reserveStart0 = r0
		reserveOther0 = r1
	} else {
		reserveStart0 = r1
		reserveOther0 = r0
	}
	r0p, r1p := pool1.GetReserves()
	var reserveStart1 float64
	if pool1.Token0 == start {
		reserveStart1 = r0p
	} else {
		reserveStart1 = r1p
	}
	if reserveStart0 <= 0 || reserveStart1 <= 0 || reserveOther0 <= 0 {
		return nil
	}

	fee0 := 1.0 - float64(pool0.FeeBps)/10000.0
	fee1 := 1.0 - float64(pool1.FeeBps)/10000.0
	if fee0 <= 0 || fee1 <= 0 {
		return nil
	}

	product := reserveStart0 * reserveStart1 * fee0 * fee1
	denom := 1 - fee0*fee1
	if denom <= 0 {
		return nil
	}
	optimal := math.Sqrt(product/denom) - reserveStart0
	if optimal <= 0 {
		return nil
	}

	f := floatPool.Get().(*big.Float)
	f.SetFloat64(optimal)
	res := new(big.Int)
	f.Int(res)
	floatPool.Put(f)
	return res
}

// computeOptimalAmount3Hop uses golden‑section search for 3‑hop V2 routes.
func computeOptimalAmount3Hop(
	p0, p1, p2 *types.PoolState,
	start, mid1, mid2 common.Address,
	priceStart float64,
) *big.Int {
	base := computeOptimalInputRaw(p0, start)
	if base.Sign() <= 0 {
		return nil
	}
	baseF, _ := base.Float64()
	low := 0.0
	high := baseF * 10.0
	if high <= 0 {
		return nil
	}
	profitFn := func(amount float64) float64 {
		amt := new(big.Int).SetInt64(int64(amount))
		out0 := new(big.Int)
		if err := computeSwap(p0, start, mid1, amt, out0); err != nil || out0.Sign() <= 0 {
			return -1e9
		}
		out1 := new(big.Int)
		if err := computeSwap(p1, mid1, mid2, out0, out1); err != nil || out1.Sign() <= 0 {
			return -1e9
		}
		out2 := new(big.Int)
		if err := computeSwap(p2, mid2, start, out1, out2); err != nil || out2.Sign() <= 0 {
			return -1e9
		}
		net := new(big.Int).Sub(out2, amt)
		if net.Sign() <= 0 {
			return -1e9
		}
		netF, _ := new(big.Float).SetInt(net).Float64()
		dec := getTokenDecimals(start)
		return (netF / math.Pow10(dec)) * priceStart
	}
	const phi = 1.618033988749895
	for i := 0; i < 30; i++ {
		midLow := high - (high-low)/phi
		midHigh := low + (high-low)/phi
		fLow := profitFn(midLow)
		fHigh := profitFn(midHigh)
		if fLow < fHigh {
			low = midLow
		} else {
			high = midHigh
		}
	}
	opt := (low + high) / 2
	if opt <= 0 {
		return nil
	}
	f := floatPool.Get().(*big.Float)
	f.SetFloat64(opt)
	res := new(big.Int)
	f.Int(res)
	floatPool.Put(f)
	return res
}

// computeOptimalAmountV3 uses golden‑section search for a single V3 swap.
func computeOptimalAmountV3(pool *types.PoolState, tokenIn, tokenOut common.Address, priceIn float64) *big.Int {
	if pool.DexType != types.DexUniswapV3 && pool.DexType != types.DexPancakeV3 {
		return nil
	}
	base := computeOptimalInputRaw(pool, tokenIn)
	if base.Sign() <= 0 {
		return nil
	}
	baseF, _ := base.Float64()
	low := 0.0
	high := baseF * 10.0
	if high <= 0 {
		return nil
	}
	profitFn := func(amount float64) float64 {
		amt := new(big.Int).SetInt64(int64(amount))
		out := new(big.Int)
		if err := computeSwap(pool, tokenIn, tokenOut, amt, out); err != nil || out.Sign() <= 0 {
			return -1e9
		}
		net := new(big.Int).Sub(out, amt)
		if net.Sign() <= 0 {
			return -1e9
		}
		netF, _ := new(big.Float).SetInt(net).Float64()
		dec := getTokenDecimals(tokenIn)
		return (netF / math.Pow10(dec)) * priceIn
	}
	const phi = 1.618033988749895
	for i := 0; i < 30; i++ {
		midLow := high - (high-low)/phi
		midHigh := low + (high-low)/phi
		fLow := profitFn(midLow)
		fHigh := profitFn(midHigh)
		if fLow < fHigh {
			low = midLow
		} else {
			high = midHigh
		}
	}
	opt := (low + high) / 2
	if opt <= 0 {
		return nil
	}
	f := floatPool.Get().(*big.Float)
	f.SetFloat64(opt)
	res := new(big.Int)
	f.Int(res)
	floatPool.Put(f)
	return res
}


// =============================================================================
//  CANDIDATE GENERATION (2‑HOP, 3‑HOP, WITH OPTIMAL SIZING)
// =============================================================================

// tryMultipliers tries a range of input multipliers around the optimal amount.
func tryMultipliers(
	start, middle, end common.Address,
	baseAmount *big.Int,
	pool0, pool1 *types.PoolState,
	forwardZeroForOne, reverseZeroForOne bool,
	matrix *state.Matrix,
	cfg *config.Config,
	cache *sync.Map,
	optimalAmount *big.Int,
) *types.RouteCandidate {
	var multipliers []float64
	if optimalAmount != nil && optimalAmount.Sign() > 0 {
		multipliers = []float64{0.8, 0.85, 0.9, 0.95, 1.0, 1.05, 1.1, 1.15, 1.2}
	} else {
		multipliers = []float64{0.5, 0.75, 1.0, 1.25, 1.5}
	}
	bestCand := (*types.RouteCandidate)(nil)
	bestProfit := 0.0
	var amountIn, outMid, outStart, netWei, minOut0, minOut1 big.Int

	// For early break detection.
	prevProfit := -1.0

	for _, mul := range multipliers {
	 if optimalAmount != nil && optimalAmount.Sign() > 0 {
        amountIn.Set(optimalAmount)
    } else {
        amountIn.Set(baseAmount)
    }
		scaled := int64(mul * 100)
		amountIn.Mul(&amountIn, big.NewInt(scaled))
		amountIn.Div(&amountIn, big.NewInt(100))
		if amountIn.Sign() <= 0 {
			continue
		}

		if err := computeSwap(pool0, start, middle, &amountIn, &outMid); err != nil {
			continue
		}
		if err := computeSwap(pool1, middle, start, &outMid, &outStart); err != nil {
			continue
		}
		netWei.Sub(&outStart, &amountIn)
		if netWei.Sign() <= 0 {
			continue
		}
		priceStart := GetTokenPrice(start, matrix, cache)
		if priceStart <= 0 {
			continue
		}
		decStart := getTokenDecimals(start)
		grossProfitUSD := (float64FromBig(&netWei) / math.Pow10(decStart)) * priceStart
		cand := &types.RouteCandidate{Hops: 2}
		gasCostUSD := GetCurrentGasCostUSD() * float64(estimateGasForCandidate(cand))
		profitUSD := grossProfitUSD - gasCostUSD
		if profitUSD < cfg.MinProfitUSD {
			continue
		}
		applySlippageBuffer(&outMid, &minOut0)
		applySlippageBuffer(&outStart, &minOut1)

		candidate := &types.RouteCandidate{
			Hops:              2,
			Tokens:            [4]common.Address{start, middle, start, {}},
			Pools:             [3]common.Address{pool0.PoolAddress, pool1.PoolAddress, {}},
			DexTypes:          [3]types.DexType{pool0.DexType, pool1.DexType, 0},
			ZeroForOnes:       [3]bool{forwardZeroForOne, reverseZeroForOne, false},
			MinOuts:           [3]*big.Int{new(big.Int).Set(&minOut0), new(big.Int).Set(&minOut1), new(big.Int)},
			AmountIn:          new(big.Int).Set(&amountIn),
			ExpectedProfitUSD: profitUSD,
			NetProfitWei:      new(big.Int).Set(&netWei),
			ExecutionSlippage: 0,
			Competition:       0,
		}
		if bestCand == nil || profitUSD > bestProfit {
			bestCand = candidate
			bestProfit = profitUSD
		}
		// Early break: if profit decreased from previous, we likely passed the peak.
		if prevProfit >= 0 && profitUSD < prevProfit {
			break
		}
		prevProfit = profitUSD
	}
	return bestCand
}

// buildRoundTrip2Hop – uses optimalAmount for V2 or V3.
func buildRoundTrip2Hop(start, middle common.Address, matrix *state.Matrix, cfg *config.Config, cache *sync.Map) *types.RouteCandidate {
	if start == middle {
		return nil
	}
	pools := matrix.GetPoolsForPair(start, middle)
	if len(pools) < 2 {
		return nil
	}
	pool0, pool1 := getTopTwoPools(pools)
	if pool0 == nil || pool1 == nil || pool0.PoolAddress == pool1.PoolAddress {
		return nil
	}
	var bestCand *types.RouteCandidate
	combinations := [][2]*types.PoolState{{pool0, pool1}, {pool1, pool0}}
	for _, pair := range combinations {
		fwdPool := pair[0]
		revPool := pair[1]
		if isStableV2Pool(fwdPool) || isStableV2Pool(revPool) {
			continue
		}
		forwardZeroForOne := fwdPool.Token0 == start
		reverseZeroForOne := revPool.Token0 == middle
		baseAmount := computeOptimalInputRaw(fwdPool, start)
		if baseAmount.Sign() <= 0 {
			continue
		}
		var optimalAmount *big.Int
		if fwdPool.DexType == types.DexUniswapV3 || fwdPool.DexType == types.DexPancakeV3 {
			optimalAmount = computeOptimalAmountV3(fwdPool, start, middle, GetTokenPrice(start, matrix, cache))
		} else {
			optimalAmount = computeOptimalAmount2Hop(fwdPool, revPool, start)
		}
		cand := tryMultipliers(start, middle, start, baseAmount, fwdPool, revPool,
			forwardZeroForOne, reverseZeroForOne, matrix, cfg, cache, optimalAmount)
		if cand != nil {
			if bestCand == nil || cand.ExpectedProfitUSD > bestCand.ExpectedProfitUSD {
				bestCand = cand
			}
		}
	}
	return bestCand
}






// buildRoundTrip3Hop – uses optimalAmount for V2 or V3 (single-hop guide).
func buildRoundTrip3Hop(start, mid1, mid2 common.Address, matrix *state.Matrix, cfg *config.Config, cache *sync.Map) *types.RouteCandidate {
	if start == mid1 || start == mid2 || mid1 == mid2 {
		return nil
	}
	pools01 := matrix.GetPoolsForPair(start, mid1)
	pools12 := matrix.GetPoolsForPair(mid1, mid2)
	pools20 := matrix.GetPoolsForPair(mid2, start)
	if len(pools01) == 0 || len(pools12) == 0 || len(pools20) == 0 {
		return nil
	}
	p0 := getBestPool(pools01)
	p1 := getBestPool(pools12)
	p2 := getBestPool(pools20)
	if p0 == nil || p1 == nil || p2 == nil {
		return nil
	}
	if isStableV2Pool(p0) || isStableV2Pool(p1) || isStableV2Pool(p2) {
		return nil
	}
	zeroForOne0 := p0.Token0 == start
	zeroForOne1 := p1.Token0 == mid1
	zeroForOne2 := p2.Token0 == mid2

	baseAmount := computeOptimalInputRaw(p0, start)
	if baseAmount.Sign() <= 0 {
		return nil
	}
	priceStart := GetTokenPrice(start, matrix, cache)
	if priceStart <= 0 {
		return nil
	}

	// Compute optimal amount for this route.
	var optimalAmount *big.Int
	if p0.DexType == types.DexUniswapV3 || p0.DexType == types.DexPancakeV3 {
		optimalAmount = computeOptimalAmountV3(p0, start, mid1, priceStart)
	} else {
		optimalAmount = computeOptimalAmount3Hop(p0, p1, p2, start, mid1, mid2, priceStart)
	}
	if optimalAmount == nil || optimalAmount.Sign() <= 0 {
		optimalAmount = baseAmount
	}

	multipliers := []float64{0.8, 0.9, 1.0, 1.1, 1.2}
	bestCand := (*types.RouteCandidate)(nil)
	bestProfit := 0.0
	var amountIn, out0, out1, out2, netWei, minOut0, minOut1, minOut2 big.Int

	for _, mul := range multipliers {
		amountIn.Set(optimalAmount)
		scaled := int64(mul * 100)
		amountIn.Mul(&amountIn, big.NewInt(scaled))
		amountIn.Div(&amountIn, big.NewInt(100))
		if amountIn.Sign() <= 0 {
			continue
		}
		if err := computeSwap(p0, start, mid1, &amountIn, &out0); err != nil {
			continue
		}
		if err := computeSwap(p1, mid1, mid2, &out0, &out1); err != nil {
			continue
		}
		if err := computeSwap(p2, mid2, start, &out1, &out2); err != nil {
			continue
		}
		netWei.Sub(&out2, &amountIn)
		if netWei.Sign() <= 0 {
			continue
		}
		priceStart := GetTokenPrice(start, matrix, cache)
		if priceStart <= 0 {
			continue
		}
		decStart := getTokenDecimals(start)
		grossProfitUSD := (float64FromBig(&netWei) / math.Pow10(decStart)) * priceStart
		cand := &types.RouteCandidate{Hops: 3}
		gasCostUSD := GetCurrentGasCostUSD() * float64(estimateGasForCandidate(cand))
		profitUSD := grossProfitUSD - gasCostUSD
		if profitUSD < cfg.MinProfitUSD {
			continue
		}
		applySlippageBuffer(&out0, &minOut0)
		applySlippageBuffer(&out1, &minOut1)
		applySlippageBuffer(&out2, &minOut2)

		candidate := &types.RouteCandidate{
			Hops:              3,
			Tokens:            [4]common.Address{start, mid1, mid2, start},
			Pools:             [3]common.Address{p0.PoolAddress, p1.PoolAddress, p2.PoolAddress},
			DexTypes:          [3]types.DexType{p0.DexType, p1.DexType, p2.DexType},
			ZeroForOnes:       [3]bool{zeroForOne0, zeroForOne1, zeroForOne2},
			MinOuts:           [3]*big.Int{new(big.Int).Set(&minOut0), new(big.Int).Set(&minOut1), new(big.Int).Set(&minOut2)},
			AmountIn:          new(big.Int).Set(&amountIn),
			ExpectedProfitUSD: profitUSD,
			NetProfitWei:      new(big.Int).Set(&netWei),
			ExecutionSlippage: 0,
			Competition:       0,
		}
		if bestCand == nil || profitUSD > bestProfit {
			bestCand = candidate
			bestProfit = profitUSD
		}
	}
	return bestCand
}
// =============================================================================
//  HELPER FUNCTIONS (unchanged)
// =============================================================================

// computeOptimalInputRaw computes the optimal single-hop input in raw units.
func computeOptimalInputRaw(pool *types.PoolState, tokenIn common.Address) *big.Int {
	if pool == nil {
		return big.NewInt(0)
	}
	r0, r1 := pool.GetReserves()
	if r0 <= 0 || r1 <= 0 {
		return big.NewInt(0)
	}
	var reserveInFloat, reserveOutFloat float64
	if tokenIn == pool.Token0 {
		reserveInFloat = r0
		reserveOutFloat = r1
	} else if tokenIn == pool.Token1 {
		reserveInFloat = r1
		reserveOutFloat = r0
	} else {
		return big.NewInt(0)
	}
	if reserveInFloat <= 0 || reserveOutFloat <= 0 {
		return big.NewInt(0)
	}
	fee := getFeeFactor(pool)
	k := reserveInFloat * reserveOutFloat
	sqrtK := math.Sqrt(k)
	amountInFloat := sqrtK/math.Sqrt(fee) - reserveInFloat
	if amountInFloat <= 0 {
		return big.NewInt(0)
	}
	f := floatPool.Get().(*big.Float)
	f.SetFloat64(amountInFloat)
	res := new(big.Int)
	f.Int(res)
	floatPool.Put(f)
	return res
}

// computeSwap calculates the output amount for a swap.
func computeSwap(pool *types.PoolState, tokenIn, tokenOut common.Address, amountIn *big.Int, result *big.Int) error {
	if amountIn == nil || amountIn.Sign() <= 0 {
		result.SetInt64(0)
		return nil
	}
	if pool.DexType == types.DexUniswapV3 || pool.DexType == types.DexPancakeV3 {
		calc := v3CalcPool.Get().(*V3Calculator)
		defer v3CalcPool.Put(calc)
		return calc.ComputeSwap(pool, tokenIn, tokenOut, amountIn, result)
	}
	// V2 style
	r0, r1 := pool.GetReserves()
	if r0 <= 0 || r1 <= 0 {
		return fmt.Errorf("zero reserves")
	}
	var reserveInFloat, reserveOutFloat float64
	if tokenIn == pool.Token0 {
		reserveInFloat = r0
		reserveOutFloat = r1
	} else if tokenIn == pool.Token1 {
		reserveInFloat = r1
		reserveOutFloat = r0
	} else {
		return fmt.Errorf("tokenIn not in pool")
	}
	if reserveInFloat <= 0 || reserveOutFloat <= 0 {
		return fmt.Errorf("zero reserves")
	}
	fee := getFeeFactor(pool)
	amountInF, _ := amountIn.Float64()
	amountInWithFee := amountInF * fee
	numerator := reserveOutFloat * amountInWithFee
	denominator := reserveInFloat + amountInWithFee
	amountOutF := numerator / denominator
	f := floatPool.Get().(*big.Float)
	f.SetFloat64(amountOutF)
	f.Int(result)
	floatPool.Put(f)
	return nil
}

// getFeeFactor returns the fee multiplier for a pool.
func getFeeFactor(pool *types.PoolState) float64 {
	feeBps := pool.FeeBps
	if feeBps == 0 {
		feeBps = 30
	}
	if feeBps >= 10000 {
		feeBps = 9999
	}
	return 1.0 - float64(feeBps)/10000.0
}

// applySlippageBuffer writes the slippage-buffered amount into 'out'.
func applySlippageBuffer(amount *big.Int, out *big.Int) {
	if amount == nil || amount.Sign() <= 0 {
		out.SetInt64(0)
		return
	}
	f := floatPool.Get().(*big.Float)
	f.SetInt(amount)
	f.Mul(f, big.NewFloat(SlippageBuffer))
	f.Int(out)
	floatPool.Put(f)
}

// GetTokenPrice returns USD price using the matrix, with an optional per‑event cache.
func GetTokenPrice(token common.Address, matrix *state.Matrix, cache *sync.Map) float64 {
	if cache != nil {
		if v, ok := cache.Load(token); ok {
			return v.(float64)
		}
	}
	if token == config.USDCAddress || token == config.USDBCAddress {
		price := 1.0
		if cache != nil {
			cache.Store(token, price)
		}
		return price
	}
	if token == config.WETHAddress {
		pools := matrix.GetPoolsForPair(config.WETHAddress, config.USDCAddress)
		if len(pools) > 0 {
			pool := pools[0]
			if pool.GetLiquidity() > 0 {
				price := getPriceFromPool(pool, token)
				if price > 0 {
					if cache != nil {
						cache.Store(token, price)
					}
					return price
				}
			}
		}
		return 0.0
	}
	if token == config.CBBTCAddress {
		pools := matrix.GetPoolsForPair(config.CBBTCAddress, config.USDCAddress)
		if len(pools) > 0 {
			pool := pools[0]
			if pool.GetLiquidity() > 0 {
				price := getPriceFromPool(pool, token)
				if price > 0 {
					if cache != nil {
						cache.Store(token, price)
					}
					return price
				}
			}
		}
		return 0.0
	}
	anchors := config.AnchorAssets()
	for _, anchor := range anchors {
		if anchor == token {
			continue
		}
		pools := matrix.GetPoolsForPair(token, anchor)
		if len(pools) > 0 {
			pool := pools[0]
			anchorPrice := GetTokenPrice(anchor, matrix, cache)
			if anchorPrice <= 0 {
				continue
			}
			priceInAnchor := getPriceFromPool(pool, token)
			if priceInAnchor <= 0 {
				continue
			}
			price := priceInAnchor * anchorPrice
			if price > 0 {
				if cache != nil {
					cache.Store(token, price)
				}
				return price
			}
		}
	}
	return 0.0
}

// getPriceFromPool computes the price of token in terms of the other token.
func getPriceFromPool(pool *types.PoolState, token common.Address) float64 {
	if pool == nil {
		return 0
	}
	r0, r1 := pool.GetReserves()
	if r0 <= 0 || r1 <= 0 {
		return 0
	}
	var reserveInFloat, reserveOutFloat float64
	var decIn, decOut int
	if token == pool.Token0 {
		reserveInFloat = r0
		reserveOutFloat = r1
		decIn = getTokenDecimals(pool.Token0)
		decOut = getTokenDecimals(pool.Token1)
	} else if token == pool.Token1 {
		reserveInFloat = r1
		reserveOutFloat = r0
		decIn = getTokenDecimals(pool.Token1)
		decOut = getTokenDecimals(pool.Token0)
	} else {
		return 0
	}
	if reserveInFloat <= 0 || reserveOutFloat <= 0 {
		return 0
	}
	factor := math.Pow10(decIn - decOut)
	price := (reserveOutFloat / reserveInFloat) * factor
	return price
}

// getTokenDecimals returns decimals for known tokens.
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

// float64FromBig converts a big.Int to float64 using a pooled big.Float.
func float64FromBig(v *big.Int) float64 {
	if v == nil {
		return 0
	}
	f := floatPool.Get().(*big.Float)
	f.SetInt(v)
	val, _ := f.Float64()
	floatPool.Put(f)
	return val
}
