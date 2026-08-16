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
	MaxSlippagePct   = 3.0
	MaxHops          = 3
	SlippageBuffer   = 0.99
	HopPenalty3Hop   = 0.05
	MinScore         = 0.01
	MemeAdmissionProfit = 0.50 // USD gross – used by gatekeeper for meme qualification
)
// v3CalcPool hands each goroutine its own calculator buffers.
var v3CalcPool = sync.Pool{
	New: func() interface{} {
		return NewV3Calculator()
	},
}

// floatPool reuses big.Float for conversions, avoiding allocations.
var floatPool = sync.Pool{
	New: func() interface{} {
		return new(big.Float)
	},
}

var (
	currentGasCostUSDPerGas atomic.Value // float64
	currentL1BaseFeeUSD     atomic.Value // float64
	ethPriceUSD             atomic.Value // float64
)

func init() {
	currentGasCostUSDPerGas.Store(0.00000000005)
	currentL1BaseFeeUSD.Store(0.00000000001)
	ethPriceUSD.Store(3000.0)
}

// SetEthPrice sets the ETH/USD price for gas cost calculations.
func SetEthPrice(price float64) {
	if price > 0 {
		ethPriceUSD.Store(price)
	}
}

// GetEthPrice returns the current ETH price in USD.
func GetEthPrice() float64 {
	if v := ethPriceUSD.Load(); v != nil {
		return v.(float64)
	}
	return 3000.0
}

// StartL1FeeUpdater launches a background goroutine that fetches the L1 base fee.
func StartL1FeeUpdater(client *ethclient.Client, ctx context.Context) {
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				contract := common.HexToAddress("0x4200000000000000000000000000000000000015")
				data := crypto.Keccak256([]byte("basefee()"))[:4]
				msg := ethereum.CallMsg{
					To:   &contract,
					Data: data,
				}
				ctx2, cancel := context.WithTimeout(ctx, 1*time.Second)
				result, err := client.CallContract(ctx2, msg, nil)
				cancel()
				if err != nil {
					continue
				}
				l1BaseFee := new(big.Int).SetBytes(result)
				// Convert to USD using live ETH price.
				l1BaseFeeWei, _ := new(big.Float).SetInt(l1BaseFee).Float64()
				ethPrice := GetEthPrice()
				l1BaseFeeUSDPerGas := l1BaseFeeWei * ethPrice / 1e18
				if l1BaseFeeUSDPerGas > 0 {
					currentL1BaseFeeUSD.Store(l1BaseFeeUSDPerGas)
				}
				l2GasCostUSD := 0.5 * ethPrice / 1e9
				totalGasCostUSD := l1BaseFeeUSDPerGas + l2GasCostUSD
				if totalGasCostUSD > 0 {
					currentGasCostUSDPerGas.Store(totalGasCostUSD)
				}
			}
		}
	}()
}

// GetCurrentL1BaseFeeUSD returns the current L1 base fee in USD per gas unit.
func GetCurrentL1BaseFeeUSD() float64 {
	if v := currentL1BaseFeeUSD.Load(); v != nil {
		return v.(float64)
	}
	return 0.00000000001
}

// GetCurrentGasCostUSD returns the current estimated gas cost in USD per gas unit.
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

// getBestPool returns the pool with the highest liquidity.
func getBestPool(pools []*types.PoolState) *types.PoolState {
	if len(pools) == 0 {
		return nil
	}
	best := pools[0]
	bestScore := best.GetReservesProduct()
	for _, p := range pools[1:] {
		score := p.GetReservesProduct()
		if score > bestScore {
			best = p
			bestScore = score
		}
	}
	return best
}

// getTopTwoPools returns the two pools with the highest liquidity.
func getTopTwoPools(pools []*types.PoolState) (*types.PoolState, *types.PoolState) {
	if len(pools) < 2 {
		return nil, nil
	}
	sorted := make([]*types.PoolState, len(pools))
	copy(sorted, pools)
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
var taskCh = make(chan func(), 100) // buffered to avoid blocking

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

// EvaluateEvent generates and evaluates candidate routes, ranking them by competition‑adjusted score.
// It now uses a worker pool to evaluate routes in parallel.
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

	// Create a per‑event price cache.
	var priceCache sync.Map

	// Result channel for this evaluation.
	resultCh := make(chan *types.RouteCandidate, 10)
	var wg sync.WaitGroup

	// Helper to enqueue a task.
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
			anchor := anchor // capture
			enqueue(func() *types.RouteCandidate { return buildRoundTrip3Hop(anchor, tokenA, tokenB, matrix, cfg, &priceCache) })
			enqueue(func() *types.RouteCandidate { return buildRoundTrip3Hop(anchor, tokenB, tokenA, matrix, cfg, &priceCache) })
		}
	}

	// Close result channel when all tasks are done.
	go func() {
		wg.Wait()
		close(resultCh)
	}()

	// Collect candidates.
	candidates := make([]*types.RouteCandidate, 0)
	for cand := range resultCh {
		candidates = append(candidates, cand)
	}

	// Compute competition score and sort.
	for _, cand := range candidates {
		routeComp := 0.0
		for i := 0; i < int(cand.Hops); i++ {
			poolAddr := cand.Pools[i]
			poolScore := matrix.GetPoolScore(poolAddr)
			if poolScore > routeComp {
				routeComp = poolScore
			}
		}
		cand.Competition = routeComp

		hopPenalty := 0.0
		if cand.Hops == 3 {
			hopPenalty = HopPenalty3Hop
		}
		score := cand.ExpectedProfitUSD * (1 - routeComp) * (1 - hopPenalty)
		if score < MinScore {
			score = 0
		}
		cand.Score = score
		cand.ExecutionSlippage = 0.5
	}

	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].Score > candidates[j].Score
	})

	return candidates
}

// tryMultipliers tries a range of input multipliers and returns the candidate with the highest net profit.
// All big.Int buffers are declared on the stack and reused.
func tryMultipliers(
	start, middle, end common.Address,
	baseAmount *big.Int,
	pool0, pool1 *types.PoolState,
	forwardZeroForOne, reverseZeroForOne bool,
	matrix *state.Matrix,
	cfg *config.Config,
	cache *sync.Map,
) *types.RouteCandidate {
	multipliers := []float64{0.5, 0.75, 1.0, 1.25, 1.5}
	bestCand := (*types.RouteCandidate)(nil)
	bestProfit := 0.0

	// Stack-allocated buffers reused across multipliers.
	var amountIn, outMid, outStart, netWei, minOut0, minOut1 big.Int

	for _, mul := range multipliers {
		amountIn.Set(baseAmount)
		// Multiply by mul (as integer scaling: mul*100 / 100)
		scaled := int64(mul * 100)
		amountIn.Mul(&amountIn, big.NewInt(scaled))
		amountIn.Div(&amountIn, big.NewInt(100))

		if amountIn.Sign() <= 0 {
			continue
		}

		// First hop
		if err := computeSwap(pool0, start, middle, &amountIn, &outMid); err != nil {
			continue
		}
		// Second hop
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

		// Apply slippage buffer using pooled big.Float? We'll reuse a simple function.
		applySlippageBuffer(&outMid, &minOut0)
		applySlippageBuffer(&outStart, &minOut1)

		// Create candidate – we still allocate a new candidate and its big.Int fields,
		// but these are few and acceptable. For truly zero-allocation, we'd pool candidates,
		// but that complicates ownership. We'll keep this allocation.
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
	}
	return bestCand
}

// buildRoundTrip2Hop builds a 2‑hop round‑trip using two different pools.
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
	combinations := [][2]*types.PoolState{
		{pool0, pool1},
		{pool1, pool0},
	}
	for _, pair := range combinations {
		fwdPool := pair[0]
		revPool := pair[1]

		forwardZeroForOne := fwdPool.Token0 == start
		reverseZeroForOne := revPool.Token0 == middle

		baseAmount := computeOptimalInputRaw(fwdPool, start)
		if baseAmount.Sign() <= 0 {
			continue
		}

		cand := tryMultipliers(start, middle, start, baseAmount, fwdPool, revPool,
			forwardZeroForOne, reverseZeroForOne, matrix, cfg, cache)
		if cand != nil {
			if bestCand == nil || cand.ExpectedProfitUSD > bestCand.ExpectedProfitUSD {
				bestCand = cand
			}
		}
	}
	return bestCand
}

// buildRoundTrip3Hop builds a 3‑hop round‑trip: start -> mid1 -> mid2 -> start.
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

	zeroForOne0 := p0.Token0 == start
	zeroForOne1 := p1.Token0 == mid1
	zeroForOne2 := p2.Token0 == mid2

	baseAmount := computeOptimalInputRaw(p0, start)
	if baseAmount.Sign() <= 0 {
		return nil
	}

	multipliers := []float64{0.5, 0.75, 1.0, 1.25, 1.5}
	bestCand := (*types.RouteCandidate)(nil)
	bestProfit := 0.0

	// Stack-allocated buffers reused.
	var amountIn, out0, out1, out2, netWei, minOut0, minOut1, minOut2 big.Int

	for _, mul := range multipliers {
		amountIn.Set(baseAmount)
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
	// Use pooled big.Float for conversion.
	f := floatPool.Get().(*big.Float)
	f.SetFloat64(amountInFloat)
	res := new(big.Int)
	f.Int(res)
	floatPool.Put(f)
	return res
}

// computeSwap calculates the output amount for a swap.
// V2 path uses float64 arithmetic with pooled big.Float to avoid allocations.
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

	// V2 style: constant product formula using float64.
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

	// Convert amountIn to float64 (using Int.Float64, which is allocation-free).
	amountInF, _ := amountIn.Float64()
	amountInWithFee := amountInF * fee
	numerator := reserveOutFloat * amountInWithFee
	denominator := reserveInFloat + amountInWithFee
	amountOutF := numerator / denominator

	// Convert back to big.Int using pooled big.Float.
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
// If cache is non‑nil, it will store and retrieve prices to avoid recomputation.
func GetTokenPrice(token common.Address, matrix *state.Matrix, cache *sync.Map) float64 {
	// Check cache first.
	if cache != nil {
		if v, ok := cache.Load(token); ok {
			return v.(float64)
		}
	}

	// Stablecoins
	if token == config.USDCAddress || token == config.USDBCAddress {
		price := 1.0
		if cache != nil {
			cache.Store(token, price)
		}
		return price
	}
	// WETH
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
	// cbBTC
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
	// Other tokens: try anchors.
	anchors := config.AnchorAssets()
	for _, anchor := range anchors {
		if anchor == token {
			continue
		}
		pools := matrix.GetPoolsForPair(token, anchor)
		if len(pools) > 0 {
			pool := pools[0]
			anchorPrice := GetTokenPrice(anchor, matrix, cache) // pass cache recursively
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
