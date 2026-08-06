package solver

import (
	"context"
	"fmt"
	"math"
	"math/big"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"

	"my-mev-bot/config"
	"my-mev-bot/state"
	"my-mev-bot/types"
)

const (
	MaxSlippagePct   = 3.0
	MaxHops          = 3
	SlippageBuffer   = 0.99  // 1% buffer (minOut = actualOut * 0.99) – increased for volatile pools
	HopPenalty3Hop   = 0.05
	MinScore         = 0.01
)

// v3CalcPool hands each goroutine its own calculator buffers.
var v3CalcPool = sync.Pool{
	New: func() interface{} {
		return NewV3Calculator()
	},
}

var (
	// currentGasCostUSDPerGas is the combined L1 + L2 per‑gas cost in USD (used by solver's rough estimate).
	currentGasCostUSDPerGas atomic.Value // float64
	// currentL1BaseFeeUSD is the L1 base fee only, in USD per gas (used for accurate per‑tx calculation).
	currentL1BaseFeeUSD atomic.Value // float64
	// ethPriceUSD is the current ETH price in USD, updated by background goroutine or config.
	ethPriceUSD atomic.Value // float64
)

func init() {
	currentGasCostUSDPerGas.Store(0.00000000005) // fallback ~50 gwei equivalent
	currentL1BaseFeeUSD.Store(0.00000000001)     // fallback 0.01 gwei
	ethPriceUSD.Store(3000.0)                    // fallback
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

// StartL1FeeUpdater launches a background goroutine that fetches the L1 base fee
// from the Base L1BlockOracle and updates the global gas cost estimate.
// It should be called once from main.
func StartL1FeeUpdater(client *ethclient.Client, ctx context.Context) {
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				// L1Block contract on Base: 0x4200000000000000000000000000000000000015
				// function basefee() returns uint256.
				contract := common.HexToAddress("0x4200000000000000000000000000000000000015")
				// Use 4-byte selector for basefee()
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
				// Convert to USD using live ETH price, avoiding Int64 overflow.
				l1BaseFeeWei, _ := new(big.Float).SetInt(l1BaseFee).Float64()
				ethPrice := GetEthPrice()
				l1BaseFeeUSDPerGas := l1BaseFeeWei * ethPrice / 1e18
				if l1BaseFeeUSDPerGas > 0 {
					currentL1BaseFeeUSD.Store(l1BaseFeeUSDPerGas)
				}
				// Combined cost: L1 base fee + 0.5 gwei tip (kept for solver's rough estimate)
				// Note: L2 execution cost is computed separately using gasUsed and priority fee.
				// This value is only used as a rough estimate in the solver's early filtering.
				l2GasCostUSD := 0.5 * ethPrice / 1e9 // 0.5 gwei in USD
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
	return 0.00000000001 // fallback
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
	base := uint64(150000) // flash loan overhead
	if cand.Hops == 2 {
		base += 150000
	} else {
		base += 250000
	}
	return uint64(float64(base) * 1.2) // 20% buffer
}

// getBestPool returns the pool with the highest liquidity (product of reserves) from a list.
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

// getTopTwoPools returns the two pools with the highest liquidity, without mutating the input slice.
func getTopTwoPools(pools []*types.PoolState) (*types.PoolState, *types.PoolState) {
	if len(pools) < 2 {
		return nil, nil
	}
	// Copy the slice to avoid reordering the matrix-owned backing array.
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

// EvaluateEvent generates and evaluates candidate routes, ranking them by competition‑adjusted score.
// It now produces round‑trip routes: start token == end token (the loan token).
func EvaluateEvent(log *types.SwapLog, matrix *state.Matrix, cfg *config.Config) []*types.RouteCandidate {
	if log == nil || matrix == nil {
		return nil
	}
	// If TokenIn/TokenOut are still zero, the event is unusable – do not guess.
	if log.TokenIn == (common.Address{}) || log.TokenOut == (common.Address{}) {
		// This should not happen after UpdateFromLog. Return nil to avoid mispricing.
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
	var candidates []*types.RouteCandidate

	if isAnchorA && isAnchorB {
		// both anchors – skip direct (no opportunity)
	} else if isAnchorA && !isAnchorB {
		// anchor -> meme -> anchor (round-trip)
		cand := buildRoundTrip2Hop(tokenA, tokenB, matrix, cfg)
		if cand != nil && cand.ExpectedProfitUSD > cfg.MinProfitUSD {
			candidates = append(candidates, cand)
		}
	} else if !isAnchorA && isAnchorB {
		cand := buildRoundTrip2Hop(tokenB, tokenA, matrix, cfg)
		if cand != nil && cand.ExpectedProfitUSD > cfg.MinProfitUSD {
			candidates = append(candidates, cand)
		}
	} else {
		// both memes – try 3‑hop round‑trips with each anchor
		for _, anchor := range anchors {
			cand := buildRoundTrip3Hop(anchor, tokenA, tokenB, matrix, cfg)
			if cand != nil && cand.ExpectedProfitUSD > cfg.MinProfitUSD {
				candidates = append(candidates, cand)
			}
			cand2 := buildRoundTrip3Hop(anchor, tokenB, tokenA, matrix, cfg)
			if cand2 != nil && cand2.ExpectedProfitUSD > cfg.MinProfitUSD {
				candidates = append(candidates, cand2)
			}
		}
	}

	// Compute competition score for each candidate and sort.
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

		cand.ExecutionSlippage = 0.5 // placeholder
	}

	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].Score > candidates[j].Score
	})

	return candidates
}

// tryMultipliers tries a range of input multipliers around the base optimal amount
// and returns the candidate with the highest net profit.
func tryMultipliers(
	start, middle, end common.Address,
	baseAmount *big.Int,
	pool0, pool1 *types.PoolState,
	forwardZeroForOne, reverseZeroForOne bool,
	matrix *state.Matrix,
	cfg *config.Config,
) *types.RouteCandidate {
	multipliers := []float64{0.5, 0.75, 1.0, 1.25, 1.5}
	bestCand := (*types.RouteCandidate)(nil)
	bestProfit := 0.0

	// Pre‑allocate result buffers for both hops to reuse them across multipliers.
	outMid := new(big.Int)
	outStart := new(big.Int)

	for _, mul := range multipliers {
		amountIn := new(big.Int).Set(baseAmount)
		amountIn = amountIn.Mul(amountIn, big.NewInt(int64(mul*100))) // mul is float, so scale by 100.
		amountIn.Div(amountIn, big.NewInt(100))

		if amountIn.Sign() <= 0 {
			continue
		}

		// First hop: start -> middle
		if err := computeSwap(pool0, start, middle, amountIn, outMid); err != nil {
			continue
		}
		// Second hop: middle -> start (reverse)
		if err := computeSwap(pool1, middle, start, outMid, outStart); err != nil {
			continue
		}
		netWei := new(big.Int).Sub(outStart, amountIn)
		if netWei.Sign() <= 0 {
			continue
		}
		priceStart := GetTokenPrice(start, matrix)
		if priceStart <= 0 {
			continue
		}
		decStart := getTokenDecimals(start)
		grossProfitUSD := (float64FromBig(netWei) / math.Pow10(decStart)) * priceStart

		// Subtract gas cost (L2 execution cost only, L1 fee is approximate and not included here).
		// The actual gas cost in USD is computed later in main.go using the actual gasUsed and priority fee.
		// Here we use a rough estimate to filter candidates.
		cand := &types.RouteCandidate{Hops: 2}
		gasCostUSD := GetCurrentGasCostUSD() * float64(estimateGasForCandidate(cand))
		profitUSD := grossProfitUSD - gasCostUSD
		if profitUSD < cfg.MinProfitUSD {
			continue
		}

		minOut0 := applySlippageBuffer(outMid)
		minOut1 := applySlippageBuffer(outStart)

		candidate := &types.RouteCandidate{
			Hops:              2,
			Tokens:            [4]common.Address{start, middle, start, {}},
			Pools:             [3]common.Address{pool0.PoolAddress, pool1.PoolAddress, {}},
			DexTypes:          [3]types.DexType{pool0.DexType, pool1.DexType, 0},
			ZeroForOnes:       [3]bool{forwardZeroForOne, reverseZeroForOne, false},
			MinOuts:           [3]*big.Int{minOut0, minOut1, new(big.Int)},
			AmountIn:          new(big.Int).Set(amountIn),
			ExpectedProfitUSD: profitUSD,
			NetProfitWei:      new(big.Int).Set(netWei),
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
// start -> middle (pool0), middle -> start (pool1).
func buildRoundTrip2Hop(start, middle common.Address, matrix *state.Matrix, cfg *config.Config) *types.RouteCandidate {
	if start == middle {
		return nil
	}
	pools := matrix.GetPoolsForPair(start, middle)
	if len(pools) < 2 {
		// Need at least two pools for true arbitrage; otherwise skip.
		return nil
	}

	// Get two most liquid pools.
	pool0, pool1 := getTopTwoPools(pools)
	if pool0 == nil || pool1 == nil || pool0.PoolAddress == pool1.PoolAddress {
		return nil
	}

	// Try both directions: pool0 for forward, pool1 for reverse, and also the opposite.
	// We'll try both combinations and pick the best.
	var bestCand *types.RouteCandidate
	combinations := [][2]*types.PoolState{
		{pool0, pool1},
		{pool1, pool0},
	}
	for _, pair := range combinations {
		fwdPool := pair[0]
		revPool := pair[1]

		// Forward direction: start -> middle.
		forwardZeroForOne := fwdPool.Token0 == start
		// Reverse direction: middle -> start.
		reverseZeroForOne := revPool.Token0 == middle

		// Compute base optimal input (raw units, no decimal scaling).
		baseAmount := computeOptimalInputRaw(fwdPool, start)
		if baseAmount.Sign() <= 0 {
			continue
		}

		// Try a range of multipliers to find the best size for the full route.
		cand := tryMultipliers(start, middle, start, baseAmount, fwdPool, revPool,
			forwardZeroForOne, reverseZeroForOne, matrix, cfg)
		if cand != nil {
			if bestCand == nil || cand.ExpectedProfitUSD > bestCand.ExpectedProfitUSD {
				bestCand = cand
			}
		}
	}
	return bestCand
}

// buildRoundTrip3Hop builds a 3‑hop round‑trip: start -> mid1 -> mid2 -> start.
// It uses the best pool for each pair.
func buildRoundTrip3Hop(start, mid1, mid2 common.Address, matrix *state.Matrix, cfg *config.Config) *types.RouteCandidate {
	if start == mid1 || start == mid2 || mid1 == mid2 {
		return nil
	}
	pools01 := matrix.GetPoolsForPair(start, mid1)
	pools12 := matrix.GetPoolsForPair(mid1, mid2)
	pools20 := matrix.GetPoolsForPair(mid2, start)
	if len(pools01) == 0 || len(pools12) == 0 || len(pools20) == 0 {
		return nil
	}
	// Use the best pool for each pair to reduce combinatorial explosion.
	p0 := getBestPool(pools01)
	p1 := getBestPool(pools12)
	p2 := getBestPool(pools20)
	if p0 == nil || p1 == nil || p2 == nil {
		return nil
	}

	// Determine directions for each hop.
	zeroForOne0 := p0.Token0 == start
	zeroForOne1 := p1.Token0 == mid1
	zeroForOne2 := p2.Token0 == mid2

	// Compute base optimal input (raw units, no scaling).
	baseAmount := computeOptimalInputRaw(p0, start)
	if baseAmount.Sign() <= 0 {
		return nil
	}

	// Try multipliers.
	multipliers := []float64{0.5, 0.75, 1.0, 1.25, 1.5}
	bestCand := (*types.RouteCandidate)(nil)
	bestProfit := 0.0

	// Pre‑allocate output buffers for the three hops (reused across multipliers).
	out0 := new(big.Int)
	out1 := new(big.Int)
	out2 := new(big.Int)

	for _, mul := range multipliers {
		amountIn := new(big.Int).Set(baseAmount)
		amountIn = amountIn.Mul(amountIn, big.NewInt(int64(mul*100)))
		amountIn.Div(amountIn, big.NewInt(100))
		if amountIn.Sign() <= 0 {
			continue
		}

		// Execute the three swaps using the new computeSwap signature.
		if err := computeSwap(p0, start, mid1, amountIn, out0); err != nil {
			continue
		}
		if err := computeSwap(p1, mid1, mid2, out0, out1); err != nil {
			continue
		}
		if err := computeSwap(p2, mid2, start, out1, out2); err != nil {
			continue
		}

		netWei := new(big.Int).Sub(out2, amountIn)
		if netWei.Sign() <= 0 {
			continue
		}
		priceStart := GetTokenPrice(start, matrix)
		if priceStart <= 0 {
			continue
		}
		decStart := getTokenDecimals(start)
		grossProfitUSD := (float64FromBig(netWei) / math.Pow10(decStart)) * priceStart

		// Estimate gas cost for 3‑hop candidate.
		cand := &types.RouteCandidate{Hops: 3}
		gasCostUSD := GetCurrentGasCostUSD() * float64(estimateGasForCandidate(cand))
		profitUSD := grossProfitUSD - gasCostUSD
		if profitUSD < cfg.MinProfitUSD {
			continue
		}

		minOut0 := applySlippageBuffer(out0)
		minOut1 := applySlippageBuffer(out1)
		minOut2 := applySlippageBuffer(out2)

		candidate := &types.RouteCandidate{
			Hops:              3,
			Tokens:            [4]common.Address{start, mid1, mid2, start},
			Pools:             [3]common.Address{p0.PoolAddress, p1.PoolAddress, p2.PoolAddress},
			DexTypes:          [3]types.DexType{p0.DexType, p1.DexType, p2.DexType},
			ZeroForOnes:       [3]bool{zeroForOne0, zeroForOne1, zeroForOne2},
			MinOuts:           [3]*big.Int{minOut0, minOut1, minOut2},
			AmountIn:          new(big.Int).Set(amountIn),
			ExpectedProfitUSD: profitUSD,
			NetProfitWei:      new(big.Int).Set(netWei),
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

// computeOptimalInputRaw computes the optimal single-hop input in raw units (no decimal scaling).
// It uses the classic formula sqrt(k/fee) - reserveIn.
func computeOptimalInputRaw(pool *types.PoolState, tokenIn common.Address) *big.Int {
	if pool == nil {
		return big.NewInt(0)
	}
	r0, r1 := pool.GetReserves()
	if r0 <= 0 || r1 <= 0 {
		return big.NewInt(0)
	}
	// Determine which reserve is input.
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
	// Convert float to big.Int (raw units, no decimal scaling).
	amountIn := new(big.Int)
	new(big.Float).SetFloat64(amountInFloat).Int(amountIn)
	return amountIn
}

// computeSwap calculates the output amount for a swap.
// It writes the result into the provided `result` *big.Int.
// Returns an error on failure.
func computeSwap(pool *types.PoolState, tokenIn, tokenOut common.Address, amountIn *big.Int, result *big.Int) error {
	if amountIn == nil || amountIn.Sign() <= 0 {
		result.SetInt64(0)
		return nil
	}

	// For V3 pools, use exact math with zero allocations.
	if pool.DexType == types.DexUniswapV3 || pool.DexType == types.DexPancakeV3 {
		calc := v3CalcPool.Get().(*V3Calculator)
		defer v3CalcPool.Put(calc)
		return calc.ComputeSwap(pool, tokenIn, tokenOut, amountIn, result)
	}

	// V2 style: constant product formula (uses float for simplicity).
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
	amountInFloat := new(big.Float).SetInt(amountIn)
	amountInWithFee := new(big.Float).Mul(amountInFloat, big.NewFloat(fee))
	numerator := new(big.Float).Mul(big.NewFloat(reserveOutFloat), amountInWithFee)
	denominator := new(big.Float).Add(big.NewFloat(reserveInFloat), amountInWithFee)
	amountOutFloat := new(big.Float).Quo(numerator, denominator)
	amountOutFloat.Int(result)
	return nil
}

// getFeeFactor returns the fee multiplier for a pool.
// For V3, it computes 1 - feeBps/10000 (since feeBps is in basis points).
// For V2, it uses pool.FeeBps if set, otherwise defaults to 0.3%.
func getFeeFactor(pool *types.PoolState) float64 {
	feeBps := pool.FeeBps
	if feeBps == 0 {
		feeBps = 30 // default 0.3%
	}
	// Clamp to valid range.
	if feeBps >= 10000 {
		feeBps = 9999
	}
	// fee factor = 1 - (feeBps / 10000)
	return 1.0 - float64(feeBps)/10000.0
}

func applySlippageBuffer(amount *big.Int) *big.Int {
	if amount == nil || amount.Sign() <= 0 {
		return new(big.Int)
	}
	f := new(big.Float).SetInt(amount)
	f.Mul(f, big.NewFloat(SlippageBuffer))
	res := new(big.Int)
	f.Int(res)
	return res
}

// computeProfit is kept for compatibility but no longer used in route building;
// we compute net wei directly. We keep it for potential logging.
func computeProfit(startToken, endToken common.Address, amountIn, amountOut *big.Int, matrix *state.Matrix, cfg *config.Config) float64 {
	return 0
}

// GetTokenPrice returns USD price using the matrix.
// For WETH, cbBTC it uses the USDC pools; for stablecoins returns 1.0;
// for others, tries to find a pool with an anchor to compute price.
// All price calculations are scaled by the correct token decimals.
// Returns 0.0 if no reliable price source exists.
func GetTokenPrice(token common.Address, matrix *state.Matrix) float64 {
	// Stablecoins
	if token == config.USDCAddress || token == config.USDBCAddress {
		return 1.0
	}
	// WETH
	if token == config.WETHAddress {
		pools := matrix.GetPoolsForPair(config.WETHAddress, config.USDCAddress)
		if len(pools) == 0 {
			return 0.0
		}
		pool := pools[0]
		if pool.GetLiquidity() > 0 {
			return getPriceFromPool(pool, token)
		}
		return 0.0
	}
	// cbBTC
	if token == config.CBBTCAddress {
		pools := matrix.GetPoolsForPair(config.CBBTCAddress, config.USDCAddress)
		if len(pools) == 0 {
			return 0.0
		}
		pool := pools[0]
		if pool.GetLiquidity() > 0 {
			return getPriceFromPool(pool, token)
		}
		return 0.0
	}
	// For any other token, try to find a pool with an anchor (USDC, WETH, etc.)
	anchors := config.AnchorAssets()
	for _, anchor := range anchors {
		if anchor == token {
			continue
		}
		pools := matrix.GetPoolsForPair(token, anchor)
		if len(pools) > 0 {
			pool := pools[0]
			anchorPrice := GetTokenPrice(anchor, matrix)
			if anchorPrice <= 0 {
				continue
			}
			priceInAnchor := getPriceFromPool(pool, token)
			if priceInAnchor <= 0 {
				continue
			}
			return priceInAnchor * anchorPrice
		}
	}
	// No price source found – do not guess $1.00.
	return 0.0
}

// getPriceFromPool computes the price of token (one of the pool's tokens) in terms of the other token.
// The price is returned as "price of token in terms of the other token", scaled by decimals.
// For example, if pool is WETH/USDC and token is WETH, it returns USDC per WETH.
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

func getZeroForOne(pool *types.PoolState, tokenIn, tokenOut common.Address) bool {
	if pool.Token0 == tokenIn && pool.Token1 == tokenOut {
		return true
	}
	if pool.Token0 == tokenOut && pool.Token1 == tokenIn {
		return false
	}
	return false
}

// float64FromBig converts a big.Int to float64 (approximation).
func float64FromBig(v *big.Int) float64 {
	if v == nil {
		return 0
	}
	f, _ := new(big.Float).SetInt(v).Float64()
	return f
}