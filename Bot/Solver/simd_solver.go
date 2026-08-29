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
	MemeAdmissionProfit = 0.50
)

type PriceEntry struct {
    Price     float64
    Timestamp int64
}

var decimalsCache = struct {
	sync.RWMutex
	m map[common.Address]uint8
}{m: make(map[common.Address]uint8)}

var v3CalcPool = sync.Pool{
	New: func() interface{} { return NewV3Calculator() },
}
var floatPool = sync.Pool{
	New: func() interface{} { return new(big.Float) },
}

var (
	currentL1BaseFeeUSD     atomic.Value
	currentL2BaseFeeUSD     atomic.Value
	currentL1GasCostPerByte atomic.Value
	ethPriceUSD             atomic.Value
)
var (
    priceLocksMu     sync.Mutex
    priceComputeLocks = make(map[common.Address]*sync.Mutex)
)

// getPriceLock returns a per‑token mutex, creating one if needed.
func getPriceLock(token common.Address) *sync.Mutex {
    priceLocksMu.Lock()
    defer priceLocksMu.Unlock()
    mu, ok := priceComputeLocks[token]
    if !ok {
        mu = &sync.Mutex{}
        priceComputeLocks[token] = mu
    }
    return mu
}
func init() {
	currentL1BaseFeeUSD.Store(0.0)
	currentL2BaseFeeUSD.Store(0.0)
	currentL1GasCostPerByte.Store(0.0)
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

func GetCurrentL1BaseFeeUSD() float64 {
	if v := currentL1BaseFeeUSD.Load(); v != nil {
		return v.(float64)
	}
	return 0.0
}

func GetCurrentL2BaseFeeUSD() float64 {
	if v := currentL2BaseFeeUSD.Load(); v != nil {
		return v.(float64)
	}
	return 0.0
}

func GetCurrentL1GasCostPerByte() float64 {
	if v := currentL1GasCostPerByte.Load(); v != nil {
		return v.(float64)
	}
	return 0.0
}

func FetchCurrentFees(client *ethclient.Client) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	l1Contract := common.HexToAddress("0x4200000000000000000000000000000000000015")
	l1Data := crypto.Keccak256([]byte("l1BaseFee()"))[:4]
	blobData := crypto.Keccak256([]byte("blobBaseFee()"))[:4]
	msgL1 := ethereum.CallMsg{To: &l1Contract, Data: l1Data}
	msgBlob := ethereum.CallMsg{To: &l1Contract, Data: blobData}

	l1Res, err1 := client.CallContract(ctx, msgL1, nil)
	blobRes, err2 := client.CallContract(ctx, msgBlob, nil)
	if err1 == nil && err2 == nil && len(l1Res) >= 32 && len(blobRes) >= 32 {
		l1BaseFeeWei := new(big.Int).SetBytes(l1Res)
		blobBaseFeeWei := new(big.Int).SetBytes(blobRes)
		ethPrice := GetEthPrice()
		l1TotalWei := new(big.Int).Mul(l1BaseFeeWei, big.NewInt(16))
		l1TotalWei.Add(l1TotalWei, blobBaseFeeWei)
		costPerByte := new(big.Float).Quo(
			new(big.Float).Mul(new(big.Float).SetInt(l1TotalWei), big.NewFloat(ethPrice)),
			big.NewFloat(1e18),
		)
		costPerByteFloat, _ := costPerByte.Float64()
		if costPerByteFloat > 0 {
			currentL1GasCostPerByte.Store(costPerByteFloat)
		}
		l1BaseFeeUSDVal := new(big.Float).Quo(
			new(big.Float).Mul(new(big.Float).SetInt(l1BaseFeeWei), big.NewFloat(ethPrice)),
			big.NewFloat(1e18),
		)
		l1BaseFeeUSD, _ := l1BaseFeeUSDVal.Float64()
		if l1BaseFeeUSD > 0 {
			currentL1BaseFeeUSD.Store(l1BaseFeeUSD)
		}
	}

	header, err := client.HeaderByNumber(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to get latest block header: %w", err)
	}
	l2BaseFeeWei := header.BaseFee
	if l2BaseFeeWei != nil {
		ethPrice := GetEthPrice()
		l2BaseFeeUSDVal := new(big.Float).Quo(
			new(big.Float).Mul(new(big.Float).SetInt(l2BaseFeeWei), big.NewFloat(ethPrice)),
			big.NewFloat(1e18),
		)
		l2BaseFeeUSD, _ := l2BaseFeeUSDVal.Float64()
		if l2BaseFeeUSD > 0 {
			currentL2BaseFeeUSD.Store(l2BaseFeeUSD)
		}
	}
	return nil
}

func StartL1FeeUpdater(client *ethclient.Client, ctx context.Context) {
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				_ = FetchCurrentFees(client)
			}
		}
	}()
}

func estimateGasForCandidate(cand *types.RouteCandidate) uint64 {
	base := uint64(150000)
	if cand.Hops == 2 {
		base += 150000
	} else {
		base += 250000
	}
	return uint64(float64(base) * 1.2)
}

func isStableV2Pool(pool *types.PoolState) bool {
	return pool.DexType == types.DexAerodromeV2 && pool.Stable
}

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
		enqueue(func() *types.RouteCandidate { return BuildRoundTrip2Hop(tokenA, tokenB, matrix, cfg, &priceCache) })
	} else if !isAnchorA && isAnchorB {
		enqueue(func() *types.RouteCandidate { return BuildRoundTrip2Hop(tokenB, tokenA, matrix, cfg, &priceCache) })
	} else {
		for _, anchor := range anchors {
			anchor := anchor
			enqueue(func() *types.RouteCandidate { return BuildRoundTrip3Hop(anchor, tokenA, tokenB, matrix, cfg, &priceCache) })
			enqueue(func() *types.RouteCandidate { return BuildRoundTrip3Hop(anchor, tokenB, tokenA, matrix, cfg, &priceCache) })
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

	l1CostPerByte := GetCurrentL1GasCostPerByte()
	baseCalldata := 2000

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

		baseProfit := cand.ExpectedProfitUSD
		if baseProfit <= 0 {
			cand.Score = 0
			continue
		}
		compPenalty := math.Exp(-0.7 * math.Max(0, routeComp))
		volatility := 0.5
		stats := matrix.GetPoolStats(cand.Pools[0])
		if stats != nil {
			volatility = stats.PriceVolatility
		}
		volatilityMultiplier := 1.0 + 0.4*(1.0/(1.0+math.Exp(-5.0*(volatility-0.5))))
		historyMultiplier := 1.0
		if stats != nil {
			total := stats.Win + stats.Fail
			if total >= 5 {
				winRate := float64(stats.Win) / float64(total)
				historyMultiplier = 0.85 + 0.30*winRate
			}
		}
		crowdPenalty := 1.0
		if stats != nil {
			swaps := float64(stats.SwapsWindow1m)
			crowdPenalty = 1.0 - 0.40/(1.0+math.Exp(-0.1*(swaps-50.0)))
		}
		hopPenalty := 1.0
		if cand.Hops == 3 {
			hopPenalty = 1.0 - HopPenalty3Hop
		}
		score := baseProfit * compPenalty * volatilityMultiplier * historyMultiplier * crowdPenalty * hopPenalty
		if math.IsNaN(score) || math.IsInf(score, 0) || score < 0 {
			score = 0
		}
		cand.Score = score
		cand.ExecutionSlippage = 0.5

		calldataBytes := baseCalldata + 500*int(cand.Hops)
		l1Cost := l1CostPerByte * float64(calldataBytes)
		minProfit := cfg.MinProfitUSD + l1Cost + 0.50
		if cand.ExpectedProfitUSD < minProfit {
			cand.Score = 0
		}
	}

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
	if fee <= 0 || fee >= 1 {
		return big.NewInt(0)
	}
	fReserveIn := new(big.Float).SetPrec(256).SetFloat64(reserveInFloat)
	fReserveOut := new(big.Float).SetPrec(256).SetFloat64(reserveOutFloat)
	fFee := new(big.Float).SetPrec(256).SetFloat64(fee)
	k := new(big.Float).Mul(fReserveIn, fReserveOut)
	sqrtK := new(big.Float).Sqrt(k)
	sqrtFee := new(big.Float).Sqrt(fFee)
	optimal := new(big.Float).Sub(new(big.Float).Quo(sqrtK, sqrtFee), fReserveIn)
	if optimal.Sign() <= 0 {
		return big.NewInt(0)
	}
	res := new(big.Int)
	optimal.Int(res)
	return res
}

func computeOptimalAmount2Hop(pool0, pool1 *types.PoolState, start common.Address) *big.Int {
	if pool0 == nil || pool1 == nil {
		return nil
	}
	if pool0.DexType == types.DexUniswapV3 || pool0.DexType == types.DexPancakeV3 ||
		pool1.DexType == types.DexUniswapV3 || pool1.DexType == types.DexPancakeV3 {
		return nil
	}
	if isStableV2Pool(pool0) || isStableV2Pool(pool1) {
		return nil
	}
	r0, _ := pool0.GetReserves()
	r1, _ := pool1.GetReserves()
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
	fR0 := new(big.Float).SetPrec(256).SetFloat64(reserveStart0)
	fR1 := new(big.Float).SetPrec(256).SetFloat64(reserveStart1)
	fFee0 := new(big.Float).SetPrec(256).SetFloat64(fee0)
	fFee1 := new(big.Float).SetPrec(256).SetFloat64(fee1)
	product := new(big.Float).Mul(
		new(big.Float).Mul(fR0, fR1),
		new(big.Float).Mul(fFee0, fFee1),
	)
	feeProduct := new(big.Float).Mul(fFee0, fFee1)
	denom := new(big.Float).Sub(big.NewFloat(1), feeProduct)
	if denom.Sign() <= 0 {
		return nil
	}
	ratio := new(big.Float).Quo(product, denom)
	sqrt := new(big.Float).Sqrt(ratio)
	optimal := new(big.Float).Sub(sqrt, fR0)
	if optimal.Sign() <= 0 {
		return nil
	}
	res := new(big.Int)
	optimal.Int(res)
	return res
}

func computeOptimalAmount3Hop(
	p0, p1, p2 *types.PoolState,
	start, mid1, mid2 common.Address,
	priceStart float64,
) *big.Int {
	if p0 == nil || p1 == nil || p2 == nil {
		return nil
	}
	if isStableV2Pool(p0) || isStableV2Pool(p1) || isStableV2Pool(p2) {
		return nil
	}
	base := computeOptimalInputRaw(p0, start)
	if base.Sign() <= 0 {
		return nil
	}
	baseF, _ := new(big.Float).SetInt(base).Float64()
	if baseF <= 0 {
		return nil
	}
	low := 0.0
	high := baseF * 10.0
	if high <= 0 {
		return nil
	}
	dec := getTokenDecimals(start)
	profitFn := func(amount float64) float64 {
		amt := new(big.Int).SetInt64(int64(amount))
		out0 := new(big.Int)
		if err := ComputeSwap(p0, start, mid1, amt, out0); err != nil || out0.Sign() <= 0 {
			return -1e9
		}
		out1 := new(big.Int)
		if err := ComputeSwap(p1, mid1, mid2, out0, out1); err != nil || out1.Sign() <= 0 {
			return -1e9
		}
		out2 := new(big.Int)
		if err := ComputeSwap(p2, mid2, start, out1, out2); err != nil || out2.Sign() <= 0 {
			return -1e9
		}
		net := new(big.Int).Sub(out2, amt)
		if net.Sign() <= 0 {
			return -1e9
		}
		netF := new(big.Float).SetInt(net)
		div := new(big.Float).SetFloat64(math.Pow10(dec))
		profitUSD, _ := new(big.Float).Mul(new(big.Float).Quo(netF, div), big.NewFloat(priceStart)).Float64()
		return profitUSD
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
	return new(big.Int).SetInt64(int64(opt))
}

func computeOptimalAmountV3(pool *types.PoolState, tokenIn, tokenOut common.Address, priceIn float64) *big.Int {
	if pool == nil {
		return nil
	}
	if pool.DexType != types.DexUniswapV3 && pool.DexType != types.DexPancakeV3 {
		return nil
	}
	base := computeOptimalInputRaw(pool, tokenIn)
	if base.Sign() <= 0 {
		return nil
	}
	baseF, _ := new(big.Float).SetInt(base).Float64()
	if baseF <= 0 {
		return nil
	}
	low := 0.0
	high := baseF * 10.0
	if high <= 0 {
		return nil
	}
	dec := getTokenDecimals(tokenIn)
	profitFn := func(amount float64) float64 {
		amt := new(big.Int).SetInt64(int64(amount))
		out := new(big.Int)
		if err := ComputeSwap(pool, tokenIn, tokenOut, amt, out); err != nil || out.Sign() <= 0 {
			return -1e9
		}
		net := new(big.Int).Sub(out, amt)
		if net.Sign() <= 0 {
			return -1e9
		}
		netF := new(big.Float).SetInt(net)
		div := new(big.Float).SetFloat64(math.Pow10(dec))
		profitUSD, _ := new(big.Float).Mul(new(big.Float).Quo(netF, div), big.NewFloat(priceIn)).Float64()
		return profitUSD
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
	return new(big.Int).SetInt64(int64(opt))
}

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
		if err := ComputeSwap(pool0, start, middle, &amountIn, &outMid); err != nil {
			continue
		}
		if err := ComputeSwap(pool1, middle, start, &outMid, &outStart); err != nil {
			continue
		}
		netWei.Sub(&outStart, &amountIn)
		if netWei.Sign() <= 0 {
			continue
		}
		priceStart, ok := GetTokenPrice(start, matrix, cache)
		if !ok || priceStart <= 0 {
			continue
		}
		decStart := getTokenDecimals(start)
		grossProfitUSD := (float64FromBig(&netWei) / math.Pow10(decStart)) * priceStart
		if grossProfitUSD < cfg.MinProfitUSD {
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
			FeeBps:            [3]uint32{pool0.FeeBps, pool1.FeeBps, 0},
			AmountIn:          new(big.Int).Set(&amountIn),
			ExpectedProfitUSD: grossProfitUSD,
			NetProfitWei:      new(big.Int).Set(&netWei),
			ExecutionSlippage: 0,
			Competition:       0,
		}
		if bestCand == nil || grossProfitUSD > bestProfit {
			bestCand = candidate
			bestProfit = grossProfitUSD
		}
		if prevProfit >= 0 && grossProfitUSD < prevProfit {
			break
		}
		prevProfit = grossProfitUSD
	}
	return bestCand
}

func BuildRoundTrip2Hop(start, middle common.Address, matrix *state.Matrix, cfg *config.Config, cache *sync.Map) *types.RouteCandidate {
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
			priceStart, ok := GetTokenPrice(start, matrix, cache)
			if !ok || priceStart <= 0 {
				continue
			}
			optimalAmount = computeOptimalAmountV3(fwdPool, start, middle, priceStart)
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

func BuildRoundTrip3Hop(start, mid1, mid2 common.Address, matrix *state.Matrix, cfg *config.Config, cache *sync.Map) *types.RouteCandidate {
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
	priceStart, ok := GetTokenPrice(start, matrix, cache)
	if !ok || priceStart <= 0 {
		return nil
	}

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
		if err := ComputeSwap(p0, start, mid1, &amountIn, &out0); err != nil {
			continue
		}
		if err := ComputeSwap(p1, mid1, mid2, &out0, &out1); err != nil {
			continue
		}
		if err := ComputeSwap(p2, mid2, start, &out1, &out2); err != nil {
			continue
		}
		netWei.Sub(&out2, &amountIn)
		if netWei.Sign() <= 0 {
			continue
		}
		priceStart, ok := GetTokenPrice(start, matrix, cache)
		if !ok || priceStart <= 0 {
			continue
		}
		decStart := getTokenDecimals(start)
		grossProfitUSD := (float64FromBig(&netWei) / math.Pow10(decStart)) * priceStart
		if grossProfitUSD < cfg.MinProfitUSD {
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
			FeeBps:            [3]uint32{p0.FeeBps, p1.FeeBps, p2.FeeBps},
			AmountIn:          new(big.Int).Set(&amountIn),
			ExpectedProfitUSD: grossProfitUSD,
			NetProfitWei:      new(big.Int).Set(&netWei),
			ExecutionSlippage: 0,
			Competition:       0,
		}
		if bestCand == nil || grossProfitUSD > bestProfit {
			bestCand = candidate
			bestProfit = grossProfitUSD
		}
	}
	return bestCand
}

func ComputeSwap(pool *types.PoolState, tokenIn, tokenOut common.Address, amountIn *big.Int, result *big.Int) error {
	if amountIn == nil || amountIn.Sign() <= 0 {
		result.SetInt64(0)
		return nil
	}
	if pool.DexType == types.DexUniswapV3 || pool.DexType == types.DexPancakeV3 {
		calc := v3CalcPool.Get().(*V3Calculator)
		defer v3CalcPool.Put(calc)
		return calc.ComputeSwap(pool, tokenIn, tokenOut, amountIn, result)
	}
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

// GetTokenPrice returns the USD price of a token and a boolean indicating success.
// The price cache entries expire after 1 second.
// Uses a per‑token mutex to prevent duplicate computation.
func GetTokenPrice(token common.Address, matrix *state.Matrix, cache *sync.Map) (float64, bool) {
    now := time.Now().UnixNano()

    // Fast path: read from cache (read‑only, no lock needed)
    if cache != nil {
        if v, ok := cache.Load(token); ok {
            if entry, ok := v.(*PriceEntry); ok {
                if now-entry.Timestamp < int64(time.Second) {
                    return entry.Price, true
                }
            }
        }
    }

    // Acquire per‑token lock to prevent duplicate computation
    mu := getPriceLock(token)
    mu.Lock()
    defer mu.Unlock()

    // Double‑check after acquiring lock
    if cache != nil {
        if v, ok := cache.Load(token); ok {
            if entry, ok := v.(*PriceEntry); ok {
                if now-entry.Timestamp < int64(time.Second) {
                    return entry.Price, true
                }
            }
        }
    }

    // ---- Compute price (unchanged logic) ----
    var price float64
    if token == config.USDCAddress || token == config.USDBCAddress {
        price = 1.0
    } else if token == config.WETHAddress {
        pools := matrix.GetPoolsForPair(config.WETHAddress, config.USDCAddress)
        if len(pools) > 0 {
            pool := pools[0]
            if pool.GetLiquidity() > 0 {
                price = getPriceFromPool(pool, token)
            }
        }
    } else if token == config.CBBTCAddress {
        pools := matrix.GetPoolsForPair(config.CBBTCAddress, config.USDCAddress)
        if len(pools) > 0 {
            pool := pools[0]
            if pool.GetLiquidity() > 0 {
                price = getPriceFromPool(pool, token)
            }
        }
    } else {
        anchors := config.AnchorAssets()
        for _, anchor := range anchors {
            if anchor == token {
                continue
            }
            pools := matrix.GetPoolsForPair(token, anchor)
            if len(pools) > 0 {
                pool := pools[0]
                anchorPrice, ok := GetTokenPrice(anchor, matrix, cache)
                if !ok || anchorPrice <= 0 {
                    continue
                }
                priceInAnchor := getPriceFromPool(pool, token)
                if priceInAnchor <= 0 {
                    continue
                }
                price = priceInAnchor * anchorPrice
                break
            }
        }
    }

    if price > 0 {
        if cache != nil {
            cache.Store(token, &PriceEntry{Price: price, Timestamp: now})
        }
        return price, true
    }
    return 0, false
}
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

func GetTokenDecimals(token common.Address) int {
	decimalsCache.RLock()
	if d, ok := decimalsCache.m[token]; ok {
		decimalsCache.RUnlock()
		return int(d)
	}
	decimalsCache.RUnlock()
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

func SetTokenDecimals(token common.Address, decimals uint8) {
	decimalsCache.Lock()
	defer decimalsCache.Unlock()
	decimalsCache.m[token] = decimals
}

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

// getTokenDecimals (local version – kept for internal use; prefer GetTokenDecimals)
func getTokenDecimals(token common.Address) int {
	return GetTokenDecimals(token)
}
