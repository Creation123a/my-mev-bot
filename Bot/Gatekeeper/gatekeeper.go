package gatekeeper

import (
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"math"
	"math/big"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"

	"my-mev-bot/Bot/Config"
	"my-mev-bot/Bot/Execution"
	"my-mev-bot/Bot/Solver"
	"my-mev-bot/Bot/State"
	"my-mev-bot/Bot/Types"
)

var v3PoolABI *abi.ABI
var ticksABI *abi.ABI

func init() {
	parsed, err := abi.JSON(strings.NewReader(`[{"inputs":[{"internalType":"int16","name":"wordPosition","type":"int16"}],"name":"tickBitmap","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"}]`))
	if err != nil {
		panic(fmt.Sprintf("failed to parse V3 tickBitmap ABI: %v", err))
	}
	v3PoolABI = &parsed

	parsedTicks, err := abi.JSON(strings.NewReader(`[{"inputs":[{"internalType":"int24","name":"tick","type":"int24"}],"name":"ticks","outputs":[{"internalType":"uint128","name":"liquidityGross","type":"uint128"},{"internalType":"int128","name":"liquidityNet","type":"int128"},{"internalType":"uint256","name":"feeGrowthOutside0X128","type":"uint256"},{"internalType":"uint256","name":"feeGrowthOutside1X128","type":"uint256"},{"internalType":"int56","name":"tickCumulativeOutside","type":"int56"},{"internalType":"uint160","name":"secondsPerLiquidityOutsideX128","type":"uint160"},{"internalType":"uint32","name":"secondsOutside","type":"uint32"}],"stateMutability":"view","type":"function"}]`))
	if err != nil {
		panic(err)
	}
	ticksABI = &parsedTicks
}

const multicallAddress = "0xca11bde05977b3631167028862be2a173976ca11"

type DiscoveryCandidate struct {
	PoolAddress  common.Address
	Token0       common.Address
	Token1       common.Address
	CurrentBlock uint64
}

type Gatekeeper struct {
	client         *ethclient.Client
	gevm           *execution.GEVMSimulator
	candidateQueue chan DiscoveryCandidate
	factoryBlocks  map[common.Address]uint64
	factoryCounts  map[common.Address]int
	mu             sync.Mutex

	memeCache *state.LRUCache
	dexCache  *state.LRUCache
	pairCache *state.LRUCache
	blacklist *state.Blacklist
	matrix    *state.Matrix

	factoryCache sync.Map
	callSem      chan struct{}
	owner        common.Address
	decimalsMu   sync.Mutex
	decimalsCache map[common.Address]uint8
	poolTickRefreshMu   sync.RWMutex
    poolTickRefresh     map[common.Address]uint64
}

func New(
	client *ethclient.Client,
	gevm *execution.GEVMSimulator,
	memeCache *state.LRUCache,
	dexCache *state.LRUCache,
	pairCache *state.LRUCache,
	blacklist *state.Blacklist,
	matrix *state.Matrix,
	owner common.Address,
) *Gatekeeper {
	gk := &Gatekeeper{
		client:         client,
		gevm:           gevm,
		candidateQueue: make(chan DiscoveryCandidate, 2048),
		factoryBlocks:  make(map[common.Address]uint64),
		factoryCounts:  make(map[common.Address]int),
		memeCache:      memeCache,
		dexCache:       dexCache,
		pairCache:      pairCache,
		blacklist:      blacklist,
		matrix:         matrix,
		callSem:        make(chan struct{}, 8),
		owner:          owner,
		poolTickRefresh: make(map[common.Address]uint64),
	}
	gk.decimalsCache = make(map[common.Address]uint8)
	gk.startWorkers()
	return gk
}
// refreshV3TickDataLoop runs periodically to refresh V3 tick data for all pools.
func (gk *Gatekeeper) refreshV3TickDataLoop(ctx context.Context) {
    ticker := time.NewTicker(30 * time.Second)
    defer ticker.Stop()

    for {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
            header, err := gk.client.HeaderByNumber(ctx, nil)
            if err != nil {
                log.Printf("[Gatekeeper] Failed to get current block for tick refresh: %v", err)
                continue
            }
            currentBlock := header.Number.Uint64()

            var toRefresh []common.Address
            gk.matrix.RangePools(func(addr common.Address, pool *types.PoolState) bool {
                if pool.DexType != types.DexUniswapV3 && pool.DexType != types.DexPancakeV3 {
                    return true
                }
                gk.poolTickRefreshMu.RLock()
                lastRefresh := gk.poolTickRefresh[addr]
                gk.poolTickRefreshMu.RUnlock()
                if lastRefresh == 0 || currentBlock-lastRefresh > 100 {
                    toRefresh = append(toRefresh, addr)
                }
                return true
            })

            for _, addr := range toRefresh {
                pool := gk.matrix.GetPool(addr)
                if pool == nil {
                    continue
                }
                gk.fetchV3TickDataSync(ctx, addr, pool.Tick, currentBlock)
                gk.poolTickRefreshMu.Lock()
                gk.poolTickRefresh[addr] = currentBlock
                gk.poolTickRefreshMu.Unlock()
            }
        }
    }
}
func (gk *Gatekeeper) startWorkers() {
    for i := 0; i < 4; i++ {
        go gk.worker()
    }
    // Start the tick refresh loop
    go gk.refreshV3TickDataLoop(context.Background())
    log.Printf("[Gatekeeper] Started 4 background dynamic discovery workers and tick refresh loop")
}

func (gk *Gatekeeper) ProcessLog(log *types.SwapLog) {
	if gk.blacklist.IsBlacklisted(log.Address) {
		return
	}
	select {
	case gk.candidateQueue <- DiscoveryCandidate{
		PoolAddress:  log.Address,
		Token0:       log.TokenIn,
		Token1:       log.TokenOut,
		CurrentBlock: log.BlockNumber,
	}:
	default:
	}
}

func (gk *Gatekeeper) worker() {
	ctx := context.Background()
	for cand := range gk.candidateQueue {
		if gk.blacklist.IsBlacklisted(cand.PoolAddress) {
			continue
		}

		factoryAddr, err := gk.getFactory(ctx, cand.PoolAddress)
		if err == nil && factoryAddr != (common.Address{}) {
			gk.mu.Lock()
			lastBlock := gk.factoryBlocks[factoryAddr]
			if cand.CurrentBlock > lastBlock+100 {
				gk.factoryCounts[factoryAddr] = 0
			}
			gk.factoryBlocks[factoryAddr] = cand.CurrentBlock
			gk.factoryCounts[factoryAddr]++
			count := gk.factoryCounts[factoryAddr]
			gk.mu.Unlock()

			if count >= 2 && !gk.dexCache.Get(factoryAddr) {
				gk.dexCache.Put(factoryAddr)
				log.Printf("[Gatekeeper] DEX Factory promoted dynamically to LRU: %s (swaps: %d)", factoryAddr.Hex(), count)
			}
		}

		if err := gk.registerPoolBatch(ctx, cand); err != nil {
			continue
		}

		gk.qualifyTokenLayer(ctx, cand.Token0, cand.Token1, cand.PoolAddress)
		gk.qualifyTokenLayer(ctx, cand.Token1, cand.Token0, cand.PoolAddress)
	}
}

func (gk *Gatekeeper) getFactory(ctx context.Context, pool common.Address) (common.Address, error) {
	if val, ok := gk.factoryCache.Load(pool); ok {
		return val.(common.Address), nil
	}
	select {
	case gk.callSem <- struct{}{}:
		defer func() { <-gk.callSem }()
	case <-time.After(2 * time.Second):
		return common.Address{}, fmt.Errorf("RPC concurrency limit reached")
	}
	factory, err := gk.matrix.CallFactory(ctx, pool)
	if err != nil {
		return common.Address{}, err
	}
	gk.factoryCache.Store(pool, factory)
	return factory, nil
}

// ---- registerPoolBatch with synchronous V3 tick data fetch ----
func (gk *Gatekeeper) registerPoolBatch(ctx context.Context, cand DiscoveryCandidate) error {
	if existingPool := gk.matrix.GetPool(cand.PoolAddress); existingPool != nil {
		return nil
	}

	var calls []multicallCall
	calls = append(calls, multicallCall{Target: cand.PoolAddress, CallData: packCallFixed("token0")})
	calls = append(calls, multicallCall{Target: cand.PoolAddress, CallData: packCallFixed("token1")})
	calls = append(calls, multicallCall{Target: cand.PoolAddress, CallData: packCallFixed("fee")})
	calls = append(calls, multicallCall{Target: cand.PoolAddress, CallData: packCallFixed("slot0")})
	calls = append(calls, multicallCall{Target: cand.PoolAddress, CallData: packCallFixed("liquidity")})
	calls = append(calls, multicallCall{Target: cand.PoolAddress, CallData: packCallFixed("getReserves")})
	calls = append(calls, multicallCall{Target: cand.PoolAddress, CallData: packCallFixed("stable")})

	results, err := gk.multicall(ctx, calls)
	if err != nil {
		return fmt.Errorf("multicall failed: %w", err)
	}

	if len(results) < 7 || !results[0].Success || len(results[0].Data) < 32 || !results[1].Success || len(results[1].Data) < 32 {
		return fmt.Errorf("token0/token1 call failed")
	}

	token0 := common.BytesToAddress(results[0].Data[12:32])
	token1 := common.BytesToAddress(results[1].Data[12:32])

	isV3 := results[3].Success && len(results[3].Data) >= 64

	var dexType types.DexType
	if isV3 {
		dexType = types.DexUniswapV3
	} else {
		dexType = types.DexAerodromeV2
	}

	poolState := &types.PoolState{
		PoolAddress: cand.PoolAddress,
		Token0:      token0,
		Token1:      token1,
		DexType:     dexType,
	}

	if isV3 {
		if results[2].Success && len(results[2].Data) >= 32 {
			rawFee := new(big.Int).SetBytes(results[2].Data[:32])
			poolState.FeeBps = uint32(rawFee.Uint64() / 100)
		} else {
			poolState.FeeBps = 30
		}
		poolState.SqrtPriceX96 = new(big.Int).SetBytes(results[3].Data[:32])
		poolState.Tick = int32(binary.BigEndian.Uint32(results[3].Data[60:64]))
		poolState.Slot0Packed = types.PackV3Slot0(poolState.SqrtPriceX96, poolState.Tick)
		if results[4].Success && len(results[4].Data) >= 32 {
			poolState.Liquidity = new(big.Int).SetBytes(results[4].Data[:32])
		}
		// ---- FIX: Fetch V3 tick data synchronously ----
	gk.fetchV3TickDataSync(ctx, cand.PoolAddress, poolState.Tick, cand.CurrentBlock)
	} else {
		if !results[5].Success || len(results[5].Data) < 64 {
			return fmt.Errorf("getReserves failed")
		}
		poolState.Reserve0 = new(big.Int).SetBytes(results[5].Data[:32])
		poolState.Reserve1 = new(big.Int).SetBytes(results[5].Data[32:64])
		poolState.Reserve0Float = float64FromBig(poolState.Reserve0)
		poolState.Reserve1Float = float64FromBig(poolState.Reserve1)
		if results[6].Success && len(results[6].Data) >= 32 {
			poolState.Stable = new(big.Int).SetBytes(results[6].Data).Uint64() != 0
		}
		// ---- FIX: fetch V2 fee (convert pips to bps) ----
		poolState.FeeBps = gk.fetchV2Fee(ctx, cand.PoolAddress)
	}

	gk.matrix.RegisterPool(poolState)

	if gk.gevm != nil {
		if err := gk.gevm.WarmUpAddress(cand.PoolAddress); err != nil {
			log.Printf("[Gatekeeper] Failed to warm up pool %s: %v", cand.PoolAddress.Hex(), err)
		}
		_ = gk.gevm.WarmUpAddress(token0)
		_ = gk.gevm.WarmUpAddress(token1)
	}

	log.Printf("[Gatekeeper] Dynamically registered %s pool %s (%s/%s) fee=%d bps",
		dexType, cand.PoolAddress.Hex(), token0.Hex()[:6], token1.Hex()[:6], poolState.FeeBps)
	return nil
}

// ---- fetchV2Fee: returns basis points (converts from pips) ----
func (gk *Gatekeeper) fetchV2Fee(ctx context.Context, pool common.Address) uint32 {
	feeData, err := gk.client.CallContract(ctx, ethereum.CallMsg{
		To:   &pool,
		Data: common.FromHex("0xddca3f43"),
	}, nil)
	if err == nil && len(feeData) >= 32 {
		fee := new(big.Int).SetBytes(feeData)
		// V2 fee() returns pips (e.g., 3000 = 0.3%). Convert to bps by dividing by 100.
		feeBps := uint32(fee.Uint64() / 100)
		if feeBps > 0 && feeBps <= 1000 {
			return feeBps
		}
	}
	return 30 // default 0.3%
}

func (gk *Gatekeeper) fetchTickSpacing(ctx context.Context, pool common.Address) int32 {
	data, err := gk.client.CallContract(ctx, ethereum.CallMsg{
		To:   &pool,
		Data: common.FromHex("0xd0c93a7c"),
	}, nil)
	if err == nil && len(data) >= 32 {
		return int32(new(big.Int).SetBytes(data).Int64())
	}
	return 60
}

// ---- Synchronous V3 tick data fetch (blocks until complete) ----
func (gk *Gatekeeper) fetchV3TickDataSync(ctx context.Context, pool common.Address, currentTick int32, block uint64) {
    tickSpacing := gk.fetchTickSpacing(ctx, pool)

    const wordRadius = 5
    currentWord := currentTick >> 8
    startWord := currentWord - wordRadius
    endWord := currentWord + wordRadius

    bitmap := make(map[int32]*big.Int)
    for w := startWord; w <= endWord; w++ {
        data, err := v3PoolABI.Pack("tickBitmap", int16(w))
        if err != nil {
            continue
        }
        msg := ethereum.CallMsg{To: &pool, Data: data}
        out, err := gk.client.CallContract(ctx, msg, nil)
        if err != nil || len(out) < 32 {
            continue
        }
        word := new(big.Int).SetBytes(out)
        if word.Sign() != 0 {
            bitmap[w] = word
        }
    }

    var setTicks []int32
    for w, word := range bitmap {
        for bit := 0; bit < 256; bit++ {
            if word.Bit(bit) == 1 {
                tick := w*256 + int32(bit)
                setTicks = append(setTicks, tick)
            }
        }
    }

    liquidityNet := make(map[int32]*big.Int)
    if len(setTicks) > 0 {
        tickCalls := make([]multicallCall, len(setTicks))
        for i, tick := range setTicks {
            data, err := ticksABI.Pack("ticks", tick)
            if err != nil {
                continue
            }
            tickCalls[i] = multicallCall{Target: pool, CallData: data}
        }
        tickResults, err := gk.multicall(ctx, tickCalls)
        if err == nil {
            for i, res := range tickResults {
                if !res.Success {
                    continue
                }
                var vals []interface{}
                if err := ticksABI.UnpackIntoInterface(&vals, "ticks", res.Data); err == nil && len(vals) >= 2 {
                    if liqNet, ok := vals[1].(*big.Int); ok && i < len(setTicks) {
                        liquidityNet[setTicks[i]] = liqNet
                    }
                }
            }
        }
    }

    gk.matrix.SetPoolTickData(pool, bitmap, liquidityNet, tickSpacing)

    // Store refresh block
    gk.poolTickRefreshMu.Lock()
    gk.poolTickRefresh[pool] = block
    gk.poolTickRefreshMu.Unlock()
}
func (gk *Gatekeeper) multicall(ctx context.Context, calls []multicallCall) ([]struct {
	Success bool
	Data    []byte
}, error) {
	if len(calls) == 0 {
		return nil, nil
	}

	const multicallABI = `[{"inputs":[{"internalType":"bool","name":"requireSuccess","type":"bool"},{"components":[{"internalType":"address","name":"target","type":"address"},{"internalType":"bytes","name":"callData","type":"bytes"}],"name":"calls","type":"tuple[]"}],"name":"tryAggregate","outputs":[{"components":[{"internalType":"bool","name":"success","type":"bool"},{"internalType":"bytes","name":"returnData","type":"bytes"}],"name":"returnData","type":"tuple[]"}],"stateMutability":"nonpayable","type":"function"}]`
	parsed, err := abi.JSON(strings.NewReader(multicallABI))
	if err != nil {
		return nil, fmt.Errorf("failed to parse multicall ABI: %w", err)
	}

	type CallTuple struct {
		Target   common.Address
		CallData []byte
	}
	callTuples := make([]CallTuple, len(calls))
	for i, c := range calls {
		callTuples[i] = CallTuple{Target: c.Target, CallData: c.CallData}
	}

	packedData, err := parsed.Pack("tryAggregate", false, callTuples)
	if err != nil {
		return nil, fmt.Errorf("failed to pack multicall: %w", err)
	}

	toAddr := common.HexToAddress(multicallAddress)
	msg := ethereum.CallMsg{
		To:   &toAddr,
		Data: packedData,
		From: gk.owner,
	}

	rawOutput, err := gk.client.CallContract(ctx, msg, nil)
	if err != nil {
		return nil, fmt.Errorf("eth_call failed: %w", err)
	}

	type ResultTuple struct {
		Success    bool
		ReturnData []byte
	}
	var results []ResultTuple
	err = parsed.UnpackIntoInterface(&results, "tryAggregate", rawOutput)
	if err != nil {
		return nil, fmt.Errorf("failed to unpack multicall result: %w", err)
	}

	outResults := make([]struct {
		Success bool
		Data    []byte
	}, len(results))
	for i, r := range results {
		outResults[i].Success = r.Success
		outResults[i].Data = r.ReturnData
	}
	return outResults, nil
}

type multicallCall struct {
	Target   common.Address
	CallData []byte
}

func packCallFixed(method string) []byte {
	switch method {
	case "token0":
		return common.FromHex("0x0dfe1681")
	case "token1":
		return common.FromHex("0xd21220a7")
	case "fee":
		return common.FromHex("0xddca3f43")
	case "slot0":
		return common.FromHex("0x3850c7bd")
	case "liquidity":
		return common.FromHex("0x1a686502")
	case "getReserves":
		return common.FromHex("0x0902f1ac")
	case "stable":
		return common.FromHex("0xa9da1a34")
	default:
		return nil
	}
}

// ---- FIX: Added missing isBaseAsset method ----
func (gk *Gatekeeper) isBaseAsset(addr common.Address) bool {
	anchors := config.AnchorAssets()
	for _, a := range anchors {
		if a == addr {
			return true
		}
	}
	return false
}

func (gk *Gatekeeper) qualifyTokenLayer(ctx context.Context, target, paired common.Address, pool common.Address) {
	if gk.blacklist.IsBlacklisted(target) {
		return
	}
	isBase := gk.isBaseAsset(target)
	if isBase {
		if gk.memeCache.Get(paired) && !gk.pairCache.Get(target) {
			gk.pairCache.Put(target)
			log.Printf("[Gatekeeper] Base pair promoted to LRU: %s", target.Hex())
		}
	} else {
		if gk.memeCache.Get(target) {
			return
		}
		if !gk.isBaseAsset(paired) {
			return
		}
		gk.getOrFetchTokenDecimals(ctx, target)
		passed, err := gk.simulateHoneypot(pool, target, paired)
		if err != nil || !passed {
			gk.blacklist.Add(target)
			log.Printf("[Gatekeeper] Honeypot detected, blacklisted: %s", target.Hex())
			return
		}
		gk.memeCache.Put(target)
		log.Printf("[Gatekeeper] Qualified meme coin promoted to LRU: %s", target.Hex())
	}
}

func (gk *Gatekeeper) getOrFetchTokenDecimals(ctx context.Context, token common.Address) uint8 {
	gk.decimalsMu.Lock()
	if d, ok := gk.decimalsCache[token]; ok {
		gk.decimalsMu.Unlock()
		return d
	}
	gk.decimalsMu.Unlock()

	data := common.FromHex("0x313ce567")
	msg := ethereum.CallMsg{To: &token, Data: data}
	out, err := gk.client.CallContract(ctx, msg, nil)
	var decimals uint8 = 18
	if err == nil && len(out) >= 32 {
		decimals = uint8(new(big.Int).SetBytes(out).Uint64())
	}
	solver.SetTokenDecimals(token, decimals)
	gk.decimalsMu.Lock()
	gk.decimalsCache[token] = decimals
	gk.decimalsMu.Unlock()
	return decimals
}

// ---- FIX: Stricter honeypot with V3 liquidity check ----
func (gk *Gatekeeper) simulateHoneypot(pool, memeToken, baseToken common.Address) (bool, error) {
	poolState := gk.matrix.GetPool(pool)
	if poolState == nil {
		return false, fmt.Errorf("pool not in matrix")
	}

	var liquidityUSD float64
	if poolState.DexType == types.DexUniswapV3 || poolState.DexType == types.DexPancakeV3 {
		// ---- FIX: Add V3 liquidity estimation ----
		basePrice, ok := solver.GetTokenPrice(baseToken, gk.matrix, nil)
		if !ok || basePrice <= 0 {
			basePrice = 1.0
		}
		// Approximate USD liquidity from sqrtPriceX96 and liquidity
		// liquidity is in wei of token0*deltaP – rough estimate: liquidity * sqrtPrice / 1e18
		sqrtPrice := new(big.Float).SetInt(poolState.SqrtPriceX96)
		liq := new(big.Float).SetInt(poolState.Liquidity)
		// sqrtPrice / 2^96
		sqrtPrice.Quo(sqrtPrice, new(big.Float).SetFloat64(math.Pow(2, 96)))
		// liquidityUSD = liquidity * sqrtPrice / 1e18 (converted to USD)
		liqVal, _ := liq.Float64()
		sqrtVal, _ := sqrtPrice.Float64()
		liquidityUSD = liqVal * sqrtVal / 1e18 * basePrice
		if liquidityUSD < 10000.0 {
			return false, fmt.Errorf("V3 liquidity too low: $%.2f", liquidityUSD)
		}
	} else {
		r0, r1 := poolState.GetReserves()
		if r0 > 0 && r1 > 0 {
			basePrice, ok := solver.GetTokenPrice(baseToken, gk.matrix, nil)
			if !ok || basePrice <= 0 {
				basePrice = 1.0
			}
			sqrtProduct := math.Sqrt(r0 * r1)
			liquidityUSD = (sqrtProduct / math.Pow10(12)) * basePrice
			if liquidityUSD < 10000.0 {
				return false, fmt.Errorf("liquidity too low: $%.2f", liquidityUSD)
			}
		}
	}

	basePrice, ok := solver.GetTokenPrice(baseToken, gk.matrix, nil)
	if !ok || basePrice <= 0 {
		basePrice = 1.0
	}
	usdAmount := 0.10
	baseDecimals := getTokenDecimals(baseToken)
	var amountIn *big.Int
	if basePrice > 0 {
		amountInFloat := (usdAmount / basePrice) * math.Pow10(baseDecimals)
		amountIn = new(big.Int).SetInt64(int64(amountInFloat))
		if amountIn.Sign() <= 0 {
			amountIn = big.NewInt(1)
		}
	} else {
		amountIn = big.NewInt(1)
	}

	out1 := new(big.Int)
	if err := solver.ComputeSwap(poolState, baseToken, memeToken, amountIn, out1); err != nil {
		return false, err
	}
	if out1.Sign() <= 0 {
		return false, fmt.Errorf("buy output zero")
	}

	out2 := new(big.Int)
	if err := solver.ComputeSwap(poolState, memeToken, baseToken, out1, out2); err != nil {
		return false, err
	}
	if out2.Sign() <= 0 {
		return false, fmt.Errorf("sell output zero")
	}

	minExpected := new(big.Int).Mul(amountIn, big.NewInt(90))
	minExpected.Div(minExpected, big.NewInt(100))
	if out2.Cmp(minExpected) < 0 {
		return false, fmt.Errorf("excessive sell tax (returned %s, expected >= %s)", out2.String(), minExpected.String())
	}

	return true, nil
}

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

func float64FromBig(v *big.Int) float64 {
	if v == nil {
		return 0
	}
	f, _ := new(big.Float).SetInt(v).Float64()
	return f
}
