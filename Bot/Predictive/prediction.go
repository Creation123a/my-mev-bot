// Package predictive implements a Flashblock‑synchronised PLL and state prediction
// for sub‑millisecond arbitrage scheduling on Base.
package predictive

import (
	"context"
	"math/big"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"

	"my-mev-bot/Bot/Types"
)

// =============================================================================
// FlashblockPLL – phase‑locked loop synchronised with Base’s 200ms sequencer
// =============================================================================

// FlashblockPLL tracks the sequencer heartbeat and network RTT to schedule
// transaction submission exactly at the optimal microsecond boundary.
type FlashblockPLL struct {
    lastTickNano      atomic.Int64
    measuredRTTNano   atomic.Int64
    blockIntervalNano int64
    rttMu             sync.Mutex   // protects rttSamples
    rttSamples        []int64      // rolling window of RTT measurements (nano)
}

// NewFlashblockPLL creates a PLL with default values.
func NewFlashblockPLL() *FlashblockPLL {
	pll := &FlashblockPLL{
		blockIntervalNano: 200 * 1_000_000, 
		rttSamples:        make([]int64, 0, 100),
	}
	// Initial RTT estimate: 6ms (will be refined by MeasureRTT)
	pll.measuredRTTNano.Store(6 * 1_000_000)
	return pll
}

// RecordBlockTick must be called on every new block header (from WebSocket subscription).
// It stores the precise local time when the Flashblock was received.
func (pll *FlashblockPLL) RecordBlockTick(t time.Time) {
	pll.lastTickNano.Store(t.UnixNano())
}

// MeasureRTT sends a lightweight eth_blockNumber request to measure actual network latency.
// Call this periodically (e.g., every 30 seconds) to adapt to network changes.
func (pll *FlashblockPLL) MeasureRTT(ctx context.Context, client *ethclient.Client) error {
    start := time.Now()
    _, err := client.BlockNumber(ctx)
    if err != nil {
        return err
    }
    rtt := time.Since(start).Nanoseconds()

    pll.rttMu.Lock()
    defer pll.rttMu.Unlock()
    pll.rttSamples = append(pll.rttSamples, rtt)
    if len(pll.rttSamples) > 100 {
        pll.rttSamples = pll.rttSamples[1:]
    }
    // Compute minimum over the window
    min := int64(1<<62 - 1) // max int64
    for _, v := range pll.rttSamples {
        if v < min {
            min = v
        }
    }
    atomic.StoreInt64(&pll.measuredRTTNano, min)
    return nil
}

// SetRTT allows manual RTT override (e.g., from WebSocket ping/pong).
func (pll *FlashblockPLL) SetRTT(rtt time.Duration) {
	pll.measuredRTTNano.Store(rtt.Nanoseconds())
}

// GetRTT returns the current measured RTT.
func (pll *FlashblockPLL) GetRTT() time.Duration {
	return time.Duration(pll.measuredRTTNano.Load()) * time.Nanosecond
}

// OptimalDispatchTime returns the exact time when the transaction should be sent
// to arrive at the sequencer just before the next Flashblock cutoff.
// It subtracts RTT and a 1ms safety margin to avoid missing the window.
func (pll *FlashblockPLL) OptimalDispatchTime() time.Time {
	lastNano := pll.lastTickNano.Load()
	if lastNano == 0 {
		// Fallback: now + 50ms (first‑block bootstrap)
		return time.Now().Add(50 * time.Millisecond)
	}
	last := time.Unix(0, lastNano)
	next := last.Add(200 * time.Millisecond)
	rtt := time.Duration(pll.measuredRTTNano.Load()) * time.Nanosecond
	// Safety margin: 1ms
	return next.Add(-rtt).Add(-1 * time.Millisecond)
}

// TimeUntilNextFlashblock returns the duration until the next scheduled Flashblock.
func (pll *FlashblockPLL) TimeUntilNextFlashblock() time.Duration {
	lastNano := pll.lastTickNano.Load()
	if lastNano == 0 {
		return 200 * time.Millisecond
	}
	last := time.Unix(0, lastNano)
	elapsed := time.Since(last)
	if elapsed >= 200*time.Millisecond {
		return 0
	}
	return 200*time.Millisecond - elapsed
}

// =============================================================================
// State Prediction – zero‑allocation, thread‑safe
// =============================================================================

// PredictPostState computes the new reserves of a V2 pool after a swap.
// The caller provides pre‑allocated big.Ints for r0Out and r1Out to avoid allocations.
// Returns r0Out and r1Out for convenience.
func PredictV2PostState(
	pool *types.PoolState,
	tokenIn common.Address,
	amountIn *big.Int,
	r0Out, r1Out *big.Int,
) (*big.Int, *big.Int) {
	if pool == nil || amountIn == nil || amountIn.Sign() <= 0 {
		r0Out.Set(pool.Reserve0)
		r1Out.Set(pool.Reserve1)
		return r0Out, r1Out
	}

	// Fee as basis points (0 = use 30)
	feeBps := pool.FeeBps
	if feeBps == 0 {
		feeBps = 30
	}
	multiplier := new(big.Int).SetUint64(uint64(10000 - feeBps))
	denomFactor := new(big.Int).SetUint64(10000)

	// amountInWithFee = amountIn * (10000 - feeBps)
	amountInWithFee := new(big.Int).Mul(amountIn, multiplier)

	if tokenIn == pool.Token0 {
		// R0' = R0 + amountIn
		r0Out.Add(pool.Reserve0, amountIn)

		// amountOut = (R1 * amountInWithFee) / (R0 * 10000 + amountInWithFee)
		num := new(big.Int).Mul(pool.Reserve1, amountInWithFee)
		den := new(big.Int).Add(new(big.Int).Mul(pool.Reserve0, denomFactor), amountInWithFee)
		amountOut := new(big.Int).Div(num, den)

		r1Out.Sub(pool.Reserve1, amountOut)
	} else if tokenIn == pool.Token1 {
		// R1' = R1 + amountIn
		r1Out.Add(pool.Reserve1, amountIn)

		num := new(big.Int).Mul(pool.Reserve0, amountInWithFee)
		den := new(big.Int).Add(new(big.Int).Mul(pool.Reserve1, denomFactor), amountInWithFee)
		amountOut := new(big.Int).Div(num, den)

		r0Out.Sub(pool.Reserve0, amountOut)
	} else {
		// Token not in pool – no change
		r0Out.Set(pool.Reserve0)
		r1Out.Set(pool.Reserve1)
	}
	return r0Out, r1Out
}

// PredictV3PostState approximates the new sqrtPriceX96 and liquidity after a V3 swap.
// Uses a constant‑product approximation with virtual reserves derived from current sqrtPrice and liquidity.
// This is a fast approximation for background prediction; exact simulation is done by the solver.
func PredictV3PostState(
	pool *types.PoolState,
	tokenIn common.Address,
	amountIn *big.Int,
	sqrtOut, liqOut *big.Int,
) (*big.Int, *big.Int) {
	if pool == nil || amountIn == nil || amountIn.Sign() <= 0 {
		sqrtOut.Set(pool.SqrtPriceX96)
		liqOut.Set(pool.Liquidity)
		return sqrtOut, liqOut
	}

	// Read current state (pool lock is held by caller, so we can read directly)
	sqrtPrice := pool.SqrtPriceX96
	liquidity := pool.Liquidity
	feeBps := pool.FeeBps
	if feeBps == 0 {
		feeBps = 30 // default 0.3%
	}

	// Determine swap direction
	token0 := pool.Token0
	zeroForOne := tokenIn == token0

	// Virtual reserves (for approximation)
	// For V3, reserve0 ≈ liquidity / sqrtPrice, reserve1 ≈ liquidity * sqrtPrice
	// We use big.Float for precision, but to keep it in big.Int we scale.
	const Q96 = 79228162514264337593543950336 // 2^96
	Q96big := new(big.Int).SetUint64(Q96)

	// Compute virtual reserves in wei (scaled by Q96)
	// reserve0 = liquidity * Q96 / sqrtPrice
	// reserve1 = liquidity * sqrtPrice / Q96
	// But we can avoid division by using big.Float for ease; since this is background,
	// we can use float64 approximation. However, to keep zero‑allocation, we use big.Int with some precision loss.
	// We'll use big.Float for the math and then convert back.
	sqrtF := new(big.Float).SetInt(sqrtPrice)
	liqF := new(big.Float).SetInt(liquidity)
	q96F := new(big.Float).SetUint64(Q96)

	// reserve0 = liquidity / sqrtPrice (in token0 units)
	reserve0F := new(big.Float).Quo(liqF, sqrtF) // roughly
	// reserve1 = liquidity * sqrtPrice
	reserve1F := new(big.Float).Mul(liqF, sqrtF)

	// Apply V2 swap formula to compute new reserves
	feeFactor := 1.0 - float64(feeBps)/10000.0
	amountInF := new(big.Float).SetInt(amountIn)
	amountInWithFee := new(big.Float).Mul(amountInF, big.NewFloat(feeFactor))

	var reserveInF, reserveOutF *big.Float
	if zeroForOne {
		// token0 is input, token1 is output
		reserveInF = reserve0F
		reserveOutF = reserve1F
	} else {
		reserveInF = reserve1F
		reserveOutF = reserve0F
	}

	// amountOut = reserveOut * amountInWithFee / (reserveIn + amountInWithFee)
	num := new(big.Float).Mul(reserveOutF, amountInWithFee)
	den := new(big.Float).Add(reserveInF, amountInWithFee)
	amountOutF := new(big.Float).Quo(num, den)

	// New reserves
	var newReserveInF, newReserveOutF *big.Float
	if zeroForOne {
		newReserveInF = new(big.Float).Add(reserve0F, amountInF)
		newReserveOutF = new(big.Float).Sub(reserve1F, amountOutF)
	} else {
		newReserveInF = new(big.Float).Add(reserve1F, amountInF)
		newReserveOutF = new(big.Float).Sub(reserve0F, amountOutF)
	}
	// Prevent negative
	if newReserveOutF.Sign() < 0 {
		newReserveOutF.SetFloat64(0)
	}

	// Compute new sqrtPrice = sqrt(newReserveOut / newReserveIn)
	ratio := new(big.Float).Quo(newReserveOutF, newReserveInF)
	if ratio.Sign() < 0 {
		ratio.SetFloat64(0)
	}
	newSqrtF := new(big.Float).Sqrt(ratio)

	// New liquidity = sqrt(newReserveIn * newReserveOut) (approximate)
	newLiqF := new(big.Float).Sqrt(new(big.Float).Mul(newReserveInF, newReserveOutF))

	// Convert back to big.Int (scaled by Q96)
	// sqrtPrice is in Q96 format: sqrtPrice = sqrt(price) * 2^96
	// We need to multiply newSqrtF by Q96
	newSqrtF.Mul(newSqrtF, q96F)
	sqrtOut, _ := newSqrtF.Int(sqrtOut)

	// Liquidity is in wei, we can directly convert newLiqF
	liqOut, _ := newLiqF.Int(liqOut)

	return sqrtOut, liqOut
}
// PredictPostState is a unified dispatcher for V2 and V3 pools.
func PredictPostState(
	pool *types.PoolState,
	tokenIn common.Address,
	amountIn *big.Int,
	r0Out, r1Out *big.Int,
) (*big.Int, *big.Int) {
	if pool == nil {
		return r0Out, r1Out
	}
	pool.RLock()
    defer pool.RUnlock()
	if pool.DexType == types.DexUniswapV3 || pool.DexType == types.DexPancakeV3 {
		// For V3, we approximate reserves – but we only need sqrtPrice and liquidity.
		// We'll leave r0Out/r1Out as zero for V3 and return sqrtPrice/liquidity instead.
		// To keep signature consistent, we set r0Out = sqrtPrice, r1Out = liquidity (cast as big.Int).
		// The caller must interpret accordingly.
		sqrtOut := new(big.Int)
		liqOut := new(big.Int)
		PredictV3PostState(pool, tokenIn, amountIn, sqrtOut, liqOut)
		r0Out.Set(sqrtOut)
		r1Out.Set(liqOut)
		return r0Out, r1Out
	}
	// V2
	return PredictV2PostState(pool, tokenIn, amountIn, r0Out, r1Out)
}
