package solver

import (
	"fmt"
	"math/big"
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"my-mev-bot/Bot/Types"
)

const (
    MIN_TICK = -887272
    MAX_TICK = 887272
    TICK_RANGE_SIZE = MAX_TICK - MIN_TICK + 1 // 1,774,545
)

var (
    tickSqrtPriceCache []*big.Int
    cacheMu            sync.RWMutex
    cacheOnce          sync.Once
)

func initTickSqrtCache() {
    cacheOnce.Do(func() {
        tickSqrtPriceCache = make([]*big.Int, TICK_RANGE_SIZE)
    })
}
// getSqrtPriceAtTickExact returns the exact sqrtPrice for a tick using cached values.
// The cache is populated lazily; first call for a given tick triggers the Uniswap V3 exact computation.
// Thread‑safe with RWMutex and uses a fixed‑size slice (bounded memory).
func getSqrtPriceAtTickExact(tick int32) *big.Int {
    // Ensure cache is initialised once
    cacheOnce.Do(initTickSqrtCache)

    // Calculate index (tick range is -887272 to 887272)
    idx := int(tick - MIN_TICK)

    // Fast path: read lock to check cache
    cacheMu.RLock()
    if val := tickSqrtPriceCache[idx]; val != nil {
        cacheMu.RUnlock()
        return val
    }
    cacheMu.RUnlock()

    // Cache miss – compute (write lock needed)
    cacheMu.Lock()
    defer cacheMu.Unlock()

    // Double‑check: another goroutine might have filled it while we waited.
    if val := tickSqrtPriceCache[idx]; val != nil {
        return val
    }

    // ---------- Computation (exact Uniswap V3 TickMath) ----------
    absTick := tick
    if absTick < 0 {
        absTick = -absTick
    }

    var ratio *big.Int
    if absTick&0x1 != 0 {
        ratio = new(big.Int)
        ratio.SetString("fffcb933bd6fad37aa2d162d1a594001", 16)
    } else {
        ratio = new(big.Int).Lsh(big.NewInt(1), 128)
    }

    // Bitwise constants (same as original)
    if absTick&0x2 != 0 {
        c := new(big.Int)
        c.SetString("fff97272373d413259a46990580e213a", 16)
        ratio.Mul(ratio, c)
        ratio.Rsh(ratio, 128)
    }
    if absTick&0x4 != 0 {
        c := new(big.Int)
        c.SetString("fff2e50f5f656932ef12357cf3c7fdcc", 16)
        ratio.Mul(ratio, c)
        ratio.Rsh(ratio, 128)
    }
    if absTick&0x8 != 0 {
        c := new(big.Int)
        c.SetString("ffe5caca7e10e4e61c3624eaa0941cd0", 16)
        ratio.Mul(ratio, c)
        ratio.Rsh(ratio, 128)
    }
    if absTick&0x10 != 0 {
        c := new(big.Int)
        c.SetString("ffcb9843d60f6159c9db58835c926644", 16)
        ratio.Mul(ratio, c)
        ratio.Rsh(ratio, 128)
    }
    if absTick&0x20 != 0 {
        c := new(big.Int)
        c.SetString("ff973b41fa98c081472e6896dfb254c0", 16)
        ratio.Mul(ratio, c)
        ratio.Rsh(ratio, 128)
    }
    if absTick&0x40 != 0 {
        c := new(big.Int)
        c.SetString("ff2ea16466c96a3843ec78b326b52861", 16)
        ratio.Mul(ratio, c)
        ratio.Rsh(ratio, 128)
    }
    if absTick&0x80 != 0 {
        c := new(big.Int)
        c.SetString("fe5dee046a99a2a811c461f1969c3053", 16)
        ratio.Mul(ratio, c)
        ratio.Rsh(ratio, 128)
    }
    if absTick&0x100 != 0 {
        c := new(big.Int)
        c.SetString("fcbe86c7900a88aedcffc83b479aa3a4", 16)
        ratio.Mul(ratio, c)
        ratio.Rsh(ratio, 128)
    }
    if absTick&0x200 != 0 {
        c := new(big.Int)
        c.SetString("f987a7253ac413176f2b074cf7815e54", 16)
        ratio.Mul(ratio, c)
        ratio.Rsh(ratio, 128)
    }
    if absTick&0x400 != 0 {
        c := new(big.Int)
        c.SetString("f3392b0822b70005940c7a398e4b70f3", 16)
        ratio.Mul(ratio, c)
        ratio.Rsh(ratio, 128)
    }
    if absTick&0x800 != 0 {
        c := new(big.Int)
        c.SetString("e7159475a2c29b7443b29c7fa6e889d9", 16)
        ratio.Mul(ratio, c)
        ratio.Rsh(ratio, 128)
    }
    if absTick&0x1000 != 0 {
        c := new(big.Int)
        c.SetString("d097f3bdfd2022b8845ad8f792aa5825", 16)
        ratio.Mul(ratio, c)
        ratio.Rsh(ratio, 128)
    }
    if absTick&0x2000 != 0 {
        c := new(big.Int)
        c.SetString("a9f746462d870fdf8a65dc1f90e061e5", 16)
        ratio.Mul(ratio, c)
        ratio.Rsh(ratio, 128)
    }
    if absTick&0x4000 != 0 {
        c := new(big.Int)
        c.SetString("70d869a156d2a1b890bb3df62baf32f7", 16)
        ratio.Mul(ratio, c)
        ratio.Rsh(ratio, 128)
    }
    if absTick&0x8000 != 0 {
        c := new(big.Int)
        c.SetString("31be135f97d08fd981231505542fcfa6", 16)
        ratio.Mul(ratio, c)
        ratio.Rsh(ratio, 128)
    }
    if absTick&0x10000 != 0 {
        c := new(big.Int)
        c.SetString("9aa508b5b7a84e1c677de54f3e99bc9", 16)
        ratio.Mul(ratio, c)
        ratio.Rsh(ratio, 128)
    }
    if absTick&0x20000 != 0 {
        c := new(big.Int)
        c.SetString("5d6af8dedb81196699c329225ee604", 16)
        ratio.Mul(ratio, c)
        ratio.Rsh(ratio, 128)
    }
    if absTick&0x40000 != 0 {
        c := new(big.Int)
        c.SetString("2216e584f5fa1ea926041bedfe98", 16)
        ratio.Mul(ratio, c)
        ratio.Rsh(ratio, 128)
    }
    if absTick&0x80000 != 0 {
        c := new(big.Int)
        c.SetString("48a170391f7dc42444e8fa2", 16)
        ratio.Mul(ratio, c)
        ratio.Rsh(ratio, 128)
    }

    if tick > 0 {
        maxUint256 := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 256), big.NewInt(1))
        ratio.Div(maxUint256, ratio)
    }

    shift := uint(32)
    one := big.NewInt(1)
    mod := new(big.Int).Mod(ratio, new(big.Int).Lsh(one, shift))
    res := new(big.Int).Rsh(ratio, shift)
    if mod.Sign() != 0 {
        res.Add(res, one)
    }

    // ---------- Store in cache (under write lock) ----------
    tickSqrtPriceCache[idx] = res
    return res
}
const Q96 = 79228162514264337593543950336

// V3Calculator – holds reusable big.Int buffers.
type V3Calculator struct {
	sqrtPriceX96      big.Int
	liquidity         big.Int
	amountInAfterFee  big.Int
	sqrtPriceNextX96  big.Int
	diff              big.Int
	numerator         big.Int
	denominator       big.Int
	amountOut         big.Int
	product           big.Int
	liquidityMinusOne big.Int
	priceDelta        big.Int
	newSqrtPrice      big.Int
	Q96big            big.Int
	maxSqrt           big.Int
	minSqrt           big.Int
	tmp               big.Int
	one               big.Int

	// Reusable buffers for exact swap
	amountInRemaining big.Int
	sqrtPriceLimit    big.Int
	stepSqrtPrice     big.Int
	stepAmountIn      big.Int
	stepAmountOut     big.Int
	nextTick          big.Int
	wordPos           big.Int
	bitPos            big.Int
	tickBitmapWord    big.Int
	tickSpacing       int32
}

func NewV3Calculator() *V3Calculator {
	c := &V3Calculator{}
	c.Q96big.SetString("79228162514264337593543950336", 10)
	c.maxSqrt.SetString("1461446703485210103287273052203988822378723970342", 10)
	c.minSqrt.SetInt64(4295128739)
	c.one.SetInt64(1)
	c.tickSpacing = 60
	return c
}

// ComputeSwap – main entry for V3 swaps.
func (c *V3Calculator) ComputeSwap(pool *types.PoolState, tokenIn, tokenOut common.Address, amountIn *big.Int, result *big.Int) error {
	if amountIn == nil || amountIn.Sign() <= 0 {
		result.SetInt64(0)
		return nil
	}

	zeroForOne := pool.Token0 == tokenIn && pool.Token1 == tokenOut
	if !zeroForOne && !(pool.Token0 == tokenOut && pool.Token1 == tokenIn) {
		return fmt.Errorf("tokenIn/tokenOut not in pool")
	}

	pool.CopySqrtAndLiquidity(&c.sqrtPriceX96, &c.liquidity)
	if c.sqrtPriceX96.Sign() == 0 {
		return fmt.Errorf("zero sqrtPriceX96")
	}
	if c.liquidity.Sign() == 0 {
		return fmt.Errorf("zero liquidity")
	}

	// Check if we have tick data for exact simulation.
	pool.tickMu.RLock()
	hasTickData := pool.TickBitmap != nil && len(pool.TickBitmap) > 0 && pool.LiquidityNet != nil && len(pool.LiquidityNet) > 0
	pool.tickMu.RUnlock()

	if hasTickData {
		return c.computeSwapExact(pool, amountIn, zeroForOne, result)
	}

	// Fallback: constant‑liquidity approximation (existing logic, kept intact)
	feeBps := pool.FeeBps
	if feeBps <= 100 {
		feeBps = feeBps * 100
	}
	if feeBps >= 10000 {
		feeBps = 9999
	}
	if feeBps == 0 {
		feeBps = 3000
	}
	feeDenominator := big.NewInt(1000000)
	feeNumerator := big.NewInt(int64(1000000 - feeBps))
	c.amountInAfterFee.Mul(amountIn, feeNumerator)
	c.amountInAfterFee.Div(&c.amountInAfterFee, feeDenominator)

	if zeroForOne {
		c.tmp.Sub(&c.sqrtPriceX96, &c.minSqrt)
		c.tmp.Mul(&c.tmp, &c.liquidity)
		c.tmp.Mul(&c.tmp, &c.Q96big)
		c.numerator.Set(&c.tmp)
		c.denominator.Mul(&c.sqrtPriceX96, &c.minSqrt)
		c.tmp.Div(&c.numerator, &c.denominator)
		if c.amountInAfterFee.Cmp(&c.tmp) > 0 {
			c.amountInAfterFee.Set(&c.tmp)
		}
	} else {
		c.tmp.Sub(&c.maxSqrt, &c.sqrtPriceX96)
		c.tmp.Mul(&c.tmp, &c.liquidity)
		c.tmp.Div(&c.tmp, &c.Q96big)
		if c.amountInAfterFee.Cmp(&c.tmp) > 0 {
			c.amountInAfterFee.Set(&c.tmp)
		}
	}
	if c.amountInAfterFee.Sign() == 0 {
		result.SetInt64(0)
		return nil
	}

	if zeroForOne {
		if err := c.getNextSqrtPriceFromAmount0RoundingUp(&c.sqrtPriceX96, &c.liquidity, &c.amountInAfterFee, &c.sqrtPriceNextX96); err != nil {
			return err
		}
		if c.sqrtPriceNextX96.Cmp(&c.sqrtPriceX96) >= 0 {
			return fmt.Errorf("sqrtPriceNext >= sqrtPriceX96 for zeroForOne")
		}
		c.diff.Sub(&c.sqrtPriceX96, &c.sqrtPriceNextX96)
		c.numerator.Mul(&c.liquidity, &c.diff)
		result.Div(&c.numerator, &c.Q96big)
	} else {
		if err := c.getNextSqrtPriceFromAmount1RoundingDown(&c.sqrtPriceX96, &c.liquidity, &c.amountInAfterFee, &c.sqrtPriceNextX96); err != nil {
			return err
		}
		if c.sqrtPriceNextX96.Cmp(&c.sqrtPriceX96) <= 0 {
			return fmt.Errorf("sqrtPriceNext <= sqrtPriceX96 for zeroForOne=false")
		}
		c.diff.Sub(&c.sqrtPriceNextX96, &c.sqrtPriceX96)
		c.numerator.Mul(&c.liquidity, &c.Q96big)
		c.numerator.Mul(&c.numerator, &c.diff)
		c.denominator.Mul(&c.sqrtPriceX96, &c.sqrtPriceNextX96)
		result.Div(&c.numerator, &c.denominator)
	}
	return nil
}

// computeSwapExact – full Uniswap V3 tick‑crossing with bitmap and exact sqrtPrice.
func (c *V3Calculator) computeSwapExact(pool *types.PoolState, amountIn *big.Int, zeroForOne bool, result *big.Int) error {
	sqrtPrice := new(big.Int).Set(pool.SqrtPriceX96)
	liquidity := new(big.Int).Set(pool.Liquidity)
	tick := pool.Tick
	tickSpacing := pool.TickSpacing
	if tickSpacing == 0 {
		tickSpacing = 60
	}

	amountRemaining := new(big.Int).Set(amountIn)
	amountOut := new(big.Int)

	feeBps := pool.FeeBps
	if feeBps <= 100 {
		feeBps = feeBps * 100
	}
	if feeBps >= 10000 {
		feeBps = 9999
	}
	if feeBps == 0 {
		feeBps = 3000
	}
	feeDenominator := big.NewInt(1000000)
	feeNumerator := big.NewInt(int64(1000000 - feeBps))

	for amountRemaining.Sign() > 0 {
		// Find next initialized tick in the direction.
		nextTick, found := c.getNextInitializedTick(pool, tick, tickSpacing, zeroForOne)
		if !found {
			// No more liquidity; we've reached the price limit.
			break
		}

		// Get exact sqrtPrice at that tick.
		sqrtPriceNext := getSqrtPriceAtTickExact(nextTick)

		// Compute amount needed to reach next tick.
		var amountNeeded *big.Int
		if zeroForOne {
			amountNeeded = c.getAmount0Delta(sqrtPrice, sqrtPriceNext, liquidity, true)
		} else {
			amountNeeded = c.getAmount1Delta(sqrtPrice, sqrtPriceNext, liquidity, true)
		}
		// Apply fee.
		amountNeededWithFee := new(big.Int).Mul(amountNeeded, feeDenominator)
		amountNeededWithFee.Div(amountNeededWithFee, feeNumerator)

		if amountRemaining.Cmp(amountNeededWithFee) >= 0 {
			// Cross the tick.
			if zeroForOne {
				stepOut := c.getAmount1Delta(sqrtPrice, sqrtPriceNext, liquidity, false)
				amountOut.Add(amountOut, stepOut)
			} else {
				stepOut := c.getAmount0Delta(sqrtPrice, sqrtPriceNext, liquidity, false)
				amountOut.Add(amountOut, stepOut)
			}
			amountRemaining.Sub(amountRemaining, amountNeededWithFee)

			// Update liquidity via liquidityNet.
			if liqDelta := pool.GetLiquidityNet(nextTick); liqDelta != nil {
				if zeroForOne {
					liquidity.Sub(liquidity, liqDelta)
				} else {
					liquidity.Add(liquidity, liqDelta)
				}
			}
			// Move to next tick.
			sqrtPrice.Set(sqrtPriceNext)
			tick = nextTick
			if liquidity.Sign() <= 0 {
				break
			}
		} else {
			// Partial swap within current tick.
			amountInAfterFee := new(big.Int).Mul(amountRemaining, feeNumerator)
			amountInAfterFee.Div(amountInAfterFee, feeDenominator)

			var newSqrtPrice *big.Int
			if zeroForOne {
				newSqrtPrice = c.getNextSqrtPriceFromAmount0RoundingUpPartial(sqrtPrice, liquidity, amountInAfterFee)
			} else {
				newSqrtPrice = c.getNextSqrtPriceFromAmount1RoundingDownPartial(sqrtPrice, liquidity, amountInAfterFee)
			}
			if newSqrtPrice == nil {
				return fmt.Errorf("failed to compute new sqrtPrice")
			}

			if zeroForOne {
				stepOut := c.getAmount1Delta(sqrtPrice, newSqrtPrice, liquidity, false)
				amountOut.Add(amountOut, stepOut)
			} else {
				stepOut := c.getAmount0Delta(sqrtPrice, newSqrtPrice, liquidity, false)
				amountOut.Add(amountOut, stepOut)
			}
			amountRemaining.SetInt64(0)
			break
		}
	}

	result.Set(amountOut)
	return nil
}

// getNextInitializedTick – scans bitmap to find the next initialized tick in direction.
func (c *V3Calculator) getNextInitializedTick(pool *types.PoolState, tick int32, tickSpacing int32, zeroForOne bool) (int32, bool) {
    wordIndex := tick >> 8
    bitOffset := tick & 255

    if zeroForOne {
        for w := wordIndex; ; w-- {
            word := pool.GetTickBitmapWord(w)
            if word == nil || word.Sign() == 0 {
                if w == -3465 { break }
                continue
            }
            startBit := bitOffset - 1
            if w < wordIndex { startBit = 255 }
            if startBit < 0 { startBit = 0 } // handle negative
            for b := startBit; b >= 0; b-- {
                if word.Bit(b) == 1 {
                    tickFound := int32(w*256 + b)
                    if tickFound >= tick { continue }
                    return tickFound, true
                }
            }
            bitOffset = 255
            if w == -3465 { break }
        }
    } else {
        for w := wordIndex; w <= 3465; w++ {
            word := pool.GetTickBitmapWord(w)
            if word == nil || word.Sign() == 0 {
                if w == 3465 { break }
                continue
            }
            startBit := bitOffset + 1
            if w > wordIndex { startBit = 0 }
            for b := startBit; b < 256; b++ {
                if word.Bit(b) == 1 {
                    tickFound := int32(w*256 + b)
                    if tickFound <= tick { continue }
                    return tickFound, true
                }
            }
            bitOffset = -1
            if w == 3465 { break }
        }
    }
    return 0, false
}

// ---- Canonical Uniswap V3 amount deltas ----

func (c *V3Calculator) getAmount0Delta(sqrtPriceA, sqrtPriceB *big.Int, liquidity *big.Int, roundUp bool) *big.Int {
	diff := new(big.Int).Sub(sqrtPriceB, sqrtPriceA)
	if diff.Sign() < 0 {
		diff.Neg(diff)
	}
	num := new(big.Int).Mul(liquidity, diff)
	num.Mul(num, new(big.Int).SetUint64(1<<96))
	den := new(big.Int).Mul(sqrtPriceA, sqrtPriceB)
	res := new(big.Int).Div(num, den)
	if roundUp && res.Sign() > 0 {
		if new(big.Int).Mod(num, den).Sign() > 0 {
			res.Add(res, big.NewInt(1))
		}
	}
	return res
}

func (c *V3Calculator) getAmount1Delta(sqrtPriceA, sqrtPriceB *big.Int, liquidity *big.Int, roundUp bool) *big.Int {
	diff := new(big.Int).Sub(sqrtPriceB, sqrtPriceA)
	if diff.Sign() < 0 {
		diff.Neg(diff)
	}
	num := new(big.Int).Mul(liquidity, diff)
	shift := new(big.Int).SetUint64(1 << 96)
	res := new(big.Int).Div(num, shift)
	if roundUp && res.Sign() > 0 {
		if new(big.Int).Mod(num, shift).Sign() > 0 {
			res.Add(res, big.NewInt(1))
		}
	}
	return res
}

// ---- Partial helpers – unchanged ----

func (c *V3Calculator) getNextSqrtPriceFromAmount0RoundingUpPartial(sqrtPriceX96, liquidity, amount *big.Int) *big.Int {
	if amount.Sign() == 0 {
		return new(big.Int).Set(sqrtPriceX96)
	}
	c.numerator.Mul(liquidity, sqrtPriceX96)
	c.numerator.Mul(&c.numerator, &c.Q96big)

	c.denominator.Mul(liquidity, &c.Q96big)
	c.product.Mul(amount, sqrtPriceX96)
	c.denominator.Add(&c.denominator, &c.product)

	c.tmp.Sub(&c.denominator, &c.one)
	c.numerator.Add(&c.numerator, &c.tmp)
	result := new(big.Int).Div(&c.numerator, &c.denominator)
	if result.Cmp(&c.minSqrt) < 0 {
		result.Set(&c.minSqrt)
	}
	if result.Cmp(&c.maxSqrt) > 0 {
		result.Set(&c.maxSqrt)
	}
	return result
}

func (c *V3Calculator) getNextSqrtPriceFromAmount1RoundingDownPartial(sqrtPriceX96, liquidity, amount *big.Int) *big.Int {
	if amount.Sign() == 0 {
		return new(big.Int).Set(sqrtPriceX96)
	}
	c.product.Mul(amount, &c.Q96big)
	c.priceDelta.Div(&c.product, liquidity)
	result := new(big.Int).Add(sqrtPriceX96, &c.priceDelta)
	if result.Cmp(&c.minSqrt) < 0 {
		result.Set(&c.minSqrt)
	}
	if result.Cmp(&c.maxSqrt) > 0 {
		result.Set(&c.maxSqrt)
	}
	return result
}

func (c *V3Calculator) getNextSqrtPriceFromAmount0RoundingUp(sqrtPriceX96, liquidity, amount, result *big.Int) error {
	if amount.Sign() == 0 {
		result.Set(sqrtPriceX96)
		return nil
	}
	c.numerator.Mul(liquidity, sqrtPriceX96)
	c.numerator.Mul(&c.numerator, &c.Q96big)

	c.denominator.Mul(liquidity, &c.Q96big)
	c.product.Mul(amount, sqrtPriceX96)
	c.denominator.Add(&c.denominator, &c.product)

	c.tmp.Sub(&c.denominator, &c.one)
	c.numerator.Add(&c.numerator, &c.tmp)
	result.Div(&c.numerator, &c.denominator)

	if result.Cmp(&c.minSqrt) < 0 {
		result.Set(&c.minSqrt)
	}
	if result.Cmp(&c.maxSqrt) > 0 {
		result.Set(&c.maxSqrt)
	}
	return nil
}

func (c *V3Calculator) getNextSqrtPriceFromAmount1RoundingDown(sqrtPriceX96, liquidity, amount, result *big.Int) error {
	if amount.Sign() == 0 {
		result.Set(sqrtPriceX96)
		return nil
	}
	c.product.Mul(amount, &c.Q96big)
	c.priceDelta.Div(&c.product, liquidity)
	result.Add(sqrtPriceX96, &c.priceDelta)

	if result.Cmp(&c.minSqrt) < 0 {
		result.Set(&c.minSqrt)
	}
	if result.Cmp(&c.maxSqrt) > 0 {
		result.Set(&c.maxSqrt)
	}
	return nil
}
