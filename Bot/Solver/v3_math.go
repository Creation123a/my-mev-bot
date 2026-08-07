package solver

import (
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"my-mev-bot/types"
)

const (
	Q96 = 79228162514264337593543950336

	// Uniswap V3 sqrt price bounds (Q64.96)
	MIN_SQRT_RATIO = 4295128739   // 2^32 - 2^32 / 2^32? Actually 4295128740 is MIN_SQRT_RATIO per Uniswap V3.
	MAX_SQRT_RATIO = 1461446703485210103287273052203988822378723970342 // (2^128 - 1) / 2? Actually correct constant.
)

// V3Calculator is a stateless struct that holds reusable big.Int buffers.
// It is safe to use one instance per goroutine.
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
	tmp               big.Int // temporary buffer
}

// NewV3Calculator creates a new calculator with pre‑allocated buffers.
func NewV3Calculator() *V3Calculator {
	c := &V3Calculator{}
	c.Q96big.SetString("79228162514264337593543950336", 10)
	c.maxSqrt.SetString("1461446703485210103287273052203988822378723970342", 10)
	c.minSqrt.SetInt64(4295128739) // or 4295128740
	return c
}

// ComputeSwap calculates the output amount for a V3 swap using the calculator's buffers.
// It stores the result in `result` and returns an error.
// This function caps the input to stay within the global sqrt price bounds to avoid
// unrealistic quotes when the input would cross the entire price range. The result
// should be treated as an upper bound and validated by simulation.
func (c *V3Calculator) ComputeSwap(pool *types.PoolState, tokenIn, tokenOut common.Address, amountIn *big.Int, result *big.Int) error {
	if amountIn == nil || amountIn.Sign() <= 0 {
		result.SetInt64(0)
		return nil
	}

	// Determine swap direction.
	zeroForOne := pool.Token0 == tokenIn && pool.Token1 == tokenOut
	if !zeroForOne && !(pool.Token0 == tokenOut && pool.Token1 == tokenIn) {
		return fmt.Errorf("tokenIn/tokenOut not in pool")
	}

	// Copy pool state into buffers.
	c.sqrtPriceX96.Set(pool.SqrtPriceX96)
	if c.sqrtPriceX96.Sign() == 0 {
		return fmt.Errorf("zero sqrtPriceX96")
	}
	c.liquidity.Set(pool.Liquidity)
	if c.liquidity.Sign() == 0 {
		return fmt.Errorf("zero liquidity")
	}
	feeBps := pool.FeeBps

	// Clamp feeBps to valid range [0, 10000).
	if feeBps >= 10000 {
		feeBps = 9999 // clamp to max valid basis points
	}
	if feeBps == 0 {
		feeBps = 30 // default 0.3% in basis points
	}

	// Fee calculation: feeBps is in basis points (e.g., 30 = 0.3%).
	feeDenominator := big.NewInt(10000)
	feeNumerator := big.NewInt(int64(10000 - feeBps))
	c.amountInAfterFee.Mul(amountIn, feeNumerator)
	c.amountInAfterFee.Div(&c.amountInAfterFee, feeDenominator)

	// Cap the effective input to avoid crossing the global sqrt price bounds.
	// This prevents overestimation for extremely large trades.
	// For zeroForOne (token0 input), price decreases; minimum sqrt price is minSqrt.
	if zeroForOne {
		// amount0_max = liquidity * Q96 * (sqrtPrice - minSqrt) / (sqrtPrice * minSqrt)
		c.tmp.Sub(&c.sqrtPriceX96, &c.minSqrt)          // (sqrtPrice - minSqrt)
		c.tmp.Mul(&c.tmp, &c.liquidity)                 // liquidity * (sqrtPrice - minSqrt)
		c.tmp.Mul(&c.tmp, &c.Q96big)                    // ... * Q96
		c.numerator.Set(&c.tmp)
		c.denominator.Mul(&c.sqrtPriceX96, &c.minSqrt)  // sqrtPrice * minSqrt
		c.tmp.Div(&c.numerator, &c.denominator)         // amount0_max
		if c.amountInAfterFee.Cmp(&c.tmp) > 0 {
			c.amountInAfterFee.Set(&c.tmp)
		}
	} else {
		// amount1_max = liquidity * (maxSqrt - sqrtPrice) / Q96
		c.tmp.Sub(&c.maxSqrt, &c.sqrtPriceX96)          // (maxSqrt - sqrtPrice)
		c.tmp.Mul(&c.tmp, &c.liquidity)                 // liquidity * (maxSqrt - sqrtPrice)
		c.tmp.Div(&c.tmp, &c.Q96big)                    // amount1_max
		if c.amountInAfterFee.Cmp(&c.tmp) > 0 {
			c.amountInAfterFee.Set(&c.tmp)
		}
	}
	if c.amountInAfterFee.Sign() == 0 {
		result.SetInt64(0)
		return nil
	}

	if zeroForOne {
		// Swap token0 → token1.
		if err := c.getNextSqrtPriceFromAmount0RoundingUp(&c.sqrtPriceX96, &c.liquidity, &c.amountInAfterFee, &c.sqrtPriceNextX96); err != nil {
			return err
		}
		if c.sqrtPriceNextX96.Cmp(&c.sqrtPriceX96) >= 0 {
			return fmt.Errorf("sqrtPriceNext >= sqrtPriceX96 for zeroForOne")
		}
		// amount1 = liquidity * (sqrtP - sqrtNext) / Q96
		c.diff.Sub(&c.sqrtPriceX96, &c.sqrtPriceNextX96)
		c.numerator.Mul(&c.liquidity, &c.diff)
		result.Div(&c.numerator, &c.Q96big)
	} else {
		// Swap token1 → token0.
		if err := c.getNextSqrtPriceFromAmount1RoundingDown(&c.sqrtPriceX96, &c.liquidity, &c.amountInAfterFee, &c.sqrtPriceNextX96); err != nil {
			return err
		}
		if c.sqrtPriceNextX96.Cmp(&c.sqrtPriceX96) <= 0 {
			return fmt.Errorf("sqrtPriceNext <= sqrtPriceX96 for zeroForOne=false")
		}
		// amount0 = liquidity * Q96 * (sqrtNext - sqrtP) / (sqrtP * sqrtNext)
		c.diff.Sub(&c.sqrtPriceNextX96, &c.sqrtPriceX96)
		c.numerator.Mul(&c.liquidity, &c.Q96big)
		c.numerator.Mul(&c.numerator, &c.diff)
		c.denominator.Mul(&c.sqrtPriceX96, &c.sqrtPriceNextX96)
		result.Div(&c.numerator, &c.denominator)
	}
	return nil
}

// getNextSqrtPriceFromAmount0RoundingUp implements Uniswap V3's formula for adding token0.
// Uses the correct formula: sqrtNext = ceil(L * sqrtP * Q96 / (L * Q96 + amount0 * sqrtP))
func (c *V3Calculator) getNextSqrtPriceFromAmount0RoundingUp(sqrtPriceX96, liquidity, amount, result *big.Int) error {
	if amount.Sign() == 0 {
		result.Set(sqrtPriceX96)
		return nil
	}
	// Compute numerator = liquidity * sqrtPriceX96 * Q96
	c.numerator.Mul(liquidity, sqrtPriceX96)
	c.numerator.Mul(&c.numerator, &c.Q96big)

	// Compute denominator = liquidity * Q96 + amount * sqrtPriceX96
	c.denominator.Mul(liquidity, &c.Q96big)
	c.product.Mul(amount, sqrtPriceX96)
	c.denominator.Add(&c.denominator, &c.product)

	// result = ceil(numerator / denominator) -> add denominator - 1 before division.
	c.numerator.Add(&c.numerator, new(big.Int).Sub(&c.denominator, big.NewInt(1)))
	result.Div(&c.numerator, &c.denominator)

	// Ensure result is within bounds.
	if result.Cmp(&c.minSqrt) < 0 {
		result.Set(&c.minSqrt)
	}
	if result.Cmp(&c.maxSqrt) > 0 {
		result.Set(&c.maxSqrt)
	}
	return nil
}

// getNextSqrtPriceFromAmount1RoundingDown implements Uniswap V3's formula for adding token1.
// sqrtNext = floor(sqrtP + amount1 * Q96 / liquidity)
func (c *V3Calculator) getNextSqrtPriceFromAmount1RoundingDown(sqrtPriceX96, liquidity, amount, result *big.Int) error {
	if amount.Sign() == 0 {
		result.Set(sqrtPriceX96)
		return nil
	}
	// priceDelta = amount * Q96 / liquidity
	c.product.Mul(amount, &c.Q96big)
	c.priceDelta.Div(&c.product, liquidity)
	result.Add(sqrtPriceX96, &c.priceDelta)

	// Ensure result is within bounds.
	if result.Cmp(&c.minSqrt) < 0 {
		result.Set(&c.minSqrt)
	}
	if result.Cmp(&c.maxSqrt) > 0 {
		result.Set(&c.maxSqrt)
	}
	return nil
}
