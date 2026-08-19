package solver

import (
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"my-mev-bot/Bot/Types"
)

const (
	Q96 = 79228162514264337593543950336
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
	one               big.Int // value 1, reused to avoid allocation
}

// NewV3Calculator creates a new calculator with pre‑allocated buffers.
func NewV3Calculator() *V3Calculator {
	c := &V3Calculator{}
	c.Q96big.SetString("79228162514264337593543950336", 10)
	c.maxSqrt.SetString("1461446703485210103287273052203988822378723970342", 10)
	c.minSqrt.SetInt64(4295128739)
	c.one.SetInt64(1)
	return c
}

// ComputeSwap calculates the output amount for a V3 swap.
func (c *V3Calculator) ComputeSwap(pool *types.PoolState, tokenIn, tokenOut common.Address, amountIn *big.Int, result *big.Int) error {
	if amountIn == nil || amountIn.Sign() <= 0 {
		result.SetInt64(0)
		return nil
	}

	zeroForOne := pool.Token0 == tokenIn && pool.Token1 == tokenOut
	if !zeroForOne && !(pool.Token0 == tokenOut && pool.Token1 == tokenIn) {
		return fmt.Errorf("tokenIn/tokenOut not in pool")
	}

	// --- FIX: Use safe getter to avoid data race ---
	pool.CopySqrtAndLiquidity(&c.sqrtPriceX96, &c.liquidity)
	// ------------------------------------------------

	if c.sqrtPriceX96.Sign() == 0 {
		return fmt.Errorf("zero sqrtPriceX96")
	}
	if c.liquidity.Sign() == 0 {
		return fmt.Errorf("zero liquidity")
	}
	feeBps := pool.FeeBps

	// FIX: Normalise fee to pips if it's stored as basis points (<=100).
	// Uniswap V3 fees are in pips (e.g., 500, 3000, 10000).
	if feeBps <= 100 {
		feeBps = feeBps * 100
	}
	if feeBps >= 10000 {
		feeBps = 9999
	}
	if feeBps == 0 {
		feeBps = 3000 // default 0.3% pips
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

// getNextSqrtPriceFromAmount0RoundingUp implements Uniswap V3's formula for adding token0.
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

	// ceil division: add denominator-1 using pre-allocated buffers.
	c.tmp.Sub(&c.denominator, &c.one) // denominator - 1
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

// getNextSqrtPriceFromAmount1RoundingDown implements Uniswap V3's formula for adding token1.
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
