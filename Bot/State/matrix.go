// Package state provides a high-frequency zero-allocation state engine for Base.
package state

import (
	"bytes"
	"context"
	"fmt"
	"math/big"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"

	"my-mev-bot/Bot/Types"
)

const (
	// Capped exactly at our 6 DEX * 12 Pair limit to protect active simulation memory
	MaxPools = 36
	// MaxTokens expanded to support our 6 Base Pairs + 60 Dynamic Memes + buffer safety
	MaxTokens = 72
)

type PoolStats struct {
	mu                sync.RWMutex
	SwapsWindow1m     int64
	SwapsWindow5m     int64
	EdgeLifetimeEmaMs float64
	SimPass           uint64
	Broadcast         uint64
	Win               uint64
	Fail              uint64
	LastLiquidityUSD  float64
	LastUpdated       time.Time
	SwapTimestamps    []time.Time
}

func newPoolStats() *PoolStats {
	return &PoolStats{
		SwapTimestamps: make([]time.Time, 0, 64),
		LastUpdated:    time.Now(),
	}
}

func (ps *PoolStats) UpdateSwapWindow(now time.Time) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	ps.SwapTimestamps = append(ps.SwapTimestamps, now)
	cutoff5m := now.Add(-5 * time.Minute)
	newIdx := 0
	for _, ts := range ps.SwapTimestamps {
		if ts.After(cutoff5m) {
			ps.SwapTimestamps[newIdx] = ts
			newIdx++
		}
	}
	ps.SwapTimestamps = ps.SwapTimestamps[:newIdx]

	cutoff1m := now.Add(-1 * time.Minute)
	ps.SwapsWindow1m = 0
	ps.SwapsWindow5m = int64(len(ps.SwapTimestamps))
	for _, ts := range ps.SwapTimestamps {
		if ts.After(cutoff1m) {
			ps.SwapsWindow1m++
		}
	}
}

type Matrix struct {
	mu               sync.RWMutex
	pools            map[common.Address]*types.PoolState
	affectedPaths    map[common.Address][]*types.Path
	tokenPairToPools map[[2]common.Address][]*types.PoolState
	poolStats        map[common.Address]*PoolStats

	tokenToSlot map[common.Address]uint8
	slotToToken [MaxTokens]common.Address
	matrix      [MaxTokens][MaxTokens]uint64

	ethClient *ethclient.Client

	// Only factory ABI kept for CallFactory – used by gatekeeper.
	factoryABI abi.ABI
}

func NewMatrix() *Matrix {
	// Only the factory ABI is needed for CallFactory.
	factoryABI, _ := abi.JSON(strings.NewReader(`[{"type":"function","name":"factory","inputs":[],"outputs":[{"type":"address"}]}]`))

	m := &Matrix{
		pools:            make(map[common.Address]*types.PoolState, MaxPools),
		affectedPaths:    make(map[common.Address][]*types.Path, MaxPools),
		tokenPairToPools: make(map[[2]common.Address][]*types.PoolState, MaxPools),
		poolStats:        make(map[common.Address]*PoolStats, MaxPools),
		tokenToSlot:      make(map[common.Address]uint8, MaxTokens),
		factoryABI:       factoryABI,
	}

	return m
}

func (m *Matrix) SetEthClient(client *ethclient.Client) { m.ethClient = client }

// CallFactory fetches the factory address from a pool contract.
// Used by the gatekeeper for dynamic discovery.
func (m *Matrix) CallFactory(ctx context.Context, pool common.Address) (common.Address, error) {
	data, _ := m.factoryABI.Pack("factory")
	out, err := m.ethClient.CallContract(ctx, ethereum.CallMsg{To: &pool, Data: data}, nil)
	if err != nil {
		return common.Address{}, err
	}
	var result common.Address
	_ = m.factoryABI.UnpackIntoInterface(&result, "factory", out)
	return result, nil
}

func (m *Matrix) RegisterPath(path *types.Path) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, pool := range path.Pools {
		m.normalizePoolState(pool)
		if _, exists := m.pools[pool.PoolAddress]; !exists {
			m.pools[pool.PoolAddress] = pool
			m.addToTokenPairIndexLocked(pool)
			m.poolStats[pool.PoolAddress] = newPoolStats()
		}
	}
	for _, pool := range path.Pools {
		m.affectedPaths[pool.PoolAddress] = append(m.affectedPaths[pool.PoolAddress], path)
	}
}

func (m *Matrix) RegisterPool(pool *types.PoolState) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.pools[pool.PoolAddress]; exists {
		return
	}
	m.normalizePoolState(pool)
	pool.LastUpdated = time.Now()
	m.pools[pool.PoolAddress] = pool
	m.addToTokenPairIndexLocked(pool)
	m.poolStats[pool.PoolAddress] = newPoolStats()

	// High-performance cache limit maintenance
	if len(m.pools) > MaxPools {
		m.evictLRULocked()
	}
}

func (m *Matrix) evictLRULocked() {
	if len(m.pools) == 0 {
		return
	}
	var oldest time.Time
	var oldestAddr common.Address
	for addr, p := range m.pools {
		if oldest.IsZero() || p.LastUpdated.Before(oldest) {
			oldest = p.LastUpdated
			oldestAddr = addr
		}
	}
	if oldestAddr != (common.Address{}) {
		delete(m.pools, oldestAddr)
		delete(m.poolStats, oldestAddr)
		for pair, pools := range m.tokenPairToPools {
			var newSlice []*types.PoolState
			for _, p := range pools {
				if p.PoolAddress != oldestAddr {
					newSlice = append(newSlice, p)
				}
			}
			if len(newSlice) == 0 {
				delete(m.tokenPairToPools, pair)
			} else {
				m.tokenPairToPools[pair] = newSlice
			}
		}
	}
}

func (m *Matrix) normalizePoolState(pool *types.PoolState) {
	if pool.Reserve0 == nil {
		pool.Reserve0 = new(big.Int)
	}
	if pool.Reserve1 == nil {
		pool.Reserve1 = new(big.Int)
	}
	if pool.SqrtPriceX96 == nil {
		pool.SqrtPriceX96 = new(big.Int)
	}
	if pool.Liquidity == nil {
		pool.Liquidity = new(big.Int)
	}
}

func (m *Matrix) addToTokenPairIndexLocked(pool *types.PoolState) {
	key := canonicalPair(pool.Token0, pool.Token1)
	m.tokenPairToPools[key] = append(m.tokenPairToPools[key], pool)
}

func canonicalPair(a, b common.Address) [2]common.Address {
	if bytes.Compare(a.Bytes(), b.Bytes()) < 0 {
		return [2]common.Address{a, b}
	}
	return [2]common.Address{b, a}
}

// UpdateFromLog updates a known pool's state from a swap log.
// It assumes the pool already exists in the matrix (unknown pools are handled by the gatekeeper).
func (m *Matrix) UpdateFromLog(log *types.SwapLog) []*types.Path {
	m.mu.RLock()
	pool := m.pools[log.Address]
	m.mu.RUnlock()
	if pool == nil {
		return nil
	}

	pool.Lock()
	oldReserve0 := new(big.Int).Set(pool.Reserve0)
	oldReserve1 := new(big.Int).Set(pool.Reserve1)
	oldSqrtPrice := new(big.Int).Set(pool.SqrtPriceX96)

	switch pool.DexType {
	case types.DexUniswapV3, types.DexPancakeV3:
		m.updateV3Pool(pool, log, oldSqrtPrice)
	default:
		m.updateV2Pool(pool, log, oldReserve0, oldReserve1)
	}
	pool.LastUpdated = time.Now()
	pool.Unlock()

	m.mu.RLock()
	stats := m.poolStats[log.Address]
	paths := m.affectedPaths[log.Address]
	m.mu.RUnlock()

	if stats != nil {
		stats.UpdateSwapWindow(time.Now())
		var liquidityUSD float64
		pool.RLock()
		if pool.DexType == types.DexUniswapV3 || pool.DexType == types.DexPancakeV3 {
			liquidityUSD = pool.LiquidityFloat / 1e18
		} else {
			sqrtProduct := new(big.Float).Sqrt(new(big.Float).Mul(big.NewFloat(pool.Reserve0Float), big.NewFloat(pool.Reserve1Float)))
			val, _ := sqrtProduct.Float64()
			liquidityUSD = val / 1e12
		}
		pool.RUnlock()
		stats.mu.Lock()
		stats.LastLiquidityUSD = liquidityUSD
		stats.LastUpdated = time.Now()
		stats.mu.Unlock()
	}

	return paths
}

// updateV2Pool – robust direction inference.
func (m *Matrix) updateV2Pool(pool *types.PoolState, log *types.SwapLog, oldR0, oldR1 *big.Int) {
	if log.AmountIn == nil || log.AmountOut == nil {
		return
	}

	if log.TokenIn != (common.Address{}) && log.TokenOut != (common.Address{}) {
		if log.TokenIn == pool.Token0 && log.TokenOut == pool.Token1 {
			pool.Reserve0.Add(pool.Reserve0, log.AmountIn)
			pool.Reserve1.Sub(pool.Reserve1, log.AmountOut)
		} else if log.TokenIn == pool.Token1 && log.TokenOut == pool.Token0 {
			pool.Reserve1.Add(pool.Reserve1, log.AmountIn)
			pool.Reserve0.Sub(pool.Reserve0, log.AmountOut)
		} else {
			pool.Reserve0.Add(pool.Reserve0, log.AmountIn)
			pool.Reserve1.Sub(pool.Reserve1, log.AmountOut)
		}
	} else {
		amountIn := log.AmountIn
		amountOut := log.AmountOut
		feeBps := pool.FeeBps
		if feeBps == 0 || feeBps >= 10000 {
			feeBps = 30
		}
		feeNumerator := uint64(10000 - feeBps)

		computeExpectedOut := func(reserveIn, reserveOut, amountIn *big.Int) *big.Int {
			num := new(big.Int).Mul(reserveOut, amountIn)
			num.Mul(num, new(big.Int).SetUint64(feeNumerator))
			den := new(big.Int).Mul(reserveIn, new(big.Int).SetUint64(10000))
			den.Add(den, new(big.Int).Mul(amountIn, new(big.Int).SetUint64(feeNumerator)))
			return new(big.Int).Div(num, den)
		}

		newR0A := new(big.Int).Add(oldR0, amountIn)
		newR1A := new(big.Int).Sub(oldR1, amountOut)
		newR0B := new(big.Int).Sub(oldR0, amountOut)
		newR1B := new(big.Int).Add(oldR1, amountIn)
		okA := newR1A.Sign() >= 0
		okB := newR0B.Sign() >= 0

		expectedOutA := computeExpectedOut(oldR0, oldR1, amountIn)
		expectedOutB := computeExpectedOut(oldR1, oldR0, amountIn)

		tolerance := new(big.Int).Div(amountOut, big.NewInt(1000))
		if tolerance.Sign() == 0 {
			tolerance = big.NewInt(1)
		}
		diffA := new(big.Int).Sub(expectedOutA, amountOut)
		if diffA.Sign() < 0 {
			diffA.Neg(diffA)
		}
		diffB := new(big.Int).Sub(expectedOutB, amountOut)
		if diffB.Sign() < 0 {
			diffB.Neg(diffB)
		}

		matchA := diffA.Cmp(tolerance) <= 0
		matchB := diffB.Cmp(tolerance) <= 0

		if okA && matchA && !okB {
			pool.Reserve0.Set(newR0A)
			pool.Reserve1.Set(newR1A)
			log.TokenIn = pool.Token0
			log.TokenOut = pool.Token1
		} else if okB && matchB && !okA {
			pool.Reserve0.Set(newR0B)
			pool.Reserve1.Set(newR1B)
			log.TokenIn = pool.Token1
			log.TokenOut = pool.Token0
		} else if okA && matchA && okB && matchB {
			if diffA.Cmp(diffB) <= 0 {
				pool.Reserve0.Set(newR0A)
				pool.Reserve1.Set(newR1A)
				log.TokenIn = pool.Token0
				log.TokenOut = pool.Token1
			} else {
				pool.Reserve0.Set(newR0B)
				pool.Reserve1.Set(newR1B)
				log.TokenIn = pool.Token1
				log.TokenOut = pool.Token0
			}
		} else {
			if okA {
				pool.Reserve0.Set(newR0A)
				pool.Reserve1.Set(newR1A)
				log.TokenIn = pool.Token0
				log.TokenOut = pool.Token1
			} else if okB {
				pool.Reserve0.Set(newR0B)
				pool.Reserve1.Set(newR1B)
				log.TokenIn = pool.Token1
				log.TokenOut = pool.Token0
			} else {
				pool.Reserve0.Set(oldR0)
				pool.Reserve1.Set(oldR1)
				log.TokenIn = pool.Token0
				log.TokenOut = pool.Token1
			}
		}
	}

	if pool.Reserve0.Sign() < 0 {
		pool.Reserve0.SetInt64(0)
	}
	if pool.Reserve1.Sign() < 0 {
		pool.Reserve1.SetInt64(0)
	}
	pool.Reserve0Float = float64FromBig(pool.Reserve0)
	pool.Reserve1Float = float64FromBig(pool.Reserve1)
}

// updateV3Pool – correctly computes reserve floats and sets direction.
func (m *Matrix) updateV3Pool(pool *types.PoolState, log *types.SwapLog, oldSqrt *big.Int) {
	if log.SqrtPriceX96 != nil {
		pool.SqrtPriceX96.Set(log.SqrtPriceX96)
		pool.SqrtPriceX96Float = float64FromBig(pool.SqrtPriceX96)
	}
	if log.Liquidity != nil {
		pool.Liquidity.Set(log.Liquidity)
		pool.LiquidityFloat = float64FromBig(pool.Liquidity)
	}
	if log.Tick != 0 || log.SqrtPriceX96 != nil {
		pool.Tick = log.Tick
	}

	const Q96 = 79228162514264337593543950336.0
	if pool.SqrtPriceX96Float > 0 && pool.LiquidityFloat > 0 {
		sqrtPrice := pool.SqrtPriceX96Float / Q96
		pool.Reserve0Float = pool.LiquidityFloat / sqrtPrice
		pool.Reserve1Float = pool.LiquidityFloat * sqrtPrice
	} else {
		pool.Reserve0Float = 0
		pool.Reserve1Float = 0
	}

	if log.TokenIn != (common.Address{}) && log.TokenOut != (common.Address{}) {
		// Already set
	} else if oldSqrt != nil && oldSqrt.Sign() > 0 && pool.SqrtPriceX96.Cmp(oldSqrt) > 0 {
		log.TokenIn = pool.Token1
		log.TokenOut = pool.Token0
	} else if oldSqrt != nil && oldSqrt.Sign() > 0 && pool.SqrtPriceX96.Cmp(oldSqrt) < 0 {
		log.TokenIn = pool.Token0
		log.TokenOut = pool.Token1
	} else {
		log.TokenIn = pool.Token0
		log.TokenOut = pool.Token1
	}
}

func float64FromBig(v *big.Int) float64 {
	if v == nil {
		return 0
	}
	f, _ := new(big.Float).SetInt(v).Float64()
	return f
}

func (m *Matrix) GetPoolStats(poolAddr common.Address) *PoolStats {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.poolStats[poolAddr]
}

func (m *Matrix) GetPoolScore(poolAddr common.Address) float64 {
	stats := m.GetPoolStats(poolAddr)
	if stats == nil {
		return 0.5
	}
	stats.mu.RLock()
	defer stats.mu.RUnlock()

	heat := float64(stats.SwapsWindow1m) / 100.0
	if heat > 1.0 {
		heat = 1.0
	}
	var missRate float64
	if stats.Broadcast > 10 {
		missRate = 1.0 - float64(stats.Win)/float64(stats.Broadcast)
	}
	thinness := 0.0
	if stats.LastLiquidityUSD > 0 {
		thinness = 1.0 - (stats.LastLiquidityUSD / 1000000.0)
		if thinness < 0 {
			thinness = 0
		}
		if thinness > 1 {
			thinness = 1
		}
	}
	score := 0.35*heat + 0.20*missRate + 0.10*thinness
	if score > 1 {
		score = 1
	}
	if score < 0 {
		score = 0
	}
	return score
}

func (m *Matrix) RecordBroadcast(poolAddr common.Address) {
	if stats := m.poolStats[poolAddr]; stats != nil {
		stats.mu.Lock()
		stats.Broadcast++
		stats.mu.Unlock()
	}
}

func (m *Matrix) RecordWin(poolAddr common.Address) {
	if stats := m.poolStats[poolAddr]; stats != nil {
		stats.mu.Lock()
		stats.Win++
		stats.mu.Unlock()
	}
}

func (m *Matrix) RecordFail(poolAddr common.Address) {
	if stats := m.poolStats[poolAddr]; stats != nil {
		stats.mu.Lock()
		stats.Fail++
		stats.mu.Unlock()
	}
}

func (m *Matrix) GetPathsForPool(poolAddress common.Address) []*types.Path {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.affectedPaths[poolAddress]
}

func (m *Matrix) GetPool(address common.Address) *types.PoolState {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.pools[address]
}

func (m *Matrix) GetPoolsForPair(tokenA, tokenB common.Address) []*types.PoolState {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.tokenPairToPools[canonicalPair(tokenA, tokenB)]
}

func (m *Matrix) GetPools() map[common.Address]*types.PoolState {
	m.mu.RLock()
	defer m.mu.RUnlock()
	cpy := make(map[common.Address]*types.PoolState, len(m.pools))
	for k, v := range m.pools {
		cpy[k] = v
	}
	return cpy
}

func (m *Matrix) RegisterToken(token common.Address, slot uint8) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if slot >= MaxTokens {
		return
	}
	m.tokenToSlot[token] = slot
	m.slotToToken[slot] = token
}

func (m *Matrix) GetSlot(token common.Address) uint8 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if slot, ok := m.tokenToSlot[token]; ok {
		return slot
	}
	return 255
}

func (m *Matrix) UpdateMatrixValue(row, col uint8, value uint64) {
	if row >= MaxTokens || col >= MaxTokens {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.matrix[row][col] = value
}

func (m *Matrix) GetMatrixValue(row, col uint8) uint64 {
	if row >= MaxTokens || col >= MaxTokens {
		return 0
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.matrix[row][col]
}
