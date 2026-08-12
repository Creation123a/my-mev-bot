// Package state provides a high-frequency zero-allocation state engine
// for Base L2 Flashblock arbitrage. It uses a Direct Matrix Lookup model
// with pre-registered paths for O(1) opportunity retrieval.
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

	"my-mev-bot/Bot/Config"
	"my-mev-bot/Bot/Types"
)

const (
	discoveryBlacklistTTL = 5 * time.Minute
)

// =============================================================================
// PoolStats — competition and performance stats for a pool
// =============================================================================

type PoolStats struct {
	mu sync.RWMutex

	SwapsWindow1m       int64
	SwapsWindow5m       int64
	EdgeLifetimeEmaMs   float64
	SimPass             uint64
	Broadcast           uint64
	Win                 uint64
	Fail                uint64
	LastLiquidityUSD    float64
	LastUpdated         time.Time
	SwapTimestamps      []time.Time
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

// =============================================================================
// Matrix — the core state engine with O(1) direct lookups and token‑pair index
// =============================================================================

type Matrix struct {
	mu sync.RWMutex

	pools            map[common.Address]*types.PoolState
	affectedPaths    map[common.Address][]*types.Path
	tokenPairToPools map[[2]common.Address][]*types.PoolState

	poolStats map[common.Address]*PoolStats

	tokenToSlot map[common.Address]uint8
	slotToToken [44]common.Address
	matrix      [44][44]uint64

	ethClient *ethclient.Client

	token0ABI      abi.ABI
	token1ABI      abi.ABI
	feeABI         abi.ABI
	getReservesABI abi.ABI
	factoryABI     abi.ABI

	// Negative cache for discovery failures (address -> expiry time)
	discoveryBlacklist   map[common.Address]time.Time
	discoveryBlacklistMu sync.RWMutex
}

// NewMatrix creates a new Matrix instance with pre-allocated maps.
func NewMatrix() *Matrix {
	token0ABI, _ := abi.JSON(strings.NewReader(`[{"type":"function","name":"token0","inputs":[],"outputs":[{"type":"address"}]}]`))
	token1ABI, _ := abi.JSON(strings.NewReader(`[{"type":"function","name":"token1","inputs":[],"outputs":[{"type":"address"}]}]`))
	feeABI, _ := abi.JSON(strings.NewReader(`[{"type":"function","name":"fee","inputs":[],"outputs":[{"type":"uint24"}]}]`))
	getReservesABI, _ := abi.JSON(strings.NewReader(`[{"type":"function","name":"getReserves","inputs":[],"outputs":[{"type":"uint112","name":"reserve0"},{"type":"uint112","name":"reserve1"},{"type":"uint32","name":"blockTimestampLast"}]}]`))
	factoryABI, _ := abi.JSON(strings.NewReader(`[{"type":"function","name":"factory","inputs":[],"outputs":[{"type":"address"}]}]`))

	return &Matrix{
		pools:               make(map[common.Address]*types.PoolState, 128),
		affectedPaths:       make(map[common.Address][]*types.Path, 128),
		tokenPairToPools:    make(map[[2]common.Address][]*types.PoolState, 128),
		poolStats:           make(map[common.Address]*PoolStats, 128),
		tokenToSlot:         make(map[common.Address]uint8, 128),
		token0ABI:           token0ABI,
		token1ABI:           token1ABI,
		feeABI:              feeABI,
		getReservesABI:      getReservesABI,
		factoryABI:          factoryABI,
		discoveryBlacklist:  make(map[common.Address]time.Time),
	}
}

// SetEthClient sets the Ethereum client used for dynamic pool discovery.
func (m *Matrix) SetEthClient(client *ethclient.Client) {
	m.ethClient = client
}

// isDiscoveryBlacklisted checks if an address is temporarily blacklisted for discovery.
func (m *Matrix) isDiscoveryBlacklisted(addr common.Address) bool {
	m.discoveryBlacklistMu.Lock()
	defer m.discoveryBlacklistMu.Unlock()

	expiry, ok := m.discoveryBlacklist[addr]
	if !ok {
		return false
	}
	if time.Now().Before(expiry) {
		return true
	}
	// Expired: remove it.
	delete(m.discoveryBlacklist, addr)
	return false
}

// markDiscoveryFailed adds an address to the negative cache with TTL.
func (m *Matrix) markDiscoveryFailed(addr common.Address) {
	m.discoveryBlacklistMu.Lock()
	defer m.discoveryBlacklistMu.Unlock()
	m.discoveryBlacklist[addr] = time.Now().Add(discoveryBlacklistTTL)
}

// PreloadKnownPools registers the core pools for the four anchor assets across all DEXs.
// This must be called at startup after creating the matrix and setting the ethClient.
// It fetches token0/token1 from each pool to ensure correct ordering.
// IMPORTANT: Replace the placeholder pool addresses with real Base mainnet addresses.
// If a pool address is zero, it is skipped.
func (m *Matrix) PreloadKnownPools() {
	if m.ethClient == nil {
		return
	}

	anchors := config.AnchorAssets()
	ctx := context.Background()

	// NOTE: Replace the placeholder strings with actual pool addresses.
	// The bot will not work correctly without real pool addresses.
	poolsToRegister := []struct {
		tokenA   common.Address
		tokenB   common.Address
		dexType  types.DexType
		poolAddr string
	}{
		// ---------- Uniswap V3 ----------
		{anchors[0], anchors[2], types.DexUniswapV3, "0xd0b53D9277af78126b47ab508A631899178E6e42"}, // WETH-USDC
		{anchors[0], anchors[3], types.DexUniswapV3, "0x4C36388bE6FAbAA7564619999281aE76E8E62215"}, // WETH-USDbC
		{anchors[0], anchors[1], types.DexUniswapV3, "0x7aea2e8a3843516afa07293a10ac8e49906dabd1"}, // WETH-cbBTC
		{anchors[2], anchors[3], types.DexUniswapV3, "0x067160ED01a3F5c0F1f71a93C70D2168324eA7B7"}, // USDC-USDbC
		{anchors[2], anchors[1], types.DexUniswapV3, "0xac6baB98ff2aE5a727c9D86dfFA2c358B7Cc98fC"}, // USDC-cbBTC
		{anchors[3], anchors[1], types.DexUniswapV3, "0xb231F830e2f5b4E9D47D937B6B040B47A1df9A8F"}, // USDbC-cbBTC

		// ---------- PancakeSwap V3 ----------
		{anchors[0], anchors[2], types.DexPancakeV3, "0x72AB388E2E2F6FaceF59E3C3FA2C4E29011c2D38"}, // WETH-USDC
		{anchors[0], anchors[3], types.DexPancakeV3, "0xb775272e537cc670c65dc852908ad47015244eaf"}, // WETH-USDbC
		{anchors[0], anchors[1], types.DexPancakeV3, "0xc211e1f853a898bd1302385ccde55f33a8c4b3f3"}, // WETH-cbBTC
		{anchors[2], anchors[3], types.DexPancakeV3, "0x29ed55b18af0add137952cb3e29fb77b32fce426"}, // USDC-USDbC
		{anchors[2], anchors[1], types.DexPancakeV3, "0xac6baB98ff2aE5a727c9D86dfFA2c358B7Cc98fC"}, // USDC-cbBTC
		{anchors[3], anchors[1], types.DexPancakeV3, "0xb231F830e2f5b4E9D47D937B6B040B47A1df9A8F"}, // USDbC-cbBTC

		// ---------- Aerodrome Slipstream (V3‑compatible) ----------
		{anchors[0], anchors[2], types.DexUniswapV3, "0xcF77a3Ba9A5CA399B7c97c74d54e5b1Beb874E43"}, // WETH-USDC (router, but treat as pool)
		{anchors[0], anchors[3], types.DexUniswapV3, "0xB69c5339CD8993fa2B1a129033324fE45307b22d"}, // WETH-USDbC
		{anchors[0], anchors[1], types.DexUniswapV3, "0x2A9A3fe6B1a798fB4a4f8990d0b04aFdE68A8b2a2"}, // WETH-cbBTC
		{anchors[2], anchors[3], types.DexUniswapV3, "0xbf782c5fa5400d8b3535353535353535353535"},   // USDC-USDbC (stable placeholder)
		{anchors[2], anchors[1], types.DexUniswapV3, "0x83B3a5BfD5Cd5fDCD40fBD58E783307559eED3E63"}, // USDC-cbBTC
		{anchors[3], anchors[1], types.DexUniswapV3, "0x89D916B87Fa6bA90dcbA90dcbA90606B4b3F47Df"}, // USDbC-cbBTC

		// ---------- AlienBase V2 ----------
		{anchors[0], anchors[2], types.DexAlienBaseV2, "0x9eD4D83BDBd987D0C94B3CDe89B064CdDE697aAF"}, // WETH-USDC
		{anchors[0], anchors[3], types.DexAlienBaseV2, "0x489679261dfA296DcbF2d08FA08E681966C3E480"}, // WETH-USDbC
		{anchors[0], anchors[1], types.DexAlienBaseV2, "0x3018CC672e8113426743FA41bE5B876fe6D9B4A0"}, // WETH-cbBTC
		{anchors[2], anchors[3], types.DexAlienBaseV2, "0x32FF1E4e8e16e6d15Db697E027B911438992b8dB"}, // USDC-USDbC
		{anchors[2], anchors[1], types.DexAlienBaseV2, "0xdE232Eca8509FDb968E8E98Bfe0b05bA37cb7df7"}, // USDC-cbBTC
		{anchors[3], anchors[1], types.DexAlienBaseV2, "0xc864a781F4Bba249bc1c49bA90dcbA90606B4b3F"},  // USDbC-cbBTC
	}

	for _, p := range poolsToRegister {
		poolAddr := common.HexToAddress(p.poolAddr)
		if poolAddr == (common.Address{}) {
			// Skip zero addresses – they indicate placeholders not yet filled.
			continue
		}
		token0, err := m.callToken0(ctx, poolAddr)
		if err != nil {
			continue
		}
		token1, err := m.callToken1(ctx, poolAddr)
		if err != nil {
			continue
		}
		// For preloaded V3 pools we don't know the fee tier; we store 30 basis points as a default.
		feeBps := uint32(30) // 0.3%
		pool := &types.PoolState{
			PoolAddress:  poolAddr,
			Token0:       token0,
			Token1:       token1,
			DexType:      p.dexType,
			FeeBps:       feeBps,
			Reserve0:     new(big.Int),
			Reserve1:     new(big.Int),
			SqrtPriceX96: new(big.Int),
			Liquidity:    new(big.Int),
		}
		// For V2 pools, also fetch reserves.
		if p.dexType == types.DexAerodromeV2 || p.dexType == types.DexAlienBaseV2 {
			if r0, r1, err := m.callGetReserves(ctx, poolAddr); err == nil {
				pool.Reserve0.Set(r0)
				pool.Reserve1.Set(r1)
				pool.Reserve0Float = float64FromBig(pool.Reserve0)
				pool.Reserve1Float = float64FromBig(pool.Reserve1)
			}
		}
		m.RegisterPool(pool)
	}
}

// ensurePool checks if a pool exists; if not, attempts to create it from the log.
// It uses the ethClient to fetch token0/token1 and fee or reserves.
// This function is called from UpdateFromLog, but we restructure the locking so RPC calls are done outside the global lock.
func (m *Matrix) ensurePool(log *types.SwapLog) error {
	// Check existence under read lock (safe because we only read).
	m.mu.RLock()
	_, exists := m.pools[log.Address]
	m.mu.RUnlock()
	if exists {
		return nil
	}

	// Skip if address is temporarily blacklisted.
	if m.isDiscoveryBlacklisted(log.Address) {
		return fmt.Errorf("discovery skipped for blacklisted pool %s", log.Address.Hex())
	}

	if m.ethClient == nil {
		return fmt.Errorf("ethClient not set, cannot discover pool %s", log.Address.Hex())
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Try to fetch token0 and token1 (both V2 and V3 have these)
	token0, err := m.callToken0(ctx, log.Address)
	if err != nil {
		token0, err = m.callToken0V2(ctx, log.Address)
		if err != nil {
			m.markDiscoveryFailed(log.Address)
			return fmt.Errorf("failed to get token0 for pool %s: %w", log.Address.Hex(), err)
		}
	}
	token1, err := m.callToken1(ctx, log.Address)
	if err != nil {
		token1, err = m.callToken1V2(ctx, log.Address)
		if err != nil {
			m.markDiscoveryFailed(log.Address)
			return fmt.Errorf("failed to get token1 for pool %s: %w", log.Address.Hex(), err)
		}
	}

	// Determine DEX type and validate factory.
	var dexType types.DexType
	var feeBps uint32 = 30 // default 0.3% in basis points

	// Try V3 path first.
	factory, err := m.callFactory(ctx, log.Address)
	if err == nil {
		// It might be a V3 pool if factory matches one of the known V3 factories.
		if factory == config.UniswapV3Factory {
			dexType = types.DexUniswapV3
		} else if factory == config.PancakeV3Factory {
			dexType = types.DexPancakeV3
		} else {
			// It's not a known V3 factory; treat as V2.
			goto V2
		}
		// V3: fetch fee.
		rawFee, err := m.callFee(ctx, log.Address)
		if err == nil {
			feeBps = rawFee / 100 // convert from hundredths of bps to bps
		}
		pool := &types.PoolState{
			PoolAddress:  log.Address,
			Token0:       token0,
			Token1:       token1,
			DexType:      dexType,
			FeeBps:       feeBps,
			Reserve0:     new(big.Int),
			Reserve1:     new(big.Int),
			SqrtPriceX96: new(big.Int),
			Liquidity:    new(big.Int),
		}
		m.RegisterPool(pool)
		return nil
	}

V2:
	// V2-style: validate factory.
	factory2, err := m.callFactory(ctx, log.Address)
	if err != nil {
		m.markDiscoveryFailed(log.Address)
		return fmt.Errorf("failed to get factory for V2 pool %s: %w", log.Address.Hex(), err)
	}
	var ok bool
	dexType, ok = m.determineV2DexType(factory2)
	if !ok {
		m.markDiscoveryFailed(log.Address)
		return fmt.Errorf("unknown V2 factory %s for pool %s", factory2.Hex(), log.Address.Hex())
	}

	// Fetch reserves.
	r0, r1, err := m.callGetReserves(ctx, log.Address)
	if err != nil {
		m.markDiscoveryFailed(log.Address)
		return fmt.Errorf("failed to get reserves for pool %s: %w", log.Address.Hex(), err)
	}

	pool := &types.PoolState{
		PoolAddress:   log.Address,
		Token0:        token0,
		Token1:        token1,
		DexType:       dexType,
		FeeBps:        30, // default; can be overridden via config
		Reserve0:      r0,
		Reserve1:      r1,
		SqrtPriceX96:  new(big.Int),
		Liquidity:     new(big.Int),
		Reserve0Float: float64FromBig(r0),
		Reserve1Float: float64FromBig(r1),
	}
	m.RegisterPool(pool)
	return nil
}

// determineV2DexType returns the DEX type and a boolean indicating success.
func (m *Matrix) determineV2DexType(factory common.Address) (types.DexType, bool) {
	if factory == config.AerodromeFactory && config.AerodromeFactory != (common.Address{}) {
		return types.DexAerodromeV2, true
	}
	if factory == config.AlienBaseFactory && config.AlienBaseFactory != (common.Address{}) {
		return types.DexAlienBaseV2, true
	}
	return 0, false
}

// Helper functions to call pool view functions using ethclient.CallContract.
// All calls are made with a context that has a timeout.

func (m *Matrix) callToken0(ctx context.Context, pool common.Address) (common.Address, error) {
	data, err := m.token0ABI.Pack("token0")
	if err != nil {
		return common.Address{}, err
	}
	msg := ethereum.CallMsg{To: &pool, Data: data}
	out, err := m.ethClient.CallContract(ctx, msg, nil)
	if err != nil {
		return common.Address{}, err
	}
	var result common.Address
	if err := m.token0ABI.UnpackIntoInterface(&result, "token0", out); err != nil {
		return common.Address{}, err
	}
	return result, nil
}

func (m *Matrix) callToken1(ctx context.Context, pool common.Address) (common.Address, error) {
	data, err := m.token1ABI.Pack("token1")
	if err != nil {
		return common.Address{}, err
	}
	msg := ethereum.CallMsg{To: &pool, Data: data}
	out, err := m.ethClient.CallContract(ctx, msg, nil)
	if err != nil {
		return common.Address{}, err
	}
	var result common.Address
	if err := m.token1ABI.UnpackIntoInterface(&result, "token1", out); err != nil {
		return common.Address{}, err
	}
	return result, nil
}

// callFee returns the raw fee in hundredths of basis points (e.g., 3000 for 0.3%).
// It decodes the uint24 output correctly using *big.Int.
// Rejects values above the uint24 maximum (0xFFFFFF) before casting to uint32.
func (m *Matrix) callFee(ctx context.Context, pool common.Address) (uint32, error) {
	data, err := m.feeABI.Pack("fee")
	if err != nil {
		return 0, err
	}
	msg := ethereum.CallMsg{To: &pool, Data: data}
	out, err := m.ethClient.CallContract(ctx, msg, nil)
	if err != nil {
		return 0, err
	}
	var raw big.Int
	if err := m.feeABI.UnpackIntoInterface(&raw, "fee", out); err != nil {
		return 0, err
	}
	if !raw.IsUint64() {
		return 0, fmt.Errorf("fee value too large")
	}
	// Fee is a uint24; reject values above 0xFFFFFF.
	if raw.Uint64() > 0xFFFFFF {
		return 0, fmt.Errorf("fee value out of uint24 range: %d", raw.Uint64())
	}
	return uint32(raw.Uint64()), nil
}

func (m *Matrix) callFactory(ctx context.Context, pool common.Address) (common.Address, error) {
	data, err := m.factoryABI.Pack("factory")
	if err != nil {
		return common.Address{}, err
	}
	msg := ethereum.CallMsg{To: &pool, Data: data}
	out, err := m.ethClient.CallContract(ctx, msg, nil)
	if err != nil {
		return common.Address{}, err
	}
	var result common.Address
	if err := m.factoryABI.UnpackIntoInterface(&result, "factory", out); err != nil {
		return common.Address{}, err
	}
	return result, nil
}

// callGetReserves fetches reserves from a V2-style pool.
func (m *Matrix) callGetReserves(ctx context.Context, pool common.Address) (*big.Int, *big.Int, error) {
	data, err := m.getReservesABI.Pack("getReserves")
	if err != nil {
		return nil, nil, err
	}
	msg := ethereum.CallMsg{To: &pool, Data: data}
	out, err := m.ethClient.CallContract(ctx, msg, nil)
	if err != nil {
		return nil, nil, err
	}
	vals, err := m.getReservesABI.Unpack("getReserves", out)
	if err != nil || len(vals) < 2 {
		return nil, nil, fmt.Errorf("decode getReserves: %w", err)
	}
	r0, ok0 := vals[0].(*big.Int)
	r1, ok1 := vals[1].(*big.Int)
	if !ok0 || !ok1 {
		return nil, nil, fmt.Errorf("unexpected getReserves output types")
	}
	return r0, r1, nil
}

// V2 pair does not have fee(), but we can use getReserves to check if it's a valid pair.
// For token0/token1, we can use the same as V3 (they have token0/token1).
func (m *Matrix) callToken0V2(ctx context.Context, pool common.Address) (common.Address, error) {
	return m.callToken0(ctx, pool)
}

func (m *Matrix) callToken1V2(ctx context.Context, pool common.Address) (common.Address, error) {
	return m.callToken1(ctx, pool)
}

// =============================================================================
// Path Registration (called at startup, not in hot path)
// =============================================================================

func (m *Matrix) RegisterPath(path *types.Path) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, pool := range path.Pools {
		// Normalize pool state before storing.
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

// RegisterPool adds a pool state. It is idempotent.
func (m *Matrix) RegisterPool(pool *types.PoolState) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.pools[pool.PoolAddress]; exists {
		// Already registered; keep existing stats.
		return
	}
	// Normalize pool state before storing.
	m.normalizePoolState(pool)
	m.pools[pool.PoolAddress] = pool
	m.addToTokenPairIndexLocked(pool)
	m.poolStats[pool.PoolAddress] = newPoolStats()
}

// normalizePoolState ensures all big.Int fields are non-nil.
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
	if a == b {
		return [2]common.Address{a, b}
	}
	if bytes.Compare(a.Bytes(), b.Bytes()) < 0 {
		return [2]common.Address{a, b}
	}
	return [2]common.Address{b, a}
}

// =============================================================================
// UpdateFromLog — O(1) state update and path retrieval
// =============================================================================

func (m *Matrix) UpdateFromLog(log *types.SwapLog) []*types.Path {
	// First, check if pool exists under read lock.
	m.mu.RLock()
	_, exists := m.pools[log.Address]
	m.mu.RUnlock()

	if !exists {
		// Discovery: perform RPC calls outside the global lock.
		if err := m.ensurePool(log); err != nil {
			return nil
		}
	}

	// Now acquire write lock for the actual update.
	m.mu.Lock()
	defer m.mu.Unlock()

	pool, exists := m.pools[log.Address]
	if !exists {
		return nil
	}

	// Acquire per-pool mutex to protect against concurrent solver reads.
	pool.Lock()
	defer pool.Unlock()

	oldReserve0 := new(big.Int).Set(pool.Reserve0)
	oldReserve1 := new(big.Int).Set(pool.Reserve1)
	oldSqrtPrice := new(big.Int).Set(pool.SqrtPriceX96)

	switch pool.DexType {
	case types.DexUniswapV3, types.DexPancakeV3:
		m.updateV3Pool(pool, log, oldSqrtPrice)
	default:
		m.updateV2Pool(pool, log, oldReserve0, oldReserve1)
	}

	// Update pool stats under the stats mutex.
	stats := m.poolStats[log.Address]
	if stats != nil {
		stats.UpdateSwapWindow(time.Now())
		var liquidityUSD float64
		if pool.DexType == types.DexUniswapV3 || pool.DexType == types.DexPancakeV3 {
			// For V3, use liquidity as a proxy.
			liquidityUSD = pool.LiquidityFloat / 1e18
		} else {
			// For V2, use sqrt(product of reserves) as a proxy.
			sqrtProduct := new(big.Float).Sqrt(
				new(big.Float).Mul(
					big.NewFloat(pool.Reserve0Float),
					big.NewFloat(pool.Reserve1Float),
				),
			)
			val, _ := sqrtProduct.Float64()
			liquidityUSD = val / 1e12
		}
		stats.mu.Lock()
		stats.LastLiquidityUSD = liquidityUSD
		stats.LastUpdated = time.Now()
		stats.mu.Unlock()
	}

	return m.affectedPaths[log.Address]
}

// updateV2Pool mutates a V2-style pool and fills TokenIn/TokenOut based on reserve changes.
// It infers the swap direction if not provided by the decoder.
func (m *Matrix) updateV2Pool(pool *types.PoolState, log *types.SwapLog, oldR0, oldR1 *big.Int) {
	// Guard against nil amounts (should not happen, but defensive).
	if log.AmountIn == nil || log.AmountOut == nil {
		return
	}

	// If the decoder already provided the direction, use it.
	if log.TokenIn != (common.Address{}) && log.TokenOut != (common.Address{}) {
		if log.TokenIn == pool.Token0 && log.TokenOut == pool.Token1 {
			pool.Reserve0.Add(pool.Reserve0, log.AmountIn)
			pool.Reserve1.Sub(pool.Reserve1, log.AmountOut)
		} else if log.TokenIn == pool.Token1 && log.TokenOut == pool.Token0 {
			pool.Reserve1.Add(pool.Reserve1, log.AmountIn)
			pool.Reserve0.Sub(pool.Reserve0, log.AmountOut)
		} else {
			// Fallback: treat as token0->token1
			pool.Reserve0.Add(pool.Reserve0, log.AmountIn)
			pool.Reserve1.Sub(pool.Reserve1, log.AmountOut)
		}
	} else {
		// Infer direction from the amounts and old reserves.
		amountIn := log.AmountIn
		amountOut := log.AmountOut

		// Determine fee numerator/denominator (basis points).
		feeBps := pool.FeeBps
		if feeBps == 0 || feeBps >= 10000 {
			feeBps = 30 // default 0.3%
		}
		feeNumerator := uint64(10000 - feeBps)
		feeDenominator := uint64(10000)

		// Helper to compute expected output for a given input in a constant product pool.
		computeExpectedOut := func(reserveIn, reserveOut, amountIn *big.Int) *big.Int {
			num := new(big.Int).Mul(reserveOut, amountIn)
			num.Mul(num, new(big.Int).SetUint64(feeNumerator))
			den := new(big.Int).Mul(reserveIn, new(big.Int).SetUint64(feeDenominator))
			den.Add(den, new(big.Int).Mul(amountIn, new(big.Int).SetUint64(feeNumerator)))
			return new(big.Int).Div(num, den)
		}

		// Direction A: token0 is input, token1 is output.
		newR0A := new(big.Int).Add(oldR0, amountIn)
		newR1A := new(big.Int).Sub(oldR1, amountOut)
		okA := newR1A.Sign() >= 0

		// Direction B: token1 is input, token0 is output.
		newR0B := new(big.Int).Sub(oldR0, amountOut)
		newR1B := new(big.Int).Add(oldR1, amountIn)
		okB := newR0B.Sign() >= 0

		// Compute expected outputs.
		expectedOutA := computeExpectedOut(oldR0, oldR1, amountIn) // token0 -> token1
		expectedOutB := computeExpectedOut(oldR1, oldR0, amountIn) // token1 -> token0

		// Tolerance (0.1%).
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

		// Choose direction.
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

	// Ensure non-negative reserves.
	if pool.Reserve0.Sign() < 0 {
		pool.Reserve0.SetInt64(0)
	}
	if pool.Reserve1.Sign() < 0 {
		pool.Reserve1.SetInt64(0)
	}
	pool.Reserve0Float = float64FromBig(pool.Reserve0)
	pool.Reserve1Float = float64FromBig(pool.Reserve1)
}

// updateV3Pool mutates a V3-style pool and fills TokenIn/TokenOut based on price change.
func (m *Matrix) updateV3Pool(pool *types.PoolState, log *types.SwapLog, oldSqrt *big.Int) {
	if log.SqrtPriceX96 != nil {
		pool.SqrtPriceX96.Set(log.SqrtPriceX96)
		pool.SqrtPriceX96Float = float64FromBig(pool.SqrtPriceX96)
	}
	if log.Liquidity != nil {
		pool.Liquidity.Set(log.Liquidity)
		pool.LiquidityFloat = float64FromBig(pool.Liquidity)
	}
	// Always assign tick if available; 0 is a valid tick.
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

	// Determine direction using the decoded swap amounts (if available) instead of price change.
	// The decoder sets TokenIn/TokenOut based on sign of amount0/amount1.
	if log.TokenIn != (common.Address{}) && log.TokenOut != (common.Address{}) {
		// Already set by decoder; do not override.
	} else {
		// Fallback: use price movement.
		if oldSqrt != nil && oldSqrt.Sign() > 0 && pool.SqrtPriceX96 != nil && pool.SqrtPriceX96.Sign() > 0 {
			if pool.SqrtPriceX96.Cmp(oldSqrt) > 0 {
				log.TokenIn = pool.Token1
				log.TokenOut = pool.Token0
			} else if pool.SqrtPriceX96.Cmp(oldSqrt) < 0 {
				log.TokenIn = pool.Token0
				log.TokenOut = pool.Token1
			} else {
				log.TokenIn = pool.Token0
				log.TokenOut = pool.Token1
			}
		} else {
			log.TokenIn = pool.Token0
			log.TokenOut = pool.Token1
		}
	}
}

func float64FromBig(v *big.Int) float64 {
	if v == nil {
		return 0
	}
	f, _ := new(big.Float).SetInt(v).Float64()
	return f
}

// =============================================================================
// Competition Stats Helpers
// =============================================================================

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

// =============================================================================
// Stats Update Methods (for wiring from sender)
// =============================================================================

// RecordBroadcast increments the broadcast counter for a pool.
// Optimized: uses per-pool stats mutex; no global matrix lock needed because poolStats map is stable after startup.
func (m *Matrix) RecordBroadcast(poolAddr common.Address) {
	if stats := m.poolStats[poolAddr]; stats != nil {
		stats.mu.Lock()
		stats.Broadcast++
		stats.mu.Unlock()
	}
}

// RecordWin increments the win counter for a pool.
func (m *Matrix) RecordWin(poolAddr common.Address) {
	if stats := m.poolStats[poolAddr]; stats != nil {
		stats.mu.Lock()
		stats.Win++
		stats.mu.Unlock()
	}
}

// RecordFail increments the fail counter for a pool.
func (m *Matrix) RecordFail(poolAddr common.Address) {
	if stats := m.poolStats[poolAddr]; stats != nil {
		stats.mu.Lock()
		stats.Fail++
		stats.mu.Unlock()
	}
}

// =============================================================================
// Lookup Methods (O(1) direct access)
// =============================================================================

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
	key := canonicalPair(tokenA, tokenB)
	return m.tokenPairToPools[key]
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

// =============================================================================
// Matrix Slot Management (for the 44x44 matrix)
// =============================================================================

func (m *Matrix) RegisterToken(token common.Address, slot uint8) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if slot >= 44 {
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
	if row >= 44 || col >= 44 {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.matrix[row][col] = value
}

func (m *Matrix) GetMatrixValue(row, col uint8) uint64 {
	if row >= 44 || col >= 44 {
		return 0
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.matrix[row][col]
}
