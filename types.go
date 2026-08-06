// Package types defines the canonical, unified data structures used across all packages.
// All structs are designed for zero-allocation hot paths; use Reset() methods to reuse buffers.
package types

import (
	"math/big"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

// =============================================================================
// Constants
// =============================================================================

// MaxHops defines the maximum number of swap hops in a route (3-hop max).
const MaxHops = 3

// DexType identifies the DEX protocol for each hop.
type DexType uint8

const (
	DexUniswapV3 DexType = iota
	DexPancakeV3
	DexAerodromeV2
	DexAlienBaseV2
)

// =============================================================================
// Path — a pre‑computed cyclic arbitrage route (used by matrix)
// =============================================================================

type Path struct {
	Pools        []*PoolState
	TokenPath    []common.Address
	MinProfitUSD float64
}

// =============================================================================
// SwapLog — parsed event stream data from Flashblocks
// =============================================================================

type SwapLog struct {
	Address     common.Address
	Topics      []common.Hash
	Data        []byte
	BlockNumber uint64
	TxIndex     uint
	TxHash      common.Hash
	Timestamp   time.Time

	// Convenience fields for fast path evaluation (parsed from log data)
	TokenIn   common.Address
	TokenOut  common.Address
	AmountIn  *big.Int
	AmountOut *big.Int

	// Float counterparts for zero-allocation math
	AmountInFloat  float64
	AmountOutFloat float64

	// V3-specific state (decoded from event data)
	SqrtPriceX96     *big.Int
	Liquidity        *big.Int
	Tick             int32
	SqrtPriceX96Float float64
	LiquidityFloat    float64
}

// Reset reuses the SwapLog without new allocations.
func (s *SwapLog) Reset() {
	s.Address = common.Address{}
	s.Topics = s.Topics[:0]
	s.Data = s.Data[:0]
	s.BlockNumber = 0
	s.TxIndex = 0
	s.TxHash = common.Hash{}
	s.Timestamp = time.Time{}
	s.TokenIn = common.Address{}
	s.TokenOut = common.Address{}
	if s.AmountIn == nil {
		s.AmountIn = new(big.Int)
	} else {
		s.AmountIn.SetInt64(0)
	}
	if s.AmountOut == nil {
		s.AmountOut = new(big.Int)
	} else {
		s.AmountOut.SetInt64(0)
	}
	if s.SqrtPriceX96 == nil {
		s.SqrtPriceX96 = new(big.Int)
	} else {
		s.SqrtPriceX96.SetInt64(0)
	}
	if s.Liquidity == nil {
		s.Liquidity = new(big.Int)
	} else {
		s.Liquidity.SetInt64(0)
	}
	s.Tick = 0
	s.AmountInFloat = 0
	s.AmountOutFloat = 0
	s.SqrtPriceX96Float = 0
	s.LiquidityFloat = 0
}

// IsZero returns true if the log is empty.
func (s *SwapLog) IsZero() bool {
	return s.Address == common.Address{} && s.BlockNumber == 0
}

// =============================================================================
// PoolState — RAM adjacency matrix node state
// =============================================================================

type PoolState struct {
	PoolAddress common.Address
	Token0      common.Address
	Token1      common.Address
	DexType     DexType
	FeeBps      uint32

	// V2 reserves
	Reserve0 *big.Int
	Reserve1 *big.Int
	// V3 state
	SqrtPriceX96 *big.Int
	Liquidity    *big.Int
	Tick         int32

	// Float helpers for zero-allocation fast math (updated on every pool state change)
	Reserve0Float     float64
	Reserve1Float     float64
	SqrtPriceX96Float float64
	LiquidityFloat    float64

	LastUpdated time.Time

	// mu protects all mutable fields (Reserve0Float, Reserve1Float, SqrtPriceX96Float,
	// LiquidityFloat, Tick, LastUpdated, and the big.Int fields when they are mutated).
	// Read locks are used in solver paths, write locks in UpdateFromLog.
	mu sync.RWMutex
}

// Reset reuses the PoolState without allocating new big.Ints.
func (p *PoolState) Reset() {
	p.PoolAddress = common.Address{}
	p.Token0 = common.Address{}
	p.Token1 = common.Address{}
	p.DexType = 0
	p.FeeBps = 0
	if p.Reserve0 == nil {
		p.Reserve0 = new(big.Int)
	} else {
		p.Reserve0.SetInt64(0)
	}
	if p.Reserve1 == nil {
		p.Reserve1 = new(big.Int)
	} else {
		p.Reserve1.SetInt64(0)
	}
	if p.SqrtPriceX96 == nil {
		p.SqrtPriceX96 = new(big.Int)
	} else {
		p.SqrtPriceX96.SetInt64(0)
	}
	if p.Liquidity == nil {
		p.Liquidity = new(big.Int)
	} else {
		p.Liquidity.SetInt64(0)
	}
	p.Tick = 0
	p.Reserve0Float = 0
	p.Reserve1Float = 0
	p.SqrtPriceX96Float = 0
	p.LiquidityFloat = 0
	p.LastUpdated = time.Time{}
}

// IsZero returns true if the pool address is empty.
func (p *PoolState) IsZero() bool {
	return p.PoolAddress == common.Address{}
}

// =============================================================================
// PoolState exported accessors (for lock-safe reading)
// =============================================================================

// GetReserves returns reserve0 and reserve1 as float64 with a read lock.
func (p *PoolState) GetReserves() (float64, float64) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.Reserve0Float, p.Reserve1Float
}

// GetReservesProduct returns reserve0 * reserve1 with a read lock.
func (p *PoolState) GetReservesProduct() float64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.Reserve0Float * p.Reserve1Float
}

// GetLiquidity returns LiquidityFloat with a read lock.
func (p *PoolState) GetLiquidity() float64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.LiquidityFloat
}

// =============================================================================
// RouteCandidate — path solver output (supports 2-hop and 3-hop routes)
// =============================================================================

type RouteCandidate struct {
	Hops uint8 // 2 or 3

	// Tokens: [start, middle, (middle2), end] – for round‑trip, start == end.
	// For 2-hop: tokens[0]=start, tokens[1]=middle, tokens[2]=start, tokens[3] unused
	// For 3-hop: tokens[0]=start, tokens[1]=memeA, tokens[2]=memeB, tokens[3]=start
	Tokens [MaxHops + 1]common.Address

	// Pools: pool for each hop
	Pools        [MaxHops]common.Address
	DexTypes     [MaxHops]DexType
	ZeroForOnes  [MaxHops]bool
	MinOuts      [MaxHops]*big.Int

	// Input amount (in first token, which is the loan token)
	AmountIn        *big.Int
	AmountInFloat   float64
	ExpectedProfitUSD float64
	ExecutionSlippage float64 // percentage (e.g., 3.0)

	// Net profit in wei of the loan token (used for on‑chain minProfitWei)
	NetProfitWei *big.Int

	// Competition-adjusted score (higher is better)
	Score float64
	// Competition score (0-1) derived from pool stats (higher = more competitive)
	Competition float64
}

// Reset reuses the RouteCandidate.
func (r *RouteCandidate) Reset() {
	r.Hops = 0
	for i := 0; i < MaxHops+1; i++ {
		r.Tokens[i] = common.Address{}
	}
	for i := 0; i < MaxHops; i++ {
		r.Pools[i] = common.Address{}
		r.DexTypes[i] = 0
		r.ZeroForOnes[i] = false
		if r.MinOuts[i] == nil {
			r.MinOuts[i] = new(big.Int)
		} else {
			r.MinOuts[i].SetInt64(0)
		}
	}
	if r.AmountIn == nil {
		r.AmountIn = new(big.Int)
	} else {
		r.AmountIn.SetInt64(0)
	}
	if r.NetProfitWei == nil {
		r.NetProfitWei = new(big.Int)
	} else {
		r.NetProfitWei.SetInt64(0)
	}
	r.AmountInFloat = 0
	r.ExpectedProfitUSD = 0
	r.ExecutionSlippage = 0
	r.Score = 0
	r.Competition = 0
}

// IsZero returns true if Hops is 0.
func (r *RouteCandidate) IsZero() bool {
	return r.Hops == 0
}

// =============================================================================
// ExecutionPayload — pre-compiled transaction struct
// =============================================================================

type ExecutionPayload struct {
	TargetExecutor common.Address
	LoanProvider   uint8 // 0 = Balancer V2, 1 = DODO
	LoanPool       common.Address
	BorrowedToken  common.Address
	BorrowedAmount *big.Int
	Calldata       []byte
	Nonce          uint64
	GasLimit       uint64
	PriorityFeeWei uint64
	MinProfitUSD   float64
	MinProfitWei   *big.Int // profit in wei of the borrowed token (used for on-chain check)
	DetectionTime  time.Time // for latency tracking
	RouteDesc      string    // human-readable route for logging
	RoutePools     []common.Address // pool addresses in the route (for reroute filtering)
}

// Reset reuses the ExecutionPayload without reallocating byte slice.
// NOTE: RoutePools is cleared by setting to its zero-length prefix; the caller
// is expected to allocate a fresh slice if the payload is reused, to avoid
// sharing the underlying array across goroutines.
func (e *ExecutionPayload) Reset() {
	e.TargetExecutor = common.Address{}
	e.LoanProvider = 0
	e.LoanPool = common.Address{}
	e.BorrowedToken = common.Address{}
	if e.BorrowedAmount == nil {
		e.BorrowedAmount = new(big.Int)
	} else {
		e.BorrowedAmount.SetInt64(0)
	}
	e.Calldata = e.Calldata[:0]
	e.Nonce = 0
	e.GasLimit = 0
	e.PriorityFeeWei = 0
	e.MinProfitUSD = 0
	if e.MinProfitWei == nil {
		e.MinProfitWei = new(big.Int)
	} else {
		e.MinProfitWei.SetInt64(0)
	}
	e.DetectionTime = time.Time{}
	e.RouteDesc = ""
	e.RoutePools = e.RoutePools[:0]
}

// IsZero returns true if the TargetExecutor is empty.
func (e *ExecutionPayload) IsZero() bool {
	return e.TargetExecutor == common.Address{}
}