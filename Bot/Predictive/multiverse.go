package predictive

import (
	"fmt"
	"math"
	"math/big"
	"sync"
	"sync/atomic"
	"time"

	"github.com/buger/jsonparser"
	"github.com/ethereum/go-ethereum/common"

	"my-mev-bot/Bot/Config"
	"my-mev-bot/Bot/Solver"
	"my-mev-bot/Bot/State"
	"my-mev-bot/Bot/Types"
)

const TotalScenarios = 5

// SpeculativeState holds a pre‑computed execution payload.
type SpeculativeState struct {
	TargetPool [20]byte
	Payload    *types.ExecutionPayload
	IsActive   uint32
	ExpiresAt  int64
}

type MultiverseCache struct {
	Branches [TotalScenarios]SpeculativeState
	mu       sync.Mutex
}

var GlobalMultiverse MultiverseCache

// Pool for reusing pre‑baked payloads (set from main).
var payloadPool *sync.Pool

func SetPayloadPool(pool *sync.Pool) {
	payloadPool = pool
}

// SeedNextSubBlockBranches pre‑computes up to 5 scenarios with zero memory leaks
// and minimises lock contention.
func SeedNextSubBlockBranches(
	currentMemePools [][20]byte,
	matrix *state.Matrix,
	cfg *config.Config,
	loanProvider uint8,
	loanPool common.Address,
	priorityFee uint64,
) {
	now := time.Now().UnixNano()
	windowEnd := now + int64(180*time.Millisecond)

	if len(currentMemePools) == 0 {
		// Still need to purge expired branches.
		GlobalMultiverse.mu.Lock()
		defer GlobalMultiverse.mu.Unlock()
		purgeExpired(now)
		return
	}

	anchors := config.AnchorAssets()
	anchorSet := make(map[common.Address]bool, len(anchors))
	for _, a := range anchors {
		anchorSet[a] = true
	}

	getBaseToken := func(pool *types.PoolState) (common.Address, common.Address, bool) {
		if anchorSet[pool.Token0] && !anchorSet[pool.Token1] {
			return pool.Token0, pool.Token1, true
		}
		if anchorSet[pool.Token1] && !anchorSet[pool.Token0] {
			return pool.Token1, pool.Token0, true
		}
		return common.Address{}, common.Address{}, false
	}
formatRoute := func(cand *types.RouteCandidate) string {
		if cand == nil || cand.Hops == 0 {
			return "unknown"
		}
		tokens := cand.Tokens[:int(cand.Hops)+1]
		dexes := cand.DexTypes[:int(cand.Hops)]
		route := ""
		for i := 0; i < int(cand.Hops); i++ {
			if i > 0 {
				route += " -> "
			}
			route += fmt.Sprintf("%s[%d]", tokens[i].Hex()[:8], dexes[i])
		}
		route += " -> " + tokens[cand.Hops].Hex()[:8]
		return route
	}

	// buildPayload creates a payload and does NOT reserve nonces or sign.
	buildPayload := func(poolAddr common.Address, loanToken common.Address) *types.ExecutionPayload {
		pool := matrix.GetPool(poolAddr)
		if pool == nil {
			return nil
		}
		base, meme, ok := getBaseToken(pool)
		if !ok {
			return nil
		}
		if loanToken != base && loanToken != meme {
			return nil
		}
		var start, middle common.Address
		if loanToken == base {
			start = base
			middle = meme
		} else {
			start = meme
			middle = base
		}

		// No priceCache allocation – pass nil to solver functions.
		cand := solver.BuildRoundTrip2Hop(start, middle, matrix, cfg, nil)
		if cand == nil {
			usdc := config.USDCAddress
			if len(matrix.GetPoolsForPair(middle, usdc)) > 0 {
				cand = solver.BuildRoundTrip3Hop(start, middle, usdc, matrix, cfg, nil)
			}
		}
		if cand == nil {
			return nil
		}

		loanAmount := cand.AmountIn
		minProfitWei := cand.NetProfitWei
		if minProfitWei == nil || minProfitWei.Sign() <= 0 {
			tokenPrice, ok := solver.GetTokenPrice(loanToken, matrix, nil)
if !ok || tokenPrice <= 0 {
    tokenPrice = 1.0
}
			decimals := getTokenDecimals(loanToken)
			profitWei := new(big.Float).Mul(
				big.NewFloat(cand.ExpectedProfitUSD/tokenPrice),
				big.NewFloat(math.Pow10(decimals)),
			)
			minProfitWei = new(big.Int)
			profitWei.Int(minProfitWei)
		}
		deadline := uint64(time.Now().Unix()) + 120

		if payloadPool == nil {
			panic("payloadPool not set in predictive package")
		}
		payload := payloadPool.Get().(*types.ExecutionPayload)
		payload.Reset()

		calldata, err := solver.BuildCalldata(
			cand,
			loanProvider,
			loanPool,
			loanToken,
			loanAmount,
			minProfitWei,
			deadline,
		)
		if err != nil {
			payloadPool.Put(payload)
			return nil
		}

		payload.TargetExecutor = cfg.ExecutorAddress
		payload.LoanProvider = loanProvider
		payload.LoanPool = loanPool
		payload.BorrowedToken = loanToken
		payload.BorrowedAmount = loanAmount
		payload.Calldata = calldata
		payload.Nonce = 0 // will be set by worker3
		payload.GasLimit = uint64(150000 + 150000*int(cand.Hops))
		payload.PriorityFeeWei = priorityFee
		payload.MinProfitUSD = cand.ExpectedProfitUSD
		payload.MinProfitWei = minProfitWei
		payload.DetectionTime = time.Now()
		payload.RouteDesc = formatRoute(cand)
		payload.RoutePools = cand.Pools[:int(cand.Hops)]
		payload.OriginalCandidate = cand
		// Leave SignedRawTx and TxHash as zero – worker3 will sign.

		return payload
	}

	// ---- Build updates outside lock ----
	type branchUpdate struct {
		idx      int
		payload  *types.ExecutionPayload
		target   [20]byte
		expires  int64
	}
	updates := make([]branchUpdate, 0, TotalScenarios)

	// Scenario 0: Buy pool0
	if pool0 := matrix.GetPool(common.BytesToAddress(currentMemePools[0][:])); pool0 != nil {
		if base, _, ok := getBaseToken(pool0); ok {
			if pl := buildPayload(pool0.PoolAddress, base); pl != nil {
				updates = append(updates, branchUpdate{0, pl, currentMemePools[0], windowEnd})
			}
		}
	}

	// Scenario 1: Sell pool0
	if pool0 := matrix.GetPool(common.BytesToAddress(currentMemePools[0][:])); pool0 != nil {
		if _, meme, ok := getBaseToken(pool0); ok {
			if pl := buildPayload(pool0.PoolAddress, meme); pl != nil {
				updates = append(updates, branchUpdate{1, pl, currentMemePools[0], windowEnd})
			}
		}
	}

	// Scenario 2: Buy pool1
	if len(currentMemePools) > 1 {
		if pool1 := matrix.GetPool(common.BytesToAddress(currentMemePools[1][:])); pool1 != nil {
			if base, _, ok := getBaseToken(pool1); ok {
				if pl := buildPayload(pool1.PoolAddress, base); pl != nil {
					updates = append(updates, branchUpdate{2, pl, currentMemePools[1], windowEnd})
				}
			}
		}
	}

	// Scenario 3: Sell pool1
	if len(currentMemePools) > 1 {
		if pool1 := matrix.GetPool(common.BytesToAddress(currentMemePools[1][:])); pool1 != nil {
			if _, meme, ok := getBaseToken(pool1); ok {
				if pl := buildPayload(pool1.PoolAddress, meme); pl != nil {
					updates = append(updates, branchUpdate{3, pl, currentMemePools[1], windowEnd})
				}
			}
		}
	}

	// Scenario 4: Buy pool2
	if len(currentMemePools) > 2 {
		if pool2 := matrix.GetPool(common.BytesToAddress(currentMemePools[2][:])); pool2 != nil {
			if base, _, ok := getBaseToken(pool2); ok {
				if pl := buildPayload(pool2.PoolAddress, base); pl != nil {
					updates = append(updates, branchUpdate{4, pl, currentMemePools[2], windowEnd})
				}
			}
		}
	}

	// ---- Lock and apply updates ----
	GlobalMultiverse.mu.Lock()
	defer GlobalMultiverse.mu.Unlock()

	for _, u := range updates {
		replaceBranch(u.idx, u.payload, u.target, u.expires)
	}
	// Purge expired branches.
	purgeExpired(now)
}

// replaceBranch swaps in a new payload, putting the old one back to pool.
// Must be called with GlobalMultiverse.mu held.
func replaceBranch(idx int, newPayload *types.ExecutionPayload, target [20]byte, expires int64) {
	branch := &GlobalMultiverse.Branches[idx]
	if branch.Payload != nil {
		payloadPool.Put(branch.Payload)
	}
	copy(branch.TargetPool[:], target[:])
	branch.Payload = newPayload
	branch.ExpiresAt = expires
	atomic.StoreUint32(&branch.IsActive, 1)
}

// purgeExpired iterates over all branches and frees expired payloads.
// Must be called with GlobalMultiverse.mu held.
func purgeExpired(now int64) {
	for i := 0; i < TotalScenarios; i++ {
		branch := &GlobalMultiverse.Branches[i]
		if atomic.LoadUint32(&branch.IsActive) == 1 && branch.ExpiresAt < now {
			if branch.Payload != nil {
				payloadPool.Put(branch.Payload)
				branch.Payload = nil
			}
			atomic.StoreUint32(&branch.IsActive, 0)
		}
	}
}

// ShortCircuitEvaluate returns the pre‑built payload if the address matches.
func ShortCircuitEvaluate(incomingRawPacket []byte) (*types.ExecutionPayload, bool) {
	if len(incomingRawPacket) < 20 {
		return nil, false
	}
	addrBytes, _, _, err := jsonparser.Get(incomingRawPacket, "address")
	if err != nil || len(addrBytes) < 42 {
		return nil, false
	}
	var poolAddr [20]byte
	if err := hexToAddressBytes(addrBytes, poolAddr[:]); err != nil {
		return nil, false
	}

	GlobalMultiverse.mu.Lock()
	defer GlobalMultiverse.mu.Unlock()

	for i := 0; i < TotalScenarios; i++ {
		branch := &GlobalMultiverse.Branches[i]
		if atomic.LoadUint32(&branch.IsActive) == 0 {
			continue
		}
		if memequal(poolAddr[:], branch.TargetPool[:]) {
			pl := branch.Payload
			branch.Payload = nil // remove reference so it's not put back on expiry
			atomic.StoreUint32(&branch.IsActive, 0)
			return pl, true
		}
	}
	return nil, false
}

// Helper functions (copied from decoder.go to avoid import cycle).
func hexToAddressBytes(hex []byte, dest []byte) error {
	if len(hex) < 42 || hex[0] != '0' || (hex[1] != 'x' && hex[1] != 'X') {
		return fmt.Errorf("invalid hex address")
	}
	src := hex[2:]
	if len(src) != 40 {
		return fmt.Errorf("address length must be 40 hex chars")
	}
	for i := 0; i < 20; i++ {
		high := fromHexChar(src[i*2])
		low := fromHexChar(src[i*2+1])
		if high == 0xff || low == 0xff {
			return fmt.Errorf("invalid hex char")
		}
		dest[i] = (high << 4) | low
	}
	return nil
}

func fromHexChar(c byte) byte {
	if c >= '0' && c <= '9' {
		return c - '0'
	}
	if c >= 'a' && c <= 'f' {
		return c - 'a' + 10
	}
	if c >= 'A' && c <= 'F' {
		return c - 'A' + 10
	}
	return 0xff
}

func memequal(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
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
