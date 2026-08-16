// Package execution provides transaction simulation and execution.
// It includes a local EVM for zero-latency simulations, with fallback to remote RPC.
package execution

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math/big"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/rawdb"
	gethstate "github.com/ethereum/go-ethereum/core/state"
	gethTypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/triedb"
	"github.com/holiman/uint256"

	"my-mev-bot/Bot/Config"
	botstate "my-mev-bot/Bot/State"
	"my-mev-bot/Bot/Types"
)
// SimBackend defines the simulation backend type.
type SimBackend string

const (
	SimBackendLocal  SimBackend = "local"
	SimBackendRemote SimBackend = "remote"
)

// Known error selectors from FlashArbExecutor.sol.
var (
	insufficientProfitSelector   = common.HexToHash("0x0b1f0c7e").Bytes()[0:4]
	insufficientOutputSelector   = common.HexToHash("0x4f5e2789").Bytes()[0:4]
	swapExecutionFailedSelector  = common.HexToHash("0x446da0c9").Bytes()[0:4]
	loanRepaymentFailedSelector  = common.HexToHash("0x09d6f424").Bytes()[0:4]
)

// revertABI for decoding custom errors.
var revertABI *abi.ABI

func init() {
	parsed, err := abi.JSON(strings.NewReader(`[
		{"type": "error", "name": "InsufficientProfit", "inputs": [{"name": "actual", "type": "uint256"}, {"name": "required", "type": "uint256"}]},
		{"type": "error", "name": "InsufficientOutput", "inputs": [{"name": "actual", "type": "uint256"}, {"name": "required", "type": "uint256"}]},
		{"type": "error", "name": "SwapExecutionFailed", "inputs": []},
		{"type": "error", "name": "LoanRepaymentFailed", "inputs": []}
	]`))
	if err != nil {
		panic(fmt.Sprintf("failed to parse revert ABI: %v", err))
	}
	revertABI = &parsed
}

// StateCache holds contract bytecode; storage is injected from the matrix.
type StateCache struct {
	code map[common.Address][]byte
	mu   sync.RWMutex
}

func NewStateCache() *StateCache {
	return &StateCache{
		code: make(map[common.Address][]byte),
	}
}

func (c *StateCache) SetCode(addr common.Address, code []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.code[addr] = code
}

func (c *StateCache) GetCode(addr common.Address) ([]byte, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	code, ok := c.code[addr]
	return code, ok
}

// WarmUp pre‑loads code for known pools and the executor contract.
func (c *StateCache) WarmUp(executor common.Address, pools []common.Address, client *ethclient.Client) error {
	ctx := context.Background()
	code, err := client.CodeAt(ctx, executor, nil)
	if err != nil {
		return fmt.Errorf("failed to get executor code: %w", err)
	}
	c.SetCode(executor, code)
	for _, pool := range pools {
		code, err := client.CodeAt(ctx, pool, nil)
		if err != nil {
			continue // skip if we can't fetch code
		}
		c.SetCode(pool, code)
	}
	return nil
}

// GEVMSimulator provides remote and native in‑process EVM simulation.
type GEVMSimulator struct {
	httpClient *http.Client
	rpcURL     string
	wsURL      string
	owner      common.Address

	remoteSem chan struct{}

	// Native EVM state – protected by evmMu.
	evmMu       sync.RWMutex
	blockCtx    vm.BlockContext
	txCtx       vm.TxContext
	chainConfig *params.ChainConfig

	cache  *StateCache
	matrix *botstate.Matrix

	// Background updater control.
	updaterCtx    context.Context
	updaterCancel context.CancelFunc
}

// NewGEVMSimulator creates a simulator with remote RPC and local EVM backends.
// wsURL is the WebSocket endpoint for real‑time block header updates.
func NewGEVMSimulator(rpcURL, wsURL string, owner common.Address) *GEVMSimulator {
	transport := &http.Transport{
		MaxIdleConns:        10,
		MaxIdleConnsPerHost: 10,
		IdleConnTimeout:     90 * time.Second,
		DisableKeepAlives:   false,
	}

	chainCfg := params.MainnetChainConfig

	blockCtx := vm.BlockContext{
		CanTransfer: core.CanTransfer,
		Transfer:    core.Transfer,
		GetHash:     func(n uint64) common.Hash { return common.Hash{} },
		BlockNumber: big.NewInt(0),
		Time:        0,
		GasLimit:    30_000_000,
		BaseFee:     big.NewInt(0),
	}

	txCtx := vm.TxContext{
		Origin:   owner,
		GasPrice: big.NewInt(0),
	}

	return &GEVMSimulator{
		httpClient: &http.Client{
			Transport: transport,
			Timeout:   10 * time.Second,
		},
		rpcURL:      rpcURL,
		wsURL:       wsURL,
		owner:       owner,
		remoteSem:   make(chan struct{}, 8),
		blockCtx:    blockCtx,
		txCtx:       txCtx,
		chainConfig: chainCfg,
		cache:       NewStateCache(),
	}
}

// SetMatrix sets the state matrix used for fresh state injection.
func (g *GEVMSimulator) SetMatrix(matrix *botstate.Matrix) {
	g.matrix = matrix
}

// SetStateCache sets a custom state cache.
func (g *GEVMSimulator) SetStateCache(cache *StateCache) {
	g.cache = cache
}

// StartWebSocketContextUpdater subscribes to new block headers via WebSocket
// and updates the local block context in real time.
func (g *GEVMSimulator) StartWebSocketContextUpdater(ctx context.Context) {
	if g.updaterCtx != nil {
		return
	}
	updaterCtx, cancel := context.WithCancel(ctx)
	g.updaterCtx = updaterCtx
	g.updaterCancel = cancel

	go func() {
		client, err := ethclient.Dial(g.wsURL)
		if err != nil {
			log.Printf("[GEVM] WebSocket dial failed: %v", err)
			return
		}
		defer client.Close()

	
		headers := make(chan *gethTypes.Header, 1)
		sub, err := client.SubscribeNewHead(ctx, headers)
		if err != nil {
			log.Printf("[GEVM] Subscription failed: %v", err)
			return
		}
		defer sub.Unsubscribe()

		for {
			select {
			case <-updaterCtx.Done():
				return
			case err := <-sub.Err():
				log.Printf("[GEVM] Subscription error: %v", err)
				return
			case head := <-headers:
				g.evmMu.Lock()
				g.blockCtx.BlockNumber = head.Number
				g.blockCtx.Time = head.Time
				g.blockCtx.BaseFee = head.BaseFee
				g.blockCtx.GasLimit = head.GasLimit
				g.txCtx.GasPrice = new(big.Int).Add(head.BaseFee, big.NewInt(1e9))
				g.evmMu.Unlock()
			}
		}
	}()
}

// StopBackgroundUpdater stops the updater.
func (g *GEVMSimulator) StopBackgroundUpdater() {
	if g.updaterCancel != nil {
		g.updaterCancel()
		g.updaterCtx = nil
		g.updaterCancel = nil
	}
}

// ChooseBackend selects the simulation backend.
func (g *GEVMSimulator) ChooseBackend(cand *types.RouteCandidate) SimBackend {
	if g.matrix != nil && len(cand.Pools) > 0 {
		for _, poolAddr := range cand.Pools[:cand.Hops] {
			if poolAddr == (common.Address{}) || g.matrix.GetPool(poolAddr) == nil {
				return SimBackendRemote
			}
		}
		return SimBackendLocal
	}
	return SimBackendRemote
}

// getBalanceSlot returns the correct storage slot for the balance of a token.
// Most ERC‑20 tokens use slot 0, but USDC and USDbC on Base use slot 9.
func getBalanceSlot(token common.Address) uint64 {
	if token == config.USDCAddress || token == config.USDBCAddress {
		return 9
	}
	return 0
}

// setERC20Balance sets the balance of a token for a given address.
// Uses the correct storage slot for the token.
func setERC20Balance(stateDB *gethstate.StateDB, token, holder common.Address, amount *big.Int) {
	if amount == nil || amount.Sign() <= 0 {
		return
	}
	slot := getBalanceSlot(token)
	// keccak256(holder || slot)
	slotBytes := common.BigToHash(big.NewInt(int64(slot))).Bytes()
	key := common.BytesToHash(crypto.Keccak256(append(holder.Bytes(), slotBytes...)))
	stateDB.SetState(token, key, common.BigToHash(amount))
}

// injectPoolState writes the current pool state from the matrix into the StateDB.
// CRITICAL: Must hold pool.RLock() before reading any fields.
func (g *GEVMSimulator) injectPoolState(stateDB *gethstate.StateDB, pool *types.PoolState) {
	if pool == nil {
		return
	}
	pool.RLock()
	defer pool.RUnlock()

	// V2 reserves: slots 0 and 1.
	if pool.DexType == types.DexAerodromeV2 || pool.DexType == types.DexAlienBaseV2 {
		stateDB.SetState(pool.PoolAddress, common.BigToHash(big.NewInt(0)), common.BigToHash(pool.Reserve0))
		stateDB.SetState(pool.PoolAddress, common.BigToHash(big.NewInt(1)), common.BigToHash(pool.Reserve1))
		// Also set ERC‑20 balances for the pool.
		setERC20Balance(stateDB, pool.Token0, pool.PoolAddress, pool.Reserve0)
		setERC20Balance(stateDB, pool.Token1, pool.PoolAddress, pool.Reserve1)
		return
	}

	// V3: slot0 packed with sqrtPriceX96, tick, and unlocked flag.
	packed := new(big.Int).Set(pool.SqrtPriceX96)
	tickVal := uint64(int64(pool.Tick)) & 0xFFFFFF
	tickBig := new(big.Int).SetUint64(tickVal)
	tickBig.Lsh(tickBig, 160)
	packed.Or(packed, tickBig)
	unlockedBit := new(big.Int).Lsh(big.NewInt(1), 240)
	packed.Or(packed, unlockedBit)
	stateDB.SetState(pool.PoolAddress, common.BigToHash(big.NewInt(0)), common.BigToHash(packed))
	stateDB.SetState(pool.PoolAddress, common.BigToHash(big.NewInt(1)), common.BigToHash(pool.Liquidity))

	// For V3 we cannot easily compute reserve amounts from sqrtPrice and liquidity,
	// but we can set a huge balance for the pool to cover any transfer.
	huge := new(big.Int).Exp(big.NewInt(10), big.NewInt(30), nil)
	setERC20Balance(stateDB, pool.Token0, pool.PoolAddress, huge)
	setERC20Balance(stateDB, pool.Token1, pool.PoolAddress, huge)
}

// SimulateNative runs a simulation using an isolated in‑process EVM.
func (g *GEVMSimulator) SimulateNative(payload *types.ExecutionPayload) (bool, uint64, error) {
	if payload == nil || payload.Calldata == nil {
		return false, 0, errors.New("invalid payload")
	}
	if g.matrix == nil {
		return false, 0, errors.New("no matrix set for local EVM state injection")
	}

	// ===== Thread‑safe state isolation =====
	// Each simulation gets its own memory database.
	localMemDB := rawdb.NewMemoryDatabase()
	localTrieDB := triedb.NewDatabase(localMemDB, nil)
	stateDB := gethstate.NewDatabaseWithNodeDB(localMemDB, localTrieDB)

	newState, err := gethstate.New(common.Hash{}, stateDB, nil)
	if err != nil {
		return false, 0, fmt.Errorf("failed to create state: %w", err)
	}

	// Inject contract code from cache.
	if g.cache != nil {
		if code, ok := g.cache.GetCode(payload.TargetExecutor); ok {
			newState.SetCode(payload.TargetExecutor, code)
		}
		for _, pool := range payload.RoutePools {
			if code, ok := g.cache.GetCode(pool); ok {
				newState.SetCode(pool, code)
			}
		}
	}

	// Inject pool states and ERC‑20 balances.
	for _, poolAddr := range payload.RoutePools {
		if poolAddr == (common.Address{}) {
			continue
		}
		if pool := g.matrix.GetPool(poolAddr); pool != nil {
			g.injectPoolState(newState, pool)
		}
	}

	// Ensure the flash‑loan provider has a huge balance of the borrowed token.
	if payload.LoanPool != (common.Address{}) && payload.BorrowedToken != (common.Address{}) {
		huge := new(big.Int).Exp(big.NewInt(10), big.NewInt(30), nil)
		setERC20Balance(newState, payload.BorrowedToken, payload.LoanPool, huge)
	}

	// Snapshot block context under lock.
	g.evmMu.RLock()
	blockCtx := g.blockCtx
	txCtx := vm.TxContext{
		Origin:   g.owner,
		GasPrice: g.txCtx.GasPrice,
	}
	g.evmMu.RUnlock()

	// Execute.
	evm := vm.NewEVM(blockCtx, txCtx, newState, g.chainConfig, vm.Config{})
	outputs, leftOverGas, err := evm.Call(
		vm.AccountRef(g.owner),
		payload.TargetExecutor,
		payload.Calldata,
		payload.GasLimit,
		uint256.NewInt(0),
	)
	gasUsed := payload.GasLimit - leftOverGas

	if err != nil {
		if len(outputs) > 0 {
			var revertMsg string
			if len(outputs) >= 4 {
				selector := outputs[:4]
				if decoded, decodeErr := decodeCustomError(selector, outputs[4:]); decodeErr == nil {
					revertMsg = decoded
				} else {
					revertMsg = fmt.Sprintf("revert data: %x", outputs)
				}
			} else {
				revertMsg = fmt.Sprintf("revert data: %x", outputs)
			}
			return false, gasUsed, fmt.Errorf("contract reverted: %s", revertMsg)
		}
		return false, gasUsed, err
	}
	return true, gasUsed, nil
}

// SimulateWithBackend runs a simulation using the selected backend.
func (g *GEVMSimulator) SimulateWithBackend(
	cand *types.RouteCandidate,
	payload *types.ExecutionPayload,
	backend SimBackend,
) (bool, uint64, error) {
	switch backend {
	case SimBackendLocal:
		return g.SimulateNative(payload)
	case SimBackendRemote:
		fallthrough
	default:
		return g.simulateRemote(payload)
	}
}

// simulateRemote performs eth_call (no gas estimation to save time).
func (g *GEVMSimulator) simulateRemote(payload *types.ExecutionPayload) (bool, uint64, error) {
	if payload == nil || payload.Calldata == nil {
		return false, 0, errors.New("invalid payload")
	}
	select {
	case g.remoteSem <- struct{}{}:
		defer func() { <-g.remoteSem }()
	case <-time.After(500 * time.Millisecond):
		return false, 0, errors.New("remote simulation concurrency limit reached")
	}

	success, _, err := g.doEthCall(g.rpcURL, payload)
	if err != nil || !success {
		return success, 0, err
	}
	// Use provided gas limit; we skip estimateGas for speed.
	return true, payload.GasLimit, nil
}

// doEthCall performs eth_call and decodes revert data.
func (g *GEVMSimulator) doEthCall(targetURL string, payload *types.ExecutionPayload) (bool, string, error) {
	callArgs := map[string]interface{}{
		"to":   payload.TargetExecutor.Hex(),
		"data": "0x" + hex.EncodeToString(payload.Calldata),
		"from": g.owner.Hex(),
	}
	reqBody := map[string]interface{}{
		"jsonrpc": "2.0",
		"method":  "eth_call",
		"params":  []interface{}{callArgs, "latest"},
		"id":      1,
	}
	reqJSON, err := json.Marshal(reqBody)
	if err != nil {
		return false, "", err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, "POST", targetURL, bytes.NewReader(reqJSON))
	if err != nil {
		return false, "", err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := g.httpClient.Do(req)
	if err != nil {
		return false, "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return false, "", err
	}
	var rpcResp struct {
		Result string `json:"result"`
		Error  struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
			Data    string `json:"data,omitempty"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &rpcResp); err != nil {
		return false, "", err
	}
	if rpcResp.Error.Code != 0 {
		if strings.Contains(rpcResp.Error.Message, "execution reverted") {
			revertMsg := rpcResp.Error.Message
			if rpcResp.Error.Data != "" {
				dataHex := strings.TrimPrefix(rpcResp.Error.Data, "0x")
				if len(dataHex) >= 8 {
					if dataBytes, err := hex.DecodeString(dataHex); err == nil && len(dataBytes) >= 4 {
						selector := dataBytes[:4]
						if decoded, decodeErr := decodeCustomError(selector, dataBytes[4:]); decodeErr == nil {
							revertMsg = decoded
						}
					}
				}
			}
			return false, "", fmt.Errorf("contract reverted: %s", revertMsg)
		}
		return false, "", fmt.Errorf("eth_call error: %s", rpcResp.Error.Message)
	}
	return true, rpcResp.Result, nil
}

// decodeCustomError decodes known Solidity errors from revert data.
func decodeCustomError(selector []byte, data []byte) (string, error) {
	var sel [4]byte
	copy(sel[:], selector)

	switch {
	case bytes.Equal(sel[:], insufficientProfitSelector):
		vals, err := revertABI.Unpack("InsufficientProfit", data)
		if err != nil || len(vals) != 2 {
			return "", err
		}
		actual, ok1 := vals[0].(*big.Int)
		required, ok2 := vals[1].(*big.Int)
		if !ok1 || !ok2 {
			return "", errors.New("unexpected types")
		}
		return fmt.Sprintf("InsufficientProfit(actual=%d, required=%d)", actual, required), nil

	case bytes.Equal(sel[:], insufficientOutputSelector):
		vals, err := revertABI.Unpack("InsufficientOutput", data)
		if err != nil || len(vals) != 2 {
			return "", err
		}
		actual, ok1 := vals[0].(*big.Int)
		required, ok2 := vals[1].(*big.Int)
		if !ok1 || !ok2 {
			return "", errors.New("unexpected types")
		}
		return fmt.Sprintf("InsufficientOutput(actual=%d, required=%d)", actual, required), nil

	case bytes.Equal(sel[:], swapExecutionFailedSelector):
		return "SwapExecutionFailed()", nil

	case bytes.Equal(sel[:], loanRepaymentFailedSelector):
		return "LoanRepaymentFailed()", nil

	default:
		return "", fmt.Errorf("unknown error selector")
	}
}

// SimulateCandidate is a wrapper that chooses the backend automatically.
func (g *GEVMSimulator) SimulateCandidate(cand *types.RouteCandidate, payload *types.ExecutionPayload) (bool, uint64, error) {
	backend := g.ChooseBackend(cand)
	return g.SimulateWithBackend(cand, payload, backend)
}

// Simulate is a legacy method for remote only.
func (g *GEVMSimulator) Simulate(candidate *types.RouteCandidate, payload *types.ExecutionPayload) (bool, uint64, error) {
	return g.SimulateWithBackend(candidate, payload, SimBackendRemote)
}
