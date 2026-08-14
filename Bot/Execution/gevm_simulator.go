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
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/rawdb"
	gethstate "github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/triedb"
	"github.com/holiman/uint256"

	botstate "my-mev-bot/Bot/State"
	"my-mev-bot/Bot/Types"
)

// SimBackend defines the simulation backend type.
type SimBackend string

const (
	// SimBackendLocal uses an in‑process native EVM (zero‑latency).
	SimBackendLocal SimBackend = "local"
	// SimBackendAnvil uses a local Anvil fork.
	SimBackendAnvil SimBackend = "anvil"
	// SimBackendRemote uses a remote eth_call RPC.
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

// StateCache holds only contract bytecode (storage is now injected from matrix).
type StateCache struct {
	code map[common.Address][]byte
	mu   sync.RWMutex
}

func NewStateCache() *StateCache {
	return &StateCache{
		code: make(map[common.Address][]byte),
	}
}

// SetCode stores contract bytecode.
func (c *StateCache) SetCode(addr common.Address, code []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.code[addr] = code
}

// GetCode retrieves contract bytecode.
func (c *StateCache) GetCode(addr common.Address) ([]byte, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	code, ok := c.code[addr]
	return code, ok
}

// WarmUp pre‑loads code for known pools and the executor contract.
// This must be called at startup before using the local EVM backend.
func (c *StateCache) WarmUp(executor common.Address, pools []common.Address, client *ethclient.Client) error {
	ctx := context.Background()

	// Fetch executor code.
	code, err := client.CodeAt(ctx, executor, nil)
	if err != nil {
		return fmt.Errorf("failed to get executor code: %w", err)
	}
	c.SetCode(executor, code)

	// For each pool, fetch code only (storage is injected from matrix later).
	for _, pool := range pools {
		code, err := client.CodeAt(ctx, pool, nil)
		if err != nil {
			continue // skip if we can't fetch code
		}
		c.SetCode(pool, code)
	}
	return nil
}

// GEVMSimulator provides remote, Anvil, and native in‑process EVM simulation.
type GEVMSimulator struct {
	httpClient *http.Client
	rpcURL     string
	owner      common.Address

	// Anvil settings – protected by healthMu.
	anvilRPCURL  string
	anvilHealthy bool
	healthMu     sync.RWMutex

	// Concurrency control for remote and anvil simulations.
	remoteSem chan struct{}
	anvilSem  chan struct{}

	// Native EVM state (in‑memory, zero‑latency).
	nativeDB    ethdb.Database
	nativeState *gethstate.StateDB
	blockCtx    vm.BlockContext
	txCtx       vm.TxContext
	chainConfig *params.ChainConfig
	evmMu       sync.RWMutex

	// State cache for fast code injection.
	cache *StateCache

	// Matrix reference for fresh state injection.
	matrix *botstate.Matrix

	// Background updater control.
	updaterCtx    context.Context
	updaterCancel context.CancelFunc
}

// NewGEVMSimulator creates a simulator with remote RPC, Anvil, and native EVM backends.
func NewGEVMSimulator(rpcURL string, owner common.Address, anvilURL string) *GEVMSimulator {
	transport := &http.Transport{
		MaxIdleConns:        10,
		MaxIdleConnsPerHost: 10,
		IdleConnTimeout:     90 * time.Second,
		DisableKeepAlives:   false,
	}

	// Initialise native in‑memory EVM state.
	memDB := rawdb.NewMemoryDatabase()
	triedbDB := triedb.NewDatabase(memDB, nil)
	stateDB := gethstate.NewDatabaseWithNodeDB(memDB, triedbDB)

	rootState, err := gethstate.New(common.Hash{}, stateDB, nil)
	if err != nil {
		panic(fmt.Sprintf("failed to init native state: %v", err))
	}

	// Use MainnetChainConfig as a baseline; Base is EVM-compatible.
	chainCfg := params.MainnetChainConfig

	// Default block context (will be updated via RPC).
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
			Timeout:   5 * time.Second,
		},
		rpcURL:       rpcURL,
		owner:        owner,
		anvilRPCURL:  anvilURL,
		anvilHealthy: false,
		remoteSem:    make(chan struct{}, 8),
		anvilSem:     make(chan struct{}, 4),
		nativeDB:     memDB,
		nativeState:  rootState,
		blockCtx:     blockCtx,
		txCtx:        txCtx,
		chainConfig:  chainCfg,
		cache:        NewStateCache(),
	}
}

// SetMatrix sets the state matrix used for fresh state injection.
func (g *GEVMSimulator) SetMatrix(matrix *botstate.Matrix) {
	g.matrix = matrix
}

// SetStateCache sets a custom state cache (useful for sharing cache across instances).
func (g *GEVMSimulator) SetStateCache(cache *StateCache) {
	g.cache = cache
}

// StartBackgroundUpdater starts a goroutine that periodically updates the block context
// from the RPC. This ensures the local EVM uses current block parameters.
// Call this after the simulator is created and before using SimulateNative.
func (g *GEVMSimulator) StartBackgroundUpdater(ctx context.Context) {
	if g.updaterCtx != nil {
		return // already running
	}
	updaterCtx, cancel := context.WithCancel(ctx)
	g.updaterCtx = updaterCtx
	g.updaterCancel = cancel

	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-updaterCtx.Done():
				return
			case <-ticker.C:
				if err := g.UpdateBlockContext(updaterCtx); err != nil {
					log.Printf("[GEVM] Warning: failed to update block context: %v\n", err)
				}
			}
		}
	}()
}

// StopBackgroundUpdater stops the background updater goroutine.
func (g *GEVMSimulator) StopBackgroundUpdater() {
	if g.updaterCancel != nil {
		g.updaterCancel()
		g.updaterCtx = nil
		g.updaterCancel = nil
	}
}

// SetAnvilURL allows setting or updating the Anvil URL.
func (g *GEVMSimulator) SetAnvilURL(url string) {
	g.healthMu.Lock()
	defer g.healthMu.Unlock()
	g.anvilRPCURL = url
}

func (g *GEVMSimulator) getAnvilURL() string {
	g.healthMu.RLock()
	defer g.healthMu.RUnlock()
	return g.anvilRPCURL
}

// IsAnvilHealthy returns true if the Anvil fork is responsive.
func (g *GEVMSimulator) IsAnvilHealthy() bool {
	g.healthMu.RLock()
	defer g.healthMu.RUnlock()
	return g.anvilHealthy && g.anvilRPCURL != ""
}

// HealthCheckAnvil pings the Anvil endpoint.
func (g *GEVMSimulator) HealthCheckAnvil() {
	url := g.getAnvilURL()
	if url == "" {
		g.healthMu.Lock()
		g.anvilHealthy = false
		g.healthMu.Unlock()
		return
	}
	reqBody := map[string]interface{}{
		"jsonrpc": "2.0",
		"method":  "eth_blockNumber",
		"params":  []interface{}{},
		"id":      1,
	}
	reqJSON, _ := json.Marshal(reqBody)
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(reqJSON))
	if err != nil {
		g.healthMu.Lock()
		g.anvilHealthy = false
		g.healthMu.Unlock()
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := g.httpClient.Do(req)
	if err != nil {
		g.healthMu.Lock()
		g.anvilHealthy = false
		g.healthMu.Unlock()
		return
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	var rpcResp struct {
		Result string `json:"result"`
	}
	if err := json.Unmarshal(body, &rpcResp); err != nil {
		g.healthMu.Lock()
		g.anvilHealthy = false
		g.healthMu.Unlock()
		return
	}
	g.healthMu.Lock()
	g.anvilHealthy = len(rpcResp.Result) > 0
	g.healthMu.Unlock()
}

// ChooseBackend selects the simulation backend based on candidate properties.
// Prefers local EVM if matrix is available and the route pools are known.
func (g *GEVMSimulator) ChooseBackend(cand *types.RouteCandidate) SimBackend {
	if g.matrix != nil && len(cand.Pools) > 0 {
		// Check that all pools in the route exist in the matrix.
		allKnown := true
		for _, poolAddr := range cand.Pools[:cand.Hops] {
			if poolAddr == (common.Address{}) {
				allKnown = false
				break
			}
			if g.matrix.GetPool(poolAddr) == nil {
				allKnown = false
				break
			}
		}
		if allKnown {
			return SimBackendLocal
		}
	}
	// Fallback to Anvil if healthy, otherwise remote.
	if g.IsAnvilHealthy() {
		return SimBackendAnvil
	}
	return SimBackendRemote
}

// UpdateBlockContext fetches the latest block header from the RPC and updates the
// native EVM block context. This should be called periodically (e.g., once per block).
func (g *GEVMSimulator) UpdateBlockContext(ctx context.Context) error {
	type header struct {
		Number     string `json:"number"`
		Timestamp  string `json:"timestamp"`
		BaseFee    string `json:"baseFeePerGas"`
		GasLimit   string `json:"gasLimit"`
	}
	reqBody := map[string]interface{}{
		"jsonrpc": "2.0",
		"method":  "eth_getBlockByNumber",
		"params":  []interface{}{"latest", false},
		"id":      1,
	}
	reqJSON, _ := json.Marshal(reqBody)
	req, err := http.NewRequestWithContext(ctx, "POST", g.rpcURL, bytes.NewReader(reqJSON))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := g.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	var rpcResp struct {
		Result header `json:"result"`
	}
	if err := json.Unmarshal(body, &rpcResp); err != nil {
		return err
	}
	if rpcResp.Result.Number == "" {
		return errors.New("empty block header")
	}
	// Parse hex strings.
	blockNum := new(big.Int)
	blockNum.SetString(rpcResp.Result.Number[2:], 16)
	timestamp := new(big.Int)
	timestamp.SetString(rpcResp.Result.Timestamp[2:], 16)
	baseFee := new(big.Int)
	baseFee.SetString(rpcResp.Result.BaseFee[2:], 16)
	gasLimit := new(big.Int)
	gasLimit.SetString(rpcResp.Result.GasLimit[2:], 16)

	g.evmMu.Lock()
	defer g.evmMu.Unlock()
	g.blockCtx.BlockNumber = blockNum
	g.blockCtx.Time = timestamp.Uint64()
	g.blockCtx.BaseFee = baseFee
	g.blockCtx.GasLimit = gasLimit.Uint64()
	// Also update tx context gas price (set to base fee + small tip).
	g.txCtx.GasPrice = new(big.Int).Add(baseFee, big.NewInt(1e9)) // 1 gwei tip
	return nil
}

// injectPoolState writes the current pool state from the matrix into the StateDB.
func (g *GEVMSimulator) injectPoolState(stateDB *gethstate.StateDB, pool *types.PoolState) {
	if pool == nil {
		return
	}

	// Ensure big.Int fields are non‑nil.
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

	// V2 reserves: slots 0 and 1.
	if pool.DexType == types.DexAerodromeV2 || pool.DexType == types.DexAlienBaseV2 {
		stateDB.SetState(pool.PoolAddress, common.BigToHash(big.NewInt(0)), common.BigToHash(pool.Reserve0))
		stateDB.SetState(pool.PoolAddress, common.BigToHash(big.NewInt(1)), common.BigToHash(pool.Reserve1))
		return
	}

	// V3: slot0 is packed as:
	//   uint160 sqrtPriceX96       (bits 0..159)
	//   int24  tick                (bits 160..183)
	//   uint16 observationIndex    (bits 184..199)
	//   uint16 observationCardinality (bits 200..215)
	//   uint16 observationCardinalityNext (bits 216..231)
	//   uint8  feeProtocol         (bits 232..239)
	//   bool   unlocked            (bit 240)
	//
	// We set sqrtPriceX96 and tick (and force unlocked = true).
	// The other fields are left as zero (acceptable for simulation).
	packed := new(big.Int).Set(pool.SqrtPriceX96) // low 160 bits

	// tick (int24) as 24‑bit two's complement.
	tickVal := uint64(int64(pool.Tick)) & 0xFFFFFF // mask to 24 bits
	tickBig := new(big.Int).SetUint64(tickVal)
	tickBig.Lsh(tickBig, 160) // move to bits 160..183
	packed.Or(packed, tickBig)

	// Set the unlocked flag (bit 240) to 1.
	unlockedBit := new(big.Int).Lsh(big.NewInt(1), 240)
	packed.Or(packed, unlockedBit)

	stateDB.SetState(pool.PoolAddress, common.BigToHash(big.NewInt(0)), common.BigToHash(packed))

	// Slot1: liquidity (uint128)
	stateDB.SetState(pool.PoolAddress, common.BigToHash(big.NewInt(1)), common.BigToHash(pool.Liquidity))

	// No other slots are required for basic V3 swaps; the pool reads fee and token0/1
	// via code views, which are already provided by StateCache.
}

// SimulateNative runs a simulation using the in‑process EVM with state injected from the matrix.
// It returns success, gasUsed, and error.
func (g *GEVMSimulator) SimulateNative(
	payload *types.ExecutionPayload,
) (bool, uint64, error) {
	if payload == nil || payload.Calldata == nil {
		return false, 0, errors.New("invalid payload")
	}
	if g.matrix == nil {
		return false, 0, errors.New("no matrix set for local EVM state injection")
	}

	g.evmMu.Lock()
	// Create a fresh state for this simulation to avoid contaminating the base state.
	root := g.nativeState.IntermediateRoot(false)
	triedbDB := triedb.NewDatabase(g.nativeDB, nil)
	stateDB := gethstate.NewDatabaseWithNodeDB(g.nativeDB, triedbDB)

	newState, err := gethstate.New(root, stateDB, nil)
	if err != nil {
		g.evmMu.Unlock()
		return false, 0, fmt.Errorf("failed to copy state: %w", err)
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
	// Inject fresh pool state from matrix.
	for _, poolAddr := range payload.RoutePools {
		if poolAddr == (common.Address{}) {
			continue
		}
		pool := g.matrix.GetPool(poolAddr)
		if pool != nil {
			g.injectPoolState(newState, pool)
		}
	}
	g.evmMu.Unlock()

	// Prepare EVM.
	blockCtx := g.blockCtx
	txCtx := vm.TxContext{
		Origin:   g.owner,
		GasPrice: g.txCtx.GasPrice,
	}
	evm := vm.NewEVM(blockCtx, txCtx, newState, g.chainConfig, vm.Config{})

	// Execute.
	outputs, leftOverGas, err := evm.Call(
		vm.AccountRef(g.owner),
		payload.TargetExecutor,
		payload.Calldata,
		payload.GasLimit,
		uint256.NewInt(0),
	)
	gasUsed := payload.GasLimit - leftOverGas

	if err != nil {
		// Attempt to decode revert data if present.
		if len(outputs) > 0 {
			var revertMsg string
			if len(outputs) >= 4 {
				selector := outputs[:4]
				decoded, decodeErr := decodeCustomError(selector, outputs[4:])
				if decodeErr == nil {
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
	case SimBackendAnvil:
		return g.simulateAnvil(payload)
	case SimBackendRemote:
		fallthrough
	default:
		return g.simulateRemote(payload)
	}
}

// simulateRemote performs eth_call + eth_estimateGas via RPC.
func (g *GEVMSimulator) simulateRemote(payload *types.ExecutionPayload) (bool, uint64, error) {
	if payload == nil || payload.Calldata == nil {
		return false, 0, errors.New("invalid payload")
	}
	// Acquire remote concurrency token.
	select {
	case g.remoteSem <- struct{}{}:
		defer func() { <-g.remoteSem }()
	case <-time.After(500 * time.Millisecond):
		return false, 0, errors.New("remote simulation concurrency limit reached")
	}

	targetURL := g.rpcURL
	success, _, err := g.doEthCall(targetURL, payload)
	if err != nil || !success {
		return success, 0, err
	}
	gasUsed, err := g.estimateGas(targetURL, payload)
	if err != nil {
		return true, 0, fmt.Errorf("gas estimation failed: %w", err)
	}
	return true, gasUsed, nil
}

// simulateAnvil runs simulation against a local Anvil fork.
func (g *GEVMSimulator) simulateAnvil(payload *types.ExecutionPayload) (bool, uint64, error) {
	if payload == nil || payload.Calldata == nil {
		return false, 0, errors.New("invalid payload")
	}
	if !g.IsAnvilHealthy() {
		return g.simulateRemote(payload)
	}
	select {
	case g.anvilSem <- struct{}{}:
		defer func() { <-g.anvilSem }()
	case <-time.After(500 * time.Millisecond):
		return false, 0, errors.New("anvil simulation concurrency limit reached")
	}
	url := g.getAnvilURL()
	if url == "" {
		return false, 0, errors.New("anvil URL not set")
	}
	success, _, err := g.doEthCall(url, payload)
	if err != nil || !success {
		return g.simulateRemote(payload)
	}
	gasUsed, err := g.estimateGas(url, payload)
	if err != nil {
		return true, 0, fmt.Errorf("anvil gas estimation failed: %w", err)
	}
	return true, gasUsed, nil
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
	req, err := http.NewRequest("POST", targetURL, bytes.NewReader(reqJSON))
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
					dataBytes, err := hex.DecodeString(dataHex)
					if err == nil && len(dataBytes) >= 4 {
						selector := dataBytes[:4]
						decoded, decodeErr := decodeCustomError(selector, dataBytes[4:])
						if decodeErr == nil {
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

// estimateGas performs eth_estimateGas.
func (g *GEVMSimulator) estimateGas(targetURL string, payload *types.ExecutionPayload) (uint64, error) {
	callArgs := map[string]interface{}{
		"to":   payload.TargetExecutor.Hex(),
		"data": "0x" + hex.EncodeToString(payload.Calldata),
		"from": g.owner.Hex(),
	}
	reqBody := map[string]interface{}{
		"jsonrpc": "2.0",
		"method":  "eth_estimateGas",
		"params":  []interface{}{callArgs},
		"id":      1,
	}
	reqJSON, err := json.Marshal(reqBody)
	if err != nil {
		return 0, err
	}
	req, err := http.NewRequest("POST", targetURL, bytes.NewReader(reqJSON))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := g.httpClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, err
	}
	var rpcResp struct {
		Result string `json:"result"`
		Error  struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &rpcResp); err != nil {
		return 0, err
	}
	if rpcResp.Error.Code != 0 {
		return 0, fmt.Errorf("eth_estimateGas error: %s", rpcResp.Error.Message)
	}
	gasHex := strings.TrimPrefix(rpcResp.Result, "0x")
	gas, err := hexutil.DecodeUint64("0x" + gasHex)
	if err != nil {
		return 0, fmt.Errorf("failed to parse gas: %w", err)
	}
	return gas, nil
}

// decodeCustomError decodes known Solidity errors from revert data.
func decodeCustomError(selector []byte, data []byte) (string, error) {
	var sel [4]byte
	copy(sel[:], selector)

	if bytes.Equal(sel[:], insufficientProfitSelector) {
		vals, err := revertABI.Unpack("InsufficientProfit", data)
		if err != nil {
			return "", err
		}
		if len(vals) == 2 {
			actual, ok1 := vals[0].(*big.Int)
			required, ok2 := vals[1].(*big.Int)
			if ok1 && ok2 {
				return fmt.Sprintf("InsufficientProfit(actual=%d, required=%d)", actual, required), nil
			}
		}
	} else if bytes.Equal(sel[:], insufficientOutputSelector) {
		vals, err := revertABI.Unpack("InsufficientOutput", data)
		if err != nil {
			return "", err
		}
		if len(vals) == 2 {
			actual, ok1 := vals[0].(*big.Int)
			required, ok2 := vals[1].(*big.Int)
			if ok1 && ok2 {
				return fmt.Sprintf("InsufficientOutput(actual=%d, required=%d)", actual, required), nil
			}
		}
	} else if bytes.Equal(sel[:], swapExecutionFailedSelector) {
		return "SwapExecutionFailed()", nil
	} else if bytes.Equal(sel[:], loanRepaymentFailedSelector) {
		return "LoanRepaymentFailed()", nil
	}
	return "", fmt.Errorf("unknown error selector")
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
