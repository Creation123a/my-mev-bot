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
			continue
		}
		c.SetCode(pool, code)
	}
	return nil
}

func (c *StateCache) WarmUpAddresses(client *ethclient.Client, addrs []common.Address) {
	ctx := context.Background()
	for _, addr := range addrs {
		code, err := client.CodeAt(ctx, addr, nil)
		if err != nil {
			log.Printf("[StateCache] WarmUp: failed to fetch code for %s: %v", addr.Hex(), err)
			continue
		}
		c.SetCode(addr, code)
	}
}

// GEVMSimulator provides remote and native in‑process EVM simulation.
type GEVMSimulator struct {
	httpClient *http.Client
	ethClient  *ethclient.Client // ADDED
	rpcURL     string
	wsURL      string
	owner      common.Address

	remoteSem chan struct{}

	evmMu       sync.RWMutex
	blockCtx    vm.BlockContext
	txCtx       vm.TxContext
	chainConfig *params.ChainConfig

	cache  *StateCache
	matrix *botstate.Matrix

	updaterCtx    context.Context
	updaterCancel context.CancelFunc
}

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

	// Create ethClient for code fetching
	ethClient, err := ethclient.Dial(rpcURL)
	if err != nil {
		log.Printf("[GEVM] Failed to dial eth client: %v", err)
		// We'll proceed without it; WarmUpAddress will fail if called.
	}

	return &GEVMSimulator{
		httpClient: &http.Client{
			Transport: transport,
			Timeout:   10 * time.Second,
		},
		ethClient:   ethClient,
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

func (g *GEVMSimulator) SetMatrix(matrix *botstate.Matrix) {
	g.matrix = matrix
}

func (g *GEVMSimulator) SetStateCache(cache *StateCache) {
	g.cache = cache
}

// WarmUpAddress fetches and caches bytecode for a given address.
func (g *GEVMSimulator) WarmUpAddress(addr common.Address) error {
	if g.ethClient == nil {
		return errors.New("ethClient not available")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	code, err := g.ethClient.CodeAt(ctx, addr, nil)
	if err != nil {
		return fmt.Errorf("failed to fetch code for %s: %w", addr.Hex(), err)
	}
	g.cache.SetCode(addr, code)
	return nil
}

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

func (g *GEVMSimulator) StopBackgroundUpdater() {
	if g.updaterCancel != nil {
		g.updaterCancel()
		g.updaterCtx = nil
		g.updaterCancel = nil
	}
}

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

func getBalanceSlot(token common.Address) uint64 {
	if token == config.USDCAddress || token == config.USDBCAddress {
		return 9
	}
	return 0
}

func setERC20Balance(stateDB *gethstate.StateDB, token, holder common.Address, amount *big.Int) {
	if amount == nil || amount.Sign() <= 0 {
		return
	}
	slot := getBalanceSlot(token)
	slotBytes := common.BigToHash(big.NewInt(int64(slot))).Bytes()
	key := common.BytesToHash(crypto.Keccak256(append(holder.Bytes(), slotBytes...)))
	stateDB.SetState(token, key, common.BigToHash(amount))
}

// injectPoolState now also injects V3 tick data.
func (g *GEVMSimulator) injectPoolState(stateDB *gethstate.StateDB, pool *types.PoolState) {
	if pool == nil {
		return
	}
	pool.RLock()
	defer pool.RUnlock()

	// V2 reserves
	if pool.DexType == types.DexAerodromeV2 || pool.DexType == types.DexAlienBaseV2 {
		stateDB.SetState(pool.PoolAddress, common.BigToHash(big.NewInt(0)), common.BigToHash(pool.Reserve0))
		stateDB.SetState(pool.PoolAddress, common.BigToHash(big.NewInt(1)), common.BigToHash(pool.Reserve1))
		stateDB.SetState(pool.PoolAddress, common.BigToHash(big.NewInt(2)), common.BigToHash(big.NewInt(time.Now().Unix())))
		setERC20Balance(stateDB, pool.Token0, pool.PoolAddress, pool.Reserve0)
		setERC20Balance(stateDB, pool.Token1, pool.PoolAddress, pool.Reserve1)
		return
	}

	// V3: slot0
	if pool.Slot0Packed != nil && pool.Slot0Packed.Sign() != 0 {
		stateDB.SetState(pool.PoolAddress, common.BigToHash(big.NewInt(0)), common.BigToHash(pool.Slot0Packed))
	} else {
		packed := new(big.Int).Set(pool.SqrtPriceX96)
		tickVal := uint64(int64(pool.Tick)) & 0xFFFFFF
		tickBig := new(big.Int).SetUint64(tickVal)
		tickBig.Lsh(tickBig, 160)
		packed.Or(packed, tickBig)
		cardBig := new(big.Int).SetUint64(1)
		cardBig.Lsh(cardBig, 200)
		packed.Or(packed, cardBig)
		cardNextBig := new(big.Int).SetUint64(1)
		cardNextBig.Lsh(cardNextBig, 216)
		packed.Or(packed, cardNextBig)
		unlockedBit := new(big.Int).Lsh(big.NewInt(1), 240)
		packed.Or(packed, unlockedBit)
		stateDB.SetState(pool.PoolAddress, common.BigToHash(big.NewInt(0)), common.BigToHash(packed))
	}

	// slot1: liquidity
	stateDB.SetState(pool.PoolAddress, common.BigToHash(big.NewInt(1)), common.BigToHash(pool.Liquidity))

	// feeGrowth defaults
	hugeFee := new(big.Int).Exp(big.NewInt(10), big.NewInt(36), nil)
	stateDB.SetState(pool.PoolAddress, common.BigToHash(big.NewInt(2)), common.BigToHash(hugeFee))
	stateDB.SetState(pool.PoolAddress, common.BigToHash(big.NewInt(3)), common.BigToHash(hugeFee))
	stateDB.SetState(pool.PoolAddress, common.BigToHash(big.NewInt(4)), common.BigToHash(new(big.Int)))

	// huge balances for the pool
	huge := new(big.Int).Exp(big.NewInt(10), big.NewInt(30), nil)
	setERC20Balance(stateDB, pool.Token0, pool.PoolAddress, huge)
	setERC20Balance(stateDB, pool.Token1, pool.PoolAddress, huge)

	// ---- NEW: Inject V3 tick bitmap and ticks ----
	pool.tickMu.RLock()
	bitmap := pool.TickBitmap
	liquidityNet := pool.LiquidityNet
	pool.tickMu.RUnlock()

	if len(bitmap) > 0 {
		// TickBitmap is mapping(int16 => uint256) at slot 3.
		// Treat bitmap as concatenated 32-byte words (word index = i)
		numWords := len(bitmap) / 32
		for i := 0; i < numWords; i++ {
			word := new(big.Int).SetBytes(bitmap[i*32 : (i+1)*32])
			if word.Sign() == 0 {
				continue
			}
			// storage key = keccak256(abi.encodePacked(uint16(i), uint256(3)))
			key := crypto.Keccak256(abi.Pack(int16(i), big.NewInt(3)))
			stateDB.SetState(pool.PoolAddress, common.BytesToHash(key), common.BigToHash(word))
		}
	}

	if len(liquidityNet) > 0 {
		// Ticks mapping(int24 => Tick.Info) at slot 2.
		// We only set liquidityNet (second field, offset +1)
		for tick, liqNet := range liquidityNet {
			if liqNet == nil || liqNet.Sign() == 0 {
				continue
			}
			// base key = keccak256(abi.encodePacked(int24(tick), uint256(2)))
			baseKey := crypto.Keccak256(abi.Pack(int24(tick), big.NewInt(2)))
			// liquidityNet is at baseKey + 1
			slotHash := common.BytesToHash(baseKey)
			slotBig := new(big.Int).SetBytes(slotHash.Bytes())
			slotBig.Add(slotBig, big.NewInt(1))
			slotKey := common.BigToHash(slotBig)
			stateDB.SetState(pool.PoolAddress, slotKey, common.BigToHash(liqNet))
		}
	}
}

func (g *GEVMSimulator) SimulateNative(payload *types.ExecutionPayload) (bool, uint64, error) {
	if payload == nil || payload.Calldata == nil {
		return false, 0, errors.New("invalid payload")
	}
	if g.matrix == nil {
		return false, 0, errors.New("no matrix set for local EVM state injection")
	}

	localMemDB := rawdb.NewMemoryDatabase()
	localTrieDB := triedb.NewDatabase(localMemDB, nil)
	stateDB := gethstate.NewDatabaseWithNodeDB(localMemDB, localTrieDB)

	newState, err := gethstate.New(common.Hash{}, stateDB, nil)
	if err != nil {
		return false, 0, fmt.Errorf("failed to create state: %w", err)
	}

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

	if payload.LoanPool != (common.Address{}) {
		if code, ok := g.cache.GetCode(payload.LoanPool); ok {
			newState.SetCode(payload.LoanPool, code)
		} else {
			log.Printf("[GEVM] Warning: loan provider code not in cache: %s", payload.LoanPool.Hex())
		}
	}

	uniqueTokens := make(map[common.Address]bool)
	uniqueTokens[payload.BorrowedToken] = true
	for _, poolAddr := range payload.RoutePools {
		if pool := g.matrix.GetPool(poolAddr); pool != nil {
			uniqueTokens[pool.Token0] = true
			uniqueTokens[pool.Token1] = true
		}
	}
	for token := range uniqueTokens {
		if token == (common.Address{}) {
			continue
		}
		if code, ok := g.cache.GetCode(token); ok {
			newState.SetCode(token, code)
		} else {
			log.Printf("[GEVM] Warning: token code not in cache: %s", token.Hex())
		}
	}

	for _, poolAddr := range payload.RoutePools {
		if poolAddr == (common.Address{}) {
			continue
		}
		if pool := g.matrix.GetPool(poolAddr); pool != nil {
			g.injectPoolState(newState, pool)
		}
	}

	if payload.LoanPool != (common.Address{}) && payload.BorrowedToken != (common.Address{}) {
		huge := new(big.Int).Exp(big.NewInt(10), big.NewInt(30), nil)
		setERC20Balance(newState, payload.BorrowedToken, payload.LoanPool, huge)
	}

	g.evmMu.RLock()
	blockCtx := g.blockCtx
	txCtx := vm.TxContext{
		Origin:   g.owner,
		GasPrice: g.txCtx.GasPrice,
	}
	g.evmMu.RUnlock()

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
	return true, payload.GasLimit, nil
}

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
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
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

func (g *GEVMSimulator) SimulateCandidate(cand *types.RouteCandidate, payload *types.ExecutionPayload) (bool, uint64, error) {
	backend := g.ChooseBackend(cand)
	return g.SimulateWithBackend(cand, payload, backend)
}

func (g *GEVMSimulator) Simulate(candidate *types.RouteCandidate, payload *types.ExecutionPayload) (bool, uint64, error) {
	return g.SimulateWithBackend(candidate, payload, SimBackendRemote)
}
