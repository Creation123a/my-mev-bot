// Package bonding – dynamic bonding curve graduation sniper.
package bonding

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/big"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	gethTypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/gorilla/websocket"

	"my-mev-bot/Bot/Config"
	"my-mev-bot/Bot/Execution"
	"my-mev-bot/Bot/State"
	"my-mev-bot/Bot/Types"
)

// =============================================================================
// Constants – factories, events, and WebSocket
// =============================================================================
// FactoryABI holds discovered function names for a bonding curve factory.
type FactoryABI struct {
    ReserveFunc string
    TargetFunc  string
}

var (
    factoryABICache   = make(map[common.Address]*FactoryABI)
    factoryCacheMu    sync.RWMutex
)

const (
	candidateMaxAge = 24 * time.Hour
	wsPingInterval  = 30 * time.Second
	wsPongWait      = 60 * time.Second
	wsReconnectWait = 2 * time.Second
)

var (
	clankerV4Factory          = common.HexToAddress("0xe85a59c628f7d27878aceb4bf3b35733630083a9")
	virtualsFactory           = common.HexToAddress("0x1A540088125d00dD3990f9dA45CA0859af4d3B01")
	uniswapV3Factory          = common.HexToAddress("0x33128a8fC17869897dcE68Ed026d694621f6FDfD") // Base Uniswap V3 factory

	clankerTokenDeployedTopic = common.HexToHash("0x9c19d4432095f921f1e002ef2fa3e30f14002fa3e30f140023a109e9e9e9a4f6")
	virtualsNewTokenTopic     = common.HexToHash("0x1ab6e1b7c35f99238eb159ef8c8430ea4b8aefdc53b9fce639fbce76189bb5da")
	poolCreatedTopic          = common.HexToHash("0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118")

	// Pre‑parsed ABIs
	clankerABI         abi.ABI
	virtualsABI        abi.ABI
	poolCreatedABI     abi.ABI
	reserveABI         abi.ABI
	reserveFallbackABI abi.ABI
	targetABI          abi.ABI
	goalABI            abi.ABI
	v3PoolABI        abi.ABI
)

var bondingABI abi.ABI

func init() {
	var err error
	clankerABI, err = abi.JSON(strings.NewReader(`[{"anonymous":false,"inputs":[{"indexed":false,"internalType":"address","name":"token","type":"address"},{"indexed":false,"internalType":"address","name":"pair","type":"address"},{"indexed":false,"internalType":"address","name":"hook","type":"address"},{"indexed":false,"internalType":"uint256","name":"amount","type":"uint256"}],"name":"TokenDeployed","type":"event"}]`))
	if err != nil {
		panic(fmt.Sprintf("clanker ABI: %v", err))
	}
	virtualsABI, err = abi.JSON(strings.NewReader(`[{"anonymous":false,"inputs":[{"indexed":false,"internalType":"address","name":"token","type":"address"},{"indexed":false,"internalType":"uint256","name":"reserve","type":"uint256"},{"indexed":false,"internalType":"string","name":"name","type":"string"},{"indexed":false,"internalType":"string","name":"symbol","type":"string"}],"name":"NewToken","type":"event"}]`))
	if err != nil {
		panic(fmt.Sprintf("virtuals ABI: %v", err))
	}
	poolCreatedABI, err = abi.JSON(strings.NewReader(`[{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"pool","type":"address"},{"indexed":true,"internalType":"address","name":"token0","type":"address"},{"indexed":true,"internalType":"address","name":"token1","type":"address"},{"indexed":false,"internalType":"uint24","name":"fee","type":"uint24"},{"indexed":false,"internalType":"int24","name":"tickLower","type":"int24"},{"indexed":false,"internalType":"int24","name":"tickUpper","type":"int24"},{"indexed":false,"internalType":"uint128","name":"liquidity","type":"uint128"}],"name":"PoolCreated","type":"event"}]`))
	if err != nil {
		panic(fmt.Sprintf("poolCreated ABI: %v", err))
	}
	reserveABI, err = abi.JSON(strings.NewReader(`[{"inputs":[],"name":"getReserve","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"}]`))
	if err != nil {
		panic(fmt.Sprintf("reserve ABI: %v", err))
	}
	reserveFallbackABI, err = abi.JSON(strings.NewReader(`[{"inputs":[],"name":"reserve","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"}]`))
	if err != nil {
		panic(fmt.Sprintf("reserve fallback ABI: %v", err))
	}
bondingABI, err = abi.JSON(strings.NewReader(`[{"type":"function","name":"executeBondingArbitrage","inputs":[{"name":"factory","type":"address"},{"name":"token","type":"address"},{"name":"baseToken","type":"address"},{"name":"amount","type":"uint256"},{"name":"deadline","type":"uint256"},{"name":"fee","type":"uint24"},{"name":"minAmountOut","type":"uint256"}],"outputs":[]}]`))
	if err != nil {
		panic(fmt.Sprintf("bonding ABI: %v", err))
	}
	targetABI, err = abi.JSON(strings.NewReader(`[{"inputs":[],"name":"target","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"}]`))
	if err != nil {
		panic(fmt.Sprintf("target ABI: %v", err))
	}
	goalABI, err = abi.JSON(strings.NewReader(`[{"inputs":[],"name":"goal","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"}]`))
	if err != nil {
		panic(fmt.Sprintf("goal ABI: %v", err))
	}
	v3PoolABI, err = abi.JSON(strings.NewReader(`[{"inputs":[],"name":"slot0","outputs":[{"internalType":"uint160","name":"sqrtPriceX96","type":"uint160"},{"internalType":"int24","name":"tick","type":"int24"},{"internalType":"uint16","name":"observationIndex","type":"uint16"},{"internalType":"uint16","name":"observationCardinality","type":"uint16"},{"internalType":"uint16","name":"observationCardinalityNext","type":"uint16"},{"internalType":"uint8","name":"feeProtocol","type":"uint8"},{"internalType":"bool","name":"unlocked","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"liquidity","outputs":[{"internalType":"uint128","name":"","type":"uint128"}],"stateMutability":"view","type":"function"}]`))
    if err != nil {
        panic(fmt.Sprintf("v3Pool ABI: %v", err))
    }
}

// BondingCandidate tracks a potential graduation.
type BondingCandidate struct {
	TokenAddress    common.Address
	Factory         common.Address
	CurrentReserve  *big.Int
	TargetThreshold *big.Int
	Progress        float64
	LastUpdate      time.Time
	Graduating      bool
	PoolAddress     common.Address
	Calldata        []byte
	GasEstimate     uint64
	Fee             uint32
	ReadyToGraduate bool
	PairAddress common.Address
}

// Tracker monitors bonding curve factories and triggers flash‑loan arbitrage.
type Tracker struct {
	client          *ethclient.Client
	gevm            *execution.GEVMSimulator
	blacklist       *state.Blacklist
	platformCache   *state.LRUCache
	coinCache       *state.LRUCache
	matrix          *state.Matrix
	executionChan   chan<- *types.ExecutionPayload
	payloadPool     *sync.Pool
	bondingExecutor common.Address
	priorityFee     uint64
	pollInterval    time.Duration
	baseToken       common.Address
	wsURL           string

	activeFactories   []common.Address
	factoriesMu       sync.RWMutex
	candidates        map[common.Address]*BondingCandidate
	mu                sync.Mutex
	ctx               context.Context
	cancel            context.CancelFunc
	lastProcessedBlock map[common.Address]uint64

	// WebSocket connection
	wsConn   *websocket.Conn
	wsMu     sync.Mutex
	wsClosed atomic.Bool

	// Log processing (P1)
	logChan chan gethTypes.Log
}

// NewTracker creates a new bonding tracker.
func NewTracker(
	client *ethclient.Client,
	blacklist *state.Blacklist,
	platformCache, coinCache *state.LRUCache,
	seedFactories []common.Address,
	bondingExecutor common.Address,
	priorityFee uint64,
	executionChan chan<- *types.ExecutionPayload,
	matrix *state.Matrix,
	payloadPool *sync.Pool,
	pollIntervalMs int,
) *Tracker {
	t := &Tracker{
		client:          client,
		blacklist:       blacklist,
		platformCache:   platformCache,
		coinCache:       coinCache,
		matrix:          matrix,
		executionChan:   executionChan,
		payloadPool:     payloadPool,
		bondingExecutor: bondingExecutor,
		priorityFee:     priorityFee,
		pollInterval:    time.Duration(pollIntervalMs) * time.Millisecond,
		baseToken:       config.WETHAddress,
		candidates:      make(map[common.Address]*BondingCandidate),
		lastProcessedBlock: make(map[common.Address]uint64),
		logChan:         make(chan gethTypes.Log, 256),
	}
	t.factoriesMu.Lock()
	t.activeFactories = append(t.activeFactories, seedFactories...)
	t.factoriesMu.Unlock()
	for _, f := range seedFactories {
		platformCache.Put(f)
	}
	return t
}
// resolveFactoryABI discovers the correct function names for a bonding curve factory.
// It probes common patterns and caches the result.
// tracker.go – replace resolveFactoryABI with this:
func (t *Tracker) resolveContractABI(contract common.Address) *FactoryABI {
    factoryCacheMu.RLock()
    if abi, ok := factoryABICache[contract]; ok {
        factoryCacheMu.RUnlock()
        return abi
    }
    factoryCacheMu.RUnlock()

    ctx := t.ctx
    if ctx == nil { ctx = context.Background() }

    candidatesReserve := []string{"getReserve", "reserve", "currentReserve"}
    candidatesTarget := []string{"target", "goal", "getTarget"}

    var foundReserve, foundTarget string
    for _, fn := range candidatesReserve {
        data := functionSelector(fn)
        msg := ethereum.CallMsg{To: &contract, Data: data}
        _, err := t.client.CallContract(ctx, msg, nil)
        if err == nil { foundReserve = fn; break }
    }
    for _, fn := range candidatesTarget {
        data := functionSelector(fn)
        msg := ethereum.CallMsg{To: &contract, Data: data}
        _, err := t.client.CallContract(ctx, msg, nil)
        if err == nil { foundTarget = fn; break }
    }

    if foundReserve == "" { foundReserve = "getReserve" }
    if foundTarget == "" { foundTarget = "target" }

    abi := &FactoryABI{ReserveFunc: foundReserve, TargetFunc: foundTarget}
    factoryCacheMu.Lock()
    factoryABICache[contract] = abi
    factoryCacheMu.Unlock()
    return abi
}

// functionSelector returns the 4‑byte selector for a function with no arguments.
func functionSelector(name string) []byte {
    hash := crypto.Keccak256([]byte(name + "()"))
    return hash[:4]
}
// fetchV3PoolState calls slot0() and liquidity() on a Uniswap V3 pool.
func (t *Tracker) fetchV3PoolState(ctx context.Context, pool common.Address) (sqrtPrice *big.Int, tick int32, liquidity *big.Int, err error) {
    contract := bind.NewBoundContract(pool, v3PoolABI, t.client, t.client, t.client)
    
    // Call slot0
    var out []interface{}
    err = contract.Call(&bind.CallOpts{Context: ctx}, &out, "slot0")
    if err != nil {
        return nil, 0, nil, fmt.Errorf("slot0 failed: %w", err)
    }
    if len(out) < 7 {
        return nil, 0, nil, fmt.Errorf("slot0 returned insufficient values")
    }
    sqrtPrice = out[0].(*big.Int)
    tick = int32(out[1].(*big.Int).Int64())

    // Call liquidity
    var liqOut []interface{}
    err = contract.Call(&bind.CallOpts{Context: ctx}, &liqOut, "liquidity")
    if err != nil {
        return nil, 0, nil, fmt.Errorf("liquidity failed: %w", err)
    }
    if len(liqOut) == 0 {
        return nil, 0, nil, fmt.Errorf("liquidity returned no value")
    }
    liquidity = liqOut[0].(*big.Int)
    return sqrtPrice, int32(tick), liquidity, nil
}
// SetWSURL sets the WebSocket endpoint for real‑time event subscriptions.
func (t *Tracker) SetWSURL(url string) {
	t.wsURL = url
}

// SetGEVM injects the simulator for pre‑flight validation (T3).
func (t *Tracker) SetGEVM(gevm *execution.GEVMSimulator) {
	t.gevm = gevm
}

// AddFactory adds a new bonding curve factory for monitoring (T4).
func (t *Tracker) AddFactory(factory common.Address) {
    t.factoriesMu.Lock()
    defer t.factoriesMu.Unlock()
    for _, f := range t.activeFactories {
        if f == factory { return }
    }
    t.activeFactories = append(t.activeFactories, factory)
    t.platformCache.Put(factory)
    // Force WebSocket reconnection to rebuild filter.
    t.wsMu.Lock()
    if t.wsConn != nil {
        t.wsConn.Close()
    }
    t.wsClosed.Store(true)
    t.wsMu.Unlock()
    log.Printf("[Bonding] Added new factory: %s, WebSocket will reconnect with updated filter", factory.Hex())
}

// Run starts the main loop and WebSocket subscribers.
func (t *Tracker) Run(ctx context.Context) {
	t.ctx, t.cancel = context.WithCancel(ctx)
	defer t.cancel()

	// Start log processors (P1)
	for i := 0; i < 4; i++ {
		go t.logProcessor(t.ctx)
	}

	// Start WebSocket subscribers for real‑time events (T1)
	if t.wsURL != "" {
		go t.runWebSocketSubscription(t.ctx)
	} else {
		log.Printf("[Bonding] WebSocket URL not set; falling back to polling only.")
	}

	// Keep polling as a fallback (reduced frequency)
	pollTicker := time.NewTicker(5 * time.Second)
	defer pollTicker.Stop()

	cleanupTicker := time.NewTicker(10 * time.Minute)
	defer cleanupTicker.Stop()

	for {
		select {
		case <-t.ctx.Done():
			return
		case <-pollTicker.C:
			if t.wsURL == "" || t.wsClosed.Load() {
				t.processActiveFactories()
				t.checkPoolCreations()
			}
			t.checkCandidatesProgress(t.ctx)
		case <-cleanupTicker.C:
			t.cleanupStaleCandidates()
		}
	}
}

// ---- Log processor (P1) ----

func (t *Tracker) logProcessor(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case vLog := <-t.logChan:
			t.handleWebSocketLog(vLog)
		}
	}
}

// ---- WebSocket Subscription (T1) ----

func (t *Tracker) runWebSocketSubscription(ctx context.Context) {
	addresses := t.getActiveFactories()
	addresses = append(addresses, uniswapV3Factory)

	filter := map[string]interface{}{
		"address": addresses,
		"topics": [][]interface{}{
			{clankerTokenDeployedTopic, virtualsNewTokenTopic, poolCreatedTopic},
		},
	}

	subReq := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "eth_subscribe",
		"params":  []interface{}{"logs", filter},
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
			conn, err := t.dialWebSocket(ctx)
			if err != nil {
				log.Printf("[Bonding] WebSocket dial failed: %v", err)
				time.Sleep(wsReconnectWait)
				continue
			}
			t.wsMu.Lock()
			if t.wsConn != nil {
				t.wsConn.Close()
			}
			t.wsConn = conn
			t.wsClosed.Store(false)

			// Setup ping/pong (P3)
			conn.SetPongHandler(func(appData string) error {
				return conn.SetReadDeadline(time.Now().Add(wsPongWait))
			})
			conn.SetPingHandler(func(appData string) error {
				if err := conn.WriteControl(websocket.PongMessage, []byte(appData), time.Now().Add(5*time.Second)); err != nil {
					return err
				}
				return conn.SetReadDeadline(time.Now().Add(wsPongWait))
			})

			// Start ping ticker
			pingTicker := time.NewTicker(wsPingInterval)
			go func() {
				for {
					select {
					case <-ctx.Done():
						pingTicker.Stop()
						return
					case <-pingTicker.C:
						t.wsMu.Lock()
						if t.wsConn != nil {
							if err := t.wsConn.WriteControl(websocket.PingMessage, nil, time.Now().Add(5*time.Second)); err != nil {
								log.Printf("[Bonding] Ping failed: %v", err)
							}
						}
						t.wsMu.Unlock()
					}
				}
			}()

			t.wsMu.Unlock()

			if err := conn.WriteJSON(subReq); err != nil {
				log.Printf("[Bonding] Subscription send failed: %v", err)
				conn.Close()
				t.wsClosed.Store(true)
				time.Sleep(wsReconnectWait)
				continue
			}

			// Read loop
			for {
				var msg struct {
					JSONRPC string          `json:"jsonrpc"`
					Method  string          `json:"method"`
					Params  json.RawMessage `json:"params"`
				}
				err := conn.ReadJSON(&msg)
				if err != nil {
					log.Printf("[Bonding] WebSocket read error: %v", err)
					conn.Close()
					t.wsClosed.Store(true)
					break
				}
				if msg.Method != "eth_subscription" {
					continue
				}
				var subData struct {
					Result json.RawMessage `json:"result"`
				}
				if err := json.Unmarshal(msg.Params, &subData); err != nil {
					continue
				}
				var vLog gethTypes.Log
				if err := json.Unmarshal(subData.Result, &vLog); err != nil {
					continue
				}
				// Offload to worker (P1)
				select {
				case t.logChan <- vLog:
				default:
					log.Printf("[Bonding] Log channel full, dropping event")
				}
			}
		}
	}
}

func (t *Tracker) dialWebSocket(ctx context.Context) (*websocket.Conn, error) {
	if t.wsURL == "" {
		return nil, fmt.Errorf("WebSocket URL not set")
	}
	dialer := websocket.Dialer{
		ReadBufferSize:  65536,
		WriteBufferSize: 65536,
	}
	conn, _, err := dialer.DialContext(ctx, t.wsURL, nil)
	return conn, err
}

func (t *Tracker) handleWebSocketLog(vLog gethTypes.Log) {
	if len(vLog.Topics) == 0 {
		return
	}
	topic0 := vLog.Topics[0]
	if topic0 == clankerTokenDeployedTopic || topic0 == virtualsNewTokenTopic {
		var factory common.Address
		if vLog.Address == clankerV4Factory {
			factory = clankerV4Factory
		} else if vLog.Address == virtualsFactory {
			factory = virtualsFactory
		} else {
			factory = vLog.Address
		}
		t.handleNewTokenLog(vLog, factory)
	} else if topic0 == poolCreatedTopic {
		t.handlePoolCreated(vLog)
	}
}

// ---- Fallback polling methods ----

func (t *Tracker) getActiveFactories() []common.Address {
	t.factoriesMu.RLock()
	defer t.factoriesMu.RUnlock()
	cpy := make([]common.Address, len(t.activeFactories))
	copy(cpy, t.activeFactories)
	return cpy
}

func (t *Tracker) processActiveFactories() {
	ctx := t.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	t.factoriesMu.RLock()
	factories := make([]common.Address, len(t.activeFactories))
	copy(factories, t.activeFactories)
	t.factoriesMu.RUnlock()

	for _, f := range factories {
		t.processFactoryLogs(f, ctx)
	}
}

func (t *Tracker) processFactoryLogs(factory common.Address, ctx context.Context) {
	var topics [][]common.Hash
	if factory == clankerV4Factory {
		topics = [][]common.Hash{{clankerTokenDeployedTopic}}
	} else if factory == virtualsFactory {
		topics = [][]common.Hash{{virtualsNewTokenTopic}}
	} else {
		return
	}

	header, err := t.client.HeaderByNumber(ctx, nil)
	if err != nil {
		return
	}
	currentBlock := header.Number.Uint64()

	t.factoriesMu.RLock()
	fromBlock := t.lastProcessedBlock[factory]
	t.factoriesMu.RUnlock()
	if fromBlock == 0 {
		if currentBlock > 2000 {
			fromBlock = currentBlock - 2000
		} else {
			fromBlock = 0
		}
	} else {
		if fromBlock > 0 {
			fromBlock--
		}
	}
	query := ethereum.FilterQuery{
		Addresses: []common.Address{factory},
		Topics:    topics,
		FromBlock: new(big.Int).SetUint64(fromBlock),
		ToBlock:   nil,
	}
	logs, err := t.client.FilterLogs(ctx, query)
	if err != nil {
		return
	}
	for _, vLog := range logs {
		t.handleNewTokenLog(vLog, factory)
	}
	t.factoriesMu.Lock()
	t.lastProcessedBlock[factory] = currentBlock
	t.factoriesMu.Unlock()
}

func (t *Tracker) checkPoolCreations() {
	ctx := t.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	t.factoriesMu.RLock()
	factories := make([]common.Address, 0, len(t.activeFactories)+1)
factories = append(factories, t.activeFactories...)
factories = append(factories, uniswapV3Factory)
	t.factoriesMu.RUnlock()

	header, err := t.client.HeaderByNumber(ctx, nil)
	if err != nil {
		return
	}
	fromBlock := new(big.Int).Sub(header.Number, big.NewInt(100))
	if fromBlock.Sign() < 0 {
		fromBlock = big.NewInt(0)
	}
	query := ethereum.FilterQuery{
		Addresses: factories,
		Topics:    [][]common.Hash{{poolCreatedTopic}},
		FromBlock: fromBlock,
		ToBlock:   nil,
	}
	logs, err := t.client.FilterLogs(ctx, query)
	if err != nil {
		return
	}
	for _, vLog := range logs {
		t.handlePoolCreated(vLog)
	}
}

// ---- Core event handlers ----

// handleNewTokenLog parses TokenDeployed or NewToken events.
func (t *Tracker) handleNewTokenLog(vLog gethTypes.Log, factory common.Address) {
    var tokenAddr common.Address
    var reserve, target *big.Int
    var pairAddr common.Address // for Clanker

    if vLog.Topics[0] == clankerTokenDeployedTopic {
        var ev struct {
            Token  common.Address
            Pair   common.Address
            Hook   common.Address
            Amount *big.Int
        }
        if err := clankerABI.UnpackIntoInterface(&ev, "TokenDeployed", vLog.Data); err != nil {
            log.Printf("[Bonding] Failed to unpack Clanker event: %v", err)
            return
        }
        tokenAddr = ev.Token
        reserve = ev.Amount
        pairAddr = ev.Pair // store pair address for later
        target = t.fetchTargetFromPair(ev.Pair, factory)
        if target == nil || target.Sign() == 0 {
            target = new(big.Int) // zero, will be retried in polling
        }
    } else if vLog.Topics[0] == virtualsNewTokenTopic {
        var ev struct {
            Token   common.Address
            Reserve *big.Int
            Name    string
            Symbol  string
        }
        if err := virtualsABI.UnpackIntoInterface(&ev, "NewToken", vLog.Data); err != nil {
            log.Printf("[Bonding] Failed to unpack Virtuals event: %v", err)
            return
        }
        tokenAddr = ev.Token
        reserve = ev.Reserve
        target = t.fetchTargetFromToken(tokenAddr, factory)
        if target == nil || target.Sign() == 0 {
            target = new(big.Int) // zero, will be retried in polling
        }
    } else {
        return
    }

    t.mu.Lock()
    defer t.mu.Unlock()
    cand, ok := t.candidates[tokenAddr]
    if !ok {
        cand = &BondingCandidate{
            TokenAddress:    tokenAddr,
            Factory:         factory,
            CurrentReserve:  reserve,
            TargetThreshold: target,
            LastUpdate:      time.Now(),
            PairAddress:     pairAddr, // set for Clanker (zero for Virtuals)
        }
        t.candidates[tokenAddr] = cand
    } else {
        cand.CurrentReserve = reserve
        cand.TargetThreshold = target
        cand.LastUpdate = time.Now()
        // For Clanker, ensure the pair address is updated (in case it changed)
        if factory == clankerV4Factory {
            cand.PairAddress = pairAddr
        }
    }
    if target != nil && target.Sign() > 0 {
        progress := new(big.Float).Quo(
            new(big.Float).SetInt(reserve),
            new(big.Float).SetInt(target),
        )
        cand.Progress, _ = progress.Float64()
    } else {
        cand.Progress = 0
    }
    if cand.Progress >= 0.999 && !cand.ReadyToGraduate {
        cand.ReadyToGraduate = true
        log.Printf("[Bonding] Candidate reached 99.9%%: %s (progress %.4f), waiting for pool creation", tokenAddr.Hex(), cand.Progress)
        t.coinCache.Put(tokenAddr)
    }
}
// handlePoolCreated processes a PoolCreated event and triggers the arbitrage.
func (t *Tracker) handlePoolCreated(vLog gethTypes.Log) {
    if len(vLog.Topics) < 4 {
        return
    }
    token0 := common.BytesToAddress(vLog.Topics[1].Bytes())
    token1 := common.BytesToAddress(vLog.Topics[2].Bytes())
    feePips := uint32(new(big.Int).SetBytes(vLog.Topics[3].Bytes()).Uint64())
    if len(vLog.Data) < 32 {
        return
    }
    poolAddr := common.BytesToAddress(vLog.Data[len(vLog.Data)-20:])

    t.mu.Lock()
    var cand *BondingCandidate
    for _, c := range t.candidates {
        if (c.TokenAddress == token0 || c.TokenAddress == token1) && (c.Graduating || c.ReadyToGraduate) {
            cand = c
            break
        }
    }
    if cand != nil {
        cand.PoolAddress = poolAddr
        cand.Fee = feePips
    }
    t.mu.Unlock()
    if cand == nil {
        return
    }

    // ---- Fetch actual pool state ----
    ctx := t.ctx
    if ctx == nil {
        ctx = context.Background()
    }
    sqrtPriceX96, tick, liquidity, err := t.fetchV3PoolState(ctx, poolAddr)
if err != nil {
    log.Printf("[Bonding] Failed to fetch pool state for %s: %v", poolAddr.Hex(), err)
    sqrtPriceX96 = new(big.Int)
    liquidity = new(big.Int)
    tick = 0
}
// ---- NEW: Validate pool state ----
if sqrtPriceX96.Sign() == 0 || liquidity.Sign() == 0 {
    log.Printf("[Bonding] Pool %s has zero sqrtPrice or liquidity, skipping graduation", poolAddr.Hex())
    return
}

    // ---- Register the new pool in the matrix ----
    newPool := &types.PoolState{
        PoolAddress:   poolAddr,
        Token0:        token0,
        Token1:        token1,
        DexType:       types.DexUniswapV3,
        FeeBps:        feePips / 100,
        SqrtPriceX96:  sqrtPriceX96,
        Liquidity:     liquidity,
        Tick:          tick,
        LastUpdated:   time.Now(),
        Slot0Packed:   types.PackV3Slot0(sqrtPriceX96, tick),
    }
    t.matrix.RegisterPool(newPool)
    // ---- Warm up the pool in GEVM ----
if t.gevm != nil {
    if err := t.gevm.WarmUpAddress(poolAddr); err != nil {
        log.Printf("[Bonding] Failed to warm up pool %s: %v", poolAddr.Hex(), err)
    }
}
    log.Printf("[Bonding] Registered new pool in matrix: %s (sqrtPrice=%s, liquidity=%s)",
        poolAddr.Hex(), sqrtPriceX96.String(), liquidity.String())

    // ---- Graduate and qualify ----
    if cand.ReadyToGraduate || cand.Graduating {
        if !cand.Graduating {
            cand.Graduating = true
            log.Printf("[Bonding] Candidate now fully graduated and ready: %s", cand.TokenAddress.Hex())
        }
        t.qualifyCandidate(cand)
    }
}
// ---- Candidate Progress Polling ----

// checkCandidatesProgress – polls for reserve and detects graduation.
func (t *Tracker) checkCandidatesProgress(ctx context.Context) {
    // 1. Collect candidate addresses and current state (lock only)
    type candidateState struct {
        addr            common.Address
        pairAddr        common.Address
        targetThreshold *big.Int
        factory         common.Address
    }
    t.mu.Lock()
    states := make([]candidateState, 0, len(t.candidates))
    for addr, cand := range t.candidates {
        if cand.Graduating {
            continue
        }
        states = append(states, candidateState{
            addr:            addr,
            pairAddr:        cand.PairAddress,
            targetThreshold: cand.TargetThreshold,
            factory:         cand.Factory,
        })
    }
    t.mu.Unlock()

    if len(states) == 0 {
        return
    }

    // 2. Fetch reserves without the lock
    var toQualify []*BondingCandidate
    for _, st := range states {
        // ---- Handle unknown target (zero or nil) ----
        if st.targetThreshold == nil || st.targetThreshold.Sign() <= 0 {
            var newTarget *big.Int
            if st.factory == clankerV4Factory {
                newTarget = t.fetchTargetFromPair(st.pairAddr, st.factory)
            } else {
                newTarget = t.fetchTargetFromToken(st.addr, st.factory)
            }
            if newTarget != nil && newTarget.Sign() > 0 {
                t.mu.Lock()
                if cand, ok := t.candidates[st.addr]; ok && cand.TargetThreshold.Sign() <= 0 {
                    cand.TargetThreshold = newTarget
                }
                t.mu.Unlock()
                // Re‑evaluate this candidate in the next polling cycle
                continue
            }
            // Still unknown – skip this candidate for now
            continue
        }

        // ---- Fetch current reserve ----
        reserve, err := t.fetchCurrentReserve(ctx, st.addr, st.factory)
        if err != nil {
            continue
        }

        // ---- Compute progress ----
        progress, _ := new(big.Float).Quo(
            new(big.Float).SetInt(reserve),
            new(big.Float).SetInt(st.targetThreshold),
        ).Float64()

        // ---- Update candidate under lock ----
        t.mu.Lock()
        if cand, ok := t.candidates[st.addr]; ok && !cand.Graduating {
            cand.CurrentReserve = reserve
            cand.Progress = progress
            cand.LastUpdate = time.Now()
            if progress >= 0.999 {
                if cand.PoolAddress != (common.Address{}) && cand.Fee != 0 {
                    cand.Graduating = true
                    toQualify = append(toQualify, cand)
                    log.Printf("[Bonding] Candidate hit 99.9%% via polling with pool known: %s", st.addr.Hex())
                } else {
                    cand.ReadyToGraduate = true
                    log.Printf("[Bonding] Candidate hit 99.9%% via polling, waiting for pool: %s", st.addr.Hex())
                }
            }
        }
        t.mu.Unlock()
    }

    // 4. Process qualified candidates outside the lock
    for _, cand := range toQualify {
        t.qualifyCandidate(cand)
    }
}

/// qualifyCandidate with GEVM simulation (optimised lock)
func (t *Tracker) qualifyCandidate(cand *BondingCandidate) {
	// ===== Take a snapshot of all needed fields under lock =====
	t.mu.Lock()
if cand == nil || cand.Graduating { 
		t.mu.Unlock()
		return
	}
	poolAddr := cand.PoolAddress
	fee := cand.Fee
	if poolAddr == (common.Address{}) || fee == 0 {
		t.mu.Unlock()
		log.Printf("[Bonding] Candidate %s missing pool address or fee (%d), waiting for PoolCreated event",
			cand.TokenAddress.Hex(), fee)
		return
	}
	target := new(big.Int).Set(cand.TargetThreshold)
	reserve := new(big.Int).Set(cand.CurrentReserve)
	tokenAddr := cand.TokenAddress
	factory := cand.Factory
	// Mark as graduating (if not already) to avoid duplicate sends
	if !cand.Graduating {
		cand.Graduating = true
	}
	t.mu.Unlock()
	// ===== End of snapshot =====

	if target == nil || reserve == nil || target.Sign() <= 0 || reserve.Sign() <= 0 {
		return
	}
	remaining := new(big.Int).Sub(target, reserve)
	if remaining.Sign() <= 0 {
		return
	}
	deadline := uint64(time.Now().Unix()) + 120
	minAmountOut := new(big.Int).Mul(remaining, big.NewInt(90))
	minAmountOut.Div(minAmountOut, big.NewInt(100)) // 90% slippage protection
	if minAmountOut.Sign() == 0 {
		minAmountOut = big.NewInt(1)
	}
	if t.bondingExecutor == (common.Address{}) {
		log.Printf("[Bonding] BondingExecutor address not set; skipping")
		return
	}

	// Build calldata using the snapshot values
	var calldata []byte
var err error

	calldata, err = buildFlashLoanCalldata(
		factory,
		tokenAddr,
		t.baseToken,
		remaining,
		deadline,
		fee,
		minAmountOut,
	)
	if err != nil {
		log.Printf("[Bonding] build calldata error: %v", err)
		return
	}

	gasEstimate := uint64(1_000_000)

	if t.gevm != nil {
		simPayload := &types.ExecutionPayload{
    TargetExecutor: t.bondingExecutor,
    BorrowedToken:  t.baseToken,
    Calldata:       calldata,
    GasLimit:       gasEstimate,
    PriorityFeeWei: t.priorityFee,
    LoanPool:       config.BalancerVault, // Balancer Vault address (from config)
    LoanProvider:   0,                    // 0 = Balancer
    RoutePools:     []common.Address{poolAddr, factory}, // include both pool and factory
}
		success, gasUsed, err := t.gevm.SimulateNative(simPayload)
		if err != nil || !success {
			log.Printf("[Bonding] Simulation failed for %s: %v", tokenAddr.Hex(), err)
			return
		}
		gasEstimate = gasUsed + gasUsed/5
		log.Printf("[Bonding] Simulation passed for %s, gas: %d", tokenAddr.Hex(), gasEstimate)
	} else {
		log.Printf("[Bonding] GEVM not set; sending without simulation (unsafe)")
	}

	// Update the candidate's calldata and gas estimate under lock (optional)
	t.mu.Lock()
	cand.Calldata = calldata
	cand.GasEstimate = gasEstimate
	t.mu.Unlock()

	// Send payload with explicit parameters
poolAddr := cand.PoolAddress
t.sendPayload(
    factory, 
    tokenAddr, 
    t.baseToken, 
    remaining, 
    deadline, 
    fee, 
    minAmountOut, 
    calldata, 
    gasEstimate,
    poolAddr,
)
}

// sendPayload builds the final payload and submits to execution channel.
// All parameters are explicit – no direct reads from BondingCandidate.
func (t *Tracker) sendPayload(
    factory, token, baseToken common.Address,
    remaining *big.Int,
    deadline uint64,
    fee uint32,
    minAmountOut *big.Int,
    calldata []byte,
    gasEstimate uint64,
    poolAddr common.Address, // <-- NEW PARAMETER
) {
    payload := t.payloadPool.Get().(*types.ExecutionPayload)
    payload.Reset()
    payload.TargetExecutor = t.bondingExecutor
    payload.BorrowedToken = baseToken
    payload.Calldata = calldata
    payload.GasLimit = gasEstimate
    payload.PriorityFeeWei = t.priorityFee
    payload.DetectionTime = time.Now()
    payload.RouteDesc = fmt.Sprintf("Bonding-%s", token.Hex()[:8])
    payload.MinProfitUSD = 0.01 // tiny positive value to satisfy worker2's checks
    // ---- FIX: Set RoutePools so GEVM can inject pool code ----
    payload.RoutePools = []common.Address{poolAddr, factory}
    
    // ---- FIX: Also include the factory for simulation ----
    // We'll add factory to RoutePools as well (or use ExtraAddresses).
    // Since RoutePools is used for both routing and code injection,
    // we'll just add both. But the simulator only injects RoutePools.
    // We'll just add the pool address – the factory is already warmed up.
    
    select {
    case t.executionChan <- payload:
        log.Printf("[Bonding] Submitted graduation tx for %s", token.Hex())
    default:
        log.Printf("[Bonding] Execution channel full, dropping graduation for %s", token.Hex())
        t.payloadPool.Put(payload)
    }
}
// ---- Helper functions with ABI reuse ----

func buildFlashLoanCalldata(
    factory, token, baseToken common.Address,
    amount *big.Int,
    deadline uint64,
    fee uint32,
    minAmountOut *big.Int,
) ([]byte, error) {
    return bondingABI.Pack(
        "executeBondingArbitrage",
        factory,
        token,
        baseToken,
        amount,
        new(big.Int).SetUint64(deadline),
        new(big.Int).SetUint64(uint64(fee)),
        minAmountOut,
    )
}

func (t *Tracker) fetchCurrentReserve(ctx context.Context, token common.Address, factory common.Address) (*big.Int, error) {
    // First, try hardcoded ABIs (for Clanker/Virtuals).
    contract := bind.NewBoundContract(token, reserveABI, t.client, t.client, t.client)
    var out []interface{}
    err := contract.Call(&bind.CallOpts{Context: ctx}, &out, "getReserve")
    if err == nil && len(out) > 0 {
        return out[0].(*big.Int), nil
    }
    contract2 := bind.NewBoundContract(token, reserveFallbackABI, t.client, t.client, t.client)
    var out2 []interface{}
    err = contract2.Call(&bind.CallOpts{Context: ctx}, &out2, "reserve")
    if err == nil && len(out2) > 0 {
        return out2[0].(*big.Int), nil
    }

    // Fallback: dynamic ABI discovery.
abiInfo := t.resolveContractABI(token)
    // We need to create a dynamic ABI for the discovered function.
    // Since we don't have the full ABI, we can use the bind.BoundContract with a minimal ABI.
    // We'll construct an ABI string with the discovered function.
    abiStr := fmt.Sprintf(`[{"inputs":[],"name":"%s","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"}]`, abiInfo.ReserveFunc)
    parsed, err := abi.JSON(strings.NewReader(abiStr))
    if err != nil {
        return nil, err
    }
    contract3 := bind.NewBoundContract(token, parsed, t.client, t.client, t.client)
    var out3 []interface{}
    err = contract3.Call(&bind.CallOpts{Context: ctx}, &out3, abiInfo.ReserveFunc)
    if err == nil && len(out3) > 0 {
        return out3[0].(*big.Int), nil
    }
    return nil, fmt.Errorf("reserve function not found on token %s", token.Hex())
}

func (t *Tracker) fetchTargetFromPair(pair common.Address, factory common.Address) *big.Int {
    ctx := t.ctx
    if ctx == nil {
        ctx = context.Background()
    }
    // Try hardcoded targetABI first
    contract := bind.NewBoundContract(pair, targetABI, t.client, t.client, t.client)
    var out []interface{}
    err := contract.Call(&bind.CallOpts{Context: ctx}, &out, "target")
    if err == nil && len(out) > 0 {
        return out[0].(*big.Int)
    }
    // Fallback: dynamic discovery.
abiInfo := t.resolveContractABI(pair)   
    // Build dynamic ABI for the target function.
    abiStr := fmt.Sprintf(`[{"inputs":[],"name":"%s","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"}]`, abiInfo.TargetFunc)
    parsed, err := abi.JSON(strings.NewReader(abiStr))
    if err != nil {
        return nil
    }
    contract2 := bind.NewBoundContract(pair, parsed, t.client, t.client, t.client)
    var out2 []interface{}
    err = contract2.Call(&bind.CallOpts{Context: ctx}, &out2, abiInfo.TargetFunc)
    if err == nil && len(out2) > 0 {
        return out2[0].(*big.Int)
    }
    return nil
}
func (t *Tracker) fetchTargetFromToken(token common.Address, factory common.Address) *big.Int {
    ctx := t.ctx
    if ctx == nil {
        ctx = context.Background()
    }
    // Try hardcoded goalABI first
    contract := bind.NewBoundContract(token, goalABI, t.client, t.client, t.client)
    var out []interface{}
    err := contract.Call(&bind.CallOpts{Context: ctx}, &out, "goal")
    if err == nil && len(out) > 0 {
        return out[0].(*big.Int)
    }
    // Dynamic fallback
abiInfo := t.resolveContractABI(token)
    abiStr := fmt.Sprintf(`[{"inputs":[],"name":"%s","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"}]`, abiInfo.TargetFunc)
    parsed, err := abi.JSON(strings.NewReader(abiStr))
    if err != nil {
        return nil
    }
    contract2 := bind.NewBoundContract(token, parsed, t.client, t.client, t.client)
    var out2 []interface{}
    err = contract2.Call(&bind.CallOpts{Context: ctx}, &out2, abiInfo.TargetFunc)
    if err == nil && len(out2) > 0 {
        return out2[0].(*big.Int)
    }
    return nil
}
// ---- Cleanup stale candidates ----

func (t *Tracker) cleanupStaleCandidates() {
	t.mu.Lock()
	defer t.mu.Unlock()
	for addr, cand := range t.candidates {
		if time.Since(cand.LastUpdate) > candidateMaxAge {
			delete(t.candidates, addr)
			log.Printf("[Bonding] Removed stale candidate: %s", addr.Hex())
		}
	}
}
// Clear resets all bonding candidates and pending graduation state.
func (t *Tracker) Clear() {
    t.mu.Lock()
    defer t.mu.Unlock()
    t.candidates = make(map[common.Address]*BondingCandidate)

}
