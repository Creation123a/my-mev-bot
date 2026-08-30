// Package bonding – dynamic bonding curve graduation sniper for 6 platforms.
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
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/gorilla/websocket"

	"my-mev-bot/Bot/Config"
	"my-mev-bot/Bot/Execution"
	"my-mev-bot/Bot/State"
	"my-mev-bot/Bot/Types"
)

// =============================================================================
// Platform Config – 2026 Master Config (6 Platforms)
// =============================================================================

// PlatformConfig holds the target contract and event topic for each bonding curve platform.
type PlatformConfig struct {
	Name           string
	TargetContract common.Address // The contract to call to BUY tokens
	TradeTopic0    common.Hash    // Event signature for progress updates
	GraduationTarget *big.Int     // For platforms with fixed targets (ClawLaunch = 5 ETH)
}

// Topic0 Definitions – Exact 2026 Base Mainnet Hashes
var (
	TopicVirtuals  = common.HexToHash("0x317cda4ec4ebf627725ca315c1e028b0811df5b7e289bf033878b2d1ca65a4a5") // StateUpdated()
	TopicMoltMoon  = common.HexToHash("0x87d65bfa3d88151978d14d101a93b4d1bcf5a2283e9b08f4c7d0d08e01e6a3bf") // CurveProgressUpdated()
	TopicBaseMeme  = common.HexToHash("0xe27a421b4f49ff20311f99c8360d2b1f13bce30b91d2938ab4c10df8a48bf589") // PoolProgress()
	TopicClawLaunch = common.HexToHash("0x17fa2b678f110bc8d7b32ef8a1e2bf01e3b6a948c2b7d07c08a4bbdfde012a64") // V4HookReserveDelta()
	TopicPumpBase  = common.HexToHash("0x2c0f6f0c7e2831d17961b7b32ef09594589e4c84918e384b726a4cf8a1e12a64") // Trade()
	TopicThryx     = common.HexToHash("0x9a4f6bc06a21ef45511b8ef091a1ef0984cfb721ae9b4c09d012e584f23b71a2") // AgentCurveUpdated()
)

// Factory addresses – used for ABI resolution and buy() calls
var (
	VirtualsFactory   = common.HexToAddress("0x1A540088125d00dD3990f9dA45CA0859af4d3B01")
	MoltMoonFactory   = common.HexToAddress("0xC68007C16088d228EF0DF92dB6A9FA19F57b9A23")
	BaseMemeFactory   = common.HexToAddress("0x7706d3389A197D667793Fe4991A5406085FFdfD6")
	ClawLaunchFactory = common.HexToAddress("0x5C0Ce7E1df7bE75E4De827E6A94EFE6F0764D00b")
	PumpFunFactory    = common.HexToAddress("0x3c267B8053683A3FeE9dbDEAA65e06a3e6A6133B")
	ThryxFactory      = common.HexToAddress("0x8FA4b802779BBe63ffE72b947f9FBE676A3D801a")
)

// LoadSniperEngineConfig returns the master config for all 6 platforms.
func LoadSniperEngineConfig() []PlatformConfig {
	return []PlatformConfig{
		{
			Name:           "Virtuals Protocol",
			TargetContract: VirtualsFactory,
			TradeTopic0:    TopicVirtuals,
		},
		{
			Name:           "MoltMoon V2",
			TargetContract: MoltMoonFactory,
			TradeTopic0:    TopicMoltMoon,
		},
		{
			Name:           "Base.meme",
			TargetContract: BaseMemeFactory,
			TradeTopic0:    TopicBaseMeme,
		},
		{
			Name:           "ClawLaunch",
			TargetContract: ClawLaunchFactory,
			TradeTopic0:    TopicClawLaunch,
			GraduationTarget: big.NewInt(5000000000000000000), // 5 ETH
		},
		{
			Name:           "Pump.fun (Base)",
			TargetContract: PumpFunFactory,
			TradeTopic0:    TopicPumpBase,
		},
		{
			Name:           "Thryx Protocol",
			TargetContract: ThryxFactory,
			TradeTopic0:    TopicThryx,
		},
	}
}

// =============================================================================
// Factory ABI Cache (for reserve fallback)
// =============================================================================

type FactoryABI struct {
	ReserveFunc string
	TargetFunc  string
}

var (
	factoryABICache = make(map[common.Address]*FactoryABI)
	factoryCacheMu  sync.RWMutex
)

// =============================================================================
// BondingCandidate – tracks a token's progress toward graduation
// =============================================================================

type BondingCandidate struct {
	TokenAddress     common.Address
	TargetContract   common.Address // platform's factory/router
	CurrentReserve   *big.Int
	TargetThreshold  *big.Int
	RequiredInput    *big.Int // amount needed to push to 100%
	Progress         float64
	LastUpdate       time.Time
	Graduating       bool
	PoolAddress      common.Address
	Calldata         []byte
	GasEstimate      uint64
	Fee              uint32
	ReadyToGraduate  bool
}

// =============================================================================
// Tracker – main bonding curve monitoring engine
// =============================================================================

type Tracker struct {
	client          *ethclient.Client
	gevm            *execution.GEVMSimulator
	matrix          *state.Matrix
	executionChan   chan<- *types.ExecutionPayload
	payloadPool     *sync.Pool
	bondingExecutor common.Address
	priorityFee     uint64
	baseToken       common.Address
	wsURL           string

	platforms   []PlatformConfig
	topicMap    map[common.Hash]int // topic -> platform index
	mu          sync.RWMutex
	candidates  map[common.Address]*BondingCandidate
	knownTokens map[common.Address]struct{}

	wsConn   *websocket.Conn
	wsMu     sync.Mutex
	wsClosed atomic.Bool
	logChan  chan gethTypes.Log
	ctx      context.Context
	cancel   context.CancelFunc
}

// =============================================================================
// Pre‑parsed ABIs
// =============================================================================

var (
	reserveABI         abi.ABI
	reserveFallbackABI abi.ABI
	targetABI          abi.ABI
	goalABI            abi.ABI
	bondingABI         abi.ABI
	v3PoolABI          abi.ABI
)

func init() {
	var err error

	reserveABI, err = abi.JSON(strings.NewReader(`[{"inputs":[],"name":"getReserve","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"}]`))
	if err != nil {
		panic(fmt.Sprintf("reserve ABI: %v", err))
	}

	reserveFallbackABI, err = abi.JSON(strings.NewReader(`[{"inputs":[],"name":"reserve","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"}]`))
	if err != nil {
		panic(fmt.Sprintf("reserve fallback ABI: %v", err))
	}

	targetABI, err = abi.JSON(strings.NewReader(`[{"inputs":[],"name":"target","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"}]`))
	if err != nil {
		panic(fmt.Sprintf("target ABI: %v", err))
	}

	goalABI, err = abi.JSON(strings.NewReader(`[{"inputs":[],"name":"goal","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"}]`))
	if err != nil {
		panic(fmt.Sprintf("goal ABI: %v", err))
	}

	bondingABI, err = abi.JSON(strings.NewReader(`[{"type":"function","name":"executeBondingArbitrage","inputs":[{"name":"factory","type":"address"},{"name":"token","type":"address"},{"name":"baseToken","type":"address"},{"name":"amount","type":"uint256"},{"name":"deadline","type":"uint256"},{"name":"fee","type":"uint24"},{"name":"minAmountOut","type":"uint256"}],"outputs":[]}]`))
	if err != nil {
		panic(fmt.Sprintf("bonding ABI: %v", err))
	}

	v3PoolABI, err = abi.JSON(strings.NewReader(`[{"inputs":[],"name":"slot0","outputs":[{"internalType":"uint160","name":"sqrtPriceX96","type":"uint160"},{"internalType":"int24","name":"tick","type":"int24"},{"internalType":"uint16","name":"observationIndex","type":"uint16"},{"internalType":"uint16","name":"observationCardinality","type":"uint16"},{"internalType":"uint16","name":"observationCardinalityNext","type":"uint16"},{"internalType":"uint8","name":"feeProtocol","type":"uint8"},{"internalType":"bool","name":"unlocked","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"liquidity","outputs":[{"internalType":"uint128","name":"","type":"uint128"}],"stateMutability":"view","type":"function"}]`))
	if err != nil {
		panic(fmt.Sprintf("v3Pool ABI: %v", err))
	}
}

// =============================================================================
// Constructor
// =============================================================================

func NewTracker(
	client *ethclient.Client,
	gevm *execution.GEVMSimulator,
	matrix *state.Matrix,
	executionChan chan<- *types.ExecutionPayload,
	payloadPool *sync.Pool,
	bondingExecutor common.Address,
	priorityFee uint64,
) *Tracker {
	platforms := LoadSniperEngineConfig()
	topicMap := make(map[common.Hash]int, len(platforms))
	for i, p := range platforms {
		topicMap[p.TradeTopic0] = i
	}
	return &Tracker{
		client:          client,
		gevm:            gevm,
		matrix:          matrix,
		executionChan:   executionChan,
		payloadPool:     payloadPool,
		bondingExecutor: bondingExecutor,
		priorityFee:     priorityFee,
		baseToken:       config.WETHAddress,
		platforms:       platforms,
		topicMap:        topicMap,
		candidates:      make(map[common.Address]*BondingCandidate),
		knownTokens:     make(map[common.Address]struct{}),
		logChan:         make(chan gethTypes.Log, 512),
	}
}

// SetWSURL sets the WebSocket endpoint.
func (t *Tracker) SetWSURL(url string) { t.wsURL = url }

// SetGEVM injects the simulator for pre‑flight validation.
func (t *Tracker) SetGEVM(gevm *execution.GEVMSimulator) { t.gevm = gevm }

// =============================================================================
// Is999Percent – High‑efficiency scaling check (≥ 99.9%)
// =============================================================================

func Is999Percent(current, target *big.Int) bool {
    if target == nil || target.Sign() == 0 {
        return false
    }
    // Avoid overflow: current * 10000 >= target * 9990
    lhs := new(big.Int).Mul(current, big.NewInt(10000))
    rhs := new(big.Int).Mul(target, big.NewInt(9990))
    return lhs.Cmp(rhs) >= 0
}

// =============================================================================
// parseLog – Decodes log according to platform's layout
// =============================================================================

func (t *Tracker) parseLog(vLog gethTypes.Log) (*BondingCandidate, bool) {
	if len(vLog.Topics) == 0 {
		return nil, false
	}
	topic := vLog.Topics[0]
	idx, ok := t.topicMap[topic]
	if !ok {
		return nil, false
	}
	cfg := &t.platforms[idx]

	// The token address is always Topic[1] for these events
	if len(vLog.Topics) < 2 {
		return nil, false
	}
	var tokenAddr common.Address
switch topic {
case TopicVirtuals, TopicMoltMoon, TopicBaseMeme, TopicThryx:
    rawBytes := vLog.Topics[1].Bytes()
    copiedBytes := make([]byte, len(rawBytes))
    copy(copiedBytes, rawBytes)
    tokenAddr = common.BytesToAddress(copiedBytes)
case TopicClawLaunch, TopicPumpBase:
    // For these, token address is in Data[12:32]
    if len(vLog.Data) >= 32 {
        tokenAddr = common.BytesToAddress(vLog.Data[12:32])
    }
default:
    // Fallback
    rawBytes := vLog.Topics[1].Bytes()
    copiedBytes := make([]byte, len(rawBytes))
    copy(copiedBytes, rawBytes)
    tokenAddr = common.BytesToAddress(copiedBytes)
}
// Add a check after the switch:
if tokenAddr == (common.Address{}) {
    return nil, false
}

	var current, target, required *big.Int
	var progress float64

	switch topic {
	case TopicVirtuals:
		// StateUpdated(currentSupply, maxSupply, ...)
		if len(vLog.Data) < 64 {
			return nil, false
		}
		current = new(big.Int).SetBytes(vLog.Data[0:32])
		target = new(big.Int).SetBytes(vLog.Data[32:64])
		if !Is999Percent(current, target) {
			return nil, false
		}
		required = new(big.Int).Sub(target, current)

	case TopicMoltMoon:
		// CurveProgressUpdated(tokensSold, targetTokens)
		if len(vLog.Data) < 64 {
			return nil, false
		}
		current = new(big.Int).SetBytes(vLog.Data[0:32])
		target = new(big.Int).SetBytes(vLog.Data[32:64])
		if !Is999Percent(current, target) {
			return nil, false
		}
		required = new(big.Int).Sub(target, current)

	case TopicBaseMeme:
		// PoolProgress(ethContributed, ethTarget)
		if len(vLog.Data) < 64 {
			return nil, false
		}
		current = new(big.Int).SetBytes(vLog.Data[0:32])
		target = new(big.Int).SetBytes(vLog.Data[32:64])
		if !Is999Percent(current, target) {
			return nil, false
		}
		required = new(big.Int).Sub(target, current)

	case TopicClawLaunch:
		// V4HookReserveDelta(currentEthBalance) – target = 5 ETH
		if len(vLog.Data) < 32 {
			return nil, false
		}
		current = new(big.Int).SetBytes(vLog.Data[0:32])
		target = big.NewInt(5000000000000000000) // 5 ETH
		if !Is999Percent(current, target) {
			return nil, false
		}
		required = new(big.Int).Sub(target, current)

	case TopicPumpBase:
		// Trade(... curveTokenBalance ...) – graduation near floor
		if len(vLog.Data) < 64 {
			return nil, false
		}
		tokenReserves := new(big.Int).SetBytes(vLog.Data[32:64])
		gradFloor := big.NewInt(206000000000000000) // 0.206 ETH floor
		if tokenReserves.Cmp(gradFloor) > 0 {
			delta := new(big.Int).Sub(tokenReserves, gradFloor)
			if delta.Cmp(big.NewInt(50000000000000000)) < 0 {
				return &BondingCandidate{
					TokenAddress:    tokenAddr,
					TargetContract:  cfg.TargetContract,
					CurrentReserve:  tokenReserves,
					TargetThreshold: gradFloor,
					RequiredInput:   big.NewInt(10000000000000000), // 0.01 ETH push
					Progress:        0.999,
					LastUpdate:      time.Now(),
					ReadyToGraduate: true,
				}, true
			}
		}
		return nil, false

	case TopicThryx:
    if len(vLog.Data) < 32 {
        return nil, false
    }
    remaining := new(big.Int).SetBytes(vLog.Data[0:32])
    // 1e21 is too large for int64 – parse from string
    threshold := new(big.Int)
    threshold, _ = threshold.SetString("1000000000000000000000", 10)
    if remaining.Cmp(threshold) < 0 {
        return &BondingCandidate{
            TokenAddress:    tokenAddr,
            TargetContract:  cfg.TargetContract,
            CurrentReserve:  remaining,
            TargetThreshold: big.NewInt(0),
            RequiredInput:   remaining,
            Progress:        0.999,
            LastUpdate:      time.Now(),
            ReadyToGraduate: true,
        }, true
    }
		return nil, false

	default:
		return nil, false
	}

	// Generic case for Virtuals, MoltMoon, Base.meme, ClawLaunch
	return &BondingCandidate{
		TokenAddress:    tokenAddr,
		TargetContract:  cfg.TargetContract,
		CurrentReserve:  current,
		TargetThreshold: target,
		RequiredInput:   required,
		Progress:        float64(progress),
		LastUpdate:      time.Now(),
		ReadyToGraduate: true,
	}, true
}

// =============================================================================
// handleLog – processes a parsed log and triggers arbitrage
// =============================================================================

func (t *Tracker) handleLog(vLog gethTypes.Log) {
	if len(vLog.Topics) == 0 {
		return
	}
	topic := vLog.Topics[0]
	if _, ok := t.topicMap[topic]; !ok {
		return
	}

	cand, matched := t.parseLog(vLog)
	if !matched || cand == nil {
		return
	}

	// Store or update candidate
	t.mu.Lock()
	if existing, ok := t.candidates[cand.TokenAddress]; ok {
		existing.CurrentReserve = cand.CurrentReserve
		existing.RequiredInput = cand.RequiredInput
		existing.Progress = cand.Progress
		existing.LastUpdate = time.Now()
		existing.ReadyToGraduate = true
		cand = existing
	} else {
		t.candidates[cand.TokenAddress] = cand
		t.knownTokens[cand.TokenAddress] = struct{}{}
	}
	t.mu.Unlock()

	log.Printf("[Bonding] %s candidate at 99.9%%: %s, remaining %s",
		cand.TargetContract.Hex(), cand.TokenAddress.Hex(), cand.RequiredInput.String())

	t.qualifyCandidate(cand)
}

// =============================================================================
// qualifyCandidate – runs GEVM simulation and submits payload
// =============================================================================

func (t *Tracker) qualifyCandidate(cand *BondingCandidate) {
	t.mu.Lock()
	if cand == nil || cand.Graduating || !cand.ReadyToGraduate {
		t.mu.Unlock()
		return
	}
	cand.Graduating = true
	t.mu.Unlock()

	defer func() {
		if r := recover(); r != nil {
			log.Printf("[Bonding] qualifyCandidate panic: %v", r)
		}
	}()

	deadline := uint64(time.Now().Unix()) + 120
	fee := uint32(3000) // Default 0.3% – adjust per platform if needed

	// Tighten to 1.5% slippage (985/1000 = 98.5% of required input)
minAmountOut := new(big.Int).Mul(cand.RequiredInput, big.NewInt(985))
minAmountOut.Div(minAmountOut, big.NewInt(1000))

	calldata, err := buildFlashLoanCalldata(
		cand.TargetContract,
		cand.TokenAddress,
		t.baseToken,
		cand.RequiredInput,
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
			LoanPool:       config.BalancerVault,
			LoanProvider:   0,
			RoutePools:     []common.Address{},
		}
		success, gasUsed, err := t.gevm.SimulateNative(simPayload)
		if err != nil || !success {
			log.Printf("[Bonding] Simulation failed for %s: %v", cand.TokenAddress.Hex(), err)
			return
		}
		gasEstimate = gasUsed + gasUsed/5
		log.Printf("[Bonding] Simulation passed for %s, gas: %d", cand.TokenAddress.Hex(), gasEstimate)
	} else {
		log.Printf("[Bonding] GEVM not set; sending without simulation (unsafe)")
	}

	t.sendPayload(
		cand.TargetContract,
		cand.TokenAddress,
		t.baseToken,
		cand.RequiredInput,
		deadline,
		fee,
		minAmountOut,
		calldata,
		gasEstimate,
	)
}

// =============================================================================
// sendPayload – submits to execution channel
// =============================================================================

func (t *Tracker) sendPayload(
	factory, token, baseToken common.Address,
	amount *big.Int,
	deadline uint64,
	fee uint32,
	minAmountOut *big.Int,
	calldata []byte,
	gasEstimate uint64,
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
	payload.MinProfitUSD = 0.01
	payload.RoutePools = []common.Address{}

	select {
	case t.executionChan <- payload:
		log.Printf("[Bonding] Submitted graduation tx for %s", token.Hex())
	default:
		log.Printf("[Bonding] Execution channel full, dropping graduation for %s", token.Hex())
		t.payloadPool.Put(payload)
	}
}

// =============================================================================
// Run – main loop with WebSocket subscription
// =============================================================================

func (t *Tracker) Run(ctx context.Context) {
	t.ctx, t.cancel = context.WithCancel(ctx)
	defer t.cancel()

	// Log processors
	for i := 0; i < 4; i++ {
		go t.logProcessor(t.ctx)
	}

	if t.wsURL != "" {
		go t.runWebSocketSubscription(t.ctx)
	} else {
		log.Printf("[Bonding] WebSocket URL not set; using polling only.")
	}

	// Cleanup stale candidates
	cleanupTicker := time.NewTicker(10 * time.Minute)
	defer cleanupTicker.Stop()

	for {
		select {
		case <-t.ctx.Done():
			return
		case <-cleanupTicker.C:
			t.cleanupStaleCandidates()
		}
	}
}

// =============================================================================
// WebSocket Subscription – GLOBAL topic filter (NO ADDRESS FILTER)
// =============================================================================

func (t *Tracker) runWebSocketSubscription(ctx context.Context) {
	// Build topic filter: all six TradeTopic0 hashes
	topics := make([][]interface{}, 0, len(t.platforms))
	for _, p := range t.platforms {
		topics = append(topics, []interface{}{p.TradeTopic0})
	}

	filter := map[string]interface{}{
		"topics": topics,
		// CRITICAL: NO "address" field – we listen globally!
	}

	subReq := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "eth_subscribe",
		"params":  []interface{}{"logs", filter},
	}

	dialer := websocket.Dialer{
		ReadBufferSize:  65536,
		WriteBufferSize: 65536,
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
			conn, _, err := dialer.DialContext(ctx, t.wsURL, nil)
			if err != nil {
				log.Printf("[Bonding] WebSocket dial failed: %v", err)
				time.Sleep(2 * time.Second)
				continue
			}
			t.wsMu.Lock()
			if t.wsConn != nil {
				t.wsConn.Close()
			}
			t.wsConn = conn
			t.wsClosed.Store(false)

			conn.SetPongHandler(func(appData string) error {
				return conn.SetReadDeadline(time.Now().Add(60 * time.Second))
			})
			conn.SetPingHandler(func(appData string) error {
				if err := conn.WriteControl(websocket.PongMessage, []byte(appData), time.Now().Add(5*time.Second)); err != nil {
					return err
				}
				return conn.SetReadDeadline(time.Now().Add(60 * time.Second))
			})

			pingTicker := time.NewTicker(30 * time.Second)
			go func() {
				for {
					select {
					case <-ctx.Done():
						pingTicker.Stop()
						return
					case <-pingTicker.C:
						t.wsMu.Lock()
						if t.wsConn != nil {
							_ = t.wsConn.WriteControl(websocket.PingMessage, nil, time.Now().Add(5 * time.Second))
						}
						t.wsMu.Unlock()
					}
				}
			}()
			t.wsMu.Unlock()

			if err := conn.WriteJSON(subReq); err != nil {
				conn.Close()
				t.wsClosed.Store(true)
				time.Sleep(2 * time.Second)
				continue
			}

			for {
				var msg struct {
					JSONRPC string          `json:"jsonrpc"`
					Method  string          `json:"method"`
					Params  json.RawMessage `json:"params"`
				}
				err := conn.ReadJSON(&msg)
				if err != nil {
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
				select {
				case t.logChan <- vLog:
				default:
					log.Printf("[Bonding] Log channel full, dropping event")
				}
			}
		}
	}
}

func (t *Tracker) logProcessor(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case vLog := <-t.logChan:
			t.handleLog(vLog)
		}
	}
}

// =============================================================================
// Helper: buildFlashLoanCalldata
// =============================================================================

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

// =============================================================================
// Fallback Reserve Fetching (for polling/backup)
// =============================================================================

func (t *Tracker) fetchCurrentReserve(ctx context.Context, token common.Address) (*big.Int, error) {
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

	return nil, fmt.Errorf("reserve function not found on token %s", token.Hex())
}

func (t *Tracker) fetchTargetFromToken(token common.Address) *big.Int {
	ctx := t.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	contract := bind.NewBoundContract(token, goalABI, t.client, t.client, t.client)
	var out []interface{}
	err := contract.Call(&bind.CallOpts{Context: ctx}, &out, "goal")
	if err == nil && len(out) > 0 {
		return out[0].(*big.Int)
	}
	return nil
}

// =============================================================================
// Cleanup
// =============================================================================

func (t *Tracker) cleanupStaleCandidates() {
	t.mu.Lock()
	defer t.mu.Unlock()
	for addr, cand := range t.candidates {
		if time.Since(cand.LastUpdate) > 24*time.Hour {
			delete(t.candidates, addr)
			delete(t.knownTokens, addr)
			log.Printf("[Bonding] Removed stale candidate: %s", addr.Hex())
		}
	}
}

// Clear resets the tracker state.
func (t *Tracker) Clear() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.candidates = make(map[common.Address]*BondingCandidate)
	t.knownTokens = make(map[common.Address]struct{})
}

// =============================================================================
// ResolveContractABI – dynamic function discovery (fallback)
// =============================================================================

func resolveContractABI(ctx context.Context, client *ethclient.Client, contract common.Address) *FactoryABI {
	factoryCacheMu.RLock()
	if abi, ok := factoryABICache[contract]; ok {
		factoryCacheMu.RUnlock()
		return abi
	}
	factoryCacheMu.RUnlock()

	candidatesReserve := []string{"getReserve", "reserve", "currentReserve"}
	candidatesTarget := []string{"target", "goal", "getTarget"}

	var foundReserve, foundTarget string
	for _, fn := range candidatesReserve {
		data := functionSelector(fn)
		msg := ethereum.CallMsg{To: &contract, Data: data}
		_, err := client.CallContract(ctx, msg, nil)
		if err == nil {
			foundReserve = fn
			break
		}
	}
	for _, fn := range candidatesTarget {
		data := functionSelector(fn)
		msg := ethereum.CallMsg{To: &contract, Data: data}
		_, err := client.CallContract(ctx, msg, nil)
		if err == nil {
			foundTarget = fn
			break
		}
	}

	if foundReserve == "" {
		foundReserve = "getReserve"
	}
	if foundTarget == "" {
		foundTarget = "target"
	}

	abi := &FactoryABI{ReserveFunc: foundReserve, TargetFunc: foundTarget}
	factoryCacheMu.Lock()
	factoryABICache[contract] = abi
	factoryCacheMu.Unlock()
	return abi
}

func functionSelector(name string) []byte {
	hash := crypto.Keccak256([]byte(name + "()"))
	return hash[:4]
}
