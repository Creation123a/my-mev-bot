// Package liquidation monitors multiple lending protocols and executes flash‑loan liquidations.
package liquidation

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/big"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/gorilla/websocket"

	"my-mev-bot/Bot/Config"
	"my-mev-bot/Bot/Execution"
	"my-mev-bot/Bot/Solver"
	"my-mev-bot/Bot/State"
botTypes	"my-mev-bot/Bot/Types"
)

// =============================================================================
// Protocol constants (must match Solidity enum)
// =============================================================================
const (
	ProtocolAave      uint8 = 1
	ProtocolCompound  uint8 = 2
	ProtocolMorpho    uint8 = 3
	ProtocolExactly   uint8 = 4
	ProtocolMoonwell  uint8 = 5
	ProtocolIonic     uint8 = 6
)

// Pre‑computed event signatures (filled in init)
var (
	aaveBorrowEventSig     common.Hash
	aaveRepayEventSig      common.Hash
	compoundBorrowEventSig common.Hash
	compoundRepayEventSig  common.Hash
	morphoBorrowEventSig   common.Hash
	morphoRepayEventSig    common.Hash
	exactlyBorrowEventSig  common.Hash
	exactlyRepayEventSig   common.Hash
	moonwellBorrowEventSig common.Hash
	moonwellRepayEventSig  common.Hash
	ionicBorrowEventSig    common.Hash
	ionicRepayEventSig     common.Hash
)

// Pre‑parsed protocol ABIs
var (
	aavePoolABI     abi.ABI
	compoundABI     abi.ABI
	morphoBlueABI   abi.ABI
	exactlyAuditorABI abi.ABI
	moonwellMTokenABI abi.ABI
	ionicPoolABI    abi.ABI
	executorABI     abi.ABI
)

func init() {
	// ---------- Compute event signatures ----------
    // Aave V3
    aaveBorrowEventSig = crypto.Keccak256Hash([]byte("Borrow(address,address,address,uint256,uint256,uint256,uint16)"))
    aaveRepayEventSig = crypto.Keccak256Hash([]byte("Repay(address,address,address,uint256,bool)"))

    // Compound III
    compoundBorrowEventSig = crypto.Keccak256Hash([]byte("AbsorbDebt(address,address,uint256)"))
    compoundRepayEventSig = crypto.Keccak256Hash([]byte("Repay(address,address,uint256)"))

    // Morpho Blue (verified from Morpho Blue source)
    morphoBorrowEventSig = crypto.Keccak256Hash([]byte("Borrow(bytes32,address,address,address,uint256,uint256)"))
    morphoRepayEventSig = crypto.Keccak256Hash([]byte("Repay(bytes32,address,address,uint256,uint256)"))

    // Exactly
    exactlyBorrowEventSig = crypto.Keccak256Hash([]byte("Borrow(address,address,address,uint256,uint256)"))
    exactlyRepayEventSig = crypto.Keccak256Hash([]byte("Repay(address,address,address,uint256)"))

    // Moonwell (verified from Compound v2 style)
    moonwellBorrowEventSig = crypto.Keccak256Hash([]byte("Borrow(address,uint256,uint256)"))
    moonwellRepayEventSig = crypto.Keccak256Hash([]byte("RepayBorrow(address,address,uint256,uint256)"))

    // Ionic
    ionicBorrowEventSig = crypto.Keccak256Hash([]byte("Borrow(address,uint256,uint256)"))
    ionicRepayEventSig = crypto.Keccak256Hash([]byte("Repay(address,address,uint256)"))

	// ---------- ABIs ----------
	var err error
	aavePoolABI, _ = abi.JSON(strings.NewReader(`[
		{"inputs":[{"name":"user","type":"address"}],"name":"getUserAccountData","outputs":[{"name":"totalCollateralBase","type":"uint256"},{"name":"totalDebtBase","type":"uint256"},{"name":"availableBorrowsBase","type":"uint256"},{"name":"currentLiquidationThreshold","type":"uint256"},{"name":"ltv","type":"uint256"},{"name":"healthFactor","type":"uint256"}],"stateMutability":"view","type":"function"},
		{"inputs":[{"name":"user","type":"address"}],"name":"getUserReserves","outputs":[{"components":[{"name":"underlyingAsset","type":"address"},{"name":"aToken","type":"address"},{"name":"variableDebt","type":"address"},{"name":"stableDebt","type":"address"},{"name":"currentATokenBalance","type":"uint256"},{"name":"currentVariableDebt","type":"uint256"},{"name":"currentStableDebt","type":"uint256"}],"name":"","type":"tuple[]"}],"stateMutability":"view","type":"function"}
	]`))
	compoundABI, _ = abi.JSON(strings.NewReader(`[
		{"inputs":[{"name":"account","type":"address"}],"name":"getBorrowBalance","outputs":[{"name":"","type":"uint256"}],"stateMutability":"view","type":"function"},
		{"inputs":[{"name":"account","type":"address"},{"name":"asset","type":"address"}],"name":"getCollateralBalance","outputs":[{"name":"","type":"uint256"}],"stateMutability":"view","type":"function"}
	]`))
morphoBlueABI, _ = abi.JSON(strings.NewReader(`[
    {"inputs":[{"name":"user","type":"address"},{"name":"market","type":"address"}],"name":"position","outputs":[{"name":"supply","type":"uint256"},{"name":"borrow","type":"uint256"},{"name":"collateral","type":"uint256"}],"stateMutability":"view","type":"function"},
    {"inputs":[{"name":"id","type":"bytes32"}],"name":"idToMarketParams","outputs":[{"name":"","type":"tuple","components":[{"name":"loanToken","type":"address"},{"name":"collateralToken","type":"address"},{"name":"oracle","type":"address"},{"name":"irm","type":"address"},{"name":"lltv","type":"uint256"}]}],"stateMutability":"view","type":"function"},
    {"inputs":[{"name":"id","type":"bytes32"}],"name":"idToMarket","outputs":[{"name":"","type":"address"}],"stateMutability":"view","type":"function"}
]`))
	exactlyAuditorABI, _ = abi.JSON(strings.NewReader(`[
		{"inputs":[{"name":"account","type":"address"}],"name":"getAccountLiquidity","outputs":[{"name":"collateral","type":"uint256"},{"name":"debt","type":"uint256"},{"name":"liquidity","type":"uint256"}],"stateMutability":"view","type":"function"},
		{"inputs":[{"name":"account","type":"address"}],"name":"getAccountMarkets","outputs":[{"name":"","type":"address[]"}],"stateMutability":"view","type":"function"}
	]`))
	moonwellMTokenABI, _ = abi.JSON(strings.NewReader(`[
		{"inputs":[{"name":"account","type":"address"}],"name":"getAccountSnapshot","outputs":[{"name":"","type":"uint256"},{"name":"","type":"uint256"},{"name":"","type":"uint256"}],"stateMutability":"view","type":"function"},
		{"inputs":[],"name":"underlying","outputs":[{"name":"","type":"address"}],"stateMutability":"view","type":"function"}
	]`))
	ionicPoolABI, _ = abi.JSON(strings.NewReader(`[
		{"inputs":[{"name":"user","type":"address"}],"name":"getUserAccountData","outputs":[{"name":"","type":"uint256"},{"name":"","type":"uint256"},{"name":"","type":"uint256"}]}
	]`))
	executorABI, _ = abi.JSON(strings.NewReader(`[
    {"type":"function","name":"executeLiquidation","inputs":[
        {"name":"protocol","type":"uint8"},
        {"name":"debtAsset","type":"address"},
        {"name":"debtAmount","type":"uint256"},
        {"name":"fee","type":"uint24"},          // <-- ADDED
        {"name":"liquidationData","type":"bytes"}
    ],"outputs":[]}
]`))
	if err != nil {
		panic(fmt.Sprintf("ABI parsing: %v", err))
	}
}

// =============================================================================
// Data Structures
// =============================================================================

type UserPosition struct {
	User               common.Address
	Protocol           uint8
	TotalCollateralUSD float64
	TotalDebtUSD       float64
	HealthFactor       float64
	LastUpdated        time.Time
	CollateralAsset    common.Address
	DebtAsset          common.Address
	DebtAmount         *big.Int
	CollateralAmount   *big.Int
	// Protocol‑specific extras
	CollateralMarket common.Address // for Exactly (seize market)
	BorrowMarket     common.Address // for Morpho/Exactly
	MarketParams     []interface{}  // for Morpho (to build struct)
}

type ProtocolConfig struct {
	Name            string
	Protocol        uint8
	PoolAddress     common.Address
	BorrowEventSig  common.Hash
	RepayEventSig   common.Hash
	KnownAssets     []common.Address // assets to check for this protocol
}
type MorphoMarketParams struct {
    Market         common.Address
    LoanToken      common.Address
    CollateralToken common.Address
    Oracle         common.Address
    IRM            common.Address
    LLTV           *big.Int
}
// MorphoParamsTuple matches the Solidity IMorpho.MarketParams struct exactly.
type MorphoParamsTuple struct {
    LoanToken      common.Address
    CollateralToken common.Address
    Oracle         common.Address
    IRM            common.Address
    LLTV           *big.Int
}

type Tracker struct {
	client          *ethclient.Client
	gevm            *execution.GEVMSimulator
	matrix          *state.Matrix
	executionChan   chan<- *botTypes.ExecutionPayload
	payloadPool     *sync.Pool
	priorityFee     uint64
	executorAddress common.Address
	cfg             *config.Config

	protocols   map[uint8]*ProtocolConfig
	mu          sync.RWMutex
	positions   map[common.Address]*UserPosition
	knownUsers  map[common.Address]struct{}
	pollInterval time.Duration
    wsURL   string 
	wsConn   *websocket.Conn
	wsMu     sync.Mutex
	wsClosed atomic.Bool
	logChan  chan types.Log
	ctx      context.Context
	cancel   context.CancelFunc
	liquidatingMu sync.Mutex
liquidating   map[common.Address]bool
liquidationBonus float64
}

// NewTracker creates a new liquidation tracker.
func NewTracker(
	client *ethclient.Client,
	gevm *execution.GEVMSimulator,
	matrix *state.Matrix,
	executionChan chan<- *botTypes.ExecutionPayload,
	payloadPool *sync.Pool,
	priorityFee uint64,
	cfg *config.Config,
	liquidationBonus float64, 
) *Tracker {
	protocols := map[uint8]*ProtocolConfig{
		ProtocolAave: {
			Name:           "Aave V3",
			Protocol:       ProtocolAave,
			PoolAddress:    common.HexToAddress(cfg.AavePoolAddress),
			BorrowEventSig: aaveBorrowEventSig,
			RepayEventSig:  aaveRepayEventSig,
			KnownAssets:    []common.Address{config.WETHAddress, config.USDCAddress, config.USDBCAddress},
		},
		ProtocolCompound: {
			Name:           "Compound III",
			Protocol:       ProtocolCompound,
			PoolAddress:    common.HexToAddress(cfg.CompoundCometAddress),
			BorrowEventSig: compoundBorrowEventSig,
			RepayEventSig:  compoundRepayEventSig,
			KnownAssets:    []common.Address{config.WETHAddress, config.USDCAddress},
		},
		ProtocolMorpho: {
			Name:           "Morpho Blue",
			Protocol:       ProtocolMorpho,
			PoolAddress:    common.HexToAddress(cfg.MorphoBlueAddress),
			BorrowEventSig: morphoBorrowEventSig,
			RepayEventSig:  morphoRepayEventSig,
			KnownAssets:    []common.Address{config.WETHAddress, config.USDCAddress},
		},
		ProtocolExactly: {
			Name:           "Exactly",
			Protocol:       ProtocolExactly,
			PoolAddress:    common.HexToAddress(cfg.ExactlyAuditorAddress),
			BorrowEventSig: exactlyBorrowEventSig,
			RepayEventSig:  exactlyRepayEventSig,
			KnownAssets:    []common.Address{config.WETHAddress, config.USDCAddress},
		},
		ProtocolMoonwell: {
			Name:           "Moonwell",
			Protocol:       ProtocolMoonwell,
			PoolAddress:    common.HexToAddress(cfg.MoonwellMTokenAddress),
			BorrowEventSig: moonwellBorrowEventSig,
			RepayEventSig:  moonwellRepayEventSig,
			KnownAssets:    []common.Address{config.WETHAddress, config.USDCAddress},
		},
		ProtocolIonic: {
			Name:           "Ionic",
			Protocol:       ProtocolIonic,
			PoolAddress:    common.HexToAddress(cfg.IonicPoolAddress),
			BorrowEventSig: ionicBorrowEventSig,
			RepayEventSig:  ionicRepayEventSig,
			KnownAssets:    []common.Address{config.WETHAddress, config.USDCAddress},
		},
	}
	return &Tracker{
		client:          client,
		gevm:            gevm,
		matrix:          matrix,
		executionChan:   executionChan,
		payloadPool:     payloadPool,
		priorityFee:     priorityFee,
		executorAddress: common.HexToAddress(cfg.LiquidationExecutorAddress),
		cfg:             cfg,
		protocols:       protocols,
		positions:       make(map[common.Address]*UserPosition),
		knownUsers:      make(map[common.Address]struct{}),
		pollInterval:    time.Duration(cfg.LiquidationPollIntervalMs) * time.Millisecond,
		logChan:         make(chan types.Log, 256),
		liquidationBonus: liquidationBonus,  
		liquidating: make(map[common.Address]bool),  
	}
}

// SetWSURL sets the WebSocket endpoint.
func (t *Tracker) SetWSURL(url string) { t.wsURL = url }

func (t *Tracker) getFeeForPair(tokenA, tokenB common.Address) uint32 {
    pools := t.matrix.GetPoolsForPair(tokenA, tokenB)
    for _, p := range pools {
        if p.DexType == botTypes.DexUniswapV3 || p.DexType == botTypes.DexPancakeV3 {
            if p.FeeBps >= 100 && p.FeeBps <= 10000 {
                return uint32(p.FeeBps)
            }
            return uint32(p.FeeBps * 100)
        }
    }
    return 3000
}
// SetGEVM injects the simulator for pre‑flight validation.
func (t *Tracker) SetGEVM(gevm *execution.GEVMSimulator) { t.gevm = gevm }

// Run starts the main loop and WebSocket subscribers.
func (t *Tracker) Run(ctx context.Context) {
	t.ctx, t.cancel = context.WithCancel(ctx)
	defer t.cancel()

	// Start log processors
	for i := 0; i < 2; i++ {
		go t.logProcessor(t.ctx)
	}

	// Start WebSocket subscription
	if t.wsURL != "" {
		go t.runWebSocketSubscription(t.ctx)
	} else {
		log.Printf("[Liquidation] WebSocket URL not set; using polling only.")
	}
	go func() {
    ticker := time.NewTicker(5 * time.Minute)
    defer ticker.Stop()
    for {
        select {
        case <-t.ctx.Done():
            return
        case <-ticker.C:
            t.mu.Lock()
            for addr, pos := range t.positions {
                if time.Since(pos.LastUpdated) > 10*time.Minute {
                    delete(t.positions, addr)
                    delete(t.knownUsers, addr)
                }
            }
            t.mu.Unlock()
        }
    }
}()
	// Periodic polling
	pollTicker := time.NewTicker(t.pollInterval)
	defer pollTicker.Stop()

	for {
		select {
		case <-t.ctx.Done():
			return
		case <-pollTicker.C:
			t.checkAllPositions(t.ctx)
		}
	}

}

// ---------------------------------------------------------------------
// WebSocket Subscription (real‑time events)
// ---------------------------------------------------------------------

func (t *Tracker) runWebSocketSubscription(ctx context.Context) {
	// Build list of addresses and topic filters
	addresses := make([]common.Address, 0, len(t.protocols))
	var allTopics []interface{}
	for _, cfg := range t.protocols {
		addresses = append(addresses, cfg.PoolAddress)
		allTopics = append(allTopics, cfg.BorrowEventSig, cfg.RepayEventSig)
	}
	filter := map[string]interface{}{
		"address": addresses,
		"topics":  [][]interface{}{allTopics},
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
				log.Printf("[Liquidation] WebSocket dial failed: %v", err)
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
							_ = t.wsConn.WriteControl(websocket.PingMessage, nil, time.Now().Add(5*time.Second))
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
				var vLog types.Log
				if err := json.Unmarshal(subData.Result, &vLog); err != nil {
					continue
				}
				select {
				case t.logChan <- vLog:
				default:
					log.Printf("[Liquidation] Log channel full, dropping event")
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

func (t *Tracker) handleLog(vLog types.Log) {
	if len(vLog.Topics) == 0 {
		return
	}
	topic := vLog.Topics[0]
	var proto uint8
	var user common.Address
	for p, cfg := range t.protocols {
		if vLog.Address == cfg.PoolAddress {
			if topic == cfg.BorrowEventSig || topic == cfg.RepayEventSig {
				proto = p
				break
			}
		}
	}
	if proto == 0 {
		return
	}
	// Extract user from topics (depends on protocol)
	switch proto {
	case ProtocolAave:
		if topic == aaveBorrowEventSig {
			if len(vLog.Topics) > 2 {
				user = common.BytesToAddress(vLog.Topics[2].Bytes())
			}
		} else {
			if len(vLog.Topics) > 1 {
				user = common.BytesToAddress(vLog.Topics[1].Bytes())
			}
		}
	case ProtocolCompound:
		if len(vLog.Topics) > 1 {
			user = common.BytesToAddress(vLog.Topics[1].Bytes())
		}
	case ProtocolMorpho:
		if len(vLog.Topics) > 1 {
			user = common.BytesToAddress(vLog.Topics[1].Bytes())
		}
	case ProtocolExactly:
		if len(vLog.Topics) > 1 {
			user = common.BytesToAddress(vLog.Topics[1].Bytes())
		}
	case ProtocolMoonwell:
		if len(vLog.Topics) > 1 {
			user = common.BytesToAddress(vLog.Topics[1].Bytes())
		}
	case ProtocolIonic:
		if len(vLog.Topics) > 1 {
			user = common.BytesToAddress(vLog.Topics[1].Bytes())
		}
	}
	if user == (common.Address{}) {
		return
	}
	t.updatePosition(t.ctx, user, proto)
}

// ---------------------------------------------------------------------
// Position Fetching (all protocols)
// ---------------------------------------------------------------------

func (t *Tracker) checkAllPositions(ctx context.Context) {
	t.mu.RLock()
	users := make([]common.Address, 0, len(t.knownUsers))
	for u := range t.knownUsers {
		users = append(users, u)
	}
	t.mu.RUnlock()

	for _, user := range users {
		for proto := range t.protocols {
			t.updatePosition(ctx, user, proto)
		}
	}
}

func (t *Tracker) updatePosition(ctx context.Context, user common.Address, proto uint8) {
	switch proto {
	case ProtocolAave:
		t.updateAavePosition(ctx, user)
	case ProtocolCompound:
		t.updateCompoundPosition(ctx, user)
	case ProtocolMorpho:
		t.updateMorphoPosition(ctx, user)
	case ProtocolExactly:
		t.updateExactlyPosition(ctx, user)
	case ProtocolMoonwell:
		t.updateMoonwellPosition(ctx, user)
	case ProtocolIonic:
		t.updateIonicPosition(ctx, user)
	}
}

// ---- Aave V3 (full asset detection) ----
func (t *Tracker) updateAavePosition(ctx context.Context, user common.Address) {
	pool := t.protocols[ProtocolAave].PoolAddress

	// 1. Fetch user reserves to know actual assets
	reservesData, err := t.fetchAaveUserReserves(ctx, pool, user)
	if err != nil || len(reservesData) == 0 {
		return
	}

	var totalCollateralBase, totalDebtBase big.Int
	var collateralAsset, debtAsset common.Address
	var debtAmount *big.Int

	// Iterate reserves to find positive balances
	for _, res := range reservesData {
		if res.CollateralBalance.Sign() > 0 {
			totalCollateralBase.Add(&totalCollateralBase, res.CollateralBalance)
			if collateralAsset == (common.Address{}) {
				collateralAsset = res.UnderlyingAsset
			}
		}
		if res.VariableDebt.Sign() > 0 || res.StableDebt.Sign() > 0 {
			debtAmount = new(big.Int).Add(res.VariableDebt, res.StableDebt)
			totalDebtBase.Add(&totalDebtBase, debtAmount)
			if debtAsset == (common.Address{}) {
				debtAsset = res.UnderlyingAsset
			}
		}
	}
	if totalCollateralBase.Sign() == 0 || totalDebtBase.Sign() == 0 {
		return // no active position
	}

	// Convert to USD (Aave uses 1e8 base)
	collateralUSD := float64FromBig(&totalCollateralBase) / 1e8
	debtUSD := float64FromBig(&totalDebtBase) / 1e8

	// Health factor – we need to call getUserAccountData for HF
	health, err := t.fetchAaveHealthFactor(ctx, pool, user)
	if err != nil {
		return
	}

	pos := &UserPosition{
		User:               user,
		Protocol:           ProtocolAave,
		TotalCollateralUSD: collateralUSD,
		TotalDebtUSD:       debtUSD,
		HealthFactor:       health,
		LastUpdated:        time.Now(),
		DebtAmount:         debtAmount,
		CollateralAsset:    collateralAsset,
		DebtAsset:          debtAsset,
	}
	t.storePosition(user, pos)
	if health < 1.0 {
		t.triggerLiquidation(pos)
	}
}

// Helper: fetch user reserves (returns slice)
type AaveUserReserve struct {
	UnderlyingAsset   common.Address
	CollateralBalance *big.Int
	VariableDebt      *big.Int
	StableDebt        *big.Int
}

func (t *Tracker) fetchAaveUserReserves(ctx context.Context, pool, user common.Address) ([]AaveUserReserve, error) {
	callData, err := aavePoolABI.Pack("getUserReserves", user)
	if err != nil {
		return nil, err
	}
	msg := ethereum.CallMsg{To: &pool, Data: callData}
	out, err := t.client.CallContract(ctx, msg, nil)
	if err != nil || len(out) == 0 {
		return nil, err
	}
	var results []struct {
		UnderlyingAsset   common.Address
		AToken            common.Address
		VariableDebt      common.Address
		StableDebt        common.Address
		CurrentATokenBalance *big.Int
		CurrentVariableDebt  *big.Int
		CurrentStableDebt    *big.Int
	}
	err = aavePoolABI.UnpackIntoInterface(&results, "getUserReserves", out)
	if err != nil {
		return nil, err
	}
	reserves := make([]AaveUserReserve, len(results))
	for i, r := range results {
		reserves[i] = AaveUserReserve{
			UnderlyingAsset:   r.UnderlyingAsset,
			CollateralBalance: r.CurrentATokenBalance,
			VariableDebt:      r.CurrentVariableDebt,
			StableDebt:        r.CurrentStableDebt,
		}
	}
	return reserves, nil
}

func (t *Tracker) fetchAaveHealthFactor(ctx context.Context, pool, user common.Address) (float64, error) {
	callData, _ := aavePoolABI.Pack("getUserAccountData", user)
	msg := ethereum.CallMsg{To: &pool, Data: callData}
	out, err := t.client.CallContract(ctx, msg, nil)
	if err != nil || len(out) == 0 {
		return 0, err
	}
	var results []interface{}
	if err := aavePoolABI.UnpackIntoInterface(&results, "getUserAccountData", out); err != nil || len(results) < 6 {
		return 0, err
	}
	healthFactor := results[5].(*big.Int)
	return float64FromBig(healthFactor) / 1e18, nil
}

// ---- Compound III ----
func (t *Tracker) updateCompoundPosition(ctx context.Context, user common.Address) {
	comet := t.protocols[ProtocolCompound].PoolAddress

	// 1. Borrow balance (USDC)
	callData, _ := compoundABI.Pack("getBorrowBalance", user)
	msg := ethereum.CallMsg{To: &comet, Data: callData}
	out, err := t.client.CallContract(ctx, msg, nil)
	if err != nil || len(out) == 0 {
		return
	}
	var borrowBalance *big.Int
	compoundABI.UnpackIntoInterface(&borrowBalance, "getBorrowBalance", out)

	if borrowBalance.Sign() == 0 {
		return // no debt
	}

	// 2. Find collateral: iterate known assets
	var collateralAsset common.Address
	var collateralBalance *big.Int
	for _, asset := range t.protocols[ProtocolCompound].KnownAssets {
		callData2, _ := compoundABI.Pack("getCollateralBalance", user, asset)
		msg2 := ethereum.CallMsg{To: &comet, Data: callData2}
		out2, err := t.client.CallContract(ctx, msg2, nil)
		if err != nil || len(out2) == 0 {
			continue
		}
		var bal *big.Int
		compoundABI.UnpackIntoInterface(&bal, "getCollateralBalance", out2)
		if bal.Sign() > 0 {
			collateralBalance = bal
			collateralAsset = asset
			break
		}
	}
	if collateralAsset == (common.Address{}) {
		return
	}

	ethPrice, _ := solver.GetTokenPrice(config.WETHAddress, t.matrix, nil)
	usdcPrice := 1.0
	collateralUSD := float64FromBig(collateralBalance) / 1e18 * ethPrice
	debtUSD := float64FromBig(borrowBalance) / 1e6 * usdcPrice
	health := 0.0
	if debtUSD > 0 {
		health = (collateralUSD * 0.85) / debtUSD
	}

	pos := &UserPosition{
		User:               user,
		Protocol:           ProtocolCompound,
		TotalCollateralUSD: collateralUSD,
		TotalDebtUSD:       debtUSD,
		HealthFactor:       health,
		LastUpdated:        time.Now(),
		DebtAmount:         borrowBalance,
		CollateralAsset:    collateralAsset,
		DebtAsset:          config.USDCAddress,
	}
	t.storePosition(user, pos)
	if health < 1.0 {
		t.triggerLiquidation(pos)
	}
}

// ---- Morpho Blue (dynamic market lookup) ----
func (t *Tracker) updateMorphoPosition(ctx context.Context, user common.Address) {
	morpho := t.protocols[ProtocolMorpho].PoolAddress

	// We need to know which markets the user is in. For simplicity, we'll query a known market ID.
	// In production, you'd iterate over all market IDs from a registry.
	// For this implementation, we use a hardcoded market ID for WETH/USDC (computed from token pair and oracle).
	// We'll also provide a function to derive the market ID.
	marketID := t.getMorphoMarketID(config.USDCAddress, config.WETHAddress)
	if marketID == (common.Hash{}) {
		return
	}

	// Get market params
	params, err := t.fetchMorphoMarketParams(ctx, morpho, marketID)
	if err != nil {
		return
	}

	// Fetch user position
	callData, _ := morphoBlueABI.Pack("position", user, params.Market)
	msg := ethereum.CallMsg{To: &morpho, Data: callData}
	out, err := t.client.CallContract(ctx, msg, nil)
	if err != nil || len(out) == 0 {
		return
	}
	var results []interface{}
	morphoBlueABI.UnpackIntoInterface(&results, "position", out)
	if len(results) < 3 {
		return
	}

	borrow := results[1].(*big.Int)
	collateral := results[2].(*big.Int)

	if borrow.Sign() == 0 {
		return
	}

	// Convert to USD – we need price of collateral token and loan token
	collateralPrice, ok := solver.GetTokenPrice(params.CollateralToken, t.matrix, nil)
	if !ok || collateralPrice <= 0 {
		return
	}
	loanPrice, ok := solver.GetTokenPrice(params.LoanToken, t.matrix, nil)
	if !ok || loanPrice <= 0 {
		return
	}
	collateralDecimals := solver.GetTokenDecimals(params.CollateralToken)
	loanDecimals := solver.GetTokenDecimals(params.LoanToken)
	collateralUSD := float64FromBig(collateral) / math.Pow10(collateralDecimals) * collateralPrice
	debtUSD := float64FromBig(borrow) / math.Pow10(loanDecimals) * loanPrice

	health := 0.0
	if debtUSD > 0 {
		// Morpho uses lltv: health = collateral * lltv / debt
		lltv := float64FromBig(params.LLTV) / 1e18
		health = (collateralUSD * lltv) / debtUSD
	}

	pos := &UserPosition{
    User:               user,
    Protocol:           ProtocolMorpho,
    TotalCollateralUSD: collateralUSD,
    TotalDebtUSD:       debtUSD,
    HealthFactor:       health,
    LastUpdated:        time.Now(),
    DebtAmount:         borrow,
    CollateralAmount:   collateral,    // <-- FIX: set from position call
    CollateralAsset:    params.CollateralToken,
    DebtAsset:          params.LoanToken,
    MarketParams:       []interface{}{
        params.LoanToken,
        params.CollateralToken,
        params.Oracle,
        params.IRM,      // <-- FIX: added IRM
        params.LLTV,
    },
}
}

func (t *Tracker) getMorphoMarketID(loanToken, collateralToken common.Address) common.Hash {
    // Chainlink WETH/USD oracle on Base (verified)
    oracle := common.HexToAddress("0x71041dddad3595F9CEd3DcCFBe3D1F4b0a16Bb70")
    // Adaptive Curve IRM from Morpho Blue Base deployment
    irm := common.HexToAddress("0xF02615d094Fc02fC031C35fe705e175aA4653f20") // [7†L13-L14]
    lltv := new(big.Int).SetUint64(860000000000000000) // 86% (18 decimals)

    encoded := abi.Arguments{
        {Type: mustParseType("address"), Name: "loanToken"},
        {Type: mustParseType("address"), Name: "collateralToken"},
        {Type: mustParseType("address"), Name: "oracle"},
        {Type: mustParseType("address"), Name: "irm"},
        {Type: mustParseType("uint256"), Name: "lltv"},
    }
    packed, _ := encoded.Pack(loanToken, collateralToken, oracle, irm, lltv)
    return crypto.Keccak256Hash(packed)
}

func (t *Tracker) fetchMorphoMarketParams(ctx context.Context, morpho common.Address, id common.Hash) (MorphoMarketParams, error) {
    // 1. Get market params (loanToken, collateralToken, oracle, irm, lltv)
    callData, _ := morphoBlueABI.Pack("idToMarketParams", id)
    msg := ethereum.CallMsg{To: &morpho, Data: callData}
    out, err := t.client.CallContract(ctx, msg, nil)
    if err != nil || len(out) == 0 {
        return MorphoMarketParams{}, fmt.Errorf("idToMarketParams failed: %w", err)
    }
    var results struct {
        LoanToken      common.Address
        CollateralToken common.Address
        Oracle         common.Address
        Irm            common.Address
        Lltv           *big.Int
    }
    err = morphoBlueABI.UnpackIntoInterface(&results, "idToMarketParams", out)
    if err != nil {
        return MorphoMarketParams{}, err
    }

    // 2. Get the actual market address from idToMarket
    marketCallData, _ := morphoBlueABI.Pack("idToMarket", id)
    msgMarket := ethereum.CallMsg{To: &morpho, Data: marketCallData}
    marketOut, err := t.client.CallContract(ctx, msgMarket, nil)
    if err != nil || len(marketOut) == 0 {
        return MorphoMarketParams{}, fmt.Errorf("idToMarket failed: %w", err)
    }
    var marketAddr common.Address
    err = morphoBlueABI.UnpackIntoInterface(&marketAddr, "idToMarket", marketOut)
    if err != nil {
        return MorphoMarketParams{}, err
    }

    return MorphoMarketParams{
        Market:          marketAddr,  // NOW FILLED
        LoanToken:       results.LoanToken,
        CollateralToken: results.CollateralToken,
        Oracle:          results.Oracle,
        IRM:             results.Irm,
        LLTV:            results.Lltv,
    }, nil
}

// ---- Exactly (dynamic market discovery) ----
func (t *Tracker) updateExactlyPosition(ctx context.Context, user common.Address) {
	auditor := t.protocols[ProtocolExactly].PoolAddress

	// 1. Get user's markets
	markets, err := t.fetchExactlyUserMarkets(ctx, auditor, user)
	if err != nil || len(markets) == 0 {
		return
	}

	// 2. Get account liquidity (collateral, debt)
	callData, _ := exactlyAuditorABI.Pack("getAccountLiquidity", user)
	msg := ethereum.CallMsg{To: &auditor, Data: callData}
	out, err := t.client.CallContract(ctx, msg, nil)
	if err != nil || len(out) == 0 {
		return
	}
	var results []interface{}
	exactlyAuditorABI.UnpackIntoInterface(&results, "getAccountLiquidity", out)
	if len(results) < 3 {
		return
	}
	collateral := results[0].(*big.Int)
	debt := results[1].(*big.Int)
	if collateral.Sign() == 0 || debt.Sign() == 0 {
		return
	}

	// 3. Determine collateral and debt markets
	collateralMarket := markets[0]

	// For this implementation, we assume the market is for USDC/WETH.
	collateralAsset :=  config.WETHAddress
	debtAsset :=config.USDCAddress

	collateralUSD := float64FromBig(collateral) / 1e8
	debtUSD := float64FromBig(debt) / 1e8
	health := 0.0
	if debtUSD > 0 {
		health = collateralUSD / debtUSD
	}

	pos := &UserPosition{
		User:               user,
		Protocol:           ProtocolExactly,
		TotalCollateralUSD: collateralUSD,
		TotalDebtUSD:       debtUSD,
		HealthFactor:       health,
		LastUpdated:        time.Now(),
		DebtAmount:         debt,
		CollateralAmount:   collateral,
		CollateralAsset:    collateralAsset,
		DebtAsset:          debtAsset,
		CollateralMarket:   collateralMarket,
	}
	t.storePosition(user, pos)
	if health < 1.0 {
		t.triggerLiquidation(pos)
	}
}

func (t *Tracker) fetchExactlyUserMarkets(ctx context.Context, auditor, user common.Address) ([]common.Address, error) {
	callData, _ := exactlyAuditorABI.Pack("getAccountMarkets", user)
	msg := ethereum.CallMsg{To: &auditor, Data: callData}
	out, err := t.client.CallContract(ctx, msg, nil)
	if err != nil || len(out) == 0 {
		return nil, err
	}
	var markets []common.Address
	err = exactlyAuditorABI.UnpackIntoInterface(&markets, "getAccountMarkets", out)
	if err != nil {
		return nil, err
	}
	return markets, nil
}

// ---- Moonwell (Compound v2 style) ----
func (t *Tracker) updateMoonwellPosition(ctx context.Context, user common.Address) {
	mToken := t.protocols[ProtocolMoonwell].PoolAddress // mUSDC

	// 1. Get account snapshot
	callData, _ := moonwellMTokenABI.Pack("getAccountSnapshot", user)
	msg := ethereum.CallMsg{To: &mToken, Data: callData}
	out, err := t.client.CallContract(ctx, msg, nil)
	if err != nil || len(out) == 0 {
		return
	}
	var results []interface{}
	moonwellMTokenABI.UnpackIntoInterface(&results, "getAccountSnapshot", out)
	if len(results) < 3 {
		return
	}
	collateral := results[0].(*big.Int)
	borrow := results[1].(*big.Int)
	if borrow.Sign() == 0 {
		return
	}

	// Underlying asset of mToken (USDC)
	underlying, err := t.fetchMoonwellUnderlying(ctx, mToken)
	if err != nil {
		return
	}

	ethPrice, _ := solver.GetTokenPrice(config.WETHAddress, t.matrix, nil)
	usdcPrice := 1.0
	collateralUSD := float64FromBig(collateral) / 1e6 * usdcPrice
	debtUSD := float64FromBig(borrow) / 1e18 * ethPrice
	health := 0.0
	if debtUSD > 0 {
		health = collateralUSD / debtUSD
	}

	pos := &UserPosition{
		User:               user,
		Protocol:           ProtocolMoonwell,
		TotalCollateralUSD: collateralUSD,
		TotalDebtUSD:       debtUSD,
		HealthFactor:       health,
		LastUpdated:        time.Now(),
		DebtAmount:         borrow,
		CollateralAsset:    underlying,
		DebtAsset:          config.USDCAddress, // assuming debt is WETH
	}
	t.storePosition(user, pos)
	if health < 1.0 {
		t.triggerLiquidation(pos)
	}
}

func (t *Tracker) fetchMoonwellUnderlying(ctx context.Context, mToken common.Address) (common.Address, error) {
	callData, _ := moonwellMTokenABI.Pack("underlying")
	msg := ethereum.CallMsg{To: &mToken, Data: callData}
	out, err := t.client.CallContract(ctx, msg, nil)
	if err != nil || len(out) == 0 {
		return common.Address{}, err
	}
	var underlying common.Address
	err = moonwellMTokenABI.UnpackIntoInterface(&underlying, "underlying", out)
	return underlying, err
}

// ---- Ionic (Compound v3 style) ----
func (t *Tracker) updateIonicPosition(ctx context.Context, user common.Address) {
	pool := t.protocols[ProtocolIonic].PoolAddress

	callData, _ := ionicPoolABI.Pack("getUserAccountData", user)
	msg := ethereum.CallMsg{To: &pool, Data: callData}
	out, err := t.client.CallContract(ctx, msg, nil)
	if err != nil || len(out) == 0 {
		return
	}
	var results []interface{}
	ionicPoolABI.UnpackIntoInterface(&results, "getUserAccountData", out)
	if len(results) < 3 {
		return
	}
	collateral := results[0].(*big.Int)
	debt := results[1].(*big.Int)
	if debt.Sign() == 0 {
		return
	}

	// Ionic uses base currency (USD) with 1e8 decimals
	collateralUSD := float64FromBig(collateral) / 1e8
	debtUSD := float64FromBig(debt) / 1e8
	health := 0.0
	if debtUSD > 0 {
		health = collateralUSD / debtUSD
	}

	// Asset detection – we need to know which assets. For simplicity, we'll hardcode.
	// In production, you'd query per‑asset like Compound III.
	pos := &UserPosition{
		User:               user,
		Protocol:           ProtocolIonic,
		TotalCollateralUSD: collateralUSD,
		TotalDebtUSD:       debtUSD,
		HealthFactor:       health,
		LastUpdated:        time.Now(),
		DebtAmount:         debt,
		CollateralAsset:     config.WETHAddress, 
		DebtAsset:          config.USDCAddress, 
	}
	t.storePosition(user, pos)
	if health < 1.0 {
		t.triggerLiquidation(pos)
	}
}

// ---- store and trigger (unchanged, but updated liquidation data packing) ----

func (t *Tracker) storePosition(user common.Address, pos *UserPosition) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.positions[user] = pos
	t.knownUsers[user] = struct{}{}
}
func (t *Tracker) estimateProfit(pos *UserPosition) float64 {
    // Get current token prices (already cached in Matrix)
    collPrice, ok := solver.GetTokenPrice(pos.CollateralAsset, t.matrix, nil)
    if !ok || collPrice <= 0 {
        collPrice = 1.0
    }
    debtPrice, ok := solver.GetTokenPrice(pos.DebtAsset, t.matrix, nil)
    if !ok || debtPrice <= 0 {
        debtPrice = 1.0
    }

    // Convert debt amount to USD (using decimals)
    debtDecimals := solver.GetTokenDecimals(pos.DebtAsset)
    debtUSD := float64FromBig(pos.DebtAmount) / math.Pow10(debtDecimals) * debtPrice

    // Estimate bonus (protocol-specific)
    bonusFactor := 0.05 // default 5%
    switch pos.Protocol {
    case ProtocolAave:
        // Aave bonuses: 5% for stablecoins, 7.5% for cbETH, 10% for others (approx)
        if pos.CollateralAsset == config.USDCAddress || pos.CollateralAsset == config.USDBCAddress {
            bonusFactor = 0.05
        } else if pos.CollateralAsset == config.CBBTCAddress {
            bonusFactor = 0.075
        } else {
            bonusFactor = 0.10
        }
    case ProtocolCompound:
        bonusFactor = 0.05 // WETH/USDC bonus
case ProtocolMorpho:
    bonusFactor = 0.05
    if len(pos.MarketParams) >= 5 {
        lltv := pos.MarketParams[4].(*big.Int)
        lltvFloat := float64FromBig(lltv) / 1e18
        if lltvFloat < 0.80 {
            bonusFactor = 0.10
        } else if lltvFloat < 0.86 {
            bonusFactor = 0.08
        }
    }
    case ProtocolExactly:
        bonusFactor = 0.05
    case ProtocolMoonwell:
        bonusFactor = 0.05
    case ProtocolIonic:
        bonusFactor = 0.05
    }

    // Gross profit in USD from bonus
    grossProfit := debtUSD * bonusFactor

    // Deduct swap slippage (~0.5%) and gas (~$2)
    netProfit := grossProfit * 0.995 - 2.0
    if netProfit < 0 {
        netProfit = 0
    }
    return netProfit
}

func (t *Tracker) triggerLiquidation(pos *UserPosition) {
t.liquidatingMu.Lock()
if t.liquidating[pos.User] {
    t.liquidatingMu.Unlock()
    return
}
t.liquidating[pos.User] = true
t.liquidatingMu.Unlock()
defer func() {
    t.liquidatingMu.Lock()
    delete(t.liquidating, pos.User)
    t.liquidatingMu.Unlock()
}()
estimatedProfit := t.estimateProfit(pos)
if estimatedProfit < t.cfg.LiquidationMinProfitUSD {
    log.Printf("[Liquidation] Position for %s estimated profit $%.2f below minimum $%.2f, skipping", 
        pos.User.Hex(), estimatedProfit, t.cfg.LiquidationMinProfitUSD)
    return
}

	var liquidationData []byte
	protocol := pos.Protocol
	switch protocol {
	case ProtocolAave:
		args := []interface{}{
			pos.CollateralAsset,
			pos.User,
			pos.DebtAmount,
			false,
		}
		liquidationData = packLiquidationData(protocol, args...)
	case ProtocolCompound:
		minAmount := new(big.Int).Mul(pos.DebtAmount, big.NewInt(105))
		minAmount.Div(minAmount, big.NewInt(100))
		args := []interface{}{
			pos.User,
			pos.CollateralAsset,
			minAmount,
		}
		liquidationData = packLiquidationData(protocol, args...)
	case ProtocolMorpho:
    if len(pos.MarketParams) < 5 {
        log.Printf("[Liquidation] Morpho: missing market params for %s", pos.User.Hex())
        return
    }

    tuple := MorphoParamsTuple{
        LoanToken:       pos.MarketParams[0].(common.Address),
        CollateralToken: pos.MarketParams[1].(common.Address),
        Oracle:          pos.MarketParams[2].(common.Address),
        IRM:             pos.MarketParams[3].(common.Address),
        LLTV:            pos.MarketParams[4].(*big.Int),
    }

    collateralPrice, ok := solver.GetTokenPrice(tuple.CollateralToken, t.matrix, nil)
    if !ok || collateralPrice <= 0 {
        log.Printf("[Liquidation] Morpho: collateral price missing for %s", tuple.CollateralToken.Hex())
        return
    }
    loanPrice, ok := solver.GetTokenPrice(tuple.LoanToken, t.matrix, nil)
    if !ok || loanPrice <= 0 {
        log.Printf("[Liquidation] Morpho: loan price missing for %s", tuple.LoanToken.Hex())
        return
    }

    lltvFloat := float64FromBig(tuple.LLTV) / 1e18
    incentiveFactor := 1.05
    if lltvFloat < 0.80 {
        incentiveFactor = 1.10
    } else if lltvFloat < 0.75 {
        incentiveFactor = 1.15
    }

    debtFloat := new(big.Float).SetInt(pos.DebtAmount)
    ratio := new(big.Float).Quo(big.NewFloat(loanPrice), big.NewFloat(collateralPrice))
    seizedFloat := new(big.Float).Mul(debtFloat, ratio)
    seizedFloat.Mul(seizedFloat, big.NewFloat(incentiveFactor))

    seized := new(big.Int)
    seizedFloat.Int(seized)

    if seized.Sign() <= 0 {
        log.Printf("[Liquidation] Morpho: calculated seized amount is zero for %s", pos.User.Hex())
        return
    }

    args := []interface{}{
        tuple,          // ← the struct
        pos.User,
        seized,
        pos.DebtAmount,
    }
    liquidationData = packLiquidationData(protocol, args...)

	case ProtocolExactly:
		if pos.CollateralMarket == (common.Address{}) {
			log.Printf("[Liquidation] Exactly: seizeMarket not set, skipping %s", pos.User.Hex())
			return
		}
		args := []interface{}{
			pos.User,
			pos.CollateralMarket,
			pos.DebtAmount,
		}
		liquidationData = packLiquidationData(protocol, args...)
	case ProtocolMoonwell:
		// mTokenCollateral: we need the mToken for the collateral asset.
		// For simplicity, we use the same mToken as the pool (mUSDC)
		mToken := t.protocols[ProtocolMoonwell].PoolAddress
		args := []interface{}{
			pos.User,
			mToken,
			pos.DebtAmount,
		}
		liquidationData = packLiquidationData(protocol, args...)
	case ProtocolIonic:
		args := []interface{}{
			pos.User,
			pos.CollateralAsset,
			pos.DebtAmount,
		}
		liquidationData = packLiquidationData(protocol, args...)
	default:
		log.Printf("[Liquidation] Protocol %d not supported for liquidation data", protocol)
		return
	}

	if liquidationData == nil {
		return
	}

	// Build calldata
fee := t.getFeeForPair(pos.CollateralAsset, pos.DebtAsset) // now uint32
calldata, err := executorABI.Pack("executeLiquidation",
    uint8(protocol),
    pos.DebtAsset,
    pos.DebtAmount,
    fee,                     // <-- NEW
    liquidationData,
)
	if err != nil {
		log.Printf("[Liquidation] Pack calldata error: %v", err)
		return
	}

	payload := t.payloadPool.Get().(*botTypes.ExecutionPayload)
	payload.Reset()
	payload.TargetExecutor = t.executorAddress
	payload.BorrowedToken = pos.DebtAsset
	payload.BorrowedAmount = pos.DebtAmount
	payload.Calldata = calldata
	payload.GasLimit = 800_000
	payload.PriorityFeeWei = t.priorityFee
	payload.RouteDesc = fmt.Sprintf("Liquidation-%s", pos.User.Hex()[:8])
	payload.MinProfitUSD = estimatedProfit
	payload.LoanProvider = 0
	payload.LoanPool = config.BalancerVault
	payload.OriginalCandidate = nil
	payload.RoutePools = []common.Address{}

	// GEVM simulation
	if t.gevm != nil {
		simPayload := &botTypes.ExecutionPayload{
			TargetExecutor: t.executorAddress,
			BorrowedToken:  pos.DebtAsset,
			Calldata:       calldata,
			GasLimit:       payload.GasLimit,
			PriorityFeeWei: t.priorityFee,
		}
		success, gasUsed, err := t.gevm.SimulateNative(simPayload)
		if err != nil || !success {
			log.Printf("[Liquidation] Simulation failed for %s: %v", pos.User.Hex(), err)
			t.payloadPool.Put(payload)
			return
		}
		payload.GasLimit = gasUsed + gasUsed/5
		log.Printf("[Liquidation] Simulation passed for %s, gas: %d", pos.User.Hex(), payload.GasLimit)
	} else {
		log.Printf("[Liquidation] GEVM not set; sending without simulation (unsafe)")
	}

	// Submit
	select {
	case t.executionChan <- payload:
		log.Printf("[Liquidation] Submitted liquidation tx for %s, profit $%.2f", pos.User.Hex(), estimatedProfit)
	default:
		log.Printf("[Liquidation] Execution channel full, dropping liquidation for %s", pos.User.Hex())
		t.payloadPool.Put(payload)
	}
}

// ---------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------

func packLiquidationData(protocol uint8, args ...interface{}) []byte {
	var arguments abi.Arguments
	switch protocol {
	case ProtocolAave:
		arguments = abi.Arguments{
			{Type: mustParseType("address"), Name: "collateral"},
			{Type: mustParseType("address"), Name: "user"},
			{Type: mustParseType("uint256"), Name: "debtToCover"},
			{Type: mustParseType("bool"), Name: "receiveAToken"},
		}
	case ProtocolCompound:
		arguments = abi.Arguments{
			{Type: mustParseType("address"), Name: "user"},
			{Type: mustParseType("address"), Name: "collateral"},
			{Type: mustParseType("uint256"), Name: "minAmount"},
		}
	case ProtocolMorpho:
    arguments = abi.Arguments{
        // Define the tuple type with components using NewType
tupleType, err := abi.NewType("tuple", "", []abi.ArgumentMarshaling{
    {Name: "loanToken", Type: "address"},
    {Name: "collateralToken", Type: "address"},
    {Name: "oracle", Type: "address"},
    {Name: "irm", Type: "address"},
    {Name: "lltv", Type: "uint256"},
})
if err != nil {
    log.Printf("Failed to create tuple type: %v", err)
    return nil
}
arguments = abi.Arguments{
    {Type: tupleType, Name: "params"},
    {Type: mustParseType("address"), Name: "borrower"},
    {Type: mustParseType("uint256"), Name: "seizedAssets"},
    {Type: mustParseType("uint256"), Name: "maxRepay"},
}
    }
	case ProtocolExactly:
		arguments = abi.Arguments{
			{Type: mustParseType("address"), Name: "borrower"},
			{Type: mustParseType("address"), Name: "seizeMarket"},
			{Type: mustParseType("uint256"), Name: "maxAssets"},
		}
	case ProtocolMoonwell:
		arguments = abi.Arguments{
			{Type: mustParseType("address"), Name: "borrower"},
			{Type: mustParseType("address"), Name: "mTokenCollateral"},
			{Type: mustParseType("uint256"), Name: "repayAmount"},
		}
	case ProtocolIonic:
		arguments = abi.Arguments{
			{Type: mustParseType("address"), Name: "borrower"},
			{Type: mustParseType("address"), Name: "collateral"},
			{Type: mustParseType("uint256"), Name: "repayAmount"},
		}
	default:
		return nil
	}
	data, err := arguments.Pack(args...)
	if err != nil {
		log.Printf("[Liquidation] Pack args error: %v", err)
		return nil
	}
	return data
}

func mustParseType(typ string) abi.Type {
	t, err := abi.NewType(typ, "", []abi.ArgumentMarshaling{})
	if err != nil {
		panic(err)
	}
	return t
}

func float64FromBig(v *big.Int) float64 {
	if v == nil {
		return 0
	}
	f, _ := new(big.Float).SetInt(v).Float64()
	return f
}
