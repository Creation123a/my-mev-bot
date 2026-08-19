// Package execution provides ultra‑low‑latency transaction signing and broadcasting for Base.
package execution

import (
	"context"
	"crypto/ecdsa"
	"fmt"
	"math/big"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	gethTypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"

	"my-mev-bot/Bot/Types"
)

// RetryContext – kept for compatibility but unused in hot path.
type RetryContext struct {
	Pools             []common.Address
	Tokens            []common.Address
	FailureReason     string
	OriginalCandidate *types.RouteCandidate
	RemainingTime     uint64
	LoanToken         common.Address
}

type PendingTx struct {
	Nonce   uint64
	Hash    common.Hash
	SentAt  time.Time
	Payload *types.ExecutionPayload
	Checked bool
}

type ConfirmationCallback func(nonce uint64, txHash common.Hash, receipt *gethTypes.Receipt, payload *types.ExecutionPayload)
type SolverCallback func(ctx RetryContext) []*types.RouteCandidate

type Sender struct {
	privateKey  *ecdsa.PrivateKey
	address     common.Address
	chainID     *big.Int
	ethClient   *ethclient.Client

	pendingMu   sync.Mutex
	pending     map[uint64]*PendingTx

	solverCallback SolverCallback
	simulateFunc   func(cand *types.RouteCandidate, payload *types.ExecutionPayload) (bool, uint64, error)
	confCallback   ConfirmationCallback

	ctx    context.Context
	cancel context.CancelFunc
}

func NewSender(rpcURL, privateKeyHex string) (*Sender, error) {
	privateKey, err := crypto.HexToECDSA(privateKeyHex)
	if err != nil {
		return nil, err
	}
	address := crypto.PubkeyToAddress(privateKey.PublicKey)

	ethClient, err := ethclient.Dial(rpcURL)
	if err != nil {
		return nil, err
	}

	idCtx, idCancel := context.WithTimeout(context.Background(), 3*time.Second)
	chainID, err := ethClient.ChainID(idCtx)
	idCancel()
	if err != nil {
		ethClient.Close()
		return nil, fmt.Errorf("fetch chain ID: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	return &Sender{
		privateKey: privateKey,
		address:    address,
		chainID:    chainID,
		ethClient:  ethClient,
		pending:    make(map[uint64]*PendingTx),
		ctx:        ctx,
		cancel:     cancel,
	}, nil
}

// SetReplaceConfig – kept for compatibility, does nothing on Base.
func (s *Sender) SetReplaceConfig(timeout time.Duration, multiplier float64, max int) {}

// SetSolverCallback – kept for compatibility.
func (s *Sender) SetSolverCallback(cb SolverCallback) {
	s.solverCallback = cb
}

// SetConfirmationCallback – stores callback for confirmed transactions.
func (s *Sender) SetConfirmationCallback(cb ConfirmationCallback) {
	s.confCallback = cb
}

// SetSimulateFunc – kept for compatibility.
func (s *Sender) SetSimulateFunc(fn func(cand *types.RouteCandidate, payload *types.ExecutionPayload) (bool, uint64, error)) {
	s.simulateFunc = fn
}

// RegisterPendingNonce stores payload for later confirmation.
func (s *Sender) RegisterPendingNonce(nonce uint64, payload *types.ExecutionPayload) {
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	if p, ok := s.pending[nonce]; ok {
		p.Payload = payload
	}
}

// PrepareAndSignTransaction performs synchronous signing inside worker3.
func (s *Sender) PrepareAndSignTransaction(payload *types.ExecutionPayload) ([]byte, common.Hash, error) {
	to := payload.TargetExecutor
	value := big.NewInt(0)

	gasTip := new(big.Int).SetUint64(payload.PriorityFeeWei)
	gasFee := new(big.Int).Mul(gasTip, big.NewInt(2))

	tx := gethTypes.NewTx(&gethTypes.DynamicFeeTx{
		ChainID:    s.chainID,
		Nonce:      payload.Nonce,
		GasTipCap:  gasTip,
		GasFeeCap:  gasFee,
		Gas:        payload.GasLimit,
		To:         &to,
		Value:      value,
		Data:       payload.Calldata,
		AccessList: gethTypes.AccessList{},
	})

	signer := gethTypes.LatestSignerForChainID(s.chainID)
	signedTx, err := gethTypes.SignTx(tx, signer, s.privateKey)
	if err != nil {
		return nil, common.Hash{}, fmt.Errorf("failed to sign transaction: %w", err)
	}

	rawTxBytes, err := signedTx.MarshalBinary()
	if err != nil {
		return nil, common.Hash{}, fmt.Errorf("failed transaction encoding: %w", err)
	}

	return rawTxBytes, signedTx.Hash(), nil
}

// BroadcastRawTransactionBytes delivers serialized execution packets via WebSocket.
func (s *Sender) BroadcastRawTransactionBytes(rawTx []byte) error {
	var signedTx gethTypes.Transaction
	if err := signedTx.UnmarshalBinary(rawTx); err != nil {
		return fmt.Errorf("failed binary deserialization: %w", err)
	}

	nonce := signedTx.Nonce()
	txHash := signedTx.Hash()

	ctx, cancel := context.WithTimeout(s.ctx, 2*time.Second)
	defer cancel()
	err := s.ethClient.SendTransaction(ctx, &signedTx)
	if err != nil {
		msg := err.Error()
		if !strings.Contains(msg, "already known") && !strings.Contains(msg, "nonce too low") {
			return err
		}
	}
// Register pending for confirmation tracking, preserving existing payload if present.
s.pendingMu.Lock()
pending, exists := s.pending[nonce]
if !exists {
    s.pending[nonce] = &PendingTx{
        Nonce:   nonce,
        Hash:    txHash,
        SentAt:  time.Now(),
        Payload: nil,
        Checked: false,
    }
} else {
    // Update existing entry without wiping Payload
    pending.Hash = txHash
    pending.SentAt = time.Now()
    pending.Checked = false
}
s.pendingMu.Unlock()

	go s.monitorConfirmation(nonce)
	return nil
}

// monitorConfirmation checks for receipt once after a short delay.
func (s *Sender) monitorConfirmation(nonce uint64) {
	select {
	case <-time.After(500 * time.Millisecond):
	case <-s.ctx.Done():
		return
	}

	s.pendingMu.Lock()
	pending, ok := s.pending[nonce]
	if !ok || pending.Checked {
		s.pendingMu.Unlock()
		return
	}
	pending.Checked = true
	s.pendingMu.Unlock()

	ctx, cancel := context.WithTimeout(s.ctx, 1*time.Second)
	defer cancel()
	receipt, err := s.ethClient.TransactionReceipt(ctx, pending.Hash)
	if err != nil {
		select {
		case <-time.After(500 * time.Millisecond):
		case <-s.ctx.Done():
			return
		}
		ctx2, cancel2 := context.WithTimeout(s.ctx, 1*time.Second)
		defer cancel2()
		receipt, err = s.ethClient.TransactionReceipt(ctx2, pending.Hash)
		if err != nil {
			s.pendingMu.Lock()
			delete(s.pending, nonce)
			s.pendingMu.Unlock()
			return
		}
	}

	s.pendingMu.Lock()
	payload := pending.Payload
	txHash := pending.Hash
	delete(s.pending, nonce)
	s.pendingMu.Unlock()

	if s.confCallback != nil && receipt.Status == 1 && payload != nil {
		s.confCallback(nonce, txHash, receipt, payload)
	}
}

// ConfirmNonce removes a nonce from pending map.
func (s *Sender) ConfirmNonce(nonce uint64) {
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	delete(s.pending, nonce)
}

// Close releases resources.
func (s *Sender) Close() {
	s.cancel()
	if s.ethClient != nil {
		s.ethClient.Close()
	}
}
