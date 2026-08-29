// Package execution provides ultra‑low‑latency transaction signing and broadcasting for Base.
package execution

import (
	"context"
	"crypto/ecdsa"
	"crypto/tls"
	"fmt"
	"log"
	"math/big"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	gethTypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
	"golang.org/x/net/http2"

	"my-mev-bot/Bot/Types"
)

// RetryContext holds information for the solver callback.
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

// ConfirmationCallback is called when a transaction is confirmed.
type ConfirmationCallback func(nonce uint64, txHash common.Hash, receipt *gethTypes.Receipt, payload *types.ExecutionPayload)

// SolverCallback returns alternative routes given a retry context.
type SolverCallback func(ctx RetryContext) []*types.RouteCandidate

// RebuildCalldataFunc rebuilds calldata and updates payload fields (amount, minProfit, etc.)
// for a given candidate. Returns an error if rebuilding fails.
type RebuildCalldataFunc func(payload *types.ExecutionPayload, cand *types.RouteCandidate) error

// ReleasePayloadFunc is called when a payload is abandoned (never confirmed) so it can be recycled.
type ReleasePayloadFunc func(payload *types.ExecutionPayload)

// RollbackNonceFunc is called to roll back the nonce when a transaction is abandoned.
type RollbackNonceFunc func()

// SyncNonceFunc is called to resynchronise the local nonce from the node.
type SyncNonceFunc func() error

// GetNonceFunc returns the next nonce from the local tracker.
type GetNonceFunc func() uint64

// Sender manages transaction signing, broadcasting, and retry logic.
type Sender struct {
	privateKey  *ecdsa.PrivateKey
	address     common.Address
	chainID     *big.Int
	ethClient   *ethclient.Client
	rpcClient   *rpc.Client

	pendingMu   sync.Mutex
	pending     map[uint64]*PendingTx

	solverCallback    SolverCallback
	simulateFunc      func(cand *types.RouteCandidate, payload *types.ExecutionPayload) (bool, uint64, error)
	confCallback      ConfirmationCallback
	rebuildFunc       RebuildCalldataFunc
	releasePayload    ReleasePayloadFunc
	rollbackNonce     RollbackNonceFunc
	syncNonce         SyncNonceFunc
	getNonce          GetNonceFunc // new: used to get a fresh nonce after resync

	// Retry configuration
	replaceTimeout   time.Duration
	bumpMultiplier   float64
	maxAttempts      int
	confirmTimeout   time.Duration
	maxProfitBumpPct float64

	ctx    context.Context
	cancel context.CancelFunc
	ethPriceUSD atomic.Value

	// Background monitor
	monitorDone chan struct{}
	monitorOnce sync.Once
}

// NewSender creates a new Sender.
func NewSender(rpcURL, privateKeyHex string) (*Sender, error) {
	privateKey, err := crypto.HexToECDSA(privateKeyHex)
	if err != nil {
		return nil, err
	}
	address := crypto.PubkeyToAddress(privateKey.PublicKey)

	transport := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   500 * time.Millisecond,
			KeepAlive: 30 * time.Second,
			DualStack: true,
		}).DialContext,
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: false,
			MinVersion:         tls.VersionTLS12,
		},
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   1 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		DisableKeepAlives:     false,
		ForceAttemptHTTP2:     true,
	}
	if err := http2.ConfigureTransport(transport); err != nil {
		return nil, fmt.Errorf("failed to configure HTTP/2: %w", err)
	}

	httpClient := &http.Client{
		Transport: transport,
		Timeout:   3 * time.Second,
	}

	rpcClient, err := rpc.DialHTTPWithClient(rpcURL, httpClient)
	if err != nil {
		return nil, fmt.Errorf("failed to dial RPC with custom client: %w", err)
	}
	ethClient := ethclient.NewClient(rpcClient)

	idCtx, idCancel := context.WithTimeout(context.Background(), 3*time.Second)
	chainID, err := ethClient.ChainID(idCtx)
	idCancel()
	if err != nil {
		ethClient.Close()
		return nil, fmt.Errorf("fetch chain ID: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	s := &Sender{
		privateKey:      privateKey,
		address:         address,
		chainID:         chainID,
		ethClient:       ethClient,
		rpcClient:       rpcClient,
		pending:         make(map[uint64]*PendingTx),
		ctx:             ctx,
		cancel:          cancel,
		replaceTimeout:  400 * time.Millisecond,
		bumpMultiplier:  1.5,
		maxAttempts:     5,
		confirmTimeout:  8 * time.Second,
		maxProfitBumpPct: 0.3,
		monitorDone:     make(chan struct{}),
	}
	s.startMonitor()
	return s, nil
}

// SetEthPrice sets the current ETH/USD price (thread-safe).
func (s *Sender) SetEthPrice(price float64) {
	if price > 0 {
		s.ethPriceUSD.Store(price)
	}
}

// getEthPrice returns the stored ETH price or falls back to 3000.
func (s *Sender) getEthPrice() float64 {
	if v := s.ethPriceUSD.Load(); v != nil {
		return v.(float64)
	}
	return 3000.0
}

// SetReleasePayloadFunc sets the callback for abandoned payloads.
func (s *Sender) SetReleasePayloadFunc(fn ReleasePayloadFunc) {
	s.releasePayload = fn
}

// SetRollbackNonceFunc sets the callback for rolling back a nonce on failure.
func (s *Sender) SetRollbackNonceFunc(fn RollbackNonceFunc) {
	s.rollbackNonce = fn
}

// SetSyncNonceFunc sets the callback for synchronising the nonce from the node.
func (s *Sender) SetSyncNonceFunc(fn SyncNonceFunc) {
	s.syncNonce = fn
}

// SetGetNonceFunc sets the callback for obtaining a fresh nonce.
func (s *Sender) SetGetNonceFunc(fn GetNonceFunc) {
	s.getNonce = fn
}

// SetReplaceConfig stores the replacement configuration.
func (s *Sender) SetReplaceConfig(timeout time.Duration, multiplier float64, max int) {
	s.replaceTimeout = timeout
	s.bumpMultiplier = multiplier
	s.maxAttempts = max
}

// SetSolverCallback sets the callback for alternative routes.
func (s *Sender) SetSolverCallback(cb SolverCallback) {
	s.solverCallback = cb
}

// SetConfirmationCallback sets the callback for confirmed transactions.
func (s *Sender) SetConfirmationCallback(cb ConfirmationCallback) {
	s.confCallback = cb
}

// SetSimulateFunc sets the simulation function (used for candidate validation during retry).
func (s *Sender) SetSimulateFunc(fn func(cand *types.RouteCandidate, payload *types.ExecutionPayload) (bool, uint64, error)) {
	s.simulateFunc = fn
}

// SetRebuildFunc sets the function to rebuild calldata for a new candidate.
func (s *Sender) SetRebuildFunc(fn RebuildCalldataFunc) {
	s.rebuildFunc = fn
}

// GetDynamicPriorityFee returns the current suggested priority fee (in wei) from the node.
// If RPC returns 0, falls back to baseFee * 0.1 (S7).
func (s *Sender) GetDynamicPriorityFee(ctx context.Context) (uint64, error) {
	var result string
	err := s.rpcClient.CallContext(ctx, &result, "eth_maxPriorityFeePerGas")
	if err != nil {
		return 0, err
	}
	if strings.HasPrefix(result, "0x") {
		result = result[2:]
	}
	fee := new(big.Int)
	fee.SetString(result, 16)
	if fee.Sign() == 0 {
		baseFee, err := s.getBaseFee(ctx)
		if err != nil {
			return 0, err
		}
		fee = new(big.Int).Div(baseFee, big.NewInt(10))
	}
	return fee.Uint64(), nil
}

// getBaseFee fetches the current base fee from the latest block header.
func (s *Sender) getBaseFee(ctx context.Context) (*big.Int, error) {
	header, err := s.ethClient.HeaderByNumber(ctx, nil)
	if err != nil {
		return nil, err
	}
	return header.BaseFee, nil
}

// PrepareAndSignTransaction signs a transaction synchronously with dynamic gas fee cap.
// If payload.GasFeeCap is set (non-zero), it uses that value; otherwise it computes from base fee.
func (s *Sender) PrepareAndSignTransaction(payload *types.ExecutionPayload) ([]byte, common.Hash, error) {
	ctx, cancel := context.WithTimeout(s.ctx, 1*time.Second)
	defer cancel()

	baseFee, err := s.getBaseFee(ctx)
	if err != nil {
		return nil, common.Hash{}, fmt.Errorf("failed to get base fee: %w", err)
	}
	gasTip := new(big.Int).SetUint64(payload.PriorityFeeWei)

	var gasFee *big.Int
	if payload.GasFeeCap > 0 {
		gasFee = new(big.Int).SetUint64(payload.GasFeeCap)
	} else {
		gasFee = new(big.Int).Mul(baseFee, big.NewInt(2))
		gasFee.Add(gasFee, gasTip)
	}
	payload.GasFeeCap = gasFee.Uint64()

	to := payload.TargetExecutor
	value := big.NewInt(0)

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
		return nil, common.Hash{}, fmt.Errorf("failed to sign: %w", err)
	}

	rawTxBytes, err := signedTx.MarshalBinary()
	if err != nil {
		return nil, common.Hash{}, fmt.Errorf("failed to encode: %w", err)
	}

	return rawTxBytes, signedTx.Hash(), nil
}

// BroadcastRawTransactionBytes sends a raw transaction once and registers it.
// It does NOT start a background monitor. The caller is responsible for confirmation.
func (s *Sender) BroadcastRawTransactionBytes(rawTx []byte, payload *types.ExecutionPayload) error {
	var signedTx gethTypes.Transaction
	if err := signedTx.UnmarshalBinary(rawTx); err != nil {
		return fmt.Errorf("failed to unmarshal: %w", err)
	}

	nonce := signedTx.Nonce()
	txHash := signedTx.Hash()

	ctx, cancel := context.WithTimeout(s.ctx, 2*time.Second)
	defer cancel()
	err := s.ethClient.SendTransaction(ctx, &signedTx)
	if err != nil {
		// If it's "already known", we still return nil and let the caller handle it,
		// because we will use the pending map's hash for confirmation.
		if strings.Contains(err.Error(), "already known") {
			// Register the transaction with the hash we just computed.
			s.pendingMu.Lock()
			defer s.pendingMu.Unlock()
			// Check if we already have a pending tx with this nonce; if so, keep its hash.
			if _, ok := s.pending[nonce]; ok {
    return nil
}
			s.pending[nonce] = &PendingTx{
				Nonce:   nonce,
				Hash:    txHash,
				SentAt:  time.Now(),
				Payload: payload,
				Checked: false,
			}
			return nil
		}
		return err
	}

	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	s.pending[nonce] = &PendingTx{
		Nonce:   nonce,
		Hash:    txHash,
		SentAt:  time.Now(),
		Payload: payload,
		Checked: false,
	}
	return nil
}

// BroadcastWithRetry sends a transaction and waits for confirmation with retry/replacement.
// It is the sole owner of the transaction lifecycle and handles nonce rollback on failure.
// Uses a defer with a success flag to avoid double cleanup.
func (s *Sender) BroadcastWithRetry(payload *types.ExecutionPayload, retryCtx RetryContext) error {
	nonce := payload.Nonce

	var success bool
	var sent bool
	var synced bool
	var lastErr error

defer func() {
    if success {
        return
    }
    s.pendingMu.Lock()
    delete(s.pending, nonce)
    s.pendingMu.Unlock()

    // Only roll back if we never sent AND we never synced
    if !sent && !synced && s.rollbackNonce != nil {
        s.rollbackNonce()
        log.Printf("[Sender] Rolled back nonce %d (no tx sent, no sync)", nonce)
    } else if !sent && synced {
        log.Printf("[Sender] Nonce %d NOT rolled back (synced from node)", nonce)
    } else {
        log.Printf("[Sender] Nonce %d NOT rolled back (tx was broadcast)", nonce)
    }

    if s.releasePayload != nil {
        s.releasePayload(payload)
    }
}()

	for attempt := 0; attempt < s.maxAttempts; attempt++ {
		select {
		case <-s.ctx.Done():
			return fmt.Errorf("context cancelled")
		default:
		}

		if attempt > 0 {
			timer := time.NewTimer(s.replaceTimeout)
			select {
			case <-timer.C:
				timer.Stop()
			case <-s.ctx.Done():
				timer.Stop()
				return fmt.Errorf("context cancelled during retry")
			}
		}

		// Sign transaction (use cached signed tx if available).
		rawTx, txHash, err := s.PrepareAndSignTransaction(payload)
		if err != nil {
			lastErr = fmt.Errorf("signing failed: %w", err)
			continue
		}

		logPrefix := fmt.Sprintf("[Sender] Attempt %d/%d", attempt+1, s.maxAttempts)

		if err := s.BroadcastRawTransactionBytes(rawTx, payload); err != nil {
			msg := err.Error()
		if strings.Contains(msg, "nonce too low") {
    if s.syncNonce != nil {
        if syncErr := s.syncNonce(); syncErr == nil && s.getNonce != nil {
        // Clean up any stale pending entry for the old nonce
            s.pendingMu.Lock()
            delete(s.pending, nonce)
            s.pendingMu.Unlock()
            
            newNonce := s.getNonce()
            payload.Nonce = newNonce
            nonce = newNonce 
            synced = true
            log.Printf("%s: Nonce too low, resynced to %d, retrying", logPrefix, newNonce)
            continue
        }
    }
    lastErr = fmt.Errorf("unrecoverable nonce error: %w", err)
    break
} else if strings.Contains(msg, "already known") {
				// Transaction already in mempool; use the pending tx hash.
				s.pendingMu.Lock()
				if p, ok := s.pending[nonce]; ok {
					txHash = p.Hash // use original hash
				}
				s.pendingMu.Unlock()
				sent = true
				// Fall through to confirmation loop.
			} else {
				lastErr = err
				continue
			}
		} else {
			sent = true
		}

		// ---- Wait for confirmation ----
		timeoutC := time.After(s.confirmTimeout)
		confirmed := false
		var receipt *gethTypes.Receipt
	waitLoop:
		for {
			select {
			case <-timeoutC:
				confirmed = false
				break waitLoop
			case <-s.ctx.Done():
				return fmt.Errorf("context cancelled while waiting for confirmation")
			default:
				ctxR, cancelR := context.WithTimeout(s.ctx, 100*time.Millisecond)
				receipt, _ = s.ethClient.TransactionReceipt(ctxR, txHash)
				cancelR()
				if receipt != nil {
					confirmed = true
					break waitLoop
				}
				time.Sleep(50 * time.Millisecond)
			}
		}

		if confirmed {
			s.pendingMu.Lock()
			delete(s.pending, nonce)
			s.pendingMu.Unlock()

			if s.confCallback != nil && payload != nil {
				s.confCallback(nonce, txHash, receipt, payload)
			}
			success = true
			return nil
		}

		// ---- Transaction is stuck – try to replace ----
		lastErr = fmt.Errorf("transaction stuck")

		if s.solverCallback != nil && s.rebuildFunc != nil && payload.OriginalCandidate != nil {
			altCandidates := s.solverCallback(retryCtx)
			if len(altCandidates) > 0 {
				newCand := altCandidates[0]
				if err := s.rebuildFunc(payload, newCand); err == nil {
					log.Printf("%s: Using alternative route: %s", logPrefix, payload.RouteDesc)
					retryCtx.Pools = newCand.Pools[:newCand.Hops]
					retryCtx.OriginalCandidate = newCand
					continue
				}
			}
		}

		// Bump gas
		oldFee := payload.PriorityFeeWei
		newFee := uint64(float64(oldFee) * s.bumpMultiplier)
		if newFee <= oldFee {
			newFee = oldFee + 1e9
		}
		oldCap := payload.GasFeeCap
		newCap := uint64(float64(oldCap) * s.bumpMultiplier)
		if newCap <= oldCap {
			newCap = oldCap + 1e9
		}
		expectedProfit := payload.MinProfitUSD
		gasUsed := payload.GasLimit
		ethPrice := s.getEthPrice()
		maxFeeWei := uint64((expectedProfit * 0.3 * 1e18) / (float64(gasUsed) * ethPrice))
		if maxFeeWei > 0 && newFee > maxFeeWei {
			newFee = maxFeeWei
			log.Printf("%s: Capped priority fee to %d wei (30%% of profit)", logPrefix, newFee)
		}
		payload.PriorityFeeWei = newFee
		payload.GasFeeCap = newCap
		log.Printf("%s: Bumping priority fee to %d wei, fee cap to %d wei", logPrefix, newFee, newCap)
	}

	return fmt.Errorf("broadcast failed after %d attempts: %w", s.maxAttempts, lastErr)
}

// ConfirmNonce removes a nonce from pending map.
func (s *Sender) ConfirmNonce(nonce uint64) {
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	delete(s.pending, nonce)
}

// startMonitor launches the background goroutine that checks for confirmations.
func (s *Sender) startMonitor() {
	s.monitorOnce.Do(func() {
		go func() {
			ticker := time.NewTicker(500 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-s.monitorDone:
					return
				case <-ticker.C:
					s.checkPendingConfirmations()
				}
			}
		}()
	})
}

// checkPendingConfirmations iterates over pending transactions and checks their receipt.
func (s *Sender) checkPendingConfirmations() {
	s.pendingMu.Lock()
	pendingSnapshot := make([]*PendingTx, 0, len(s.pending))
	for _, p := range s.pending {
		if p.Checked {
			continue
		}
		pendingSnapshot = append(pendingSnapshot, p)
	}
	s.pendingMu.Unlock()

	for _, p := range pendingSnapshot {
		s.pendingMu.Lock()
		p.Checked = true
		s.pendingMu.Unlock()

		ctx, cancel := context.WithTimeout(s.ctx, 1*time.Second)
		receipt, err := s.ethClient.TransactionReceipt(ctx, p.Hash)
		cancel()
		if err != nil {
			s.pendingMu.Lock()
			p.Checked = false
			s.pendingMu.Unlock()
			continue
		}
		s.pendingMu.Lock()
		delete(s.pending, p.Nonce)
		s.pendingMu.Unlock()

		if s.confCallback != nil && p.Payload != nil {
			s.confCallback(p.Nonce, p.Hash, receipt, p.Payload)
		}
	}
}

// Close releases resources and stops the monitor.
func (s *Sender) Close() {
	s.cancel()
	close(s.monitorDone)
	if s.ethClient != nil {
		s.ethClient.Close()
	}
}
