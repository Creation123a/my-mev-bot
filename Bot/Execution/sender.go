// Package execution provides transaction signing, broadcasting, and replacement.
package execution

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	gethTypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"

	"my-mev-bot/Bot/Config"
	"my-mev-bot/Bot/Solver"
	"my-mev-bot/Bot/Types"
)

type PendingTx struct {
	Nonce            uint64
	Hash             common.Hash
	SentAt           time.Time
	Replacements     int
	GasTipCap        *big.Int
	GasFeeCap        *big.Int
	GasLimit         uint64
	To               common.Address
	Data             []byte
	Value            *big.Int
	CancellationSent bool
	Payload          *types.ExecutionPayload
	FailedPool       common.Address
}

type ConfirmationCallback func(nonce uint64, txHash common.Hash, receipt *gethTypes.Receipt, payload *types.ExecutionPayload)

type SolverCallback func(failedPool common.Address) []*types.RouteCandidate

type Sender struct {
	rpcURL     string
	execRPCURL string
	privateKey *ecdsa.PrivateKey
	address    common.Address
	chainID    *big.Int
	httpClient *http.Client
	ethClient  *ethclient.Client

	pendingMu sync.Mutex
	pending   map[uint64]*PendingTx

	replaceTimeout     time.Duration
	replaceMultiplier  float64
	maxReplacements    int

	solverCallback   SolverCallback
	confCallback     ConfirmationCallback
	simulateFunc     func(cand *types.RouteCandidate, payload *types.ExecutionPayload) (bool, uint64, error)

	pendingPayloads map[uint64]*types.ExecutionPayload

	ctx    context.Context
	cancel context.CancelFunc
}

// NewSender creates a new Sender instance.
func NewSender(rpcURL, execRPCURL, privateKeyHex string) (*Sender, error) {
	privateKey, err := crypto.HexToECDSA(privateKeyHex)
	if err != nil {
		return nil, err
	}
	address := crypto.PubkeyToAddress(privateKey.PublicKey)

	transport := &http.Transport{
		MaxIdleConns:        10,
		MaxIdleConnsPerHost: 10,
		IdleConnTimeout:     90 * time.Second,
		DisableKeepAlives:   false,
	}
	client := &http.Client{
		Transport: transport,
		Timeout:   5 * time.Second,
	}
	ethClient, err := ethclient.Dial(rpcURL)
	if err != nil {
		return nil, err
	}

	idCtx, idCancel := context.WithTimeout(context.Background(), 5*time.Second)
	chainID, err := ethClient.ChainID(idCtx)
	idCancel()
	if err != nil {
		ethClient.Close()
		return nil, fmt.Errorf("fetch chain ID: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	return &Sender{
		rpcURL:            rpcURL,
		execRPCURL:        execRPCURL,
		privateKey:        privateKey,
		address:           address,
		chainID:           chainID,
		httpClient:        client,
		ethClient:         ethClient,
		pending:           make(map[uint64]*PendingTx),
		replaceTimeout:    400 * time.Millisecond,
		replaceMultiplier: 1.5,
		maxReplacements:   5,
		pendingPayloads:   make(map[uint64]*types.ExecutionPayload),
		ctx:               ctx,
		cancel:            cancel,
	}, nil
}

// SetReplaceConfig configures replacement parameters.
func (s *Sender) SetReplaceConfig(timeout time.Duration, multiplier float64, max int) {
	s.replaceTimeout = timeout
	s.replaceMultiplier = multiplier
	s.maxReplacements = max
}

// SetSolverCallback sets the callback for re‑routing.
func (s *Sender) SetSolverCallback(cb SolverCallback) {
	s.solverCallback = cb
}

// SetConfirmationCallback sets the callback for confirmed transactions.
func (s *Sender) SetConfirmationCallback(cb ConfirmationCallback) {
	s.confCallback = cb
}

// SetSimulateFunc sets the simulation function for re‑routes.
func (s *Sender) SetSimulateFunc(fn func(cand *types.RouteCandidate, payload *types.ExecutionPayload) (bool, uint64, error)) {
	s.simulateFunc = fn
}

// RegisterPendingNonce stores the payload associated with a nonce for later confirmation callback.
func (s *Sender) RegisterPendingNonce(nonce uint64, payload *types.ExecutionPayload) {
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	s.pendingPayloads[nonce] = payload
}

// Close stops monitor goroutines and releases the RPC client.
func (s *Sender) Close() {
	s.cancel()
	if s.ethClient != nil {
		s.ethClient.Close()
	}
}

// SendRawTransaction signs and broadcasts a transaction.
func (s *Sender) SendRawTransaction(payload *types.ExecutionPayload) (common.Hash, error) {
	to := payload.TargetExecutor
	value := big.NewInt(0)

	gasTip, gasFee, err := s.getDynamicGas()
	if err != nil || gasTip == nil {
		gasTip = big.NewInt(int64(payload.PriorityFeeWei))
		gasFee = new(big.Int).Mul(gasTip, big.NewInt(2))
	} else {
		cfgTip := new(big.Int).SetUint64(payload.PriorityFeeWei)
		if gasTip.Cmp(cfgTip) < 0 {
			delta := new(big.Int).Sub(cfgTip, gasTip)
			gasTip.Set(cfgTip)
			gasFee.Add(gasFee, delta) // keep base-fee component and add extra tip
		}
	}

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
		return common.Hash{}, err
	}
	rawTx, err := signedTx.MarshalBinary()
	if err != nil {
		return common.Hash{}, err
	}
	txHash := signedTx.Hash()

	if err := s.sendRawTransactionBytes(rawTx); err != nil {
		return common.Hash{}, err
	}

	failedPool := common.Address{}
	if len(payload.RoutePools) > 0 {
		failedPool = payload.RoutePools[0]
	}

	s.pendingMu.Lock()
	s.pending[payload.Nonce] = &PendingTx{
		Nonce:            payload.Nonce,
		Hash:             txHash,
		SentAt:           time.Now(),
		Replacements:     0,
		GasTipCap:        gasTip,
		GasFeeCap:        gasFee,
		GasLimit:         payload.GasLimit,
		To:               to,
		Data:             payload.Calldata,
		Value:            value,
		CancellationSent: false,
		Payload:          payload,
		FailedPool:       failedPool,
	}
	s.pendingMu.Unlock()

	go s.monitorAndReplace(payload.Nonce)

	return txHash, nil
}

func (s *Sender) sendRawTransactionBytes(rawTx []byte) error {
	hexData := "0x" + hex.EncodeToString(rawTx)
	payload := []byte(`{"jsonrpc":"2.0","method":"eth_sendRawTransaction","params":["` + hexData + `"],"id":1}`)
	req, err := http.NewRequest("POST", s.execRPCURL, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	var body bytes.Buffer
	if _, err := body.ReadFrom(resp.Body); err != nil {
		return err
	}
	if resp.StatusCode != 200 {
		return errors.New("HTTP error: " + resp.Status)
	}

	var rpcResp struct {
		Result string `json:"result"`
		Error  struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body.Bytes(), &rpcResp); err != nil {
		return err
	}
	if rpcResp.Error.Code != 0 {
		return errors.New("JSON-RPC error: " + rpcResp.Error.Message)
	}
	return nil
}

// monitorAndReplace monitors a pending transaction and replaces it if stuck.
func (s *Sender) monitorAndReplace(nonce uint64) {
	ticker := time.NewTicker(s.replaceTimeout)
	defer ticker.Stop()

	attempts := 0

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			attempts++
			if attempts > s.maxReplacements*2 {
				s.pendingMu.Lock()
				delete(s.pending, nonce)
				delete(s.pendingPayloads, nonce)
				s.pendingMu.Unlock()
				return
			}

			s.pendingMu.Lock()
			pending, ok := s.pending[nonce]
			if !ok {
				s.pendingMu.Unlock()
				return
			}
			// If max replacements already reached, clean up.
			if pending.Replacements >= s.maxReplacements {
				delete(s.pending, nonce)
				delete(s.pendingPayloads, nonce)
				s.pendingMu.Unlock()
				return
			}

			hash := pending.Hash
			replacements := pending.Replacements
			gasTipCap := new(big.Int).Set(pending.GasTipCap)
			gasFeeCap := new(big.Int).Set(pending.GasFeeCap)
			gasLimit := pending.GasLimit
			to := pending.To
			data := append([]byte{}, pending.Data...)
			value := new(big.Int).Set(pending.Value)
			cancellationSent := pending.CancellationSent
			payload := pending.Payload
			failedPool := pending.FailedPool
			s.pendingMu.Unlock()

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			receipt, err := s.ethClient.TransactionReceipt(ctx, hash)
			cancel()
			if err == nil && receipt != nil && receipt.BlockNumber != nil {
				s.pendingMu.Lock()
				delete(s.pending, nonce)
				delete(s.pendingPayloads, nonce)
				s.pendingMu.Unlock()

				if s.confCallback != nil && payload != nil && receipt.Status == 1 {
					s.confCallback(nonce, hash, receipt, payload)
				}
				return
			}

			bumpFactor := new(big.Int).SetInt64(int64(s.replaceMultiplier * 100))
			bumpedTip := new(big.Int).Div(new(big.Int).Mul(gasTipCap, bumpFactor), big.NewInt(100))
			bumpedFee := new(big.Int).Div(new(big.Int).Mul(gasFeeCap, bumpFactor), big.NewInt(100))

			// Step 1: Attempt to reroute via solver callback, with simulation.
			rerouteSuccess := false
			if s.solverCallback != nil && payload != nil {
				alternates := s.solverCallback(failedPool)
				if len(alternates) > 0 {
					for _, cand := range alternates {
						var minProfitWei *big.Int
						if payload.MinProfitWei != nil {
							minProfitWei = payload.MinProfitWei
						} else {
							minProfitWei = big.NewInt(0)
						}
						if cand.NetProfitWei != nil && cand.NetProfitWei.Sign() > 0 {
							minProfitWei = cand.NetProfitWei
						}
						newPayload := &types.ExecutionPayload{
							TargetExecutor: to,
							LoanProvider:   payload.LoanProvider,
							LoanPool:       payload.LoanPool,
							BorrowedToken:  cand.Tokens[0],
							BorrowedAmount: cand.AmountIn,
							Calldata:       nil,
							Nonce:          nonce,
							GasLimit:       gasLimit,
							PriorityFeeWei: uint64(bumpedTip.Int64()),
							MinProfitUSD:   cand.ExpectedProfitUSD,
							MinProfitWei:   minProfitWei,
							RoutePools:     cand.Pools[:cand.Hops],
						}
						calldata, err := solver.BuildCalldata(
							cand,
							newPayload.LoanProvider,
							newPayload.LoanPool,
							newPayload.BorrowedToken,
							newPayload.BorrowedAmount,
							minProfitWei,
							uint64(time.Now().Unix()+120),
						)
						if err != nil {
							continue
						}
						newPayload.Calldata = calldata

						// --- FIX: Simulate before sending ---
						if s.simulateFunc != nil {
							ok, gasUsed, err := s.simulateFunc(cand, newPayload)
							if err != nil || !ok {
								continue
							}
							newGasLimit := gasUsed + gasUsed/5
							if newGasLimit < 21000 {
								newGasLimit = 21000
							}
							newPayload.GasLimit = newGasLimit
						}

						newHash, err := s.sendReplacement(newPayload, bumpedTip, bumpedFee)
						if err == nil {
							s.pendingMu.Lock()
							if p, ok := s.pending[nonce]; ok {
								p.Hash = newHash
								p.Data = calldata
								p.Replacements++
								p.SentAt = time.Now()
								p.CancellationSent = false
								p.Payload = newPayload
								p.GasLimit = newPayload.GasLimit
								// Update FailedPool to the first pool of the new route.
								if cand.Hops > 0 {
									p.FailedPool = cand.Pools[0]
								} else {
									p.FailedPool = common.Address{}
								}
								// --- FIX: Update gas caps ---
								p.GasTipCap = new(big.Int).Set(bumpedTip)
								p.GasFeeCap = new(big.Int).Set(bumpedFee)
							}
							s.pendingMu.Unlock()
							rerouteSuccess = true
							break
						}
					}
				}
			}

			// Step 2: If no reroute succeeded, attempt cancellation (which bumps gas).
			if !rerouteSuccess && !cancellationSent {
				if err := s.sendCancellation(nonce, bumpedTip, bumpedFee, 21000, s.address, big.NewInt(0), []byte{}); err == nil {
					// Persist the cancellation flag and updated gas caps.
					s.pendingMu.Lock()
					if p, ok := s.pending[nonce]; ok {
						p.CancellationSent = true
						p.GasTipCap = new(big.Int).Set(bumpedTip)
						p.GasFeeCap = new(big.Int).Set(bumpedFee)
						p.Replacements++
						p.SentAt = time.Now()
					}
					s.pendingMu.Unlock()
					cancellationSent = true
				}
			}

			// Step 3: If cancellation failed, try a simple gas bump on the original transaction.
			if !rerouteSuccess && !cancellationSent {
				tx := gethTypes.NewTx(&gethTypes.DynamicFeeTx{
					ChainID:    s.chainID,
					Nonce:      nonce,
					GasTipCap:  bumpedTip,
					GasFeeCap:  bumpedFee,
					Gas:        gasLimit,
					To:         &to,
					Value:      value,
					Data:       data,
					AccessList: gethTypes.AccessList{},
				})
				signer := gethTypes.LatestSignerForChainID(s.chainID)
				signedTx, err := gethTypes.SignTx(tx, signer, s.privateKey)
				if err == nil {
					rawTx, _ := signedTx.MarshalBinary()
					if err := s.sendRawTransactionBytes(rawTx); err == nil {
						s.pendingMu.Lock()
						if p, ok := s.pending[nonce]; ok {
							p.Hash = signedTx.Hash()
							p.GasTipCap = new(big.Int).Set(bumpedTip)
							p.GasFeeCap = new(big.Int).Set(bumpedFee)
							p.Replacements++
							p.SentAt = time.Now()
						}
						s.pendingMu.Unlock()
					}
				}
			}
		}
	}
}

// sendCancellation sends a 0‑value self‑transfer with bumped gas.
func (s *Sender) sendCancellation(nonce uint64, gasTipCap, gasFeeCap *big.Int, gasLimit uint64, to common.Address, value *big.Int, data []byte) error {
	tx := gethTypes.NewTx(&gethTypes.DynamicFeeTx{
		ChainID:    s.chainID,
		Nonce:      nonce,
		GasTipCap:  gasTipCap,
		GasFeeCap:  gasFeeCap,
		Gas:        gasLimit,
		To:         &to,
		Value:      value,
		Data:       data,
		AccessList: gethTypes.AccessList{},
	})
	signer := gethTypes.LatestSignerForChainID(s.chainID)
	signedTx, err := gethTypes.SignTx(tx, signer, s.privateKey)
	if err != nil {
		return err
	}
	rawTx, err := signedTx.MarshalBinary()
	if err != nil {
		return err
	}
	return s.sendRawTransactionBytes(rawTx)
}

// sendReplacement sends a replacement transaction with explicit gas parameters.
func (s *Sender) sendReplacement(payload *types.ExecutionPayload, gasTip, gasFee *big.Int) (common.Hash, error) {
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
		return common.Hash{}, err
	}
	rawTx, err := signedTx.MarshalBinary()
	if err != nil {
		return common.Hash{}, err
	}
	if err := s.sendRawTransactionBytes(rawTx); err != nil {
		return common.Hash{}, err
	}
	return signedTx.Hash(), nil
}

// ConfirmNonce removes a nonce from the pending map.
func (s *Sender) ConfirmNonce(nonce uint64) {
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	delete(s.pending, nonce)
	delete(s.pendingPayloads, nonce)
}

// getDynamicGas fetches suggested gas price and derives tip/fee with a timeout.
func (s *Sender) getDynamicGas() (*big.Int, *big.Int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	gasPrice, err := s.ethClient.SuggestGasPrice(ctx)
	if err != nil {
		return nil, nil, err
	}
	tip := new(big.Int).Div(gasPrice, big.NewInt(10))
	if tip.Sign() < 0 {
		tip = big.NewInt(0)
	}
	feeCap := new(big.Int).Add(gasPrice, tip)
	return tip, feeCap, nil
}
