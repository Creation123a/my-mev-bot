package execution

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"

	"my-mev-bot/types"
)

// SimBackend defines the simulation backend type.
type SimBackend string

const (
	// SimBackendAnvil uses a local Anvil fork.
	SimBackendAnvil SimBackend = "anvil"
	// SimBackendRemote uses a remote eth_call RPC.
	SimBackendRemote SimBackend = "remote"
)

// GEVMSimulator uses remote eth_call or local Anvil to simulate contract execution.
type GEVMSimulator struct {
	httpClient *http.Client
	rpcURL     string // remote RPC URL
	owner      common.Address

	// Anvil settings – protected by healthMu.
	anvilRPCURL  string
	anvilHealthy bool
	healthMu     sync.RWMutex

	// Concurrency control for simulations (remote and anvil separate)
	remoteSem chan struct{}
	anvilSem  chan struct{}
}

// NewGEVMSimulator creates a simulator with a persistent HTTP client.
// The owner address is the EOA used as `from` in eth_call.
// If anvilURL is empty, Anvil is disabled.
func NewGEVMSimulator(rpcURL string, owner common.Address, anvilURL string) *GEVMSimulator {
	transport := &http.Transport{
		MaxIdleConns:        10,
		MaxIdleConnsPerHost: 10,
		IdleConnTimeout:     90 * time.Second,
		DisableKeepAlives:   false,
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
	}
}

// SetAnvilURL allows setting or updating the Anvil URL after creation.
// It is safe for concurrent use.
func (g *GEVMSimulator) SetAnvilURL(url string) {
	g.healthMu.Lock()
	defer g.healthMu.Unlock()
	g.anvilRPCURL = url
}

// getAnvilURL returns the current Anvil URL under read lock.
func (g *GEVMSimulator) getAnvilURL() string {
	g.healthMu.RLock()
	defer g.healthMu.RUnlock()
	return g.anvilRPCURL
}

// IsAnvilHealthy returns true if the Anvil fork is considered responsive and ready.
func (g *GEVMSimulator) IsAnvilHealthy() bool {
	g.healthMu.RLock()
	defer g.healthMu.RUnlock()
	return g.anvilHealthy && g.anvilRPCURL != ""
}

// HealthCheckAnvil pings the Anvil endpoint to check if it's responsive.
// It sets g.anvilHealthy accordingly.
func (g *GEVMSimulator) HealthCheckAnvil() {
	url := g.getAnvilURL()
	if url == "" {
		g.healthMu.Lock()
		g.anvilHealthy = false
		g.healthMu.Unlock()
		return
	}
	// Build a simple eth_blockNumber request.
	reqBody := map[string]interface{}{
		"jsonrpc": "2.0",
		"method":  "eth_blockNumber",
		"params":  []interface{}{},
		"id":      1,
	}
	reqJSON, err := json.Marshal(reqBody)
	if err != nil {
		g.healthMu.Lock()
		g.anvilHealthy = false
		g.healthMu.Unlock()
		return
	}
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
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		g.healthMu.Lock()
		g.anvilHealthy = false
		g.healthMu.Unlock()
		return
	}
	var rpcResp struct {
		Result string `json:"result"`
	}
	if err := json.Unmarshal(body, &rpcResp); err != nil {
		g.healthMu.Lock()
		g.anvilHealthy = false
		g.healthMu.Unlock()
		return
	}
	// If we got a result, consider healthy.
	g.healthMu.Lock()
	g.anvilHealthy = len(rpcResp.Result) > 0
	g.healthMu.Unlock()
}

// ChooseBackend selects the simulation backend based on candidate properties.
// This policy is aligned with the bot's competition-aware logic.
// Exported so callers outside the package can use it.
func (g *GEVMSimulator) ChooseBackend(cand *types.RouteCandidate) SimBackend {
	if cand == nil || !g.IsAnvilHealthy() {
		return SimBackendRemote
	}
	if cand.Competition >= 0.6 || cand.ExpectedProfitUSD >= 20.0 {
		return SimBackendAnvil
	}
	if cand.Competition <= 0.25 && cand.ExpectedProfitUSD < 5.0 {
		return SimBackendRemote
	}
	return SimBackendAnvil
}

// SimulateWithBackend runs a simulation using the selected backend.
// It returns success, gasUsed, and error.
// This function performs both eth_call (to check revert) and eth_estimateGas.
func (g *GEVMSimulator) SimulateWithBackend(
	cand *types.RouteCandidate,
	payload *types.ExecutionPayload,
	backend SimBackend,
) (bool, uint64, error) {
	if payload == nil || payload.Calldata == nil {
		return false, 0, errors.New("invalid payload")
	}

	// Acquire concurrency token based on backend.
	var sem chan struct{}
	switch backend {
	case SimBackendAnvil:
		sem = g.anvilSem
	case SimBackendRemote:
		sem = g.remoteSem
	default:
		return false, 0, errors.New("unknown backend")
	}
	select {
	case sem <- struct{}{}:
		defer func() { <-sem }()
	case <-time.After(500 * time.Millisecond):
		return false, 0, errors.New("simulation concurrency limit reached")
	}

	var targetURL string
	switch backend {
	case SimBackendAnvil:
		url := g.getAnvilURL()
		if url == "" {
			return false, 0, errors.New("anvil URL not set")
		}
		targetURL = url
	case SimBackendRemote:
		targetURL = g.rpcURL
	}

	// If Anvil is chosen but we suspect it's unhealthy, fallback to remote.
	if backend == SimBackendAnvil && !g.IsAnvilHealthy() {
		targetURL = g.rpcURL
	}

	// First, call eth_call to check for revert and get the result.
	success, _, err := g.doEthCall(targetURL, payload)
	if err != nil || !success {
		// If Anvil failed and we are using Anvil, retry once with remote.
		if backend == SimBackendAnvil && targetURL != g.rpcURL {
			return g.retryRemote(payload)
		}
		return success, 0, err
	}

	// If call succeeded (no revert), now estimate gas.
	gasUsed, err := g.estimateGas(targetURL, payload)
	if err != nil {
		// If Anvil failed to estimate, try remote.
		if backend == SimBackendAnvil && targetURL != g.rpcURL {
			return g.retryRemote(payload)
		}
		// Still consider the simulation a success, but return 0 gas and the error.
		// We'll treat estimate failure as a warning, but the tx might still go through.
		return true, 0, fmt.Errorf("gas estimation failed: %w", err)
	}

	return true, gasUsed, nil
}

// doEthCall performs an eth_call and returns true if the call succeeded (no revert).
// It also returns the raw result (not used) and any error.
// NOTE: Structured Solidity error decoding (custom error codes) is not implemented
// to avoid allocations; we rely on the JSON-RPC error message for revert detection.
// This is sufficient for the current bot logic.
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
			Data    string `json:"data,omitempty"` // may contain revert data
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &rpcResp); err != nil {
		return false, "", err
	}
	if rpcResp.Error.Code != 0 {
		// Check for revert reason.
		if strings.Contains(rpcResp.Error.Message, "execution reverted") {
			// We could attempt to decode rpcResp.Error.Data here, but we skip to avoid allocations.
			return false, "", fmt.Errorf("contract reverted: %s", rpcResp.Error.Message)
		}
		return false, "", fmt.Errorf("eth_call error: %s", rpcResp.Error.Message)
	}
	return true, rpcResp.Result, nil
}

// estimateGas calls eth_estimateGas for the given payload.
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
	// Result is hex string like "0x12345"
	gasHex := strings.TrimPrefix(rpcResp.Result, "0x")
	gas, err := hexutil.DecodeUint64("0x" + gasHex)
	if err != nil {
		return 0, fmt.Errorf("failed to parse gas: %w", err)
	}
	return gas, nil
}

// retryRemote is a helper to retry a simulation using the remote RPC.
// It returns success, gasUsed, error.
func (g *GEVMSimulator) retryRemote(payload *types.ExecutionPayload) (bool, uint64, error) {
	// Perform eth_call and estimateGas on remote.
	success, _, err := g.doEthCall(g.rpcURL, payload)
	if err != nil || !success {
		return false, 0, err
	}
	gasUsed, err := g.estimateGas(g.rpcURL, payload)
	if err != nil {
		// Still treat as success but gas unknown.
		return true, 0, fmt.Errorf("remote gas estimation failed: %w", err)
	}
	return true, gasUsed, nil
}

// Simulate executes an eth_call against the remote chain (default backend).
// It retains the original behavior for backward compatibility.
// Now it also returns gasUsed from estimation.
func (g *GEVMSimulator) Simulate(candidate *types.RouteCandidate, payload *types.ExecutionPayload) (bool, uint64, error) {
	return g.SimulateWithBackend(candidate, payload, SimBackendRemote)
}

// SimulateCandidate provides a simple wrapper for simulating a candidate.
// It chooses the backend using the exported ChooseBackend method.
func (g *GEVMSimulator) SimulateCandidate(cand *types.RouteCandidate, payload *types.ExecutionPayload) (bool, uint64, error) {
	backend := g.ChooseBackend(cand)
	return g.SimulateWithBackend(cand, payload, backend)
}