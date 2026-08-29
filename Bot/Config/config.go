// Package config provides configuration loading and validation for the Base L2 Flashblock Arbitrage Bot.
package config

import (
	"fmt"
	"math"
	"net/url"
	"os"
	"strconv"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/joho/godotenv"
)

// Config holds all runtime configuration parameters.
type Config struct {
	PrivateKey         string         // Raw hex private key (64 chars, with or without 0x prefix)
	BaseWSRPC          string         // WebSocket RPC endpoint for Base (e.g., wss://...)
	BaseHTTPRPC        string         // HTTP RPC endpoint for Base (e.g., https://...)
	BaseExecRPC        string         // Dedicated execution RPC endpoint for transaction submission (required)
	BaseSequencerRPC   string         // Optional direct sequencer RPC endpoint for improved inclusion (currently reserved for future use)
	AnvilRPC           string         // Optional local Anvil fork RPC endpoint for simulation
	ExecutorAddress    common.Address // Address of the deployed FlashArbExecutor contract
	MinProfitUSD       float64        // Minimum net profit in USD required to execute a trade
	MaxPriorityFeeGwei float64        // Maximum priority fee in Gwei (gas tip cap)

	// Loan provider selection
	LoanProvider    string // "BALANCER", "DODO", or "AUTO"
	DODOPoolAddress string // Address of the DODO pool to use (required if LoanProvider is DODO or AUTO)

	// Transaction replacement settings
	ReplaceTimeoutMs   int     // Timeout in milliseconds before attempting replacement
	ReplaceBumpPercent int     // Percentage to bump priority fee on replacement (e.g., 50 = 50% increase)
	MaxReplaceAttempts int     // Maximum number of replacement attempts

	// CPU optimization settings
	EnableCPUPinning bool   // Enable CPU core pinning for workers
	WorkerCoreIDs    []int  // Core IDs for workers (must have at least 3 distinct cores if pinning is enabled)

	// ===== NEW: Bonding curve arbitrage =====
	BondingExecutorAddress string  // Address of the BondingArbitrageExecutor contract
	BondingCoreID          int     // Dedicated CPU core for bonding tracker (-1 = auto)
	BondingPollIntervalMs  int     // Polling interval in milliseconds for bonding curve checks (default 500)

	// ===== NEW: Speculative multiverse =====
	SpeculativeScenarios int     // Number of speculative scenarios to pre‑compute (default 5)
	MemoryThresholdGB    float64 // Heap memory threshold in GB to trigger manual GC (default 2.0)


	// ===== NEW: Liquidation sniper =====
	LiquidationEnabled         bool
	AavePoolAddress            string
	CompoundCometAddress       string
	MorphoBlueAddress          string
	ExactlyAuditorAddress      string
	MoonwellMTokenAddress      string
	IonicPoolAddress           string
	SwapRouterAddress          string
	LiquidationExecutorAddress string // FlashLiquidationExecutor contract
	LiquidationCoreID          int
	LiquidationPollIntervalMs  int
	LiquidationMinProfitUSD    float64
}

// LoadConfig loads environment variables from .env (if present) and validates them.
// Returns a populated Config struct or an error if required fields are missing or invalid.
func LoadConfig() (*Config, error) {
	_ = godotenv.Load()

	// Read insecure RPC opt‑in.
	insecureAllowed := os.Getenv("INSECURE_RPC_ALLOWED") == "true"

	// --- Required fields ---
	privateKey := os.Getenv("PRIVATE_KEY")
	if privateKey == "" {
		return nil, fmt.Errorf("PRIVATE_KEY is required")
	}
	privateKey = strings.TrimPrefix(privateKey, "0x")
	privateKey = strings.TrimPrefix(privateKey, "0X")
	if len(privateKey) != 64 {
		return nil, fmt.Errorf("PRIVATE_KEY must be 64 hex characters (got %d)", len(privateKey))
	}
	for i, c := range privateKey {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
			return nil, fmt.Errorf("PRIVATE_KEY contains an invalid hex character at position %d", i)
		}
	}

	baseWSRPC := os.Getenv("BASE_WS_RPC")
	if baseWSRPC == "" {
		return nil, fmt.Errorf("BASE_WS_RPC is required")
	}
	if err := validateURL(baseWSRPC, "wss", insecureAllowed); err != nil {
		return nil, fmt.Errorf("invalid BASE_WS_RPC: %w", err)
	}

	baseHTTPRPC := os.Getenv("BASE_HTTP_RPC")
	if baseHTTPRPC == "" {
		return nil, fmt.Errorf("BASE_HTTP_RPC is required")
	}
	if err := validateURL(baseHTTPRPC, "https", insecureAllowed); err != nil {
		return nil, fmt.Errorf("invalid BASE_HTTP_RPC: %w", err)
	}

	baseExecRPC := os.Getenv("BASE_EXEC_RPC")
	if baseExecRPC == "" {
		return nil, fmt.Errorf("BASE_EXEC_RPC is required; please set a dedicated execution RPC endpoint")
	}
	if err := validateURL(baseExecRPC, "https", insecureAllowed); err != nil {
		return nil, fmt.Errorf("invalid BASE_EXEC_RPC: %w", err)
	}

	baseSequencerRPC := os.Getenv("BASE_SEQUENCER_RPC") // optional, not currently used
	anvilRPC := os.Getenv("ANVIL_RPC")                  // optional

	executorAddrStr := os.Getenv("FLASH_EXECUTOR_ADDRESS")
	if executorAddrStr == "" {
		return nil, fmt.Errorf("FLASH_EXECUTOR_ADDRESS is required")
	}
	if !common.IsHexAddress(executorAddrStr) {
		return nil, fmt.Errorf("FLASH_EXECUTOR_ADDRESS is not a valid Ethereum address: %s", executorAddrStr)
	}
	executorAddress := common.HexToAddress(executorAddrStr)
	if executorAddress == (common.Address{}) {
		return nil, fmt.Errorf("FLASH_EXECUTOR_ADDRESS must not be the zero address")
	}

	// --- Loan provider configuration ---
	loanProvider := os.Getenv("LOAN_PROVIDER")
	if loanProvider == "" {
		loanProvider = "BALANCER" // default
	}
	loanProvider = strings.ToUpper(loanProvider)
	if loanProvider != "BALANCER" && loanProvider != "DODO" && loanProvider != "AUTO" {
		return nil, fmt.Errorf("LOAN_PROVIDER must be one of: BALANCER, DODO, AUTO")
	}

	dodoPoolAddress := os.Getenv("DODO_POOL_ADDRESS")
	if (loanProvider == "DODO" || loanProvider == "AUTO") && dodoPoolAddress == "" {
		return nil, fmt.Errorf("DODO_POOL_ADDRESS is required when LOAN_PROVIDER is DODO or AUTO")
	}
	if dodoPoolAddress != "" && !common.IsHexAddress(dodoPoolAddress) {
		return nil, fmt.Errorf("DODO_POOL_ADDRESS is not a valid Ethereum address: %s", dodoPoolAddress)
	}
	if dodoPoolAddress != "" {
		dodoAddr := common.HexToAddress(dodoPoolAddress)
		if dodoAddr == (common.Address{}) {
			return nil, fmt.Errorf("DODO_POOL_ADDRESS must not be the zero address")
		}
	}

	// --- Optional fields with defaults and sanity checks ---
	minProfitUSD := 3.0
	if val := os.Getenv("MIN_PROFIT_USD"); val != "" {
		if parsed, err := strconv.ParseFloat(val, 64); err == nil {
			if math.IsNaN(parsed) {
				return nil, fmt.Errorf("MIN_PROFIT_USD must be a finite number")
			}
			if parsed <= 0 {
				return nil, fmt.Errorf("MIN_PROFIT_USD must be > 0")
			}
			if parsed > 100000 {
				return nil, fmt.Errorf("MIN_PROFIT_USD is unrealistically high: %f (max 100000)", parsed)
			}
			minProfitUSD = parsed
		} else {
			return nil, fmt.Errorf("MIN_PROFIT_USD must be a float: %v", err)
		}
	}

	maxPriorityFeeGwei := 0.001
	if val := os.Getenv("MAX_PRIORITY_FEE_GWEI"); val != "" {
		if parsed, err := strconv.ParseFloat(val, 64); err == nil {
			if math.IsNaN(parsed) {
				return nil, fmt.Errorf("MAX_PRIORITY_FEE_GWEI must be a finite number")
			}
			if parsed < 0 {
				return nil, fmt.Errorf("MAX_PRIORITY_FEE_GWEI must be >= 0")
			}
			if parsed > 1000 {
				return nil, fmt.Errorf("MAX_PRIORITY_FEE_GWEI is too high: %f (max 1000)", parsed)
			}
			maxPriorityFeeGwei = parsed
		} else {
			return nil, fmt.Errorf("MAX_PRIORITY_FEE_GWEI must be a float: %v", err)
		}
	}

	// --- Transaction replacement settings ---
	replaceTimeoutMs := 400
	if val := os.Getenv("REPLACE_TIMEOUT_MS"); val != "" {
		if parsed, err := strconv.Atoi(val); err == nil {
			if parsed <= 0 {
				return nil, fmt.Errorf("REPLACE_TIMEOUT_MS must be > 0")
			}
			replaceTimeoutMs = parsed
		} else {
			return nil, fmt.Errorf("REPLACE_TIMEOUT_MS must be an integer: %v", err)
		}
	}

	replaceBumpPercent := 50
	if val := os.Getenv("REPLACE_BUMP_PERCENT"); val != "" {
		if parsed, err := strconv.Atoi(val); err == nil {
			if parsed <= 0 {
				return nil, fmt.Errorf("REPLACE_BUMP_PERCENT must be > 0")
			}
			if parsed > 1000 {
				return nil, fmt.Errorf("REPLACE_BUMP_PERCENT is too high: %d (max 1000)", parsed)
			}
			replaceBumpPercent = parsed
		} else {
			return nil, fmt.Errorf("REPLACE_BUMP_PERCENT must be an integer: %v", err)
		}
	}

	maxReplaceAttempts := 5
	if val := os.Getenv("MAX_REPLACE_ATTEMPTS"); val != "" {
		if parsed, err := strconv.Atoi(val); err == nil {
			if parsed < 0 {
				return nil, fmt.Errorf("MAX_REPLACE_ATTEMPTS must be >= 0")
			}
			if parsed > 100 {
				return nil, fmt.Errorf("MAX_REPLACE_ATTEMPTS is too high: %d (max 100)", parsed)
			}
			maxReplaceAttempts = parsed
		} else {
			return nil, fmt.Errorf("MAX_REPLACE_ATTEMPTS must be an integer: %v", err)
		}
	}

	// --- CPU optimization settings ---
	enableCPUPinning := false
	if val := os.Getenv("ENABLE_CPU_PINNING"); val != "" {
		if parsed, err := strconv.ParseBool(val); err == nil {
			enableCPUPinning = parsed
		} else {
			return nil, fmt.Errorf("ENABLE_CPU_PINNING must be a boolean: %v", err)
		}
	}

	var workerCoreIDs []int
	if val := os.Getenv("WORKER_CORE_IDS"); val != "" {
		parts := strings.Split(val, ",")
		for _, part := range parts {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			core, err := strconv.Atoi(part)
			if err != nil {
				return nil, fmt.Errorf("WORKER_CORE_IDS contains invalid value: %v", err)
			}
			if core < 0 {
				return nil, fmt.Errorf("WORKER_CORE_IDS must contain non-negative integers")
			}
			workerCoreIDs = append(workerCoreIDs, core)
		}
	}

	if enableCPUPinning {
		if len(workerCoreIDs) == 0 {
			workerCoreIDs = []int{2, 3, 4}
		}
		if len(workerCoreIDs) < 3 {
			return nil, fmt.Errorf("WORKER_CORE_IDS must contain at least 3 entries when ENABLE_CPU_PINNING is true (got %d)", len(workerCoreIDs))
		}
		if workerCoreIDs[0] == workerCoreIDs[1] || workerCoreIDs[0] == workerCoreIDs[2] || workerCoreIDs[1] == workerCoreIDs[2] {
			return nil, fmt.Errorf("the first three WORKER_CORE_IDS must be distinct when ENABLE_CPU_PINNING is true")
		}
		uniqueCores := make(map[int]struct{})
		for _, c := range workerCoreIDs {
			uniqueCores[c] = struct{}{}
		}
		if len(uniqueCores) < 3 {
			return nil, fmt.Errorf("WORKER_CORE_IDS must contain at least 3 distinct core IDs when ENABLE_CPU_PINNING is true")
		}
	}

	// ===== NEW: Bonding curve arbitrage settings =====
	bondingExecutorAddress := os.Getenv("BONDING_EXECUTOR_ADDRESS")
	if bondingExecutorAddress != "" && !common.IsHexAddress(bondingExecutorAddress) {
		return nil, fmt.Errorf("BONDING_EXECUTOR_ADDRESS is not a valid Ethereum address: %s", bondingExecutorAddress)
	}
	if bondingExecutorAddress != "" && common.HexToAddress(bondingExecutorAddress) == (common.Address{}) {
		return nil, fmt.Errorf("BONDING_EXECUTOR_ADDRESS must not be the zero address")
	}

	bondingCoreID := -1
	if val := os.Getenv("BONDING_CORE_ID"); val != "" {
		parsed, err := strconv.Atoi(val)
		if err != nil {
			return nil, fmt.Errorf("BONDING_CORE_ID must be an integer: %v", err)
		}
		if parsed < -1 {
			return nil, fmt.Errorf("BONDING_CORE_ID must be >= -1")
		}
		bondingCoreID = parsed
	}

	bondingPollIntervalMs := 500
	if val := os.Getenv("BONDING_POLL_INTERVAL_MS"); val != "" {
		parsed, err := strconv.Atoi(val)
		if err != nil {
			return nil, fmt.Errorf("BONDING_POLL_INTERVAL_MS must be an integer: %v", err)
		}
		if parsed <= 0 {
			return nil, fmt.Errorf("BONDING_POLL_INTERVAL_MS must be > 0")
		}
		bondingPollIntervalMs = parsed
	}

// ===== NEW: Liquidation sniper settings =====
liquidationEnabled := os.Getenv("LIQUIDATION_ENABLED") == "true"

aavePoolAddress := os.Getenv("AAVE_POOL_ADDRESS")
compoundCometAddress := os.Getenv("COMPOUND_COMET_ADDRESS")
morphoBlueAddress := os.Getenv("MORPHO_BLUE_ADDRESS")
exactlyAuditorAddress := os.Getenv("EXACTLY_AUDITOR_ADDRESS")
moonwellMTokenAddress := os.Getenv("MOONWELL_MTOKEN_ADDRESS")
ionicPoolAddress := os.Getenv("IONIC_POOL_ADDRESS")
swapRouterAddress := os.Getenv("SWAP_ROUTER_ADDRESS")
liquidationExecutorAddress := os.Getenv("LIQUIDATION_EXECUTOR_ADDRESS")

if liquidationEnabled {
	if aavePoolAddress == "" || compoundCometAddress == "" ||
		morphoBlueAddress == "" || exactlyAuditorAddress == "" ||
		moonwellMTokenAddress == "" || ionicPoolAddress == "" {
		return nil, fmt.Errorf("all protocol addresses are required when LIQUIDATION_ENABLED is true")
	}
	if swapRouterAddress == "" {
		return nil, fmt.Errorf("SWAP_ROUTER_ADDRESS is required for liquidation")
	}
	if liquidationExecutorAddress == "" {
		return nil, fmt.Errorf("LIQUIDATION_EXECUTOR_ADDRESS is required")
	}
	if !common.IsHexAddress(liquidationExecutorAddress) {
		return nil, fmt.Errorf("LIQUIDATION_EXECUTOR_ADDRESS is not valid")
	}
}

liquidationCoreID := -1
if val := os.Getenv("LIQUIDATION_CORE_ID"); val != "" {
	parsed, err := strconv.Atoi(val)
	if err != nil {
		return nil, fmt.Errorf("LIQUIDATION_CORE_ID must be an integer: %v", err)
	}
	if parsed < -1 {
		return nil, fmt.Errorf("LIQUIDATION_CORE_ID must be >= -1")
	}
	liquidationCoreID = parsed
}

liquidationPollIntervalMs := 5000
if val := os.Getenv("LIQUIDATION_POLL_INTERVAL_MS"); val != "" {
	parsed, err := strconv.Atoi(val)
	if err != nil {
		return nil, fmt.Errorf("LIQUIDATION_POLL_INTERVAL_MS must be an integer: %v", err)
	}
	if parsed <= 0 {
		return nil, fmt.Errorf("LIQUIDATION_POLL_INTERVAL_MS must be > 0")
	}
	liquidationPollIntervalMs = parsed
}

liquidationMinProfitUSD := 1.0
if val := os.Getenv("LIQUIDATION_MIN_PROFIT_USD"); val != "" {
	parsed, err := strconv.ParseFloat(val, 64)
	if err != nil {
		return nil, fmt.Errorf("LIQUIDATION_MIN_PROFIT_USD must be a float: %v", err)
	}
	if parsed <= 0 {
		return nil, fmt.Errorf("LIQUIDATION_MIN_PROFIT_USD must be > 0")
	}
	liquidationMinProfitUSD = parsed
}

	// ===== NEW: Speculative multiverse settings =====
	speculativeScenarios := 5
	if val := os.Getenv("SPECULATIVE_SCENARIOS"); val != "" {
		parsed, err := strconv.Atoi(val)
		if err != nil {
			return nil, fmt.Errorf("SPECULATIVE_SCENARIOS must be an integer: %v", err)
		}
		if parsed <= 0 || parsed > 20 {
			return nil, fmt.Errorf("SPECULATIVE_SCENARIOS must be between 1 and 20")
		}
		speculativeScenarios = parsed
	}

	memoryThresholdGB := 2.0
	if val := os.Getenv("MEMORY_THRESHOLD_GB"); val != "" {
		parsed, err := strconv.ParseFloat(val, 64)
		if err != nil {
			return nil, fmt.Errorf("MEMORY_THRESHOLD_GB must be a float: %v", err)
		}
		if parsed <= 0 {
			return nil, fmt.Errorf("MEMORY_THRESHOLD_GB must be > 0")
		}
		memoryThresholdGB = parsed
	}

	cfg := &Config{
		PrivateKey:          privateKey,
		BaseWSRPC:           baseWSRPC,
		BaseHTTPRPC:         baseHTTPRPC,
		BaseExecRPC:         baseExecRPC,
		BaseSequencerRPC:    baseSequencerRPC,
		AnvilRPC:            anvilRPC,
		ExecutorAddress:     executorAddress,
		MinProfitUSD:        minProfitUSD,
		MaxPriorityFeeGwei:  maxPriorityFeeGwei,
		LoanProvider:        loanProvider,
		DODOPoolAddress:     dodoPoolAddress,
		ReplaceTimeoutMs:    replaceTimeoutMs,
		ReplaceBumpPercent:  replaceBumpPercent,
		MaxReplaceAttempts:  maxReplaceAttempts,
		EnableCPUPinning:    enableCPUPinning,
		WorkerCoreIDs:       workerCoreIDs,

		// New fields
		BondingExecutorAddress: bondingExecutorAddress,
		BondingCoreID:          bondingCoreID,
		BondingPollIntervalMs:  bondingPollIntervalMs,
		SpeculativeScenarios:   speculativeScenarios,
		MemoryThresholdGB:      memoryThresholdGB,
		// Liquidation fields
	LiquidationEnabled:         liquidationEnabled,
	AavePoolAddress:            aavePoolAddress,
	CompoundCometAddress:       compoundCometAddress,
	MorphoBlueAddress:          morphoBlueAddress,
	ExactlyAuditorAddress:      exactlyAuditorAddress,
	MoonwellMTokenAddress:      moonwellMTokenAddress,
	IonicPoolAddress:           ionicPoolAddress,
	SwapRouterAddress:          swapRouterAddress,
	LiquidationExecutorAddress: liquidationExecutorAddress,
	LiquidationCoreID:          liquidationCoreID,
	LiquidationPollIntervalMs:  liquidationPollIntervalMs,
	LiquidationMinProfitUSD:    liquidationMinProfitUSD,
	}

	return cfg, nil
}

// validateURL parses the URL and enforces the expected scheme, unless insecure is allowed.
func validateURL(rawURL, expectedScheme string, insecureAllowed bool) error {
	u, err := url.Parse(rawURL)
	if err != nil {
		return fmt.Errorf("invalid URL: %w", err)
	}
	if u.Host == "" {
		return fmt.Errorf("missing host")
	}
	if insecureAllowed {
		return nil
	}
	if u.Scheme != expectedScheme {
		return fmt.Errorf("expected scheme %s, got %s", expectedScheme, u.Scheme)
	}
	return nil
}
