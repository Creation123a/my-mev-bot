// Package config provides static Base L2 contract addresses as immutable globals.
// All addresses are pre-decoded at init time to avoid runtime hex parsing.
package config

import (
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"my-mev-bot/Bot/Types"
)

// =============================================================================
// 4 Anchor Assets (Matrix Slots 0–3)
// =============================================================================

var (
	// WETHAddress is the native wrapped Ether on Base.
	WETHAddress = common.HexToAddress("0x4200000000000000000000000000000000000006")

	// CBBTCAddress is Coinbase Wrapped BTC on Base.
	CBBTCAddress = common.HexToAddress("0xcbB7C0000Ab88B473b1f5aFd9ef808440eed33Bf")

	// USDCAddress is the native Circle USDC on Base.
	USDCAddress = common.HexToAddress("0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913")

	// USDBCAddress is the bridged USDC from Ethereum (legacy liquidity).
	USDBCAddress = common.HexToAddress("0xd9aAEc86B65D86f6A7B5B1b0c42FFA531710b6CA")
)

// AnchorAssets returns the four canonical anchor asset addresses in order.
func AnchorAssets() [4]common.Address {
	return [4]common.Address{
		WETHAddress,
		CBBTCAddress,
		USDCAddress,
		USDBCAddress,
	}
}

// =============================================================================
// System Oracles & 0-Fee Flash Loan Vaults
// =============================================================================

var (
	// BaseL1BlockOracle is the Base L1 block oracle contract for L1 data fee calculation.
	BaseL1BlockOracle = common.HexToAddress("0x4200000000000000000000000000000000000015")

	// BalancerVault is the Balancer V2 Vault (0% flash loans).
	BalancerVault = common.HexToAddress("0xBA12222222228d8Ba445958a75a0704d566BF2C8")

	// DODOApproveProxy is the DODO Approve Proxy (used for DODO flash loans).
	// Correct address for Base mainnet.
	DODOApproveProxy = common.HexToAddress("0x6de4d882a84A98f4CCD5D33ea6b3C99A07BAbeB1")
)

// =============================================================================
// 4 Target DEX Routers & Factories
// =============================================================================

var (
	// UniswapV3Router is the Uniswap V3 SwapRouter02.
	UniswapV3Router = common.HexToAddress("0x2626664c2603336E57B271c5C0b26F421741e481")

	// PancakeV3Router is the PancakeSwap V3 router on Base.
	PancakeV3Router = common.HexToAddress("0x1b81D678ffb9C0263b24A97847620C99d213eB14")

	// AerodromeRouterV2 is the Aerodrome V2/Slipstream router.
	AerodromeRouterV2 = common.HexToAddress("0xcF77a3Ba9A5CA399B7c97c74d54e5b1Beb874E43")

	// AlienBaseRouterV2 is the AlienBase (BaseSwap compatible) router.
	// Correct address from official AlienBase V2 on Base.
	AlienBaseRouterV2 = common.HexToAddress("0x8c1A3cF8f83074169FE5D7aD50B978e1cD6b37c7")

	// DEX factories (used for pool validation and dynamic discovery).
	// NOTE: These addresses must be set to the correct mainnet values before production use.
	// If left as zero, the DEX type detection for V2 pools will default to Aerodrome V2.
	UniswapV3Factory = common.HexToAddress("0x33128a8fC17869897dcE68Ed026d694621f6FDfD")
	PancakeV3Factory = common.HexToAddress("0x0BFbCF9fa4f9C56B0F40a671Ad40E0805A091865")
	AerodromeFactory = common.HexToAddress("0x420DD3807Eae25ad71F4229994854528A4aBc3cd") // TODO: Replace with actual Aerodrome V2 factory address
	AlienBaseFactory = common.HexToAddress("0x420DD3807Eae25ad71F4229994854528A4aBc3cd") // TODO: Replace with actual AlienBase factory address
)

// =============================================================================
// DEX Type Mapping
// =============================================================================

// routerToDexType maps each router address to its corresponding types.DexType.
var routerToDexType = map[common.Address]types.DexType{
	UniswapV3Router:   types.DexUniswapV3,
	PancakeV3Router:   types.DexPancakeV3,
	AerodromeRouterV2: types.DexAerodromeV2,
	AlienBaseRouterV2: types.DexAlienBaseV2,
}

// GetDexType returns the DexType for a given router address and a boolean indicating
// whether the router is known. Callers must reject a route when ok is false.
func GetDexType(router common.Address) (types.DexType, bool) {
	t, ok := routerToDexType[router]
	return t, ok
}

// =============================================================================
// Factory Address Validation
// =============================================================================

// ValidateFactories checks that all factory addresses required for DEX detection
// are populated. It returns an error if any required factory is the zero address.
// Call this during startup before enabling dynamic pool discovery.
func ValidateFactories() error {
	for name, addr := range map[string]common.Address{
		"UniswapV3Factory": UniswapV3Factory,
		"PancakeV3Factory": PancakeV3Factory,
		"AerodromeFactory": AerodromeFactory,
		"AlienBaseFactory": AlienBaseFactory,
	} {
		if addr == (common.Address{}) {
			return fmt.Errorf("%s is not configured (zero address)", name)
		}
	}
	return nil
}
