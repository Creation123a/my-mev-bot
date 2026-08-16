// Package config provides static Base L2 contract addresses as immutable globals.
// All addresses are pre-decoded at init time to avoid runtime hex parsing.
package config

import (
	"github.com/ethereum/go-ethereum/common"
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

