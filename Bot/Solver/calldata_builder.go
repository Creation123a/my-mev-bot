package solver

import (
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"my-mev-bot/types"
)

// NOTE: This ABI definition must exactly match the Solidity contract's
// `executeArbitrage` function signature and the `RouteData` struct.
// Any mismatch will cause all transactions to revert with "invalid route".
// Keep this in sync with the on‑chain contract.

// executeSelector is computed from the actual Solidity function signature:
// executeArbitrage(uint8,address,address,uint256,(address[],uint8[],bool[],uint256[],uint256),uint256,uint256)
var executeSelector [4]byte
var arbABI *abi.ABI

func init() {
	// Define the ABI for the executeArbitrage function
	parsed, err := abi.JSON(strings.NewReader(`[
		{
			"type": "function",
			"name": "executeArbitrage",
			"inputs": [
				{"name": "provider", "type": "uint8"},
				{"name": "loanPool", "type": "address"},
				{"name": "loanToken", "type": "address"},
				{"name": "loanAmount", "type": "uint256"},
				{
					"name": "route",
					"type": "tuple",
					"components": [
						{"name": "pools", "type": "address[]"},
						{"name": "dexTypes", "type": "uint8[]"},
						{"name": "zeroForOnes", "type": "bool[]"},
						{"name": "minOuts", "type": "uint256[]"},
						{"name": "amountIn", "type": "uint256"}
					]
				},
				{"name": "minProfitWei", "type": "uint256"},
				{"name": "deadline", "type": "uint256"}
			],
			"outputs": []
		}
	]`))
	if err != nil {
		panic(fmt.Sprintf("failed to parse ABI: %v", err))
	}
	arbABI = &parsed
	// Compute selector from the ABI
	hash := arbABI.Methods["executeArbitrage"].ID
	copy(executeSelector[:], hash[:4])
}

// BuildCalldata constructs the calldata for FlashArbExecutor.executeArbitrage
// using the actual ABI. Returns the calldata as a byte slice, or an error.
func BuildCalldata(
	cand *types.RouteCandidate,
	provider uint8,
	loanPool common.Address,
	loanToken common.Address,
	loanAmount *big.Int,
	minProfitWei *big.Int,
	deadline uint64,
) ([]byte, error) {
	if cand == nil {
		return nil, fmt.Errorf("candidate is nil")
	}
	n := int(cand.Hops)
	if n < 2 || n > 3 {
		return nil, fmt.Errorf("unsupported hop count: %d (must be 2 or 3)", n)
	}
	if cand.AmountIn == nil {
		return nil, fmt.Errorf("AmountIn is nil")
	}
	if loanAmount == nil {
		return nil, fmt.Errorf("loanAmount is nil")
	}
	if minProfitWei == nil {
		return nil, fmt.Errorf("minProfitWei is nil")
	}

	// Validate each hop's minOut and pool address.
	for i := 0; i < n; i++ {
		if cand.MinOuts[i] == nil {
			return nil, fmt.Errorf("MinOuts[%d] is nil", i)
		}
		if cand.Pools[i] == (common.Address{}) {
			return nil, fmt.Errorf("Pools[%d] is the zero address", i)
		}
	}

	// Validate deadline: must be in the future.
	if deadline <= uint64(time.Now().Unix()) {
		return nil, fmt.Errorf("deadline %d is not in the future", deadline)
	}

	// Convert dexTypes to []uint8
	dexTypes := make([]uint8, n)
	for i := 0; i < n; i++ {
		dexTypes[i] = uint8(cand.DexTypes[i])
	}

	// Convert zeroForOnes to []bool
	zeroForOnes := make([]bool, n)
	for i := 0; i < n; i++ {
		zeroForOnes[i] = cand.ZeroForOnes[i]
	}

	// Build route struct
	type RouteData struct {
		Pools        []common.Address
		DexTypes     []uint8
		ZeroForOnes  []bool
		MinOuts      []*big.Int
		AmountIn     *big.Int
	}
	route := RouteData{
		Pools:       cand.Pools[:n],
		DexTypes:    dexTypes,
		ZeroForOnes: zeroForOnes,
		MinOuts:     cand.MinOuts[:n],
		AmountIn:    cand.AmountIn,
	}

	// Prepare arguments
	args := []interface{}{
		provider,
		loanPool,
		loanToken,
		loanAmount,
		route,
		minProfitWei,
		new(big.Int).SetUint64(deadline),
	}

	// Pack using the ABI
	data, err := arbABI.Pack("executeArbitrage", args...)
	if err != nil {
		return nil, fmt.Errorf("ABI pack failed: %w", err)
	}
	// data already includes the 4-byte selector.
	return data, nil
}
