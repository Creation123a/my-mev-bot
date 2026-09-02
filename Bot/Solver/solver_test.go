package solver

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	gethTypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient/simulated"

	"my-mev-bot/Bot/Config"
	"my-mev-bot/Bot/State"
	"my-mev-bot/Bot/Types"
)


func TestDEXArbitragePipeline(t *testing.T) {
	// 1. Create a minimal matrix with one pool
	matrix := state.NewMatrix()

	// Parse big numbers using string representation to fix untyped float constant errors
	reserve0, _ := new(big.Int).SetString("100000000000000000000000", 10) // 100_000e18
	reserve1, _ := new(big.Int).SetString("300000000000000", 10)         // 300_000_000e6

	pool := &types.PoolState{ // Fixed type lookup context from state package
		PoolAddress: common.HexToAddress("0x1234567890123456789012345678901234567890"),
		Token0:      config.WETHAddress,
		Token1:      config.USDCAddress,
		FeeBps:      30,
		Reserve0:    reserve0,
		Reserve1:    reserve1,
	}
	matrix.RegisterPool(pool)

	// 2. Synthetic SwapLog
	log := &types.SwapLog{ // Fixed type lookup context from state package
		Address:     pool.PoolAddress,
		TokenIn:     config.WETHAddress,
		TokenOut:    config.USDCAddress,
		AmountIn:    big.NewInt(1e18),
		AmountOut:   big.NewInt(3000e6),
		BlockNumber: 12345,
	}

	cfg := &config.Config{
		MinProfitUSD: 1.0,
	}

	// 3. Run solver
	candidates := EvaluateEvent(log, matrix, cfg)
	if len(candidates) == 0 {
		t.Fatal("❌ No candidates generated")
	}
	cand := candidates[0]
	t.Logf("✅ Candidate found with profit $%.2f", cand.ExpectedProfitUSD)

	// 4. Build calldata
	loanProvider := uint8(0)
	loanPool := config.BalancerVault
	minProfitWei := big.NewInt(0)
	deadline := uint64(time.Now().Unix()) + 120
	calldata, err := BuildCalldata(cand, loanProvider, loanPool, config.WETHAddress, cand.AmountIn, minProfitWei, deadline)
	if err != nil {
		t.Fatalf("❌ BuildCalldata failed: %v", err)
	}
	if len(calldata) == 0 {
		t.Fatal("❌ Calldata is empty")
	}
	t.Log("✅ Calldata built successfully")

	// 5. Simulate with simulated backend
	privKey, _ := crypto.GenerateKey()
	fromAddress := crypto.PubkeyToAddress(privKey.PublicKey)
	executorAddr := common.HexToAddress("0x9999999999999999999999999999999999999999")

	// Mock executor bytecode
	mockBytecode := common.FromHex("6080604052348015600f57600080fd5b50600436106028576000355c60e01c80630000000014602c575b600080fd5b00")
	alloc := gethTypes.GenesisAlloc{ // Updated package reference to use gethTypes
		fromAddress:  {Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(21), nil)},
		executorAddr: {Code: mockBytecode, Balance: big.NewInt(0)},
	}
	simBackend := simulated.NewBackend(alloc, simulated.WithBlockGasLimit(10_000_000))
	defer simBackend.Close()
	client := simBackend.Client()

	msg := ethereum.CallMsg{
		From: fromAddress,
		To:   &executorAddr,
		Gas:  10_000_000,
		Data: calldata,
	}
	_, err = client.CallContract(context.Background(), msg, nil)
	if err != nil {
		t.Fatalf("❌ Solidity simulation reverted: %v", err)
	}
	t.Log("✅ Solidity execution succeeded")
}
