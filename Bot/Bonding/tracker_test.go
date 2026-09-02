package bonding

import (
	"context"
	"math/big"
	"sync"
	"testing"
	"time"

	"github.com"
	"github.com/common"
	gethTypes "github.com/core/types" // Aliased to fix type definition overrides
	"github.com/crypto"
	"github.com/ethclient/simulated"

	"my-mev-bot/Bot/Execution"
	"my-mev-bot/Bot/State"
	botTypes "my-mev-bot/Bot/Types"
)

func TestBondingGraduationPipeline(t *testing.T) {
	matrix := state.NewMatrix()

	// Mock Virtuals StateUpdated log at 99.9%
	tokenAddr := common.HexToAddress("0x1111111111111111111111111111111111111111")
	
	// Convert numerical strings into distinct big.Ints to eliminate untyped float conversion failures
	current, _ := new(big.Int).SetString("9990000000000000000000", 10) // 9990e18
	max, _ := new(big.Int).SetString("10000000000000000000000", 10)     // 10000e18
	
	data := append(common.LeftPadBytes(current.Bytes(), 32), common.LeftPadBytes(max.Bytes(), 32)...)
	
	// Adjusted structural wrapper referencing our aliased gethTypes mapping context
	mockLog := gethTypes.Log{
		Topics: []common.Hash{
			TopicVirtuals, // Uses the package-level definition directly without redeclaring it
			common.BytesToHash(tokenAddr.Bytes()),
		},
		Data: data,
	}

	execChan := make(chan *botTypes.ExecutionPayload, 1)
	payloadPool := &sync.Pool{New: func() interface{} { return &botTypes.ExecutionPayload{} }}
	gevm := execution.NewGEVMSimulator("", "", common.Address{})

	// BondingExecutor address from config
	bondingExecutor := common.HexToAddress("0x8888888888888888888888888888888888888888")
	tracker := NewTracker(nil, gevm, matrix, execChan, payloadPool, bondingExecutor, 1e9)
	tracker.SetWSURL("")

	tracker.handleLog(mockLog)

	// Wait for payload
	var payload *botTypes.ExecutionPayload
	select {
	case payload = <-execChan:
		if payload == nil || len(payload.Calldata) == 0 {
			t.Fatal("❌ Payload invalid")
		}
		t.Logf("✅ Bonding payload submitted: %s", payload.RouteDesc)
	case <-time.After(2 * time.Second):
		t.Fatal("❌ Timeout: no payload sent")
	}

	// Simulate with simulated backend
	privKey, _ := crypto.GenerateKey()
	fromAddress := crypto.PubkeyToAddress(privKey.PublicKey)
	mockBytecode := common.FromHex("6080604052348015600f57600080fd5b50600436106028576000355c60e01c80630000000014602c575b600080fd5b00")
	
	alloc := gethTypes.GenesisAlloc{
		fromAddress:     {Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(21), nil)},
		bondingExecutor: {Code: mockBytecode, Balance: big.NewInt(0)},
	}
	simBackend := simulated.NewBackend(alloc, simulated.WithBlockGasLimit(15_000_000))
	defer simBackend.Close()
	client := simBackend.Client()

	msg := ethereum.CallMsg{
		From: fromAddress,
		To:   &bondingExecutor,
		Gas:  15_000_000,
		Data: payload.Calldata,
	}
	_, err := client.CallContract(context.Background(), msg, nil)
	if err != nil {
		t.Fatalf("❌ Bonding contract reverted: %v", err)
	}
	t.Log("✅ Bonding contract simulation succeeded")
}
