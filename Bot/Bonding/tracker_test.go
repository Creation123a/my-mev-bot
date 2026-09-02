package bonding

import (
	"context"
	"math/big"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient/simulated"

	"my-mev-bot/Bot/Config"
	"my-mev-bot/Bot/Execution"
	"my-mev-bot/Bot/State"
	botTypes "my-mev-bot/Bot/Types"
)

// TopicVirtuals must match your actual event signature
var TopicVirtuals = crypto.Keccak256Hash([]byte("StateUpdated(uint256,uint256,uint256,bool)"))

func TestBondingGraduationPipeline(t *testing.T) {
	matrix := state.NewMatrix()

	// Mock Virtuals StateUpdated log at 99.9%
	tokenAddr := common.HexToAddress("0x1111111111111111111111111111111111111111")
	current := big.NewInt(9990e18)
	max := big.NewInt(10000e18)
	data := append(common.LeftPadBytes(current.Bytes(), 32), common.LeftPadBytes(max.Bytes(), 32)...)
	mockLog := types.Log{
		Topics: []common.Hash{
			TopicVirtuals,
			common.BytesToHash(tokenAddr.Bytes()),
		},
		Data: data,
	}

	execChan := make(chan *botTypes.ExecutionPayload, 1)
	payloadPool := &sync.Pool{New: func() interface{} { return &botTypes.ExecutionPayload{} }}
	gevm := execution.NewGEVMSimulator("", "", common.Address{})

	// BondingExecutor address from config (you can hardcode or use a test address)
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
	alloc := types.GenesisAlloc{
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