package liquidation

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

var aaveBorrowEventSig = crypto.Keccak256Hash([]byte("Borrow(address,address,address,uint256,uint256,uint16)"))

func TestLiquidationPipeline(t *testing.T) {
	matrix := state.NewMatrix()
	gevm := execution.NewGEVMSimulator("", "", common.Address{})

	userAddr := common.HexToAddress("0x2222222222222222222222222222222222222222")
	collateralAsset := config.USDCAddress
	debtAsset := config.WETHAddress
	debtAmount := big.NewInt(5e18)

	mockLog := types.Log{
		Topics: []common.Hash{
			aaveBorrowEventSig,
			common.BytesToHash(collateralAsset.Bytes()),
			common.BytesToHash(userAddr.Bytes()),
		},
		Data: common.LeftPadBytes(debtAmount.Bytes(), 32),
	}

	execChan := make(chan *botTypes.ExecutionPayload, 1)
	payloadPool := &sync.Pool{New: func() interface{} { return &botTypes.ExecutionPayload{} }}
	cfg := &config.Config{LiquidationMinProfitUSD: 1.0}
	liquidationExecutor := common.HexToAddress("0x7777777777777777777777777777777777777777")
	tracker := NewTracker(nil, gevm, matrix, execChan, payloadPool, 1e9, cfg, 0.05)

	// Pre‑store a user position (simulate what updatePosition would do)
	tracker.knownUsers[userAddr] = struct{}{}
	tracker.positions[userAddr] = &UserPosition{
		User:               userAddr,
		Protocol:           ProtocolAave,
		TotalCollateralUSD: 10000,
		TotalDebtUSD:       9500,
		HealthFactor:       0.95,
		CollateralAsset:    collateralAsset,
		DebtAsset:          debtAsset,
		DebtAmount:         debtAmount,
	}

	tracker.checkAllPositions(context.Background())

	var payload *botTypes.ExecutionPayload
	select {
	case payload = <-execChan:
		if payload == nil || len(payload.Calldata) == 0 {
			t.Fatal("❌ Payload invalid")
		}
		t.Logf("✅ Liquidation payload submitted: %s", payload.RouteDesc)
	case <-time.After(2 * time.Second):
		t.Fatal("❌ Timeout: no payload sent")
	}

	// Simulate with simulated backend
	privKey, _ := crypto.GenerateKey()
	fromAddress := crypto.PubkeyToAddress(privKey.PublicKey)
	mockBytecode := common.FromHex("6080604052348015600f57600080fd5b50600436106028576000355c60e01c80630000000014602c575b600080fd5b00")
	alloc := types.GenesisAlloc{
		fromAddress:        {Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(21), nil)},
		liquidationExecutor: {Code: mockBytecode, Balance: big.NewInt(0)},
	}
	simBackend := simulated.NewBackend(alloc, simulated.WithBlockGasLimit(20_000_000))
	defer simBackend.Close()
	client := simBackend.Client()

	msg := ethereum.CallMsg{
		From: fromAddress,
		To:   &liquidationExecutor,
		Gas:  20_000_000,
		Data: payload.Calldata,
	}
	_, err := client.CallContract(context.Background(), msg, nil)
	if err != nil {
		t.Fatalf("❌ Liquidation contract reverted: %v", err)
	}
	t.Log("✅ Liquidation contract simulation succeeded")
}