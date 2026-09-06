// cmd/mockserver/main.go
package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true }, // Prevents local CORS connection drops
}

// ------------------------------
// Global state (simulated blockchain)
// ------------------------------
var (
	mu       sync.Mutex
	reserve0 = uint64(100_000)
	reserve1 = uint64(300_000)
	blockNum = 20000000
	nonce    = uint64(0)
)

// ------------------------------
// JSON-RPC structures
// ------------------------------
type RPCRequest struct {
	ID     interface{}     `json:"id"`
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
}

type RPCResponse struct {
	Jsonrpc string      `json:"jsonrpc"`
	ID      interface{} `json:"id"`
	Result  interface{} `json:"result,omitempty"`
	Error   interface{} `json:"error,omitempty"`
}

// ------------------------------
// WebSocket handler – streams logs & handles eth_sendRawTransaction
// ------------------------------
func wsHandler(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()

	uniswapPool := "0x4752ba5DBc23f44D87826276BF6Fd6b1C372aD24"

	// Thread 1: Continuous event stream (every 2s to simulate blocks)
	go func() {
		for {
			mu.Lock()
			blockNum++

			// Natural market noise
			reserve0 = uint64(100_000 + rand.Intn(2000))
			reserve1 = uint64(300_000 + rand.Intn(6000))

			// Inject arbitrage opportunity every 30 blocks
			if blockNum%30 == 0 {
				reserve0 = 40_000 // Massive price imbalance
			}

			currentBlock := blockNum
			r0, r1 := reserve0, reserve1
			mu.Unlock()

			hexData := fmt.Sprintf("0x%064x%064x", r0, r1)

			payload := map[string]interface{}{
				"jsonrpc": "2.0",
				"method":  "eth_subscription",
				"params": map[string]interface{}{
					"subscription": "0xsubfakeid12345",
					"result": map[string]interface{}{
						"address":         uniswapPool,
						"topics":          []string{"0x1c9b1a533b99a3754884f18e1d2c9431e285d8869cc5a72049e7b2ff92e10c5a"}, // Sync event
						"data":            hexData,
						"blockNumber":     fmt.Sprintf("0x%x", currentBlock),
						"transactionHash": fmt.Sprintf("0xfakehash%x", currentBlock),
					},
				},
			}

			bytes, _ := json.Marshal(payload)
			if err := conn.WriteMessage(websocket.TextMessage, bytes); err != nil {
				break
			}
			time.Sleep(2000 * time.Millisecond) // 2s block time
		}
	}()

	// Thread 2: Handle incoming messages (eth_sendRawTransaction, eth_subscribe)
	for {
		_, msgBytes, err := conn.ReadMessage()
		if err != nil {
			break
		}

		var req RPCRequest
		if err := json.Unmarshal(msgBytes, &req); err != nil {
			continue
		}

		var resp RPCResponse
		resp.Jsonrpc = "2.0"
		resp.ID = req.ID

		switch req.Method {
		case "eth_subscribe":
			// Handshake method required by most high-performance client libraries
			resp.Result = "0xsubfakeid12345"

		case "eth_sendRawTransaction":
			mu.Lock()
			competitorWon := rand.Float32() < 0.50
			if competitorWon && blockNum%30 == 0 {
				reserve0 = 100_000
				reserve1 = 300_000
				resp.Error = map[string]interface{}{
					"code":    -32000,
					"message": "execution reverted: Slippage bounds exceeded (Frontrun)",
				}
				fmt.Println("⚔️ [MEV] WS: Bot was frontrun! Forcing retry.")
			} else {
				nonce++
				resp.Result = fmt.Sprintf("0xsuccessfulfake-txhash-%d", nonce)
				fmt.Println("🏆 [SUCCESS] WS: Bot transaction landed.")
			}
			mu.Unlock()

		case "eth_call":
			resp.Result = "0x0000000000000000000000000000000000000000000000000000000000000001"

		case "eth_getTransactionReceipt":
			resp.Result = map[string]interface{}{
				"transactionHash": "0xdeadbeef",
				"blockNumber":     "0x1",
				"status":          "0x1",
				"logs":            []interface{}{},
			}

		case "eth_chainId":
			resp.Result = "0x2105" // Base mainnet chain ID

		default:
			resp.Error = map[string]interface{}{
				"code":    -32601,
				"message": "method not supported",
			}
		}

		respBytes, _ := json.Marshal(resp)
		_ = conn.WriteMessage(websocket.TextMessage, respBytes)
	}
}

// ------------------------------
// HTTP handler for standard RPC calls
// ------------------------------
func httpHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req RPCRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var resp RPCResponse
	resp.Jsonrpc = "2.0"
	resp.ID = req.ID

	switch req.Method {
	case "eth_sendRawTransaction":
		mu.Lock()
		competitorWon := rand.Float32() < 0.50
		if competitorWon && blockNum%30 == 0 {
			reserve0 = 100_000
			reserve1 = 300_000
			resp.Error = map[string]interface{}{
				"code":    -32000,
				"message": "execution reverted: Slippage bounds exceeded (Frontrun)",
			}
			fmt.Println("⚔️ [MEV] HTTP: Bot was frontrun!")
		} else {
			nonce++
			resp.Result = fmt.Sprintf("0xsuccessfulfake-txhash-%d", nonce)
			fmt.Println("🏆 [SUCCESS] HTTP: Bot transaction landed.")
		}
		mu.Unlock()

	case "eth_call":
		resp.Result = "0x0000000000000000000000000000000000000000000000000000000000000001"

	case "eth_getTransactionReceipt":
		resp.Result = map[string]interface{}{
			"transactionHash": "0xdeadbeef",
			"blockNumber":     "0x1",
			"status":          "0x1",
			"logs":            []interface{}{},
		}

	case "eth_chainId":
		resp.Result = "0x2105"

	default:
		resp.Error = map[string]interface{}{
			"code":    -32601,
			"message": "method not supported",
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// ------------------------------
// Main
// ------------------------------
func main() {
	http.HandleFunc("/ws", wsHandler)
	http.HandleFunc("/", httpHandler)

	fmt.Println("🧪 Mock MEV Simulator running on ws://localhost:8546/ws and http://localhost:8546")
	http.ListenAndServe(":8546", nil)
}
