// cmd/mockserver/main.go
package main

import (
	"encoding/json"
	"fmt"
	"math/big"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{}

// ------------------------------
// Global simulated blockchain state
// ------------------------------
var (
	mu       sync.Mutex
	blockNum = uint64(20000000)
	nonce    = uint64(0)

	// Virtuals bonding curve state
	virtualsMaxSupply = new(big.Int).SetUint64(100_000_000_000_000_000) // 0.1 ether equivalent
	virtualsCurrent   = new(big.Int).SetUint64(99_999_000_000_000_000)  // ~99.999%
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

// Helper: properly pads a big.Int to a 32-byte (64 character) hex string
func padHex(b *big.Int) string {
	// Extract the real base-16 hex string representation of the large number
	rawHex := b.Text(16)
	// Pad it out to exactly 64 characters to align perfectly with EVM logs
	return fmt.Sprintf("%064s", rawHex)
}

// ------------------------------
// WebSocket handler – streams bonding curve logs
// ------------------------------
func wsHandler(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()

	// Real Virtuals factory address (must match tracker.go)
	virtualsFactory := "0x1A540088125d00dD3990f9dA45CA0859af4d3B01"
	topicVirtuals := "0x317cda4ec4ebf627725ca315c1e028b0811df5b7e289bf033878b2d1ca65a4a5"

	// Thread: generate events every 2 seconds
	go func() {
		for {
			mu.Lock()
			blockNum++

			// Every 20 blocks, create a graduation opportunity
			if blockNum%20 == 0 {
				// Set current supply to max - 1 wei (almost 100%)
				virtualsCurrent.Sub(virtualsMaxSupply, big.NewInt(1))
			} else {
				// Random noise: current ~99.5% of max
				noise := uint64(rand.Intn(500_000_000_000_000)) // up to 0.5%
				virtualsCurrent = new(big.Int).Sub(virtualsMaxSupply, new(big.Int).SetUint64(noise))
			}

			// Build the StateUpdated event data: currentSupply, maxSupply
			data := fmt.Sprintf("0x%s%s", padHex(virtualsCurrent), padHex(virtualsMaxSupply))

			// Create the subscription payload
			payload := map[string]interface{}{
				"jsonrpc": "2.0",
				"method":  "eth_subscription",
				"params": map[string]interface{}{
					"subscription": "0xsubfakeid12345",
					"result": map[string]interface{}{
						"address":         virtualsFactory,
						"topics":          []string{topicVirtuals},
						"data":            data,
						"blockNumber":     fmt.Sprintf("0x%x", blockNum),
						"transactionHash": fmt.Sprintf("0xfakehash%x", blockNum),
					},
				},
			}
			mu.Unlock()

			bytes, _ := json.Marshal(payload)
			if err := conn.WriteMessage(websocket.TextMessage, bytes); err != nil {
				break
			}
			time.Sleep(2000 * time.Millisecond)
		}
	}()

	// Thread: handle incoming messages (eth_sendRawTransaction)
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
		case "eth_sendRawTransaction":
			mu.Lock()
			// 50% chance competitor frontruns
			competitorWon := rand.Float32() < 0.50
			// Only frontrun if there was an opportunity (block%20==0)
			if competitorWon && blockNum%20 == 0 {
				// Competitor rebalances the curve – no profit left
				virtualsCurrent.Set(virtualsMaxSupply) // full supply
				resp.Error = map[string]interface{}{
					"code":    -32000,
					"message": "execution reverted: Slippage bounds exceeded (Frontrun)",
				}
				fmt.Println("⚔️ [MEV] Bot was frontrun! Forcing retry.")
			} else {
				nonce++
				resp.Result = fmt.Sprintf("0xsuccessfulfake-txhash-%d", nonce)
				fmt.Println("🏆 [SUCCESS] Bot transaction landed.")
			}
			mu.Unlock()

		case "eth_call":
			// Dummy return for calls
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

		respBytes, _ := json.Marshal(resp)
		_ = conn.WriteMessage(websocket.TextMessage, respBytes)
	}
}

// ------------------------------
// HTTP handler for RPC
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
		if competitorWon && blockNum%20 == 0 {
			virtualsCurrent.Set(virtualsMaxSupply)
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
	rand.Seed(time.Now().UnixNano())
	http.HandleFunc("/ws", wsHandler)
	http.HandleFunc("/", httpHandler)

	fmt.Println("🧪 Mock MEV Simulator (Virtuals Bonding) running on ws://localhost:8546/ws and http://localhost:8546")
	http.ListenAndServe(":8546", nil)
}
