// cmd/mockserver/main.go
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

// ------------------------------
// Global simulated blockchain state
// ------------------------------
var (
	mu       sync.Mutex
	blockNum = uint64(20000000)
	nonce    = uint64(0)

	// Virtuals bonding curve state
	virtualsMaxSupply = new(big.Int).SetUint64(100_000_000_000_000_000)
	virtualsCurrent   = new(big.Int).SetUint64(99_999_000_000_000_000)
)

// ------------------------------
// Robust JSON-RPC structures
// ------------------------------
type RPCRequest struct {
	Jsonrpc string          `json:"jsonrpc"`
	ID      interface{}     `json:"id"` // Loose type to avoid unmarshaling errors
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type RPCResponse struct {
	Jsonrpc string      `json:"jsonrpc"`
	ID      interface{} `json:"id"`
	Result  interface{} `json:"result,omitempty"`
	Error   interface{} `json:"error,omitempty"`
}

// Helper: properly pads a big.Int to a 32-byte (64 character) hex string
func padHex(b *big.Int) string {
	rawHex := b.Text(16)
	return fmt.Sprintf("%064s", rawHex)
}

// ------------------------------
// Core JSON-RPC Router Engine
// ------------------------------
func processRPCMethod(req RPCRequest) RPCResponse {
	var resp RPCResponse
	resp.Jsonrpc = "2.0"
	resp.ID = req.ID

	// Fallback for null or empty IDs to keep clients happy
	if resp.ID == nil {
		resp.ID = 1
	}

	switch req.Method {
	case "eth_subscribe":
		resp.Result = "0xsubfakeid12345"

	case "eth_getTransactionCount":
		mu.Lock()
		resp.Result = fmt.Sprintf("0x%x", nonce)
		mu.Unlock()

	case "eth_sendRawTransaction":
		mu.Lock()
		competitorWon := rand.Float32() < 0.50
		if competitorWon && blockNum%20 == 0 {
			virtualsCurrent.Set(virtualsMaxSupply) // Opportunity closed
			resp.Error = map[string]interface{}{
				"code":    -32000,
				"message": "execution reverted: Slippage bounds exceeded (Frontrun)",
			}
			fmt.Printf("⚔️ [MEV] Method [%s]: Bot frontrun attempt simulated.\n", req.Method)
		} else {
			nonce++
			resp.Result = fmt.Sprintf("0xsuccessfulfake-txhash-%d", nonce)
			fmt.Printf("🏆 [SUCCESS] Method [%s]: Bot transaction simulated successfully.\n", req.Method)
		}
		mu.Unlock()

	case "eth_call":
		// Standard success code (1 wrapped in 32-byte padding)
		resp.Result = "0x0000000000000000000000000000000000000000000000000000000000000001"

	case "eth_getTransactionReceipt":
		resp.Result = map[string]interface{}{
			"transactionHash": "0xdeadbeef",
			"blockNumber":     "0x1",
			"status":          "0x1",
			"logs":            []interface{}{},
		}

	case "eth_chainId":
		resp.Result = "0x2105" // Base Mainnet

	case "eth_blockNumber":
		mu.Lock()
		resp.Result = fmt.Sprintf("0x%x", blockNum)
		mu.Unlock()

	case "eth_estimateGas":
		resp.Result = "0x5208" // Standard 21000 gas units fallback

	default:
		// Catch-all response to prevent initialization crashes for telemetry or tracking methods
		resp.Result = "0x0"
		fmt.Printf("ℹ️ [RPC] Handled unsupported method smoothly: %s\n", req.Method)
	}

	return resp
}

// ------------------------------
// WebSocket handler – streams logs & handles requests
// ------------------------------
func wsHandler(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()

	virtualsFactory := "0x1A540088125d00dD3990f9dA45CA0859af4d3B01"
	topicVirtuals := "0x317cda4ec4ebf627725ca315c1e028b0811df5b7e289bf033878b2d1ca65a4a5"

	// Thread 1: Continuous Event Stream Engine
	go func() {
		for {
			mu.Lock()
			blockNum++

			if blockNum%20 == 0 {
				virtualsCurrent.Sub(virtualsMaxSupply, big.NewInt(1)) // Graduation target
			} else {
				noise := uint64(rand.Intn(500_000_000_000_000))
				virtualsCurrent = new(big.Int).Sub(virtualsMaxSupply, new(big.Int).SetUint64(noise))
			}

			currentBlock := blockNum
			data := fmt.Sprintf("0x%s%s", padHex(virtualsCurrent), padHex(virtualsMaxSupply))
			mu.Unlock()

			// Real Virtuals tracking requires two topics: [0] signature, [1] indexed token address
fakeTokenAddress := "0x000000000000000000000000000000000000babe"
paddedTokenTopic := fmt.Sprintf("0x000000000000000000000000%s", fakeTokenAddress[2:])

payload := map[string]interface{}{
    "jsonrpc": "2.0",
    "method":  "eth_subscription",
    "params": map[string]interface{}{
        "subscription": "0xsubfakeid12345",
        "result": map[string]interface{}{
            "address": virtualsFactory,
            "topics":  []string{topicVirtuals, paddedTokenTopic}, // ✅ Now has 2 topics
            "data":    data,
            "blockNumber":     fmt.Sprintf("0x%x", currentBlock),
            "transactionHash": fmt.Sprintf("0xfakehash%x", currentBlock),
        },
    },
}

			bytes, _ := json.Marshal(payload)
			if err := conn.WriteMessage(websocket.TextMessage, bytes); err != nil {
				break
			}
			time.Sleep(2000 * time.Millisecond)
		}
	}()

	// Thread 2: Incoming message handler
	for {
		_, msgBytes, err := conn.ReadMessage()
		if err != nil {
			break
		}

		var req RPCRequest
		if err := json.Unmarshal(msgBytes, &req); err != nil {
			continue
		}

		resp := processRPCMethod(req)
		respBytes, _ := json.Marshal(resp)
		_ = conn.WriteMessage(websocket.TextMessage, respBytes)
	}
}

// ------------------------------
// HTTP handler – supports batched and loose requests
// ------------------------------
func httpHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Invalid body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	w.Header().Set("Content-Type", "application/json")

	// Hardened check: Check if the incoming request is a batch array `[...]` or single object `{...}`
	if len(bodyBytes) > 0 && bodyBytes[0] == '[' {
		var reqs []RPCRequest
		if err := json.Unmarshal(bodyBytes, &reqs); err != nil {
			// Fallback placeholder error structure
			json.NewEncoder(w).Encode(RPCResponse{Jsonrpc: "2.0", ID: 1, Result: "0x0"})
			return
		}

		resps := make([]RPCResponse, len(reqs))
		for i, req := range reqs {
			resps[i] = processRPCMethod(req)
		}
		json.NewEncoder(w).Encode(resps)
		return
	}

	// Single standard RPC Request path
	var req RPCRequest
	if err := json.Unmarshal(bodyBytes, &req); err != nil {
		json.NewEncoder(w).Encode(RPCResponse{Jsonrpc: "2.0", ID: 1, Result: "0x0"})
		return
	}

	resp := processRPCMethod(req)
	json.NewEncoder(w).Encode(resp)
}

func main() {
	rand.Seed(time.Now().UnixNano())
	http.HandleFunc("/ws", wsHandler)
	http.HandleFunc("/", httpHandler)

	certPath := "./cert.pem"
	keyPath := "./key.pem"

	fmt.Printf("🧪 Mock MEV Simulator initializing TLS with cert: %s, key: %s\n", certPath, keyPath)
	err := http.ListenAndServeTLS(":8546", certPath, keyPath, nil)
	if err != nil {
		panic(fmt.Sprintf("❌ CRITICAL: TLS Server failed to boot: %v", err))
	}
}
