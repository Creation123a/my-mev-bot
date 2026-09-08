// cmd/mockserver/main.go
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}
var (
	mu    sync.Mutex
	nonce = uint64(0)
)

type RPCRequest struct {
	ID     interface{}     `json:"id"`
	Method string          `json:"method"`
	Params json.RawMessage `json:"params,omitempty"`
}
type RPCResponse struct {
	Jsonrpc string      `json:"jsonrpc"`
	ID      interface{} `json:"id"`
	Result  interface{} `json:"result,omitempty"`
	Error   interface{} `json:"error,omitempty"`
}

func handleRPC(req RPCRequest) RPCResponse {
	var resp RPCResponse
	resp.Jsonrpc = "2.0"
	resp.ID = req.ID
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
		nonce++
		mu.Unlock()
		resp.Result = "0xsuccessfulmocktransactionhash00000000000"
		fmt.Printf("🏆 [SUCCESS] Bot transaction received! Hash incremented.\n")
	case "eth_call":
		resp.Result = "0x0000000000000000000000000000000000000000000000000000000000000001"
	case "eth_chainId":
		resp.Result = "0x2105" // Base Mainnet
	case "eth_blockNumber":
		resp.Result = "0x1312d00"
	default:
		resp.Result = "0x0"
	}
	return resp
}

func wsHandler(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()

	// Thread 1: Stream recorded file
	go func() {
		file, err := os.Open("base_stream_dump.jsonl")
		if err != nil {
			fmt.Printf("❌ Failed to open base_stream_dump.jsonl: %v\n", err)
			return
		}
		defer file.Close()

		fmt.Println("🚀 Starting recorded log data stream playback...")
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			rawLine := scanner.Bytes()
			if len(rawLine) == 0 {
				continue
			}
			_ = conn.WriteMessage(websocket.TextMessage, rawLine)
			time.Sleep(500 * time.Millisecond)
		}
		fmt.Println("🏁 Finished streaming recorded files.")
	}()

	// Thread 2: Handle RPC requests
	for {
		_, msgBytes, err := conn.ReadMessage()
		if err != nil {
			break
		}
		var req RPCRequest
		if err := json.Unmarshal(msgBytes, &req); err != nil {
			continue
		}
		resp := handleRPC(req)
		respBytes, _ := json.Marshal(resp)
		_ = conn.WriteMessage(websocket.TextMessage, respBytes)
	}
}

func httpHandler(w http.ResponseWriter, r *http.Request) {
	var req RPCRequest
	_ = json.NewDecoder(r.Body).Decode(&req)
	resp := handleRPC(req)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func main() {
	http.HandleFunc("/ws", wsHandler)
	http.HandleFunc("/", httpHandler)
	fmt.Println("🧪 Replay Simulator Active over TLS on port 8546...")
	// Use the generated cert.pem and key.pem (must exist)
	_ = http.ListenAndServeTLS(":8546", "cert.pem", "key.pem", nil)
}
