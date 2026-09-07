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

var upgrader = websocket.Upgrader{}

// Global state
var (
	mu       sync.Mutex
	// For DEX arbitrage: two pools with prices
	pool1Reserve0 = uint64(100_000)
	pool1Reserve1 = uint64(300_000)
	pool2Reserve0 = uint64(100_000)
	pool2Reserve1 = uint64(310_000) // slight imbalance, will be amplified

	// For bonding curves: platform-specific state
	virtualsSupply   = uint64(0)
	virtualsMax      = uint64(1000) // target
	moltMoonSold     = uint64(0)
	moltMoonTarget   = uint64(1000)
	baseMemeContributed = uint64(0)
	baseMemeTarget   = uint64(1000)
	clawLaunchBalance = uint64(0) // in wei, target 5e18
	pumpTokenReserve  = uint64(1_000_000_000_000_000_000) // 1e18
	thryxRemaining    = uint64(1_000_000_000_000_000_000)

	blockNum = 20000000
	nonce    = uint64(0)

	// Known pool addresses (must match what your bot expects)
	// For DEX, use some random addresses that your gatekeeper may discover
	pool1 = "0x4752ba5DBc23f44D87826276BF6Fd6b1C372aD24"
	pool2 = "0x8Fc8BfD80EdDc6D9A7bEf6C5F2d6E4F7D9B1A2C3"
)

// RPC structures (same as before)
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

// Hex helper
func toHex(v uint64) string {
	return fmt.Sprintf("0x%064x", v)
}

func wsHandler(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()

	// --------------------------------------------------------------
	// Event stream: sends both Swap and Bonding curve events
	// --------------------------------------------------------------
	go func() {
		for {
			mu.Lock()
			blockNum++

			// ---- 1. DEX Arbitrage: Random price drift ----
			// Pool1: random variation
			pool1Reserve0 = uint64(100_000 + rand.Intn(5000))
			pool1Reserve1 = uint64(300_000 + rand.Intn(10000))
			// Pool2: sometimes biased to create arbitrage
			if blockNum%5 == 0 {
				pool2Reserve0 = uint64(80_000 + rand.Intn(2000)) // artificially cheap token0
				pool2Reserve1 = uint64(350_000 + rand.Intn(5000))
			} else {
				pool2Reserve0 = uint64(100_000 + rand.Intn(5000))
				pool2Reserve1 = uint64(300_000 + rand.Intn(10000))
			}

			// Send V2 Swap event for pool1 (direction: token0 -> token1)
			// We'll craft a Swap event: amount0In, amount1Out, etc.
			// Simulate a swap of 1000 token0 -> ~3000 token1
			amount0In := uint64(1000)
			amount1Out := pool1Reserve1 * amount0In / (pool1Reserve0 + amount0In) // constant product with fee
			// Data layout: amount0In, amount1In, amount0Out, amount1Out (each 32 bytes)
			dataSwap1 := fmt.Sprintf("0x%064x%064x%064x%064x", amount0In, 0, 0, amount1Out)

			// Send V2 Swap event for pool2 (opposite direction to create profit)
			amount1In := uint64(1500)
			amount0Out := pool2Reserve0 * amount1In / (pool2Reserve1 + amount1In)
			dataSwap2 := fmt.Sprintf("0x%064x%064x%064x%064x", 0, amount1In, amount0Out, 0)

			// ---- 2. Bonding curve graduation events ----
			// Progressively increase bonding curve reserves every block
			virtualsSupply = uint64(float64(virtualsSupply) + 1.5)
			if virtualsSupply > virtualsMax { virtualsSupply = virtualsMax }
			moltMoonSold = uint64(float64(moltMoonSold) + 1.2)
			if moltMoonSold > moltMoonTarget { moltMoonSold = moltMoonTarget }
			baseMemeContributed = uint64(float64(baseMemeContributed) + 1.8)
			if baseMemeContributed > baseMemeTarget { baseMemeContributed = baseMemeTarget }
			clawLaunchBalance = uint64(float64(clawLaunchBalance) + 0.5e18)
			if clawLaunchBalance > 5e18 { clawLaunchBalance = 5e18 }
			pumpTokenReserve = uint64(float64(pumpTokenReserve) * 0.999) // slowly deflate
			thryxRemaining = uint64(float64(thryxRemaining) * 0.998)

			// Every 30 blocks, force one curve to 99.9%
			if blockNum%30 == 0 {
				switch blockNum % 6 {
				case 0:
					virtualsSupply = virtualsMax - 1 // 99.9%
				case 1:
					moltMoonSold = moltMoonTarget - 1
				case 2:
					baseMemeContributed = baseMemeTarget - 1
				case 3:
					clawLaunchBalance = 5e18 - 1
				case 4:
					pumpTokenReserve = 206_000_000_000_000_000 + 1 // just above floor
				case 5:
					thryxRemaining = 1_000_000_000_000_000_000 - 1
				}
			}

			// Build event payloads
			events := []map[string]interface{}{}

			// Swap events (for DEX arbitrage)
			events = append(events, map[string]interface{}{
				"address": pool1,
				"topics": []string{
					"0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822", // V2 Swap
					"0x0000000000000000000000000000000000000000000000000000000000000000",
					"0x0000000000000000000000000000000000000000000000000000000000000000",
				},
				"data":            dataSwap1,
				"blockNumber":     fmt.Sprintf("0x%x", blockNum),
				"transactionHash": fmt.Sprintf("0xfakehash%x", blockNum),
			})
			events = append(events, map[string]interface{}{
				"address": pool2,
				"topics": []string{
					"0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822",
					"0x0000000000000000000000000000000000000000000000000000000000000000",
					"0x0000000000000000000000000000000000000000000000000000000000000000",
				},
				"data":            dataSwap2,
				"blockNumber":     fmt.Sprintf("0x%x", blockNum),
				"transactionHash": fmt.Sprintf("0xfakehash%x", blockNum+1),
			})

			// Bonding curve events (only send if progress > 0, but we'll send anyway)
			// Virtuals: StateUpdated(currentSupply, maxSupply)
			events = append(events, map[string]interface{}{
				"address": "0x1A540088125d00dD3990f9dA45CA0859af4d3B01", // Virtuals factory
				"topics": []string{
					"0x317cda4ec4ebf627725ca315c1e028b0811df5b7e289bf033878b2d1ca65a4a5",
					"0x0000000000000000000000000000000000000000000000000000000000000000", // token address placeholder (we'll fill a random one)
				},
				"data":            fmt.Sprintf("0x%064x%064x", virtualsSupply, virtualsMax),
				"blockNumber":     fmt.Sprintf("0x%x", blockNum),
				"transactionHash": fmt.Sprintf("0xfakehash%x", blockNum),
			})
			// MoltMoon: CurveProgressUpdated(tokensSold, targetTokens)
			events = append(events, map[string]interface{}{
				"address": "0xC68007C16088d228EF0DF92dB6A9FA19F57b9A23",
				"topics": []string{
					"0x87d65bfa3d88151978d14d101a93b4d1bcf5a2283e9b08f4c7d0d08e01e6a3bf",
					"0x0000000000000000000000000000000000000000000000000000000000000000",
				},
				"data":            fmt.Sprintf("0x%064x%064x", moltMoonSold, moltMoonTarget),
				"blockNumber":     fmt.Sprintf("0x%x", blockNum),
				"transactionHash": fmt.Sprintf("0xfakehash%x", blockNum),
			})
			// Base.meme: PoolProgress(ethContributed, ethTarget)
			events = append(events, map[string]interface{}{
				"address": "0x7706d3389A197D667793Fe4991A5406085FFdfD6",
				"topics": []string{
					"0xe27a421b4f49ff20311f99c8360d2b1f13bce30b91d2938ab4c10df8a48bf589",
					"0x0000000000000000000000000000000000000000000000000000000000000000",
				},
				"data":            fmt.Sprintf("0x%064x%064x", baseMemeContributed, baseMemeTarget),
				"blockNumber":     fmt.Sprintf("0x%x", blockNum),
				"transactionHash": fmt.Sprintf("0xfakehash%x", blockNum),
			})
			// ClawLaunch: V4HookReserveDelta(currentEthBalance)
			events = append(events, map[string]interface{}{
				"address": "0x5C0Ce7E1df7bE75E4De827E6A94EFE6F0764D00b",
				"topics": []string{
					"0x17fa2b678f110bc8d7b32ef8a1e2bf01e3b6a948c2b7d07c08a4bbdfde012a64",
					"0x0000000000000000000000000000000000000000000000000000000000000000",
				},
				"data":            fmt.Sprintf("0x%064x", clawLaunchBalance),
				"blockNumber":     fmt.Sprintf("0x%x", blockNum),
				"transactionHash": fmt.Sprintf("0xfakehash%x", blockNum),
			})
			// Pump.fun: Trade(curveTokenBalance) – we use the token reserves
			events = append(events, map[string]interface{}{
				"address": "0x3c267B8053683A3FeE9dbDEAA65e06a3e6A6133B",
				"topics": []string{
					"0x2c0f6f0c7e2831d17961b7b32ef09594589e4c84918e384b726a4cf8a1e12a64",
					"0x0000000000000000000000000000000000000000000000000000000000000000",
				},
				"data":            fmt.Sprintf("0x%064x%064x", uint64(0), pumpTokenReserve), // first 32 bytes are unused
				"blockNumber":     fmt.Sprintf("0x%x", blockNum),
				"transactionHash": fmt.Sprintf("0xfakehash%x", blockNum),
			})
			// Thryx: AgentCurveUpdated(remainingTokens)
			events = append(events, map[string]interface{}{
				"address": "0x8FA4b802779BBe63ffE72b947f9FBE676A3D801a",
				"topics": []string{
					"0x9a4f6bc06a21ef45511b8ef091a1ef0984cfb721ae9b4c09d012e584f23b71a2",
					"0x0000000000000000000000000000000000000000000000000000000000000000",
				},
				"data":            fmt.Sprintf("0x%064x", thryxRemaining),
				"blockNumber":     fmt.Sprintf("0x%x", blockNum),
				"transactionHash": fmt.Sprintf("0xfakehash%x", blockNum),
			})

			// Send each event as a separate subscription notification
			for _, ev := range events {
				payload := map[string]interface{}{
					"jsonrpc": "2.0",
					"method":  "eth_subscription",
					"params": map[string]interface{}{
						"subscription": "0xsubfakeid12345",
						"result":       ev,
					},
				}
				bytes, _ := json.Marshal(payload)
				if err := conn.WriteMessage(websocket.TextMessage, bytes); err != nil {
					mu.Unlock()
					return
				}
			}

			mu.Unlock()
			time.Sleep(2000 * time.Millisecond)
		}
	}()

	// ---- RPC handler (unchanged but with competition) ----
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
			competitorWon := rand.Float32() < 0.50
			if competitorWon && blockNum%30 == 0 {
				// Competitor frontruns
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

func httpHandler(w http.ResponseWriter, r *http.Request) {
	// Same as before (we'll keep it for eth_sendRawTransaction via HTTP)
	// but we'll just reuse the logic from wsHandler for simplicity.
	// For brevity, we'll just forward to a similar handler.
	// In production, you'd have a unified request handler.
	// We'll implement a minimal version here.
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
	default:
		resp.Result = "0x"
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

	fmt.Println("🧪 Mock MEV Simulator running securely on wss://localhost:8546/ws")
	
	// Natively listen using SSL/TLS certificates generated by the runner
	err := http.ListenAndServeTLS(":8546", "cert.pem", "key.pem", nil)
	if err != nil {
		fmt.Printf("❌ Server failed to start: %v\n", err)
	}
}
