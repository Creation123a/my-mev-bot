// recorder.go
package main

import (
	
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gorilla/websocket" // ✅ correct import
)

func main() {
	// ⚠️ REPLACE THIS WITH YOUR ACTUAL ALCHEMY WSS URL
	alchemyURL := "wss://base-mainnet.g.alchemy.com/v2/alch_N3vxXXHx0b6NgDUEHff7A"

	fmt.Println("🔌 Connecting to Alchemy stream...")
	dialer := websocket.DefaultDialer
	conn, _, err := dialer.Dial(alchemyURL, nil)
	if err != nil {
		panic(fmt.Sprintf("Failed to connect: %v", err))
	}
	defer conn.Close()

	// Virtuals Factory & Topic
	virtualsFactory := "0x1A540088125d00dD3990f9dA45CA0859af4d3B01"
	topicVirtuals := "0x317cda4ec4ebf627725ca315c1e028b0811df5b7e289bf033878b2d1ca65a4a5"

	handshake := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "eth_subscribe",
		"params": []interface{}{
			"logs",
			map[string]interface{}{
				"address": virtualsFactory,
				"topics":  []interface{}{topicVirtuals},
			},
		},
	}

	if err := conn.WriteJSON(handshake); err != nil {
		panic(fmt.Sprintf("Handshake failed: %v", err))
	}

	file, err := os.Create("base_stream_dump.jsonl")
	if err != nil {
		panic(err)
	}
	defer file.Close()

	fmt.Println("💾 Recording started! Letting it run for 2 minutes... Press Ctrl+C to stop early.")

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	go func() {
		for {
			_, message, err := conn.ReadMessage()
			if err != nil {
				return
			}
			file.Write(message)
			file.WriteString("\n")
			fmt.Print(".")
		}
	}()

	select {
	case <-time.After(120 * time.Second):
		fmt.Println("\n⏰ 2 minutes completed.")
	case <-stop:
		fmt.Println("\n🛑 Gracefully stopped by user.")
	}
	fmt.Println("✅ Data safely written to base_stream_dump.jsonl")
}
