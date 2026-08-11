package ingestion

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/url"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/andybalholm/brotli"
	"github.com/gorilla/websocket"

	"my-mev-bot/Bot/Types"
)

const (
	socketReadBufferSize    = 65536
	socketWriteBufferSize   = 65536
	pingInterval            = 30 * time.Second
	pongWait                = 60 * time.Second // must be > pingInterval
	minBackoff              = 250 * time.Millisecond
	maxBackoff              = 30 * time.Second
	minStableConnection     = 30 * time.Second
	decompressionBufferSize = 131072
)

// knownPoolAddresses is a list of known DEX pool addresses to filter the WebSocket subscription.
// This reduces incoming message volume by only receiving events for these pools.
// Replace the placeholder "0x..." with actual Base mainnet pool addresses.
// Leave empty or with placeholders to fallback to no address filter.
// TODO: Fill these with real Base mainnet pool addresses before production deployment.
// You can obtain them from Basescan or the respective DEX factory contracts.
// For Uniswap V3 WETH/USDC on Base, the pool address is 0x... (check Basescan).
var knownPoolAddresses = []string{
	// Uniswap V3
	// "0x...", // WETH/USDC
	// "0x...", // WETH/USDbC
	// "0x...", // cbBTC/USDC
	// "0x...", // cbBTC/USDbC
	// PancakeSwap V3
	// "0x...",
	// Aerodrome V2
	// "0x...",
	// AlienBase V2
	// "0x...",
}

// SwapLogPool is a sync.Pool of types.SwapLog to reuse objects.
var SwapLogPool = sync.Pool{
	New: func() interface{} {
		return &types.SwapLog{}
	},
}

// GetSwapLog retrieves a SwapLog from the pool.
func GetSwapLog() *types.SwapLog {
	return SwapLogPool.Get().(*types.SwapLog)
}

// PutSwapLog resets and returns a SwapLog to the pool.
func PutSwapLog(s *types.SwapLog) {
	s.Reset()
	SwapLogPool.Put(s)
}

// redactURL strips credentials, path, and query from a URL for safe logging.
func redactURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return "<invalid url>"
	}
	return u.Scheme + "://" + u.Host + "/<redacted>"
}

// isLikelyJSON returns true if the payload appears to be a JSON object or array after trimming whitespace.
// This is used to distinguish uncompressed JSON from Brotli-compressed binary data.
func isLikelyJSON(data []byte) bool {
	i := 0
	for i < len(data) {
		c := data[i]
		if c == ' ' || c == '\t' || c == '\n' || c == '\r' {
			i++
			continue
		}
		break
	}
	if i >= len(data) {
		return false
	}
	c := data[i]
	return c == '{' || c == '['
}

// StartWebSocketReader establishes a persistent WebSocket connection to Base,
// subscribes to Swap events, decompresses Brotli payloads, and forwards parsed
// SwapLogs to the provided event channel.
func StartWebSocketReader(
	ctx context.Context,
	wsURL string,
	eventChan chan<- *types.SwapLog,
	decoder *Decoder,
	statusChan chan<- string,
) {
	var validAddresses []string
	for _, addr := range knownPoolAddresses {
		trimmed := strings.TrimSpace(addr)
		if trimmed != "" && trimmed != "0x..." && trimmed != "0x0" && trimmed != "0x0000000000000000000000000000000000000000" {
			validAddresses = append(validAddresses, trimmed)
		}
	}

	var addressFilter string
	if len(validAddresses) == 0 {
		addressFilter = "null"
	} else {
		quoted := make([]string, len(validAddresses))
		for i, addr := range validAddresses {
			quoted[i] = `"` + addr + `"`
		}
		addressFilter = "[" + strings.Join(quoted, ",") + "]"
	}

	if len(validAddresses) == 0 {
		log.Println("[WebSocket] WARNING: No pool addresses configured; subscribing to ALL swap events. This may cause high bandwidth usage.")
	}

	subscriptionMsg := []byte(`{"jsonrpc":"2.0","id":1,"method":"eth_subscribe","params":["logs",{"address":` + addressFilter + `,"topics":[[ "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822", "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67" ]]}]}`)

	dialer := newLowLatencyDialer()
	var backoff time.Duration

	for {
		select {
		case <-ctx.Done():
			log.Println("[WebSocket] Shutting down reader...")
			return
		default:
		}

		log.Printf("[WebSocket] Connecting to %s...", redactURL(wsURL))
		conn, _, err := dialer.Dial(wsURL, nil)
		if err != nil {
			log.Printf("[WebSocket] Dial error: %v", err)
			backoff = nextBackoff(backoff)
			time.Sleep(backoff)
			continue
		}
		connectedAt := time.Now()

		// Configure connection keep-alive and read deadlines.
		if err := conn.SetReadDeadline(time.Now().Add(pongWait)); err != nil {
			log.Printf("[WebSocket] SetReadDeadline failed: %v", err)
			conn.Close()
			backoff = nextBackoff(backoff)
			time.Sleep(backoff)
			continue
		}
		conn.SetPongHandler(func(string) error {
			return conn.SetReadDeadline(time.Now().Add(pongWait))
		})

		var writeMu sync.Mutex

		// Start ping ticker goroutine.
		pingCtx, stopPing := context.WithCancel(ctx)
		go func() {
			ticker := time.NewTicker(pingInterval)
			defer ticker.Stop()
			for {
				select {
				case <-pingCtx.Done():
					return
				case <-ticker.C:
					writeMu.Lock()
					err := conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(5*time.Second))
					writeMu.Unlock()
					if err != nil {
						log.Printf("[WebSocket] Ping failed: %v", err)
						return
					}
				}
			}
		}()

		// Send subscription request.
		writeMu.Lock()
		err = conn.WriteMessage(websocket.TextMessage, subscriptionMsg)
		writeMu.Unlock()
		if err != nil {
			log.Printf("[WebSocket] Subscription failed: %v", err)
			stopPing()
			conn.Close()
			backoff = nextBackoff(backoff)
			time.Sleep(backoff)
			continue
		}

		log.Println("[WebSocket] Connected and subscribed to Swap events.")

		// --- SEND CONNECTED STATUS ---
		if statusChan != nil {
			select {
			case statusChan <- "connected":
			default:
			}
		}

		// Run the read loop.
		if err := readLoop(ctx, conn, eventChan, decoder, &writeMu); err != nil {
			log.Printf("[WebSocket] Read loop terminated: %v", err)
		}

		stopPing()
		conn.Close()

		// --- SEND DISCONNECTED STATUS ---
		if statusChan != nil {
			select {
			case statusChan <- "disconnected":
			default:
			}
		}

		// Apply backoff only if the connection was unstable (short-lived).
		if time.Since(connectedAt) < minStableConnection {
			backoff = nextBackoff(backoff)
			log.Printf("[WebSocket] Connection unstable; backing off %v", backoff)
			time.Sleep(backoff)
		} else {
			backoff = 0
		}
		log.Println("[WebSocket] Connection closed. Reconnecting...")
	}
}

// readLoop reads messages from the WebSocket connection, processes Brotli compression
// if present, and passes the raw JSON payload to the decoder.
func readLoop(
	ctx context.Context,
	conn *websocket.Conn,
	eventChan chan<- *types.SwapLog,
	decoder *Decoder,
	writeMu *sync.Mutex,
) error {
	// Reusable Brotli reader.
	br := brotli.NewReader(nil)

	// readBuffer and decompressionBuffer are local to this loop for safety.
	var readBuffer [decompressionBufferSize]byte
	var decompressionBuffer [decompressionBufferSize]byte

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		// Read the next WebSocket message.
		msgType, reader, err := conn.NextReader()
		if err != nil {
			return fmt.Errorf("next reader: %w", err)
		}

		// Extend read deadline on every successful read.
		if err := conn.SetReadDeadline(time.Now().Add(pongWait)); err != nil {
			return fmt.Errorf("set read deadline: %w", err)
		}

		// Ignore non-text/binary messages.
		if msgType != websocket.TextMessage && msgType != websocket.BinaryMessage {
			_, _ = reader.Read(readBuffer[:])
			continue
		}

		// Read the entire message payload into the fixed buffer.
		totalRead := 0
		truncated := false
		for {
			n, err := reader.Read(readBuffer[totalRead:decompressionBufferSize])
			totalRead += n
			if err == io.EOF {
				break
			}
			if err != nil {
				return err
			}
			if totalRead >= decompressionBufferSize {
				log.Printf("[WebSocket] Payload exceeds %d bytes; dropping message", decompressionBufferSize)
				truncated = true
				// Drain remaining data.
				for {
					var dummy [4096]byte
					_, err := reader.Read(dummy[:])
					if err == io.EOF {
						break
					}
					if err != nil {
						return err
					}
				}
				break
			}
		}
		if truncated {
			continue
		}
		payload := readBuffer[:totalRead]

		// Determine if the payload is JSON or requires Brotli decompression.
		var rawMessage []byte
		if isLikelyJSON(payload) {
			rawMessage = payload
		} else {
			br.Reset(bytes.NewReader(payload))
			totalDecomp := 0
			decompFailed := false
			for {
				if totalDecomp >= decompressionBufferSize {
					log.Printf("[WebSocket] Decompressed data exceeds buffer size; truncating")
					decompFailed = true
					break
				}
				chunk := decompressionBuffer[totalDecomp:decompressionBufferSize]
				n, err := br.Read(chunk)
				totalDecomp += n
				if err == io.EOF {
					break
				}
				if err != nil {
					log.Printf("[WebSocket] Brotli decompression failed: %v", err)
					decompFailed = true
					break
				}
				if n == 0 {
					log.Printf("[WebSocket] Brotli decompression stalled (n=0, err=nil)")
					decompFailed = true
					break
				}
			}
			if decompFailed {
				continue
			}
			rawMessage = decompressionBuffer[:totalDecomp]
		}

		// Unwrap JSON‑RPC envelope.
		var rpcMsg struct {
			JSONRPC string `json:"jsonrpc"`
			Method  string `json:"method"`
			Params  struct {
				Subscription string          `json:"subscription"`
				Result       json.RawMessage `json:"result"`
			} `json:"params"`
			Result json.RawMessage `json:"result"`
			ID     interface{}     `json:"id"`
		}
		if err := json.Unmarshal(rawMessage, &rpcMsg); err != nil {
			log.Printf("[WebSocket] JSON-RPC parse error: %v", err)
			continue
		}

		var logData []byte
		if rpcMsg.Method == "eth_subscription" {
			logData = rpcMsg.Params.Result
		} else if rpcMsg.Result != nil && rpcMsg.ID != nil {
			// Subscription response – ignore.
			continue
		} else {
			log.Printf("[WebSocket] Unknown JSON-RPC message: %s", rawMessage)
			continue
		}

		if len(logData) == 0 {
			continue
		}

		swapLog := GetSwapLog()
		if err := decoder.ParseSwapLog(logData, swapLog); err != nil {
			log.Printf("[WebSocket] Parse error: %v", err)
			PutSwapLog(swapLog)
			continue
		}

		select {
		case eventChan <- swapLog:
		default:
			PutSwapLog(swapLog)
		}
	}
}

// newLowLatencyDialer creates a WebSocket dialer with optimized TCP settings.
func newLowLatencyDialer() websocket.Dialer {
	netDialer := &net.Dialer{
		Timeout:   10 * time.Second,
		KeepAlive: 30 * time.Second,
		Control: func(network, address string, c syscall.RawConn) error {
			var err error
			controlErr := c.Control(func(fd uintptr) {
				if e := syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, syscall.TCP_NODELAY, 1); e != nil {
					err = e
				}
			})
			if controlErr != nil {
				return controlErr
			}
			return err
		},
	}

	return websocket.Dialer{
		NetDial:           netDialer.Dial,
		ReadBufferSize:    socketReadBufferSize,
		WriteBufferSize:   socketWriteBufferSize,
		EnableCompression: false,
	}
}

// nextBackoff calculates the next exponential backoff duration.
func nextBackoff(current time.Duration) time.Duration {
	if current == 0 {
		return minBackoff
	}
	current *= 2
	if current > maxBackoff {
		return maxBackoff
	}
	return current
}
