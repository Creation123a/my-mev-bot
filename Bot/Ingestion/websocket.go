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
// All pools are from Base mainnet. Duplicates are removed.
var knownPoolAddresses = []string{
	// ---------- Uniswap V3 ----------
	"0xd0b53D9277af78126b47ab508A631899178E6e42", // WETH/USDC
	"0x4C36388bE6FAbAA7564619999281aE76E8E62215", // WETH/USDbC
	"0x7aea2e8a3843516afa07293a10ac8e49906dabd1", // WETH/cbBTC
	"0x067160ED01a3F5c0F1f71a93C70D2168324eA7B7", // USDC/USDbC
	"0xac6baB98ff2aE5a727c9D86dfFA2c358B7Cc98fC", // USDC/cbBTC
	"0xb231F830e2f5b4E9D47D937B6B040B47A1df9A8F", // USDbC/cbBTC

	// ---------- PancakeSwap V3 ----------
	"0x72AB388E2E2F6FaceF59E3C3FA2C4E29011c2D38", // WETH/USDC (0.01%)
	"0xb775272e537cc670c65dc852908ad47015244eaf", // WETH/USDbC (0.05%)
	"0xc211e1f853a898bd1302385ccde55f33a8c4b3f3", // WETH/cbBTC (0.05%)
	"0x29ed55b18af0add137952cb3e29fb77b32fce426", // USDC/USDbC (0.01%)
	// USDC/cbBTC and USDbC/cbBTC are the same addresses as Uniswap V3, so omitted.

	// ---------- Aerodrome V2 ----------
	"0xcF77a3Ba9A5CA399B7c97c74d54e5b1Beb874E43", // WETH/USDC
	"0xB69c5339CD8993fa2B1a129033324fE45307b22d", // WETH/USDbC
	"0x2578365b3dfa7ffe60108e181efb79feddec2319", // WETH/cbBTC (42 chars)
"0x4e962bb3889bf030368f56810a9c96b83cb3e778", // USDC/cbBTC (42 chars)

	"0x89D916B87Fa6bA90dcbA90dcbA90606B4b3F47Df", // USDbC/cbBTC
	// USDC/USDbC stable pool address is a placeholder and omitted.

	// ---------- AlienBase V2 ----------
	"0x9eD4D83BDBd987D0C94B3CDe89B064CdDE697aAF", // WETH/USDC
	"0x489679261dfA296DcbF2d08FA08E681966C3E480", // WETH/USDbC
	"0x3018CC672e8113426743FA41bE5B876fe6D9B4A0", // WETH/cbBTC
	"0x32FF1E4e8e16e6d15Db697E027B911438992b8dB", // USDC/USDbC
	"0xdE232Eca8509FDb968E8E98Bfe0b05bA37cb7df7", // USDC/cbBTC
	"0xc864a781F4Bba249bc1c49bA90dcbA90606B4b3F", // USDbC/cbBTC
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
	// Prepare address filter.
	var validAddresses []string
	for _, addr := range knownPoolAddresses {
		trimmed := strings.TrimSpace(addr)
		if trimmed != "" && trimmed != "0x..." && trimmed != "0x0" && trimmed != "0x0000000000000000000000000000000000000000" {
			validAddresses = append(validAddresses, trimmed)
		}
	}

	if len(validAddresses) == 0 {
		log.Println("[WebSocket] WARNING: No pool addresses configured; subscribing to ALL swap events. This may cause high bandwidth usage.")
	}

	// Build topics filter: OR of both event signatures.
	topics := []interface{}{
		[]string{
			"0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822",
			"0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67",
		},
	}

	// Build filter object.
	filter := map[string]interface{}{
		"topics": topics,
	}
	if len(validAddresses) > 0 {
		filter["address"] = validAddresses
	} else {
		// Some nodes require the field to exist; use empty array.
		filter["address"] = []string{}
	}

	subParams := []interface{}{"logs", filter}
	subReq := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "eth_subscribe",
		"params":  subParams,
	}
	subscriptionMsg, err := json.Marshal(subReq)
	if err != nil {
		log.Fatalf("[WebSocket] Failed to marshal subscription request: %v", err)
	}
	// Log the request for debugging.
	log.Printf("[WebSocket] Subscription request: %s", string(subscriptionMsg))
	

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

		// Send connected status.
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

		// Send disconnected status.
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
