// Package ingestion provides low‑latency WebSocket log streaming and decoding for Base.
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

	"github.com/buger/jsonparser"
	"github.com/ethereum/go-ethereum/common"
	"github.com/gorilla/websocket"

	"my-mev-bot/Bot/Types"
)

const (
	socketReadBufferSize  = 65536
	socketWriteBufferSize = 65536
	pingInterval          = 30 * time.Second
	pongWait              = 120 * time.Second
	minBackoff            = 250 * time.Millisecond
	maxBackoff            = 30 * time.Second
	minStableConnection   = 30 * time.Second
)

// SwapLogPool reuses SwapLog objects to reduce GC pressure.
var SwapLogPool = sync.Pool{
	New: func() interface{} {
		return &types.SwapLog{}
	},
}

func GetSwapLog() *types.SwapLog {
	return SwapLogPool.Get().(*types.SwapLog)
}

func PutSwapLog(s *types.SwapLog) {
	s.Reset()
	SwapLogPool.Put(s)
}

// redactURL strips credentials and path for safe logging.
func redactURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return "<invalid url>"
	}
	return u.Scheme + "://" + u.Host + "/<redacted>"
}

// StartWebSocketReader connects to Base, subscribes to Swap events, and forwards parsed SwapLogs.
// No compression is used; all messages are expected as plain JSON over TextMessage.
func StartWebSocketReader(
	ctx context.Context,
	wsURL string,
	eventChan chan<- *types.SwapLog,
	decoder *Decoder,
	statusChan chan<- string,
	poolAddresses []common.Address,
) {
	var validAddresses []string
	for _, addr := range poolAddresses {
		if addr != (common.Address{}) {
			validAddresses = append(validAddresses, strings.ToLower(addr.Hex()))
		}
	}

	if len(validAddresses) == 0 {
		log.Println("[WebSocket] WARNING: No pool addresses specified; subscribing to all swap events (high bandwidth).")
	}

	topics := []interface{}{
		[]string{
			"0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822", // V2 Swap
			"0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67", // V3 Swap
		},
	}

	filter := map[string]interface{}{
		"topics": topics,
	}
	if len(validAddresses) > 0 {
		filter["address"] = validAddresses
	} else {
		filter["address"] = []string{}
	}

	subReq := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "eth_subscribe",
		"params":  []interface{}{"logs", filter},
	}
	subscriptionMsg, err := json.Marshal(subReq)
	if err != nil {
		log.Fatalf("[WebSocket] Failed to marshal subscription request: %v", err)
	}

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
			backoff = nextBackoff(backoff)
			time.Sleep(backoff)
			continue
		}
		connectedAt := time.Now()

		if err := conn.SetReadDeadline(time.Now().Add(pongWait)); err != nil {
			conn.Close()
			backoff = nextBackoff(backoff)
			time.Sleep(backoff)
			continue
		}

		var writeMu sync.Mutex

		conn.SetPongHandler(func(string) error {
			return conn.SetReadDeadline(time.Now().Add(pongWait))
		})

		conn.SetPingHandler(func(appData string) error {
			writeMu.Lock()
			defer writeMu.Unlock()
			if err := conn.WriteControl(websocket.PongMessage, []byte(appData), time.Now().Add(5*time.Second)); err != nil {
				return err
			}
			return conn.SetReadDeadline(time.Now().Add(pongWait))
		})

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

		writeMu.Lock()
		err = conn.WriteMessage(websocket.TextMessage, subscriptionMsg)
		writeMu.Unlock()
		if err != nil {
			stopPing()
			conn.Close()
			backoff = nextBackoff(backoff)
			time.Sleep(backoff)
			continue
		}

		log.Println("[WebSocket] Connected and subscribed to Swap events.")
		if statusChan != nil {
			select {
			case statusChan <- "connected":
			default:
			}
		}

		if err := readLoop(ctx, conn, eventChan, decoder, &writeMu); err != nil {
			log.Printf("[WebSocket] Read loop terminated: %v", err)
		}

		stopPing()
		conn.Close()
		if statusChan != nil {
			select {
			case statusChan <- "disconnected":
			default:
			}
		}

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

// readLoop reads messages from the WebSocket, extracts log data using jsonparser,
// and sends parsed SwapLogs to the event channel.
func readLoop(
	ctx context.Context,
	conn *websocket.Conn,
	eventChan chan<- *types.SwapLog,
	decoder *Decoder,
	writeMu *sync.Mutex,
) error {
	// Buffers allocated once per loop to avoid allocations.
	var readBuffer [65536]byte

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		if err := conn.SetReadDeadline(time.Now().Add(pongWait)); err != nil {
			return fmt.Errorf("set read deadline: %w", err)
		}

		msgType, reader, err := conn.NextReader()
		if err != nil {
			return fmt.Errorf("next reader: %w", err)
		}

		// We only accept TextMessage (plain JSON). Skip binary messages.
		if msgType != websocket.TextMessage {
			// Drain and discard the message.
			_, _ = io.Copy(io.Discard, reader)
			continue
		}

		// Read the payload into the pre‑allocated buffer.
		totalRead := 0
		for {
			n, err := reader.Read(readBuffer[totalRead:])
			totalRead += n
			if err == io.EOF {
				break
			}
			if err != nil {
				return err
			}
			if totalRead >= len(readBuffer) {
				// Payload too large; discard the rest.
				log.Printf("[WebSocket] Payload exceeds buffer size; dropping message")
				_, _ = io.Copy(io.Discard, reader)
				totalRead = 0 // reset to avoid processing truncated data
				break
			}
		}
		if totalRead == 0 {
			continue
		}
		raw := readBuffer[:totalRead]

		// Check if it's a subscription event (contains "method" and "subscription").
		// We use bytes.Contains for a quick check before expensive jsonparser calls.
		if !bytes.Contains(raw, []byte(`"method"`)) && !bytes.Contains(raw, []byte(`"subscription"`)) {
			// Could be a subscription response or other message; ignore.
			continue
		}

		// Extract the "params.result" object, which contains the log data.
		logData, dataType, _, err := jsonparser.Get(raw, "params", "result")
		if err != nil {
			// If not found, try "result" (for single responses)
			logData, dataType, _, err = jsonparser.Get(raw, "result")
			if err != nil || dataType != jsonparser.Object {
				continue
			}
		}
		if dataType != jsonparser.Object || len(logData) < 10 {
			continue
		}

// Reuse a SwapLog from the pool.
swapLog := GetSwapLog()

// Store a copy of the raw JSON for short‑circuit matching.
// The append reuses the slice's capacity (zero‑allocation after first use).
swapLog.RawJSON = append(swapLog.RawJSON[:0], logData...)

// Parse the log (pass the decoder).
if err := parseSwapLogZeroAlloc(logData, swapLog, decoder); err != nil {
    // If parsing fails, return the log to the pool and continue.
    PutSwapLog(swapLog)
    continue
}

// Forward to event channel (non‑blocking to avoid head‑of‑line blocking).
select {
case eventChan <- swapLog:
default:
    PutSwapLog(swapLog)
}
}
}
// newLowLatencyDialer creates a WebSocket dialer with TCP_NODELAY and other low‑latency settings.
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
		EnableCompression: false, // we don't use compression
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
