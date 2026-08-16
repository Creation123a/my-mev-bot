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

	"github.com/andybalholm/brotli"
	"github.com/ethereum/go-ethereum/common"
	"github.com/gorilla/websocket"

	"my-mev-bot/Bot/Types"
)

const (
	socketReadBufferSize    = 65536
	socketWriteBufferSize   = 65536
	pingInterval            = 30 * time.Second
	pongWait                = 120 * time.Second // increased to 120s to reduce timeouts
	minBackoff              = 250 * time.Millisecond
	maxBackoff              = 30 * time.Second
	minStableConnection     = 30 * time.Second
	decompressionBufferSize = 131072
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

// StartWebSocketReader connects to Base, subscribes to Swap events for the given pool addresses,
// decompresses Brotli payloads, and forwards parsed SwapLogs to the event channel.
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
		conn.SetPongHandler(func(string) error {
			return conn.SetReadDeadline(time.Now().Add(pongWait))
		})

		var writeMu sync.Mutex

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

// readLoop reads messages, handles Brotli decompression using the WebSocket message type,
// and parses the JSON‑RPC payload.
func readLoop(
	ctx context.Context,
	conn *websocket.Conn,
	eventChan chan<- *types.SwapLog,
	decoder *Decoder,
	writeMu *sync.Mutex,
) error {
	br := brotli.NewReader(nil)

	// Buffers allocated once per loop to avoid stack churn and improve CPU cache usage.
	var readBuffer [decompressionBufferSize]byte
	var decompressionBuffer [decompressionBufferSize]byte

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		// Refresh read deadline BEFORE each read attempt to prevent timeouts.
		if err := conn.SetReadDeadline(time.Now().Add(pongWait)); err != nil {
			return fmt.Errorf("set read deadline: %w", err)
		}

		msgType, reader, err := conn.NextReader()
		if err != nil {
			return fmt.Errorf("next reader: %w", err)
		}

		if msgType != websocket.TextMessage && msgType != websocket.BinaryMessage {
			_, _ = reader.Read(readBuffer[:])
			continue
		}

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

		var rawMessage []byte
		// Use the WebSocket message type to determine if decompression is needed – reliable.
		if msgType == websocket.TextMessage {
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

		// Flexible JSON‑RPC parsing – handles both subscription and single‑response formats.
		var rpcMsg struct {
			Method string `json:"method"`
			Params struct {
				Result json.RawMessage `json:"result"`
			} `json:"params"`
			Result json.RawMessage `json:"result"`
		}
		if err := json.Unmarshal(rawMessage, &rpcMsg); err != nil {
			log.Printf("[WebSocket] JSON-RPC parse error: %v", err)
			continue
		}

		var logData []byte
		if strings.HasSuffix(rpcMsg.Method, "subscription") {
			logData = rpcMsg.Params.Result
		} else if len(rpcMsg.Result) > 0 {
			logData = rpcMsg.Result
		} else {
			continue
		}

		if len(logData) == 0 {
			continue
		}

		swapLog := GetSwapLog()
		if err := decoder.ParseSwapLog(logData, swapLog); err != nil {
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
