package dashboard

import (
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"
)

type StatusPayload struct {
	Type   string  `json:"type"`   // "status", "log", "profit", "time", "trade"
	Value  string  `json:"value,omitempty"`
	Profit float64 `json:"profit"` // removed omitempty
	TimeMs float64 `json:"timeMs"` // removed omitempty
	Status string  `json:"status,omitempty"`
	Reason string  `json:"reason,omitempty"`
}

type DashboardServer struct {
	mu            sync.Mutex
	clients       map[chan string]struct{}
	logChan       chan string
	totalProfit   float64
	connStatus    string
	lastTradeStat string
	lastReason    string
	lastExecMs    float64
	token         string // required auth token
}

func NewDashboardServer() *DashboardServer {
	token := os.Getenv("DASHBOARD_TOKEN")
	// Token must be set for authentication.
	// If token is empty, the server will reject all requests.
	return &DashboardServer{
		clients:    make(map[chan string]struct{}),
		logChan:    make(chan string, 256),
		connStatus: "🔴 Disconnected",
		token:      token,
	}
}

// Log pushes a log entry without blocking the MEV engine hot path.
func (d *DashboardServer) Log(msg string) {
	p := StatusPayload{
		Type:  "log",
		Value: msg,
	}
	d.broadcastPayload(p)
}

// SetConnectionStatus updates and broadcasts the connection indicator.
func (d *DashboardServer) SetConnectionStatus(status string) {
	d.mu.Lock()
	d.connStatus = status
	d.mu.Unlock()

	p := StatusPayload{
		Type:  "status",
		Value: status,
	}
	d.broadcastPayload(p)
}

// SetExecutionTime updates and broadcasts the execution time.
func (d *DashboardServer) SetExecutionTime(dur time.Duration) {
	ms := float64(dur.Microseconds()) / 1000.0
	d.mu.Lock()
	d.lastExecMs = ms
	d.mu.Unlock()

	p := StatusPayload{
		Type:   "time",
		TimeMs: ms,
	}
	d.broadcastPayload(p)
}

// AddProfit accumulates net total profit and broadcasts the new aggregate total.
func (d *DashboardServer) AddProfit(profitUSD float64) {
	d.mu.Lock()
	d.totalProfit += profitUSD
	tot := d.totalProfit
	d.mu.Unlock()

	p := StatusPayload{
		Type:   "profit",
		Profit: tot,
	}
	d.broadcastPayload(p)
}

// SetTradeStatus updates and broadcasts trade outcomes.
func (d *DashboardServer) SetTradeStatus(status string, reason string) {
	d.mu.Lock()
	d.lastTradeStat = status
	d.lastReason = reason
	d.mu.Unlock()

	p := StatusPayload{
		Type:   "trade",
		Status: status,
		Reason: reason,
	}
	d.broadcastPayload(p)
}

func (d *DashboardServer) broadcastPayload(p StatusPayload) {
	data, err := json.Marshal(p)
	if err != nil {
		return
	}
	msg := string(data)

	select {
	case d.logChan <- msg:
	default:
		// Non-blocking drop on log channel overflow
	}
}

// Start runs the dashboard HTTP server with timeouts, private mux, loopback binding,
// and required token authentication.
func (d *DashboardServer) Start(addr string) error {
	// Require a non-empty token for authentication.
	if d.token == "" {
		return fmt.Errorf("DASHBOARD_TOKEN must be set for dashboard authentication")
	}

	go d.broadcaster()

	// Force loopback binding for security, unless the caller explicitly uses another address.
	if addr == ":8080" {
		addr = "127.0.0.1:8080"
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/events", d.authMiddleware(d.handleSSE))
	mux.HandleFunc("/", d.authMiddleware(d.handleUI))

	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		// WriteTimeout intentionally left zero for SSE long-polling.
	}
	return srv.ListenAndServe()
}

// authMiddleware checks for a valid token in the "token" query parameter.
// If the token is missing or mismatched, it returns 401 Unauthorized.
// Uses constant-time comparison to prevent timing attacks.
func (d *DashboardServer) authMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Token must be present and non-empty.
		token := r.URL.Query().Get("token")
		if token == "" {
			http.Error(w, "Unauthorized: missing token", http.StatusUnauthorized)
			return
		}
		// Constant-time comparison.
		if subtle.ConstantTimeCompare([]byte(token), []byte(d.token)) != 1 {
			http.Error(w, "Unauthorized: invalid token", http.StatusUnauthorized)
			return
		}
		next(w, r)
	}
}

func (d *DashboardServer) broadcaster() {
	for msg := range d.logChan {
		d.mu.Lock()
		for client := range d.clients {
			select {
			case client <- msg:
			default:
			}
		}
		d.mu.Unlock()
	}
}

func (d *DashboardServer) handleSSE(w http.ResponseWriter, r *http.Request) {
	// Check for Flusher before registering the client.
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	messageChan := make(chan string, 64)

	d.mu.Lock()
	d.clients[messageChan] = struct{}{}
	initStatus, _ := json.Marshal(StatusPayload{Type: "status", Value: d.connStatus})
	initProfit, _ := json.Marshal(StatusPayload{Type: "profit", Profit: d.totalProfit})
	initTrade, _ := json.Marshal(StatusPayload{Type: "trade", Status: d.lastTradeStat, Reason: d.lastReason})
	initTime, _ := json.Marshal(StatusPayload{Type: "time", TimeMs: d.lastExecMs})
	d.mu.Unlock()

	defer func() {
		d.mu.Lock()
		delete(d.clients, messageChan)
		d.mu.Unlock()
		close(messageChan)
	}()

	fmt.Fprintf(w, "data: %s\n\n", initStatus)
	fmt.Fprintf(w, "data: %s\n\n", initProfit)
	fmt.Fprintf(w, "data: %s\n\n", initTrade)
	fmt.Fprintf(w, "data: %s\n\n", initTime)
	flusher.Flush()

	// Loop with context cancellation to avoid goroutine leak.
	for {
		select {
		case <-r.Context().Done():
			// Client disconnected.
			return
		case msg, ok := <-messageChan:
			if !ok {
				return
			}
			if _, err := fmt.Fprintf(w, "data: %s\n\n", msg); err != nil {
				// Write failure means client is gone; stop.
				return
			}
			flusher.Flush()
		}
	}
}

func (d *DashboardServer) handleUI(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html")
	w.Write([]byte(htmlTemplate))
}

const htmlTemplate = `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>⚡ MEV Bot Control Dashboard</title>
    <style>
        * { box-sizing: border-box; margin: 0; padding: 0; }
        body { background-color: #0b0e14; color: #c9d1d9; font-family: 'JetBrains Mono', monospace, sans-serif; padding: 20px; }
        .header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px; border-bottom: 1px solid #21262d; padding-bottom: 15px; }
        .title { font-size: 1.4rem; color: #58a6ff; font-weight: bold; }
        .status-badge { background: #161b22; border: 1px solid #30363d; padding: 6px 14px; border-radius: 20px; font-size: 0.9rem; font-weight: 600; }
        
        .metrics-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 15px; margin-bottom: 20px; }
        .card { background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 16px; }
        .card-label { font-size: 0.75rem; text-transform: uppercase; color: #8b949e; letter-spacing: 0.5px; margin-bottom: 8px; }
        .card-value { font-size: 1.5rem; font-weight: bold; color: #f0f6fc; }
        
        .text-green { color: #3fb950 !important; }
        .text-red { color: #f85149 !important; }
        
        #logs { background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 15px; height: 55vh; overflow-y: auto; font-size: 0.85rem; line-height: 1.5; }
        .log-entry { padding: 4px 0; border-bottom: 1px solid #21262d; white-space: pre-wrap; word-break: break-all; }
        .log-win { color: #3fb950; }
        .log-drop { color: #f85149; }
    </style>
</head>
<body>

    <div class="header">
        <div class="title">⚡ BASE MEV BOT</div>
        <div id="connStatus" class="status-badge">🔴 Disconnected</div>
    </div>

    <div class="metrics-grid">
        <div class="card">
            <div class="card-label">Total Profit (Net)</div>
            <div id="totalProfit" class="card-value text-green">$0.00</div>
        </div>
        <div class="card">
            <div class="card-label">Execution Time</div>
            <div id="execTime" class="card-value">0.00 ms</div>
        </div>
        <div class="card">
            <div class="card-label">Last Trade Status</div>
            <div id="lastTrade" class="card-value">--</div>
            <div id="lastReason" style="font-size: 0.75rem; color: #8b949e; margin-top: 4px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;">None</div>
        </div>
    </div>

    <div id="logs"></div>

    <script>
        // Forward the token from the page URL to the SSE endpoint.
        const token = new URLSearchParams(window.location.search).get("token");
        const evtSource = new EventSource("/events" + (token ? "?token=" + encodeURIComponent(token) : ""));
        const logsDiv = document.getElementById("logs");

        evtSource.onmessage = function(event) {
            try {
                const payload = JSON.parse(event.data);
                if (payload.type === "status") {
                    document.getElementById("connStatus").textContent = payload.value;
                } else if (payload.type === "profit") {
                    document.getElementById("totalProfit").textContent = "$" + payload.profit.toFixed(2);
                } else if (payload.type === "time") {
                    document.getElementById("execTime").textContent = payload.timeMs.toFixed(2) + " ms";
                } else if (payload.type === "trade") {
                    const tradeEl = document.getElementById("lastTrade");
                    tradeEl.textContent = payload.status || "--";
                    tradeEl.className = "card-value " + (payload.status === "SUCCESS" ? "text-green" : payload.status === "FAILED" ? "text-red" : "");
                    document.getElementById("lastReason").textContent = payload.reason || "None";
                } else if (payload.type === "log") {
                    const text = payload.value || "";
                    const entry = document.createElement("div");
                    entry.className = "log-entry";
                    if (text.includes("[+] WIN") || text.includes("SUCCESS")) entry.classList.add("log-win");
                    if (text.includes("[-] DROP") || text.includes("FAIL")) entry.classList.add("log-drop");
                    entry.textContent = text;
                    logsDiv.appendChild(entry);
                    logsDiv.scrollTop = logsDiv.scrollHeight;
                }
            } catch(e) {
                // Fallback for plain text SSE frames
                const entry = document.createElement("div");
                entry.className = "log-entry";
                entry.textContent = event.data;
                logsDiv.appendChild(entry);
                logsDiv.scrollTop = logsDiv.scrollHeight;
            }
        };
    </script>
</body>
</html>`
