package main

import (
	"bytes"
	"fmt"
	"net/http"
	"os"
)

// SyncSandboxToLatest forces your BuildBear network to jump to the current live mainnet block tip.
func SyncSandboxToLatest() {
	// 1. Grab your sandbox ID automatically from your .env configuration
	sandboxID := os.Getenv("SANDBOX_ID")
	if sandboxID == "" {
		// Fallback directly to your specific sandbox ID if .env isn't loaded properly
		sandboxID = "equal-gambit-e2c65aea" 
	}

	// 2. Prepare the cloud API endpoint request
	url := fmt.Sprintf("https://buildbear.io", sandboxID)
	jsonData := []byte(`{"blockNumber": "latest"}`)

	// 3. Trigger the reset over the internet
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		fmt.Printf("❌ BuildBear Sync Error: %v\n", err)
		return
	}
	defer resp.Body.Close()

	// 4. Confirm the success status
	if resp.StatusCode == 200 {
		fmt.Println("🔄 Success: BuildBear blockchain updated to the absolute latest mainnet block!")
	} else {
		fmt.Printf("⚠️ BuildBear Sync Failed. Status Code received: %d\n", resp.StatusCode)
	}
}
