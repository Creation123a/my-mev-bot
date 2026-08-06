// Bot/State/nonce_tracker.go
package state

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
)

// NonceTracker manages local nonce generation with atomic increments,
// rollback, and remote synchronization.
type NonceTracker struct {
	nonce atomic.Uint64
}

// NewNonceTracker initializes a new nonce tracker starting at 0.
func NewNonceTracker() *NonceTracker {
	return &NonceTracker{}
}

// NextNonce returns the next sequential nonce and increments the counter atomically.
func (nt *NonceTracker) NextNonce() uint64 {
	return nt.nonce.Add(1) - 1
}

// Rollback decrements the nonce counter by 1 atomically.
// It prevents underflow; if the current nonce is 0, it does nothing.
func (nt *NonceTracker) Rollback() {
	for {
		current := nt.nonce.Load()
		if current == 0 {
			return
		}
		if nt.nonce.CompareAndSwap(current, current-1) {
			return
		}
	}
}

// GetCurrent returns the current nonce value without incrementing.
func (nt *NonceTracker) GetCurrent() uint64 {
	return nt.nonce.Load()
}

// SetNonce sets the nonce to a specific value atomically.
func (nt *NonceTracker) SetNonce(val uint64) {
	nt.nonce.Store(val)
}

// SyncFromNode queries the pending nonce for the given account from the
// Ethereum client and sets the local nonce to that value.
// Returns an error if the RPC call fails.
// Uses a 5-second timeout to prevent startup hangs.
func (nt *NonceTracker) SyncFromNode(client *ethclient.Client, account common.Address) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pendingNonce, err := client.PendingNonceAt(ctx, account)
	if err != nil {
		return err
	}
	nt.nonce.Store(pendingNonce)
	return nil
}

// ResetNonce is a convenience alias for SetNonce.
func (nt *NonceTracker) ResetNonce(val uint64) {
	nt.SetNonce(val)
}
