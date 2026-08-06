// Bot/State/blacklist.go
package state

import (
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

const (
	// DefaultTTL is the default time a token stays blacklisted.
	DefaultTTL = 5 * time.Minute
)

// Blacklist provides a thread‑safe set of token addresses to ignore.
// It supports TTL (time‑to‑live) and protects anchor addresses from being blacklisted.
type Blacklist struct {
	mu        sync.RWMutex
	m         map[common.Address]bool
	expiry    map[common.Address]time.Time
	protected map[common.Address]bool
	ttl       time.Duration
}

// NewBlacklist creates a new empty blacklist with default TTL.
func NewBlacklist() *Blacklist {
	return &Blacklist{
		m:         make(map[common.Address]bool),
		expiry:    make(map[common.Address]time.Time),
		protected: make(map[common.Address]bool),
		ttl:       DefaultTTL,
	}
}

// SetProtectedAddresses sets the list of addresses that cannot be blacklisted.
// This should be called once at startup with the anchor addresses.
func (b *Blacklist) SetProtectedAddresses(addresses []common.Address) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, addr := range addresses {
		b.protected[addr] = true
	}
	// Also clean up any existing blacklist entries for protected addresses.
	for addr := range b.protected {
		delete(b.m, addr)
		delete(b.expiry, addr)
	}
}

// SetTTL sets the time‑to‑live for blacklisted entries.
func (b *Blacklist) SetTTL(ttl time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.ttl = ttl
}

// Add marks a token as blacklisted unless it is protected.
// The token will remain blacklisted for TTL duration.
func (b *Blacklist) Add(token common.Address) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Do not blacklist protected addresses.
	if b.protected[token] {
		return
	}

	b.m[token] = true
	b.expiry[token] = time.Now().Add(b.ttl)
}

// Remove removes a token from the blacklist.
func (b *Blacklist) Remove(token common.Address) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.m, token)
	delete(b.expiry, token)
}

// IsBlacklisted returns true if the token is blacklisted and not expired.
// If the token has expired, it is removed from the blacklist and false is returned.
func (b *Blacklist) IsBlacklisted(token common.Address) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Protected addresses are never blacklisted.
	if b.protected[token] {
		return false
	}

	if _, ok := b.m[token]; !ok {
		return false
	}

	// Check expiry.
	if exp, ok := b.expiry[token]; ok && time.Now().After(exp) {
		// Expired: remove it.
		delete(b.m, token)
		delete(b.expiry, token)
		return false
	}
	return true
}
