// Bot/State/lru.go
package state

import (
	"sync"
	"sync/atomic"

	"github.com/ethereum/go-ethereum/common"
)

// CacheEntry represents a single slot in the lock-free CLOCK cache.
// All fields must be accessed atomically.
type CacheEntry struct {
	Token   atomic.Pointer[common.Address]
	Gen     atomic.Uint64
	Visited atomic.Uint32
}

// DynamicTokenCache is a lock-free CLOCK-eviction ring buffer.
// It holds 44 slots for dynamic tokens (meme coins, etc.). No slots are reserved.
// The CLOCK hand pointer cycles through all slots.
// Put is serialized with a mutex to prevent duplicate insertion races.
type DynamicTokenCache struct {
	slots [44]CacheEntry
	hand  atomic.Uint32 // CLOCK hand pointer (mod 44)
	mu    sync.Mutex    // serializes Put to avoid TOCTOU duplicate insertion
}

// NewDynamicTokenCache initializes the cache with all slots empty.
func NewDynamicTokenCache() *DynamicTokenCache {
	return &DynamicTokenCache{}
}

// Get returns true if the token exists in the cache, false otherwise.
func (c *DynamicTokenCache) Get(token common.Address) bool {
	for i := 0; i < 44; i++ {
		ent := &c.slots[i]
		if ent.Gen.Load() == 0 {
			continue
		}
		ptr := ent.Token.Load()
		if ptr != nil && *ptr == token {
			return true
		}
	}
	return false
}

// GetSlot returns the slot index (0-43) of the token, or -1 if not found.
func (c *DynamicTokenCache) GetSlot(token common.Address) int {
	for i := 0; i < 44; i++ {
		ent := &c.slots[i]
		if ent.Gen.Load() == 0 {
			continue
		}
		ptr := ent.Token.Load()
		if ptr != nil && *ptr == token {
			return i
		}
	}
	return -1
}

// Touch marks the slot containing the token as visited (CLOCK second chance).
func (c *DynamicTokenCache) Touch(token common.Address) {
	for i := 0; i < 44; i++ {
		ent := &c.slots[i]
		if ent.Gen.Load() == 0 {
			continue
		}
		ptr := ent.Token.Load()
		if ptr != nil && *ptr == token {
			ent.Visited.Store(1)
			return
		}
	}
}

// Put inserts a token into the cache using CLOCK eviction if full.
// If the token already exists, its visited bit is set to 1.
// This function is safe for concurrent use thanks to a mutex that serializes
// the entire existence check and insertion sequence.
func (c *DynamicTokenCache) Put(token common.Address) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if token already exists.
	for i := 0; i < 44; i++ {
		ent := &c.slots[i]
		if ent.Gen.Load() == 0 {
			continue
		}
		ptr := ent.Token.Load()
		if ptr != nil && *ptr == token {
			ent.Visited.Store(1)
			return
		}
	}

	// Try to find an empty slot (Gen == 0) and claim it atomically.
	for i := 0; i < 44; i++ {
		ent := &c.slots[i]
		if ent.Gen.CompareAndSwap(0, 1) {
			addr := token
			ent.Token.Store(&addr)
			ent.Visited.Store(1)
			return
		}
	}

	// No empty slot — perform CLOCK eviction.
	for {
		hand := c.hand.Load()
		idx := int(hand % 44)
		ent := &c.slots[idx]

		// Give a second chance if visited.
		if ent.Visited.CompareAndSwap(1, 0) {
			c.hand.CompareAndSwap(hand, hand+1)
			continue
		}

		// Slot is unvisited; try to claim it by incrementing generation.
		oldGen := ent.Gen.Load()
		if oldGen == 0 {
			// Shouldn't happen because we already tried empty slots, but handle it.
			c.hand.CompareAndSwap(hand, hand+1)
			continue
		}

		if ent.Gen.CompareAndSwap(oldGen, oldGen+1) {
			addr := token
			ent.Token.Store(&addr)
			ent.Visited.Store(1)
			c.hand.CompareAndSwap(hand, hand+1)
			return
		}
		// If CAS fails, another goroutine claimed the slot; retry.
	}
}

// Len returns the number of occupied slots (for debugging).
func (c *DynamicTokenCache) Len() int {
	count := 0
	for i := 0; i < 44; i++ {
		if c.slots[i].Gen.Load() != 0 {
			count++
		}
	}
	return count
}