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

// LRUCache is a lock-free CLOCK-eviction ring buffer.
// It is generic and supports any number of slots.
type LRUCache struct {
	slots []CacheEntry
	hand  atomic.Uint32 // CLOCK hand pointer (mod len(slots))
	mu    sync.Mutex    // serializes Put to avoid TOCTOU duplicate insertion
}

// NewMemeTokenCache creates a cache for meme tokens (60 slots).
func NewMemeTokenCache() *LRUCache {
	return &LRUCache{
		slots: make([]CacheEntry, 60),
	}
}

// NewDEXFactoryCache creates a cache for DEX factories (6 slots).
func NewDEXFactoryCache() *LRUCache {
	return &LRUCache{
		slots: make([]CacheEntry, 6),
	}
}

// NewBasePairCache creates a cache for base pairs (6 slots).
func NewBasePairCache() *LRUCache {
	return &LRUCache{
		slots: make([]CacheEntry, 6),
	}
}

// Get returns true if the token exists in the cache, false otherwise.
func (c *LRUCache) Get(token common.Address) bool {
	for i := 0; i < len(c.slots); i++ {
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

// GetSlot returns the slot index of the token, or -1 if not found.
func (c *LRUCache) GetSlot(token common.Address) int {
	for i := 0; i < len(c.slots); i++ {
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
func (c *LRUCache) Touch(token common.Address) {
	for i := 0; i < len(c.slots); i++ {
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
// Put inserts a token into the cache using CLOCK eviction if full.
func (c *LRUCache) Put(token common.Address) {
	c.mu.Lock()
	defer c.mu.Unlock()

	n := len(c.slots)
	addr := token

	// 1. Check if token already exists.
	for i := 0; i < n; i++ {
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

	// 2. Try to find an empty slot (Gen == 0) and claim it atomically.
	for i := 0; i < n; i++ {
		ent := &c.slots[i]
		if ent.Gen.Load() == 0 {
			ent.Token.Store(&addr) // Set pointer value BEFORE opening the slot
			ent.Visited.Store(1)
			if ent.Gen.CompareAndSwap(0, 1) {
				return
			}
		}
	}

	// 3. No empty slot — perform CLOCK eviction.
	for {
		hand := c.hand.Load()
		idx := int(hand % uint32(n))
		ent := &c.slots[idx]

		// Give a second chance if visited.
		if ent.Visited.CompareAndSwap(1, 0) {
			c.hand.CompareAndSwap(hand, hand+1)
			continue
		}

		oldGen := ent.Gen.Load()
		if oldGen == 0 {
			c.hand.CompareAndSwap(hand, hand+1)
			continue
		}

		// FIXED: Stage values inside the slot structures BEFORE incrementing Gen
		ent.Token.Store(&addr)
		ent.Visited.Store(1)

		if ent.Gen.CompareAndSwap(oldGen, oldGen+1) {
			c.hand.CompareAndSwap(hand, hand+1)
			return
		}
		// If CAS fails, another routine won the slot; loop back around safely
	}
}

// Len returns the number of occupied slots (for debugging).
func (c *LRUCache) Len() int {
	count := 0
	for i := 0; i < len(c.slots); i++ {
		if c.slots[i].Gen.Load() != 0 {
			count++
		}
	}
	return count
}
