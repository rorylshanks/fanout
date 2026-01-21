package buffer

import (
	"sync"
	"testing"
)

func TestLRUCacheBasicOperations(t *testing.T) {
	evicted := make([]string, 0)
	cache := NewLRUCache(3, func(key string, value interface{}) {
		evicted = append(evicted, key)
	})

	// Put and Get
	cache.Put("a", 1)
	cache.Put("b", 2)
	cache.Put("c", 3)

	if val, ok := cache.Get("a"); !ok || val != 1 {
		t.Errorf("expected a=1, got %v, %v", val, ok)
	}
	if val, ok := cache.Get("b"); !ok || val != 2 {
		t.Errorf("expected b=2, got %v, %v", val, ok)
	}
	if val, ok := cache.Get("c"); !ok || val != 3 {
		t.Errorf("expected c=3, got %v, %v", val, ok)
	}

	// Non-existent key
	if _, ok := cache.Get("d"); ok {
		t.Error("expected d to not exist")
	}

	if cache.Len() != 3 {
		t.Errorf("expected len=3, got %d", cache.Len())
	}
}

func TestLRUCacheEviction(t *testing.T) {
	evicted := make([]string, 0)
	cache := NewLRUCache(3, func(key string, value interface{}) {
		evicted = append(evicted, key)
	})

	cache.Put("a", 1)
	cache.Put("b", 2)
	cache.Put("c", 3)

	// Adding 4th item should evict "a" (least recently used)
	cache.Put("d", 4)

	if len(evicted) != 1 || evicted[0] != "a" {
		t.Errorf("expected eviction of 'a', got %v", evicted)
	}

	if _, ok := cache.Get("a"); ok {
		t.Error("expected 'a' to be evicted")
	}

	if val, ok := cache.Get("d"); !ok || val != 4 {
		t.Errorf("expected d=4, got %v, %v", val, ok)
	}
}

func TestLRUCacheEvictionOrder(t *testing.T) {
	evicted := make([]string, 0)
	cache := NewLRUCache(3, func(key string, value interface{}) {
		evicted = append(evicted, key)
	})

	cache.Put("a", 1)
	cache.Put("b", 2)
	cache.Put("c", 3)

	// Access "a" to make it recently used
	cache.Get("a")

	// Adding "d" should evict "b" (now least recently used)
	cache.Put("d", 4)

	if len(evicted) != 1 || evicted[0] != "b" {
		t.Errorf("expected eviction of 'b', got %v", evicted)
	}

	// "a" should still exist
	if _, ok := cache.Get("a"); !ok {
		t.Error("expected 'a' to still exist after access")
	}
}

func TestLRUCacheUpdate(t *testing.T) {
	evicted := make([]string, 0)
	cache := NewLRUCache(3, func(key string, value interface{}) {
		evicted = append(evicted, key)
	})

	cache.Put("a", 1)
	cache.Put("b", 2)
	cache.Put("c", 3)

	// Update "a" - should move it to front and update value
	cache.Put("a", 100)

	if val, ok := cache.Get("a"); !ok || val != 100 {
		t.Errorf("expected a=100 after update, got %v", val)
	}

	// Adding "d" should evict "b" (now least recently used)
	cache.Put("d", 4)

	if len(evicted) != 1 || evicted[0] != "b" {
		t.Errorf("expected eviction of 'b' after update, got %v", evicted)
	}
}

func TestLRUCacheRemove(t *testing.T) {
	evicted := make([]string, 0)
	cache := NewLRUCache(3, func(key string, value interface{}) {
		evicted = append(evicted, key)
	})

	cache.Put("a", 1)
	cache.Put("b", 2)
	cache.Put("c", 3)

	cache.Remove("b")

	if len(evicted) != 1 || evicted[0] != "b" {
		t.Errorf("expected eviction callback for 'b', got %v", evicted)
	}

	if _, ok := cache.Get("b"); ok {
		t.Error("expected 'b' to be removed")
	}

	if cache.Len() != 2 {
		t.Errorf("expected len=2 after remove, got %d", cache.Len())
	}

	// Remove non-existent key should not panic
	cache.Remove("nonexistent")
}

func TestLRUCacheClear(t *testing.T) {
	evicted := make([]string, 0)
	cache := NewLRUCache(3, func(key string, value interface{}) {
		evicted = append(evicted, key)
	})

	cache.Put("a", 1)
	cache.Put("b", 2)
	cache.Put("c", 3)

	cache.Clear()

	// All items should be evicted
	if len(evicted) != 3 {
		t.Errorf("expected 3 evictions, got %d", len(evicted))
	}

	// Clear evicts from oldest (back of list) first
	// After puts: front=[c,b,a]=back, so eviction order is a,b,c
	if evicted[0] != "a" || evicted[1] != "b" || evicted[2] != "c" {
		t.Errorf("expected eviction order [a, b, c] (oldest first), got %v", evicted)
	}

	if cache.Len() != 0 {
		t.Errorf("expected empty cache after clear, got len=%d", cache.Len())
	}
}

func TestLRUCacheKeys(t *testing.T) {
	cache := NewLRUCache(5, nil)

	cache.Put("a", 1)
	cache.Put("b", 2)
	cache.Put("c", 3)

	// Access "a" to move it to front
	cache.Get("a")

	keys := cache.Keys()

	// Keys should be in order: most recent first
	// After Get("a"): a is most recent, c is second (was last put), b is oldest
	if len(keys) != 3 {
		t.Fatalf("expected 3 keys, got %d", len(keys))
	}
	if keys[0] != "a" {
		t.Errorf("expected first key to be 'a' (most recent), got %s", keys[0])
	}
	if keys[2] != "b" {
		t.Errorf("expected last key to be 'b' (oldest), got %s", keys[2])
	}
}

func TestLRUCacheNilOnEvict(t *testing.T) {
	// Test that nil onEvict doesn't panic
	cache := NewLRUCache(2, nil)

	cache.Put("a", 1)
	cache.Put("b", 2)
	cache.Put("c", 3) // Should evict "a" without panic

	if cache.Len() != 2 {
		t.Errorf("expected len=2, got %d", cache.Len())
	}
}

func TestLRUCacheConcurrency(t *testing.T) {
	var mu sync.Mutex
	evicted := make([]string, 0)
	cache := NewLRUCache(100, func(key string, value interface{}) {
		mu.Lock()
		evicted = append(evicted, key)
		mu.Unlock()
	})

	var wg sync.WaitGroup
	numGoroutines := 10
	opsPerGoroutine := 100

	// Concurrent puts
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(base int) {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				key := string(rune('a' + (base*opsPerGoroutine+j)%26))
				cache.Put(key, base*opsPerGoroutine+j)
			}
		}(i)
	}

	// Concurrent gets
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				key := string(rune('a' + j%26))
				cache.Get(key)
			}
		}()
	}

	// Concurrent removes
	for i := 0; i < numGoroutines/2; i++ {
		wg.Add(1)
		go func(base int) {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine/10; j++ {
				key := string(rune('a' + (base+j)%26))
				cache.Remove(key)
			}
		}(i)
	}

	wg.Wait()

	// Just verify no deadlock or panic occurred
	cache.Len()
	cache.Keys()
	cache.Clear()
}

func TestLRUCacheCapacityOne(t *testing.T) {
	evicted := make([]string, 0)
	cache := NewLRUCache(1, func(key string, value interface{}) {
		evicted = append(evicted, key)
	})

	cache.Put("a", 1)
	if cache.Len() != 1 {
		t.Errorf("expected len=1, got %d", cache.Len())
	}

	cache.Put("b", 2)
	if cache.Len() != 1 {
		t.Errorf("expected len=1 after second put, got %d", cache.Len())
	}
	if len(evicted) != 1 || evicted[0] != "a" {
		t.Errorf("expected 'a' to be evicted, got %v", evicted)
	}

	if _, ok := cache.Get("a"); ok {
		t.Error("expected 'a' to not exist")
	}
	if val, ok := cache.Get("b"); !ok || val != 2 {
		t.Errorf("expected b=2, got %v", val)
	}
}

func TestLRUCacheOnEvictReceivesCorrectValue(t *testing.T) {
	evictedValues := make(map[string]interface{})
	cache := NewLRUCache(2, func(key string, value interface{}) {
		evictedValues[key] = value
	})

	cache.Put("a", 100)
	cache.Put("b", 200)
	cache.Put("c", 300) // Evicts "a" (oldest)

	if evictedValues["a"] != 100 {
		t.Errorf("expected evicted value for 'a' to be 100, got %v", evictedValues["a"])
	}

	// After: cache has [c(front), b(back)]
	// Update "b" - moves it to front, cache becomes [b(front), c(back)]
	cache.Put("b", 250)

	// Adding "d" evicts "c" (now oldest/back), not "b"
	cache.Put("d", 400)

	if evictedValues["c"] != 300 {
		t.Errorf("expected evicted value for 'c' to be 300, got %v", evictedValues["c"])
	}

	// Verify "b" still exists with updated value
	if val, ok := cache.Get("b"); !ok || val != 250 {
		t.Errorf("expected b=250 to still exist, got %v, exists=%v", val, ok)
	}
}
