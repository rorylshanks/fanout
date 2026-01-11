package buffer

import (
	"container/list"
	"sync"
)

// LRUCache is a thread-safe LRU cache for file handles
type LRUCache struct {
	mu       sync.Mutex
	capacity int
	items    map[string]*list.Element
	order    *list.List
	onEvict  func(key string, value interface{})
}

type lruEntry struct {
	key   string
	value interface{}
}

// NewLRUCache creates a new LRU cache with the given capacity
func NewLRUCache(capacity int, onEvict func(key string, value interface{})) *LRUCache {
	return &LRUCache{
		capacity: capacity,
		items:    make(map[string]*list.Element),
		order:    list.New(),
		onEvict:  onEvict,
	}
}

// Get retrieves a value from the cache, marking it as recently used
func (c *LRUCache) Get(key string) (interface{}, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.items[key]; ok {
		c.order.MoveToFront(elem)
		return elem.Value.(*lruEntry).value, true
	}
	return nil, false
}

// Put adds or updates a value in the cache
func (c *LRUCache) Put(key string, value interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.items[key]; ok {
		c.order.MoveToFront(elem)
		elem.Value.(*lruEntry).value = value
		return
	}

	// Evict if at capacity
	for c.order.Len() >= c.capacity {
		c.evictOldest()
	}

	entry := &lruEntry{key: key, value: value}
	elem := c.order.PushFront(entry)
	c.items[key] = elem
}

// Remove removes an item from the cache
func (c *LRUCache) Remove(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.items[key]; ok {
		c.removeElement(elem)
	}
}

// evictOldest removes the least recently used item (must be called with lock held)
func (c *LRUCache) evictOldest() {
	elem := c.order.Back()
	if elem != nil {
		c.removeElement(elem)
	}
}

// removeElement removes an element from the cache (must be called with lock held)
func (c *LRUCache) removeElement(elem *list.Element) {
	c.order.Remove(elem)
	entry := elem.Value.(*lruEntry)
	delete(c.items, entry.key)

	if c.onEvict != nil {
		c.onEvict(entry.key, entry.value)
	}
}

// Len returns the number of items in the cache
func (c *LRUCache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.order.Len()
}

// Clear removes all items from the cache, calling onEvict for each
func (c *LRUCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for c.order.Len() > 0 {
		c.evictOldest()
	}
}

// Keys returns all keys in the cache (most recent first)
func (c *LRUCache) Keys() []string {
	c.mu.Lock()
	defer c.mu.Unlock()

	keys := make([]string, 0, c.order.Len())
	for elem := c.order.Front(); elem != nil; elem = elem.Next() {
		keys = append(keys, elem.Value.(*lruEntry).key)
	}
	return keys
}
