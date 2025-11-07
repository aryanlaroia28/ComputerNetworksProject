package command

import (
	"container/list"
	"LiteDB/storage"
	"sync"
	"time"
)

// We will re-use the Entry struct from the main storage for simplicity in our backing store
// although for a real SQL table, this would be different.
// For this project, we'll define new structures for clarity.

// Row represents a single row in a table, mapping column names to values.
type Row map[string]interface{}

// Table represents a collection of rows.
type Table struct {
	Name    string
	Columns []string
	Rows    []Row
}

// BackingDatabase represents the "unlimited" main database (disk)
// We'll just use an in-memory map to simulate this.
var BackingDatabase map[string]*Table
var dbMutex sync.RWMutex

// CacheEntry stores the result of a query in the cache.
type CacheEntry struct {
	Query     *QueryAST // The parsed query
	Results   *Table    // The resulting table
	Timestamp time.Time // Used for LRU
}

// SemanticCache holds the in-memory cache state.
type SemanticCache struct {
	entries *list.List // Holds *CacheEntry, ordered by recency (front = newest)
	lookup  map[string]*list.Element // Maps *query string* to list element for fast direct hits
	mu      sync.RWMutex
	maxSize int
}

// Global cache instance
var SQLCache *SemanticCache

// Constants for cache simulation
const (
	CACHE_MAX_SIZE      = 5 // A small fixed size for the cache
	CACHE_MISS_PENALTY  = 100 * time.Millisecond // Fixed time to simulate cache miss
)

// InitSQLCache initializes the semantic cache.
func InitSQLCache() {
	SQLCache = &SemanticCache{
		entries: list.New(),
		lookup:  make(map[string]*list.Element),
		maxSize: CACHE_MAX_SIZE,
	}
}

// InitBackingDB populates our simulated main database with data.
func InitBackingDB() {
	dbMutex.Lock()
	defer dbMutex.Unlock()
	
	BackingDatabase = make(map[string]*Table)

	// Create a sample 'users' table
	users := &Table{
		Name:    "users",
		Columns: []string{"id", "name", "age"},
		Rows: []Row{
			{"id": 1, "name": "Alice", "age": 31},
			{"id": 2, "name": "Bob", "age": 45},
			{"id": 3, "name": "Charlie", "age": 55},
			{"id": 4, "name": "David", "age": 25},
			{"id": 5, "name": "Eve", "age": 60},
			{"id": 6, "name": "Frank", "age": 42},
			{"id": 7, "name": "Grace", "age": 97},
			{"id": 8, "name": "Heidi", "age": 83},
			{"id": 9, "name": "Ivan", "age": 76},
			{"id": 10, "name": "Judy", "age": 64},
			{"id": 11, "name": "Karl", "age": 19},
			{"id": 12, "name": "Laura", "age": 8},
		},
	}
	BackingDatabase["users"] = users

	// Create a sample 'products' table
	products := &Table{
		Name:    "products",
		Columns: []string{"id", "item", "stock"},
		Rows: []Row{
			{"id": 101, "item": "apple", "stock": 500},
			{"id": 102, "item": "banana", "stock": 200},
			{"id": 103, "item": "orange", "stock": 350},
		},
	}
	BackingDatabase["products"] = products
}

// Get from cache (and update LRU)
func (sc *SemanticCache) Get(queryString string) (*CacheEntry, bool) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	if elem, hit := sc.lookup[queryString]; hit {
		// Move to front (most recently used)
		sc.entries.MoveToFront(elem)
		entry := elem.Value.(*CacheEntry)
		entry.Timestamp = time.Now()
		return entry, true
	}
	return nil, false
}

// AddToCache adds a new entry, handling LRU eviction if full.
func (sc *SemanticCache) AddToCache(queryString string, query *QueryAST, results *Table) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	// If it already exists, just update it and move to front
	if elem, hit := sc.lookup[queryString]; hit {
		sc.entries.MoveToFront(elem)
		entry := elem.Value.(*CacheEntry)
		entry.Results = results
		entry.Timestamp = time.Now()
		return
	}

	// If cache is full, evict the least recently used item
	if sc.entries.Len() >= sc.maxSize {
		lruElement := sc.entries.Back()
		if lruElement != nil {
			lruEntry := sc.entries.Remove(lruElement).(*CacheEntry)
			// Remove from lookup map. We need the original query string,
			// which we don't have...
			// For a robust LRU, the map key should be the query string.
			// The original `lookup` map uses the query string, so we find it.
			delete(sc.lookup, lruEntry.Query.OriginalString) 
		}
	}

	// Add new entry
	entry := &CacheEntry{
		Query:     query,
		Results:   results,
		Timestamp: time.Now(),
	}
	elem := sc.entries.PushFront(entry)
	sc.lookup[queryString] = elem
}

// findSemanticHit iterates the cache (MRU to LRU) looking for a superset query.
func (sc *SemanticCache) FindSemanticHit(newQuery *QueryAST) (*Table, bool) {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	
	// Iterate from MRU (front) to LRU (back)
	for e := sc.entries.Front(); e != nil; e = e.Next() {
		cachedEntry := e.Value.(*CacheEntry)
		
		if isQuerySubset(newQuery, cachedEntry.Query) {
			// Found a superset!
			// Now, filter the superset's results in memory.
			filteredResults := filterResultsFromSuperset(cachedEntry.Results, newQuery.Where)
			
			// Update the superset's timestamp (as it was used)
			cachedEntry.Timestamp = time.Now()
			// Move to front (this requires a write lock, so we'll skip for simplicity
			// in this read-lock section, but in a real system you'd upgrade the lock)
			// For now, just returning the result is fine.
			
			return filteredResults, true
		}
	}
	
	return nil, false
}

// Dummy function, as we're not using the old storage for this.
// We keep this to satisfy the original file structure.
var _ = storage.Store