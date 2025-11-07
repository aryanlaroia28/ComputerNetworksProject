package command

import (
	"container/list"
	"MiniRedisDb/storage"
	"fmt"
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

	// --- NEW: Cache Statistics ---
	totalQueries uint64
	directHits   uint64
	semanticHits uint64
	cacheMisses  uint64
	// --- End NEW ---
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
		// --- NEW: Initialize Stats ---
		totalQueries: 0,
		directHits:   0,
		semanticHits: 0,
		cacheMisses:  0,
		// --- End NEW ---
	}
}

// InitBackingDB populates our simulated main database with data.
func InitBackingDB() {
	dbMutex.Lock()
	defer dbMutex.Unlock()

	BackingDatabase = make(map[string]*Table)

	// Create a sample 'users' table (your original data)
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
			// --- NEW: More users for 'age' queries ---
			{"id": 13, "name": "Mike", "age": 91},
			{"id": 14, "name": "Nina", "age": 92},
			{"id": 15, "name": "Oscar", "age": 88},
			// --- End NEW ---
		},
	}
	BackingDatabase["users"] = users

	// Create a sample 'products' table (your original data)
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

	// --- NEW: 'server_logs' table for our test scenario ---
	serverLogs := &Table{
		Name:    "server_logs",
		Columns: []string{"id", "server_name", "cpu_load", "status"},
		Rows: []Row{
			{"id": 1001, "server_name": "web-01", "cpu_load": 25, "status": "OK"},
			{"id": 1002, "server_name": "web-02", "cpu_load": 82, "status": "WARNING"},
			{"id": 1003, "server_name": "db-01", "cpu_load": 91, "status": "WARNING"},
			{"id": 1004, "server_name": "api-01", "cpu_load": 75, "status": "OK"},
			{"id": 1005, "server_name": "web-01", "cpu_load": 30, "status": "OK"},
			{"id": 1006, "server_name": "web-03", "cpu_load": 85, "status": "WARNING"},
			{"id": 1007, "server_name": "api-02", "cpu_load": 96, "status": "ERROR"},
			{"id": 1008, "server_name": "db-01", "cpu_load": 92, "status": "WARNING"},
			{"id": 1009, "server_name": "web-02", "cpu_load": 88, "status": "WARNING"},
			{"id": 1010, "server_name": "cache-01", "cpu_load": 15, "status": "OK"},
			{"id": 1011, "server_name": "web-01", "cpu_load": 40, "status": "OK"},
			{"id": 1012, "server_name": "api-01", "cpu_load": 81, "status": "WARNING"},
			{"id": 1013, "server_name": "db-02", "cpu_load": 99, "status": "ERROR"},
			{"id": 1014, "server_name": "web-03", "cpu_load": 89, "status": "WARNING"},
		},
	}
	BackingDatabase["server_logs"] = serverLogs
	// --- End NEW ---
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
		// --- NEW: Update Stat ---
		sc.directHits++
		// --- End NEW ---
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
			// Remove from lookup map.
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
// --- NEW: Returns the matching cached query for logging ---
func (sc *SemanticCache) FindSemanticHit(newQuery *QueryAST) (*Table, *QueryAST, bool) {
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
			// We can't move to front here without a Write lock,
			// but we can update the stat.
			
			// We'll update stats in HandleSQL as we need the RLock here.

			return filteredResults, cachedEntry.Query, true
		}
	}

	return nil, nil, false
}

// --- NEW: Function to get cache statistics ---
func (sc *SemanticCache) GetCacheStats() string {
	sc.mu.RLock()
	defer sc.mu.RUnlock()

	var directHitRatio float64 = 0
	var semanticHitRatio float64 = 0
	var missRatio float64 = 0

	if sc.totalQueries > 0 {
		directHitRatio = (float64(sc.directHits) / float64(sc.totalQueries)) * 100
		semanticHitRatio = (float64(sc.semanticHits) / float64(sc.totalQueries)) * 100
		missRatio = (float64(sc.cacheMisses) / float64(sc.totalQueries)) * 100
	}
	
	totalHits := sc.directHits + sc.semanticHits
	var totalHitRatio float64 = 0
	if sc.totalQueries > 0 {
		totalHitRatio = (float64(totalHits) / float64(sc.totalQueries)) * 100
	}


	stats := fmt.Sprintf(
		"--- SQL Cache Statistics ---\n"+
			"Total Queries: %d\n"+
			"Total Cache Hits: %d (%.2f%%)\n"+
			"  - Direct Hits:   %d (%.2f%%)\n"+
			"  - Semantic Hits: %d (%.2f%%)\n"+
			"Cache Misses: %d (%.2f%%)\n"+
			"Cache Size: %d / %d",
		sc.totalQueries,
		totalHits, totalHitRatio,
		sc.directHits, directHitRatio,
		sc.semanticHits, semanticHitRatio,
		sc.cacheMisses, missRatio,
		sc.entries.Len(), sc.maxSize,
	)
	return stats
}

// --- NEW: Helper functions to increment stats safely ---
func (sc *SemanticCache) IncrementTotalQueries() {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.totalQueries++
}

func (sc *SemanticCache) IncrementSemanticHits() {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.semanticHits++
}

func (sc *SemanticCache) IncrementCacheMisses() {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.cacheMisses++
}
// --- End NEW ---


// Dummy function, as we're not using the old storage for this.
// We keep this to satisfy the original file structure.
var _ = storage.Store