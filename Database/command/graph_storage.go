package command

import (
	"fmt"
	"sync"
)

// GraphStore will represent our graph as an adjacency list.
// The key is the node (e.g., "Alice")
// The value is a "set" (map[string]bool) of connected nodes (e.g., {"Bob": true, "Charlie": true})
var GraphStore map[string]map[string]bool
var graphMutex sync.RWMutex

// InitGraphDB initializes the graph database with hardcoded data.
func InitGraphDB() {
	fmt.Println("Initializing Graph Database...")
	graphMutex.Lock()
	defer graphMutex.Unlock()

	GraphStore = make(map[string]map[string]bool)

	// Hardcode some data
	// We'll use a helper to make it undirected (A -> B and B -> A)
	addEdge("Alice", "Bob")
	addEdge("Alice", "Charlie")
	addEdge("Bob", "David")
	addEdge("Charlie", "Eve")
	addEdge("David", "Frank")
	addEdge("Eve", "Grace")
}

// addEdge is an internal helper to create an undirected edge
// NOTE: This function is not thread-safe, it's only for use in InitGraphDB!
func addEdge(node1, node2 string) {
	// Add edge from node1 to node2
	if _, ok := GraphStore[node1]; !ok {
		GraphStore[node1] = make(map[string]bool)
	}
	GraphStore[node1][node2] = true

	// Add edge from node2 to node1
	if _, ok := GraphStore[node2]; !ok {
		GraphStore[node2] = make(map[string]bool)
	}
	GraphStore[node2][node1] = true
}

// Helper to convert a set (map[string]bool) to a RESP Array string
func formatSetAsRespArray(set map[string]bool) string {
	if len(set) == 0 {
		return "*0\r\n" // Empty array
	}

	resp := fmt.Sprintf("*%d\r\n", len(set))
	for key := range set {
		resp += fmt.Sprintf("$%d\r\n%s\r\n", len(key), key)
	}
	return resp
}