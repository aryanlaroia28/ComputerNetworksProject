package command

import (
	"fmt"
	"net"
	"strings"
)

// HandleGraphAddEdge processes G.ADDEDGE <node1> <node2>
func HandleGraphAddEdge(input string, c net.Conn) {
	parts := strings.Split(input, "\r\n")
	if len(parts) < 7 {
		c.Write([]byte("-ERR wrong number of arguments for G.ADDEDGE\r\n"))
		return
	}
	node1 := parts[4]
	node2 := parts[6]

	graphMutex.Lock()
	defer graphMutex.Unlock()

	// Add edge from node1 to node2
	if _, ok := GraphStore[node1]; !ok {
		GraphStore[node1] = make(map[string]bool)
	}
	GraphStore[node1][node2] = true

	// Add edge from node2 to node1 (undirected graph)
	if _, ok := GraphStore[node2]; !ok {
		GraphStore[node2] = make(map[string]bool)
	}
	GraphStore[node2][node1] = true

	fmt.Printf("Graph edge added: %s <-> %s\n", node1, node2)
	c.Write([]byte("+OK\r\n"))
}

// HandleGraphGetFriends processes G.GETFRIENDS <node>
func HandleGraphGetFriends(input string, c net.Conn) {
	parts := strings.Split(input, "\r\n")
	if len(parts) < 5 {
		c.Write([]byte("-ERR wrong number of arguments for G.GETFRIENDS\r\n"))
		return
	}
	node := parts[4]

	graphMutex.RLock()
	defer graphMutex.RUnlock()

	friends, exists := GraphStore[node]
	if !exists {
		c.Write([]byte("*0\r\n")) // No friends or node doesn't exist
		return
	}

	// Convert the set of friends to a RESP array
	resp := formatSetAsRespArray(friends)
	c.Write([]byte(resp))
}

// HandleGraphFOF processes G.FOF <node> (Friends of Friends)
func HandleGraphFOF(input string, c net.Conn) {
	parts := strings.Split(input, "\r\n")
	if len(parts) < 5 {
		c.Write([]byte("-ERR wrong number of arguments for G.FOF\r\n"))
		return
	}
	startNode := parts[4]

	graphMutex.RLock()
	defer graphMutex.RUnlock()

	// --- This is the core "Friends of Friends" logic ---

	// 1. Create a set of friends of friends, and a set to exclude
	fofSet := make(map[string]bool)
	excludeSet := make(map[string]bool)
	excludeSet[startNode] = true // Exclude the person themselves

	// 2. Get the direct friends (Level 1)
	directFriends, exists := GraphStore[startNode]
	if !exists {
		c.Write([]byte("*0\r\n")) // No friends, so no FOF
		return
	}

	// 3. Add direct friends to the exclude list
	for friend := range directFriends {
		excludeSet[friend] = true
	}

	// 4. Iterate through each direct friend
	for friend := range directFriends {
		// 5. Get *their* friends (Level 2)
		friendsOfFriend, exists := GraphStore[friend]
		if !exists {
			continue // This friend has no friends
		}

		// 6. Iterate through the Level 2 friends
		for fof := range friendsOfFriend {
			// 7. If this person is NOT in the exclude list, they are a FOF
			if _, excluded := excludeSet[fof]; !excluded {
				fofSet[fof] = true
			}
		}
	}

	// 8. Format and return the result
	resp := formatSetAsRespArray(fofSet)
	c.Write([]byte(resp))
}