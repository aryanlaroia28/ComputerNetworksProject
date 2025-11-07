package command

import (
	"fmt"
	"net"
	// "strconv"
	"strings"
	"time"
)

// HandleSQL is the main entry point for SQL queries.
func HandleSQL(input string, c net.Conn) {
	// --- NEW: Start timer and update total queries ---
	startTime := time.Now()
	SQLCache.IncrementTotalQueries()
	// --- End NEW ---

	// 1. Extract the raw SQL query string.
	sqlQueryString := extractSQLQuery(input)
	if sqlQueryString == "" {
		c.Write([]byte("-ERR invalid SQL command\r\n"))
		return
	}

	// 2. Parse the SQL string into an AST.
	queryAST, err := ParseSQL(sqlQueryString)
	if err != nil {
		c.Write([]byte(fmt.Sprintf("-ERR %s\r\n", err.Error())))
		return
	}

	// --- CACHE LOGIC ---

	// 3. Check for a Direct Cache Hit
	if entry, hit := SQLCache.Get(sqlQueryString); hit {
		// Cache Hit! (Get() increments the stat)
		// --- NEW: Improved Logging ---
		elapsed := time.Since(startTime)
		fmt.Printf("[QUERY: %s] \n -> Cache HIT (Direct) | Time: %s\n", sqlQueryString, elapsed)
		// --- End NEW ---
		resp := formatResults(entry.Results)
		c.Write([]byte(resp))
		return
	}

	// 4. Check for a Semantic Cache Hit
	// --- NEW: Updated signature to get cachedQuery ---
	if results, cachedQuery, hit := SQLCache.FindSemanticHit(queryAST); hit {
		// Semantic Hit!
		// --- NEW: Update Stat ---
		SQLCache.IncrementSemanticHits()
		// --- NEW: Improved Logging with AST ---
		elapsed := time.Since(startTime)
		fmt.Printf("[QUERY: %s] \n -> Cache HIT (Semantic) | Time: %s\n", sqlQueryString, elapsed)
		fmt.Println("   | Fulfilling from cached superset query:")
		fmt.Printf("   |--- Cached Query: %s\n", cachedQuery.OriginalString)
		// This prints the AST of the *cached query*
		fmt.Printf("   |--- Cached %s\n", cachedQuery.String()) 
		// --- End NEW ---

		resp := formatResults(results)
		c.Write([]byte(resp))
		return
	}

	// 5. Cache Miss
	// --- NEW: Update Stat ---
	SQLCache.IncrementCacheMisses()
	// --- End NEW ---

	// Simulate the I/O penalty for a cache miss
	time.Sleep(CACHE_MISS_PENALTY)

	// 6. Execute query against the "Backing Database"
	results, err := executeOnBackingStore(queryAST)
	if err != nil {
		c.Write([]byte(fmt.Sprintf("-ERR %s\r\n", err.Error())))
		return
	}

	// 7. Add the new result to the cache
	SQLCache.AddToCache(sqlQueryString, queryAST, results)

	// 8. Return results to client
	// --- NEW: Improved Logging ---
	elapsed := time.Since(startTime)
	fmt.Printf("[QUERY: %s] \n -> Cache MISS | Time: %s (Includes %s I/O penalty)\n", sqlQueryString, elapsed, CACHE_MISS_PENALTY)
	// --- End NEW ---

	resp := formatResults(results)
	c.Write([]byte(resp))
}

// --- NEW: Handler for SQLSTATS command ---
func HandleSQLStats(c net.Conn) {
	stats := SQLCache.GetCacheStats()
	// Format as a bulk string for the client
	resp := fmt.Sprintf("$%d\r\n%s\r\n", len(stats), stats)
	c.Write([]byte(resp))
}
// --- End NEW ---


// extractSQLQuery assumes the input is the raw buffer and finds the SQL.
// This is fragile and mimics your existing `strings.Contains`.
// A better way would be a proper RESP parser.
func extractSQLQuery(input string) string {
	// A simple heuristic: find "SELECT" and return the rest.
	// This assumes the command is just the SQL query itself.
	// e.g. "SELECT * FROM users WHERE age > 40"

	// Let's assume the server is passing the *full* RESP buffer
	// e.g. *2\r\n$3\r\nSQL\r\n$27\r\nSELECT * FROM users WHERE age > 40\r\n
	// Your existing code uses `strings.Split(input, "\r\n")`

	parts := strings.Split(input, "\r\n")

	// Let's define a new command format: "SQL <query>"
	// RESP: *2\r\n$3\r\nSQL\r\n$<len>\r\n<query>\r\n
	// parts[0] = *2
	// parts[1] = $3
	// parts[2] = SQL
	// parts[3] = $<len>
	// parts[4] = <query>
	if len(parts) > 4 && (strings.EqualFold(parts[2], "SQL") || strings.Contains(parts[4], "SELECT")) {
		// This handles "SQL <query>" or just "SELECT ..."
		if strings.Contains(parts[4], "SELECT") {
			return parts[4]
		}
		// Fallback for just "SELECT..."
	}

	// Fallback for "SELECT ..." as the first command
	if len(parts) > 4 && strings.Contains(parts[0], "SELECT") {
		// This is likely wrong, let's assume `input` is just the query.
	}

	// This is the most likely good assumption based on your `server.go`
	if strings.Contains(input, "SELECT") {
		// Find the first "SELECT" and trim
		idx := strings.Index(strings.ToUpper(input), "SELECT")
		if idx != -1 {
			// Find the end of the query (e.g., \r\n or end of string)
			query := input[idx:]
			endIdx := strings.Index(query, "\r\n")
			if endIdx != -1 {
				query = query[:endIdx]
			}
			return strings.TrimSpace(query)
		}
	}

	// Let's refine the "SQL" command assumption from above
	// *2\r\n$3\r\nSQL\r\n$27\r\nSELECT * FROM users WHERE age > 40\r\n
	if len(parts) > 4 && strings.EqualFold(parts[2], "SQL") {
		return parts[4]
	}

	// --- NEW: Fallback for SQLSTATS command ---
	if len(parts) >= 3 && strings.EqualFold(parts[2], "SQLSTATS") {
		return "SQLSTATS" // Not a query, but `extract` is the wrong place.
	}
	if strings.Contains(strings.ToUpper(input), "SQLSTATS") {
		return "SQLSTATS"
	}
	// --- End NEW ---

	return "" // No valid SQL found
}

// executeOnBackingStore runs the query against the main data.
func executeOnBackingStore(query *QueryAST) (*Table, error) {
	dbMutex.RLock()
	defer dbMutex.RUnlock()

	table, exists := BackingDatabase[query.FromTable]
	if !exists {
		return nil, fmt.Errorf("table '%s' not found", query.FromTable)
	}

	var resultRows []Row

	// Filter rows
	for _, row := range table.Rows {
		if query.Where == nil || checkCondition(row, query.Where) {
			resultRows = append(resultRows, row)
		}
	}

	// Apply column selection
	finalRows := []Row{}
	for _, row := range resultRows {
		if query.SelectColumns[0] == "*" {
			finalRows = append(finalRows, row)
		} else {
			newRow := make(Row)
			for _, col := range query.SelectColumns {
				if val, ok := row[col]; ok {
					newRow[col] = val
				}
			}
			finalRows = append(finalRows, newRow)
		}
	}

	finalCols := query.SelectColumns
	if finalCols[0] == "*" {
		finalCols = table.Columns
	}

	return &Table{
		Name:    "results",
		Columns: finalCols,
		Rows:    finalRows,
	}, nil
}

// formatResults converts a Table into a RESP bulk string.
// --- NEW: Improved formatting ---
func formatResults(table *Table) string {
	if table == nil || len(table.Rows) == 0 {
		return "$-1\r\n" // Nil bulk string (empty result)
	}

	var sb strings.Builder

	// Calculate column widths
	colWidths := make(map[string]int)
	for _, col := range table.Columns {
		colWidths[col] = len(col) // Start with header length
	}

	for _, row := range table.Rows {
		for _, col := range table.Columns {
			valStr := fmt.Sprintf("%v", row[col])
			if len(valStr) > colWidths[col] {
				colWidths[col] = len(valStr)
			}
		}
	}

	// --- Print Header ---
	var headerLine []string
	var separatorLine []string
	for _, col := range table.Columns {
		width := colWidths[col]
		headerLine = append(headerLine, fmt.Sprintf("%-*s", width, col))
		separatorLine = append(separatorLine, strings.Repeat("-", width))
	}
	sb.WriteString(strings.Join(headerLine, " | "))
	sb.WriteString("\n")
	sb.WriteString(strings.Join(separatorLine, "-+-"))
	sb.WriteString("\n")

	// --- Print Rows ---
	for _, row := range table.Rows {
		var rowLine []string
		for _, col := range table.Columns {
			width := colWidths[col]
			rowLine = append(rowLine, fmt.Sprintf("%-*v", width, row[col]))
		}
		sb.WriteString(strings.Join(rowLine, " | "))
		sb.WriteString("\n")
	}

	tableString := sb.String()
	// Add row count
	tableString += fmt.Sprintf("\n(%d rows)\n", len(table.Rows))

	return fmt.Sprintf("$%d\r\n%s\r\n", len(tableString), tableString)
}

// --- Semantic Logic ---

// isQuerySubset checks if newQuery is a semantic subset of cachedQuery.
func isQuerySubset(newQuery, cachedQuery *QueryAST) bool {
	if newQuery.FromTable != cachedQuery.FromTable {
		return false
	}

	// Check select columns (new must be subset of cached)
	if cachedQuery.SelectColumns[0] != "*" {
		// If cached isn't "*", new must have columns <= cached
		colMap := make(map[string]bool)
		for _, col := range cachedQuery.SelectColumns {
			colMap[col] = true
		}
		for _, col := range newQuery.SelectColumns {
			if col != "*" && !colMap[col] {
				return false // New query asks for a column not in cache
			}
		}
	}
	// If cached is "*", new can be anything (including "*" or "col1, col2")

	// Check WHERE clause (new must be stricter than cached)
	return isConditionSubset(newQuery.Where, cachedQuery.Where)
}

// isConditionSubset is the core semantic logic.
func isConditionSubset(newCond, cachedCond *WhereCondition) bool {
	if cachedCond == nil {
		// Cached query was "SELECT * FROM table"
		// New query is always a subset (e.g., "... WHERE age > 50")
		return true
	}

	if newCond == nil {
		// New query is "SELECT * FROM table"
		// Cached query is "... WHERE age > 40"
		// This is NOT a subset.
		return false
	}

	// Both queries have WHERE clauses.
	if newCond.Column != cachedCond.Column {
		return false // Conditions are on different columns
	}

	// Try to compare as integers
	newVal, newIsInt := newCond.GetAsInt()
	cachedVal, cachedIsInt := cachedCond.GetAsInt()

	if newIsInt && cachedIsInt {
		// This is where we implement your example:
		// new = "age > 50", cached = "age > 40"
		if newCond.Operator == ">" && cachedCond.Operator == ">" {
			return newVal >= cachedVal // 50 >= 40 -> true
		}
		// new = "age < 30", cached = "age < 40"
		if newCond.Operator == "<" && cachedCond.Operator == "<" {
			return newVal <= cachedVal // 30 <= 40 -> true
		}
		// new = "age = 55", cached = "age > 50"
		if newCond.Operator == "=" && cachedCond.Operator == ">" {
			return newVal > cachedVal // 55 > 50 -> true
		}
		// new = "age = 45", cached = "age < 50"
		if newCond.Operator == "=" && cachedCond.Operator == "<" {
			return newVal < cachedVal // 45 < 50 -> true
		}
		// ... more rules could be added here ...
	}

	// Fallback for string comparison
	if newCond.Operator == "=" && cachedCond.Operator == "=" {
		return newCond.Value == cachedCond.Value
	}
	
	// --- NEW: Handle subset for string equals ---
	// e.g. newCond = "status = 'ERROR'"
	//      cachedCond = nil (e.g. from "cpu_load > 80")
	// This is handled by the `isConditionSubset` logic in filterResults...
	// The main `isQuerySubset` just checks if the *new query's* conditions
	// are compatible with and stricter than the *cached query's*.
	
	// Our new test case:
	// newCond: cpu_load > 95
	// cachedCond: cpu_load > 80
	// This will pass: (newOp == ">" && cachedOp == ">") && (95 >= 80) == true

	return false
}

// filterResultsFromSuperset takes a cached superset and applies the new, stricter filter.
func filterResultsFromSuperset(superset *Table, newCondition *WhereCondition) *Table {
	if newCondition == nil {
		return superset // Should not happen if isConditionSubset is correct
	}

	var filteredRows []Row
	for _, row := range superset.Rows {
		if checkCondition(row, newCondition) {
			filteredRows = append(filteredRows, row)
		}
	}

	return &Table{
		Name:    "filtered_results",
		Columns: superset.Columns, // Columns are from the superset
		Rows:    filteredRows,
	}
}

// checkCondition evaluates a row against a WHERE condition.
func checkCondition(row Row, cond *WhereCondition) bool {
	if cond == nil {
		return true // No condition means the row passes
	}
	
	val, ok := row[cond.Column]
	if !ok {
		return false // Column doesn't exist in row
	}

	// Try integer comparison
	condVal, condIsInt := cond.GetAsInt()
	rowVal, rowIsInt := val.(int)

	if condIsInt && rowIsInt {
		switch cond.Operator {
		case ">":
			return rowVal > condVal
		case "<":
			return rowVal < condVal
		case "=":
			return rowVal == condVal
		}
	}

	// Try string comparison
	condValStr := cond.Value
	rowValStr := fmt.Sprintf("%v", val)
	if cond.Operator == "=" {
		return rowValStr == condValStr
	}

	return false // Unsupported operation
}