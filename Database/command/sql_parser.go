package command

import (
	"errors"
	"regexp"
	"strconv"
	"strings"
)

// QueryAST (Abstract Syntax Tree) represents a parsed SQL query.
type QueryAST struct {
	OriginalString string
	SelectColumns  []string
	FromTable      string
	Where          *WhereCondition
}

// WhereCondition represents the simple "col op val" condition.
type WhereCondition struct {
	Column   string
	Operator string
	Value    string // Store as string initially
}

// Regex to parse "SELECT <cols> FROM <table> WHERE <col> <op> <val>"
// It's simplified and assumes 'WHERE' is present.
var sqlRegex = regexp.MustCompile(`(?i)SELECT\s+(.+)\s+FROM\s+([^\s]+)\s+WHERE\s+([^\s]+)\s*([<>=])\s*(.+)`)

// Regex for queries without a WHERE clause
var sqlRegexNoWhere = regexp.MustCompile(`(?i)SELECT\s+(.+)\s+FROM\s+([^\s]+)`)

func ParseSQL(input string) (*QueryAST, error) {
	// Trim trailing semicolon if present
	input = strings.TrimSpace(input)
	if strings.HasSuffix(input, ";") {
		input = input[:len(input)-1]
	}

	ast := &QueryAST{OriginalString: input}
	
	// Try parsing with WHERE clause
	matches := sqlRegex.FindStringSubmatch(input)
	
	if matches != nil {
		// Matched: SELECT ... FROM ... WHERE ...
		colStr := strings.TrimSpace(matches[1])
		if colStr == "*" {
			ast.SelectColumns = []string{"*"}
		} else {
			ast.SelectColumns = strings.Split(strings.ReplaceAll(colStr, " ", ""), ",")
		}

		ast.FromTable = strings.TrimSpace(matches[2])
		
		ast.Where = &WhereCondition{
			Column:   strings.TrimSpace(matches[3]),
			Operator: strings.TrimSpace(matches[4]),
			Value:    strings.Trim(strings.TrimSpace(matches[5]), "'\""), // Remove quotes
		}
	} else {
		// Try parsing without WHERE clause
		matchesNoWhere := sqlRegexNoWhere.FindStringSubmatch(input)
		if matchesNoWhere != nil {
			// Matched: SELECT ... FROM ...
			colStr := strings.TrimSpace(matchesNoWhere[1])
			if colStr == "*" {
				ast.SelectColumns = []string{"*"}
			} else {
				ast.SelectColumns = strings.Split(strings.ReplaceAll(colStr, " ", ""), ",")
			}

			ast.FromTable = strings.TrimSpace(matchesNoWhere[2])
			ast.Where = nil // No WHERE clause
		} else {
			return nil, errors.New("ERR invalid or unsupported SQL query format")
		}
	}

	return ast, nil
}

// GetAsInt attempts to parse the condition's value as an integer.
func (wc *WhereCondition) GetAsInt() (int, bool) {
	i, err := strconv.Atoi(wc.Value)
	if err != nil {
		return 0, false
	}
	return i, true
}