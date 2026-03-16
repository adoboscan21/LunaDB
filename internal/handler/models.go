package handler

import "sync"

// LookupClause defines the structure for a collection join operation.
type LookupClause struct {
	FromCollection string `bson:"from"`         // The collection to join with
	LocalField     string `bson:"localField"`   // Field from the input documents
	ForeignField   string `bson:"foreignField"` // Field from the documents of the "from" collection
	As             string `bson:"as"`           // The new array field to add to the input documents
}

// UserInfo structure holds user details and permissions.
type UserInfo struct {
	Username     string            `bson:"username"`
	PasswordHash string            `bson:"password_hash"`
	IsRoot       bool              `bson:"is_root,omitempty"`
	Permissions  map[string]string `bson:"permissions,omitempty"` // Key: collection name, Value: "read" or "write". "*" for all collections.
}

// Query defines the structure for a collection query command,
// encompassing filtering, ordering, limiting, and aggregation.
type Query struct {
	Filter       map[string]any         `bson:"filter,omitempty"`       // WHERE clause equivalents (AND, OR, NOT, LIKE, BETWEEN, IN, IS NULL)
	OrderBy      []OrderByClause        `bson:"order_by,omitempty"`     // ORDER BY clause
	Limit        *int                   `bson:"limit,omitempty"`        // LIMIT clause
	Offset       int                    `bson:"offset,omitempty"`       // OFFSET clause
	Count        bool                   `bson:"count,omitempty"`        // COUNT(*) equivalent
	Aggregations map[string]Aggregation `bson:"aggregations,omitempty"` // SUM, AVG, MIN, MAX
	GroupBy      []string               `bson:"group_by,omitempty"`     // GROUP BY clause
	Having       map[string]any         `bson:"having,omitempty"`       // HAVING clause (filters aggregated results)
	Distinct     string                 `bson:"distinct,omitempty"`     // DISTINCT field
	Projection   []string               `bson:"projection,omitempty"`
	Lookups      []LookupClause         `bson:"lookups,omitempty"`
}

// OrderByClause defines a single ordering criterion.
type OrderByClause struct {
	Field     string `bson:"field"`
	Direction string `bson:"direction"` // "asc" or "desc"
}

// Aggregation defines an aggregation function.
type Aggregation struct {
	Func  string `bson:"func"`  // "sum", "avg", "min", "max", "count"
	Field string `bson:"field"` // Field to aggregate on, "*" for count
}

// Reset clears the Query structure for reuse.
func (q *Query) Reset() {
	q.Filter = nil
	q.OrderBy = nil
	q.Limit = nil
	q.Offset = 0
	q.Count = false
	q.Aggregations = nil
	q.GroupBy = nil
	q.Having = nil
	q.Distinct = ""
	q.Projection = nil
	q.Lookups = nil
}

// A pool for Query objects to reduce memory allocation overhead.
var queryPool = sync.Pool{
	New: func() any {
		return new(Query)
	},
}
