package querydsl

type boolClauses struct {
	Must    []any `json:"must,omitempty"`
	MustNot []any `json:"must_not,omitempty"`
	Filter  []any `json:"filter,omitempty"`
	Should  []any `json:"should,omitempty"`
}

func collect(clauses []Query) ([]any, error) {
	if len(clauses) == 0 {
		return nil, nil
	}
	out := make([]any, 0, len(clauses))
	for _, q := range clauses {
		src, err := q.Source()
		if err != nil {
			return nil, err
		}
		out = append(out, src)
	}
	if len(out) == 1 {
		return []any{out[0]}, nil
	}
	return out, nil
}

// BoolQuery is a compound query that matches documents using boolean
// combinations of other queries. It corresponds to the OpenSearch bool
// query in JSON DSL:
//
//	{"bool": {"must": [...], "should": [...], "must_not": [...], "filter": [...]}}
//
// Use NewBoolQuery() to create a pointer-based instance suitable for
// method chaining, or use BoolQuery{} directly for value semantics.
type BoolQuery struct {
	mustClauses        []Query
	mustNotClauses     []Query
	filterClauses      []Query
	shouldClauses      []Query
	boost              *float64
	minimumShouldMatch string
	adjustPureNegative *bool
	queryName          string
}

// NewBoolQuery returns a new, empty BoolQuery ready for method chaining.
func NewBoolQuery() *BoolQuery {
	return &BoolQuery{}
}

// Must adds one or more queries that must all match (AND semantics).
func (q *BoolQuery) Must(queries ...Query) *BoolQuery {
	q.mustClauses = append(q.mustClauses, queries...)
	return q
}

// MustNot adds one or more queries that must not match (inverted match).
func (q *BoolQuery) MustNot(queries ...Query) *BoolQuery {
	q.mustNotClauses = append(q.mustNotClauses, queries...)
	return q
}

// Filter adds one or more queries that must match; like Must but contributes
// no relevance score, making it faster for pure filtering.
func (q *BoolQuery) Filter(filters ...Query) *BoolQuery {
	q.filterClauses = append(q.filterClauses, filters...)
	return q
}

// Should adds one or more queries where at least one should match (OR semantics).
// The number of should clauses that must match is controlled by MinimumShouldMatch.
func (q *BoolQuery) Should(queries ...Query) *BoolQuery {
	q.shouldClauses = append(q.shouldClauses, queries...)
	return q
}

// Boost sets the boost factor for the entire bool query.
func (q *BoolQuery) Boost(boost float64) *BoolQuery {
	q.boost = &boost
	return q
}

// MinimumShouldMatch sets the minimum number or percentage of should clauses
// that must match, using the OpenSearch minimum_should_match syntax (e.g. "2", "75%").
func (q *BoolQuery) MinimumShouldMatch(s string) *BoolQuery {
	q.minimumShouldMatch = s
	return q
}

// MinimumNumberShouldMatch sets the minimum number of should clauses that
// must match as an integer.
func (q *BoolQuery) MinimumNumberShouldMatch(n int) *BoolQuery {
	q.minimumShouldMatch = itoa(n)
	return q
}

// AdjustPureNegative controls whether a document matching only must_not
// clauses is still included in the result set with a score of zero.
func (q *BoolQuery) AdjustPureNegative(v bool) *BoolQuery {
	q.adjustPureNegative = &v
	return q
}

// QueryName assigns a query-level name used for identifying the query
// in named query results (stored in the _name field of the JSON DSL).
func (q *BoolQuery) QueryName(name string) *BoolQuery {
	q.queryName = name
	return q
}

func (q *BoolQuery) Source() (any, error) {
	c := boolClauses{}
	var err error
	if c.Must, err = collect(q.mustClauses); err != nil {
		return nil, err
	}
	if c.MustNot, err = collect(q.mustNotClauses); err != nil {
		return nil, err
	}
	if c.Filter, err = collect(q.filterClauses); err != nil {
		return nil, err
	}
	if c.Should, err = collect(q.shouldClauses); err != nil {
		return nil, err
	}

	body := map[string]any{}

	add := func(key string, v []any) {
		switch len(v) {
		case 0:
		case 1:
			body[key] = v[0]
		default:
			body[key] = v
		}
	}
	add("must", c.Must)
	add("must_not", c.MustNot)
	add("filter", c.Filter)
	add("should", c.Should)

	if q.boost != nil {
		body["boost"] = *q.boost
	}
	if q.minimumShouldMatch != "" {
		body["minimum_should_match"] = q.minimumShouldMatch
	}
	if q.adjustPureNegative != nil {
		body["adjust_pure_negative"] = *q.adjustPureNegative
	}
	if q.queryName != "" {
		body["_name"] = q.queryName
	}
	return map[string]any{"bool": body}, nil
}
