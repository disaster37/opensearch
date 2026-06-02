package querydsl

// DisMaxQuery (disjunction max query) matches documents that match any of the provided
// sub-queries. The score is determined by the highest-scoring sub-query, optionally
// boosted by a tie_breaker factor applied to the scores of other matching sub-queries.
//
// Typical use: searching the same text across multiple fields with different analyzers.
//
// JSON DSL output:
//
//	{
//	  "dis_max": {
//	    "tie_breaker": 0.7,
//	    "queries": [
//	      {"match": {"title": "search text"}},
//	      {"match": {"body": "search text"}}
//	    ]
//	  }
//	}
type DisMaxQuery struct {
	Queries    []Query
	Boost      *float64
	TieBreaker *float64
	QueryName  string
}

// NewDisMaxQuery creates a new empty DisMaxQuery.
func NewDisMaxQuery() *DisMaxQuery {
	return &DisMaxQuery{}
}

// WithQueries sets the list of sub-queries for the dis_max query.
func (q *DisMaxQuery) WithQueries(queries ...Query) *DisMaxQuery {
	q.Queries = queries
	return q
}

// WithBoost sets the boost factor for the query.
func (q *DisMaxQuery) WithBoost(boost float64) *DisMaxQuery {
	q.Boost = &boost
	return q
}

// WithTieBreaker sets the tie_breaker factor used to blend scores from multiple matching sub-queries.
func (q *DisMaxQuery) WithTieBreaker(tieBreaker float64) *DisMaxQuery {
	q.TieBreaker = &tieBreaker
	return q
}

// WithQueryName sets the optional query name for identification in responses.
func (q *DisMaxQuery) WithQueryName(queryName string) *DisMaxQuery {
	q.QueryName = queryName
	return q
}

func (q DisMaxQuery) Source() (any, error) {
	body := map[string]any{}
	if q.TieBreaker != nil {
		body["tie_breaker"] = *q.TieBreaker
	}
	if q.Boost != nil {
		body["boost"] = *q.Boost
	}
	if q.QueryName != "" {
		body["_name"] = q.QueryName
	}
	clauses := make([]any, len(q.Queries))
	for i, sub := range q.Queries {
		src, err := sub.Source()
		if err != nil {
			return nil, err
		}
		clauses[i] = src
	}
	body["queries"] = clauses
	return map[string]any{"dis_max": body}, nil
}
