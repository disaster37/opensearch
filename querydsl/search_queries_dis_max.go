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
func NewDisMaxQuery() DisMaxQuery {
	return DisMaxQuery{}
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
