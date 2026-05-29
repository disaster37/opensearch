package querydsl

// ConstantScoreQuery wraps a filter query and returns all matching documents with a
// constant relevance score equal to the boost value. This is useful when scoring is
// not relevant and only filtering is needed.
//
// Typical use: filtering documents without affecting their ranking.
//
// JSON DSL output:
//
//	{
//	  "constant_score": {
//	    "filter": {
//	      "term": {"status": "published"}
//	    },
//	    "boost": 1.2
//	  }
//	}
type ConstantScoreQuery struct {
	Filter Query
	Boost  *float64
}

// NewConstantScoreQuery creates a new ConstantScoreQuery with the given filter.
func NewConstantScoreQuery(filter Query) ConstantScoreQuery {
	return ConstantScoreQuery{Filter: filter}
}

func (q ConstantScoreQuery) Source() (any, error) {
	src, err := q.Filter.Source()
	if err != nil {
		return nil, err
	}
	body := map[string]any{"filter": src}
	if q.Boost != nil {
		body["boost"] = *q.Boost
	}
	return map[string]any{"constant_score": body}, nil
}
