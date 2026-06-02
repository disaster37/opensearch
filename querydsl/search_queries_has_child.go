package querydsl

// HasChildQuery matches parent documents that have at least one child
// document of the specified type matching the given query. It corresponds
// to the OpenSearch has_child query in JSON DSL:
//
//	{"has_child": {"type": "answer", "query": {...}, "score_mode": "max"}}
//
// Key parameters:
//   - Type: the child document type (join relation name).
//   - Query: the query executed against child documents.
//   - ScoreMode: how child scores are aggregated into the parent score
//     ("min", "max", "avg", "sum", "none").
//   - MinChildren / MaxChildren: require a minimum or maximum number of
//     matching children.
//   - InnerHit: optional [InnerHit] to return matched child documents.
//
// Use NewHasChildQuery(childType, query) to create an instance.
type HasChildQuery struct {
	Query              Query
	Type               string
	Boost              *float64
	ScoreMode          string
	MinChildren        *int
	MaxChildren        *int
	ShortCircuitCutoff *int
	QueryName          string
	InnerHit           *InnerHit
}

// NewHasChildQuery creates a HasChildQuery for the given child type and inner query.
func NewHasChildQuery(childType string, query Query) *HasChildQuery {
	return &HasChildQuery{Type: childType, Query: query}
}

// WithBoost sets the boost factor for the query.
func (q *HasChildQuery) WithBoost(boost float64) *HasChildQuery {
	q.Boost = &boost
	return q
}

// WithScoreMode sets how child scores are aggregated into the parent score.
func (q *HasChildQuery) WithScoreMode(scoreMode string) *HasChildQuery {
	q.ScoreMode = scoreMode
	return q
}

// WithMinChildren sets the minimum number of matching child documents required.
func (q *HasChildQuery) WithMinChildren(minChildren int) *HasChildQuery {
	q.MinChildren = &minChildren
	return q
}

// WithMaxChildren sets the maximum number of matching child documents allowed.
func (q *HasChildQuery) WithMaxChildren(maxChildren int) *HasChildQuery {
	q.MaxChildren = &maxChildren
	return q
}

// WithShortCircuitCutoff sets the short_circuit_cutoff value for the query.
func (q *HasChildQuery) WithShortCircuitCutoff(cutoff int) *HasChildQuery {
	q.ShortCircuitCutoff = &cutoff
	return q
}

// WithQueryName sets the optional query name for identification in responses.
func (q *HasChildQuery) WithQueryName(queryName string) *HasChildQuery {
	q.QueryName = queryName
	return q
}

// WithInnerHit sets the inner_hits configuration to return matched child documents.
func (q *HasChildQuery) WithInnerHit(innerHit *InnerHit) *HasChildQuery {
	q.InnerHit = innerHit
	return q
}

func (q HasChildQuery) Source() (any, error) {
	src, err := q.Query.Source()
	if err != nil {
		return nil, err
	}
	body := map[string]any{"query": src, "type": q.Type}
	if q.Boost != nil {
		body["boost"] = *q.Boost
	}
	if q.ScoreMode != "" {
		body["score_mode"] = q.ScoreMode
	}
	if q.MinChildren != nil {
		body["min_children"] = *q.MinChildren
	}
	if q.MaxChildren != nil {
		body["max_children"] = *q.MaxChildren
	}
	if q.ShortCircuitCutoff != nil {
		body["short_circuit_cutoff"] = *q.ShortCircuitCutoff
	}
	if q.QueryName != "" {
		body["_name"] = q.QueryName
	}
	if q.InnerHit != nil {
		ih, err := q.InnerHit.Source()
		if err != nil {
			return nil, err
		}
		body["inner_hits"] = ih
	}
	return map[string]any{"has_child": body}, nil
}
