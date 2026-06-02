package querydsl

type spanTermInner struct {
	Value     any      `json:"value"`
	Boost     *float64 `json:"boost,omitempty"`
	QueryName string   `json:"query_name,omitempty"`
}

// SpanTermQuery matches spans containing a single term. It is the span-query
// building block used inside [SpanNearQuery], [SpanFirstQuery], and other
// compound span queries. It corresponds to the OpenSearch span_term query
// in JSON DSL:
//
//	{"span_term": {"field": {"value": "quick"}}}
//
// Use NewSpanTermQuery(field, value) to create an instance with the required
// field and term value.
type SpanTermQuery struct {
	Field string
	spanTermInner
}

// NewSpanTermQuery creates a SpanTermQuery for the given field; value is
// optional and should be provided as the first variadic argument.
func NewSpanTermQuery(field string, value ...any) *SpanTermQuery {
	q := &SpanTermQuery{Field: field}
	if len(value) > 0 {
		q.Value = value[0]
	}
	return q
}

// WithValue sets the term value for this span term query.
func (q *SpanTermQuery) WithValue(value any) *SpanTermQuery {
	q.Value = value
	return q
}

// WithBoost sets the boost factor for this query.
func (q *SpanTermQuery) WithBoost(boost float64) *SpanTermQuery {
	q.Boost = &boost
	return q
}

// WithQueryName sets the query name used for matched_filters per hit.
func (q *SpanTermQuery) WithQueryName(name string) *SpanTermQuery {
	q.QueryName = name
	return q
}

func (q SpanTermQuery) Source() (any, error) {
	inner, _ := marshalStruct(q.spanTermInner)
	return map[string]any{
		"span_term": map[string]any{q.Field: inner},
	}, nil
}
