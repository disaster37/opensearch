package querydsl

// SpanNearQuery matches spans that occur within a configurable distance of
// each other, optionally in order. It is the primary span query combinator
// for proximity-based phrase matching. It corresponds to the OpenSearch
// span_near query in JSON DSL:
//
//	{"span_near": {"clauses": [...], "slop": 5, "in_order": true}}
//
// Key parameters:
//   - Clauses: a slice of span queries whose spans must be near each other.
//   - Slop: the maximum number of intervening unmatched positions allowed.
//   - InOrder: when true, clauses must appear in document order; when false,
//     any order is acceptable.
//
// Use NewSpanNearQuery(clauses...) to create an instance with one or more
// inner span clauses.
type SpanNearQuery struct {
	Clauses   []Query
	Slop      *int
	InOrder   *bool
	Boost     *float64
	QueryName string
}

// NewSpanNearQuery creates a SpanNearQuery with the given span clauses.
func NewSpanNearQuery(clauses ...Query) SpanNearQuery {
	return SpanNearQuery{Clauses: clauses}
}

func (q SpanNearQuery) Source() (any, error) {
	c := map[string]any{}
	if len(q.Clauses) > 0 {
		clauses := make([]any, len(q.Clauses))
		for i, cl := range q.Clauses {
			src, err := cl.Source()
			if err != nil {
				return nil, err
			}
			clauses[i] = src
		}
		c["clauses"] = clauses
	}
	if q.Slop != nil {
		c["slop"] = *q.Slop
	}
	if q.InOrder != nil {
		c["in_order"] = *q.InOrder
	}
	if q.Boost != nil {
		c["boost"] = *q.Boost
	}
	if q.QueryName != "" {
		c["query_name"] = q.QueryName
	}
	return map[string]any{"span_near": c}, nil
}
