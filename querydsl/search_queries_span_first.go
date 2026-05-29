package querydsl

// SpanFirstQuery matches spans that appear near the beginning of a field.
// It wraps an inner span query via Match and constrains the end position
// to be less than or equal to End. It corresponds to the OpenSearch
// span_first query in JSON DSL:
//
//	{"span_first": {"match": {"span_term": {"field": "quick"}}, "end": 3}}
//
// Key parameters:
//   - Match: the inner span query (typically a [SpanTermQuery]).
//   - End: the maximum end position (inclusive) for a match.
//
// Use NewSpanFirstQuery(query, end) to create an instance.
type SpanFirstQuery struct {
	Match     Query
	End       int
	Boost     *float64
	QueryName string
}

// NewSpanFirstQuery creates a SpanFirstQuery wrapping the given inner span
// query with the specified maximum end position.
func NewSpanFirstQuery(query Query, end int) SpanFirstQuery {
	return SpanFirstQuery{Match: query, End: end}
}

func (q SpanFirstQuery) Source() (any, error) {
	c := map[string]any{"end": q.End}
	if q.Match != nil {
		src, err := q.Match.Source()
		if err != nil {
			return nil, err
		}
		c["match"] = src
	}
	if q.Boost != nil {
		c["boost"] = *q.Boost
	}
	if q.QueryName != "" {
		c["query_name"] = q.QueryName
	}
	return map[string]any{"span_first": c}, nil
}
