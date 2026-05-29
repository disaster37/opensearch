package querydsl

// NestedQuery wraps another query to target fields inside a nested object
// array. Documents are matched only when the wrapped query matches at least
// one nested object within the specified path. It corresponds to the
// OpenSearch nested query in JSON DSL:
//
//	{"nested": {"path": "comments", "query": {...}, "score_mode": "avg"}}
//
// Key parameters:
//   - Path: the dot-separated path to the nested field.
//   - Query: the inner query executed against each nested object.
//   - ScoreMode: how scores from matching nested docs are combined
//     ("avg", "max", "min", "sum", "none").
//   - InnerHit: optional [InnerHit] to return matched nested objects in results.
//   - IgnoreUnmapped: when true, documents without the nested mapping are
//     skipped instead of failing the query.
//
// Use NewNestedQuery(path, query) to create an instance.
type NestedQuery struct {
	Query          Query
	Path           string
	ScoreMode      string
	Boost          *float64
	QueryName      string
	InnerHit       *InnerHit
	IgnoreUnmapped *bool
}

// NewNestedQuery creates a NestedQuery for the given path and inner query.
func NewNestedQuery(path string, query Query) NestedQuery {
	return NestedQuery{Path: path, Query: query}
}

func (q NestedQuery) Source() (any, error) {
	src, err := q.Query.Source()
	if err != nil {
		return nil, err
	}
	nq := map[string]any{"query": src, "path": q.Path}
	if q.ScoreMode != "" {
		nq["score_mode"] = q.ScoreMode
	}
	if q.Boost != nil {
		nq["boost"] = *q.Boost
	}
	if q.QueryName != "" {
		nq["_name"] = q.QueryName
	}
	if q.IgnoreUnmapped != nil {
		nq["ignore_unmapped"] = *q.IgnoreUnmapped
	}
	if q.InnerHit != nil {
		ih, err := q.InnerHit.Source()
		if err != nil {
			return nil, err
		}
		nq["inner_hits"] = ih
	}
	return map[string]any{"nested": nq}, nil
}
