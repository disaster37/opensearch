package querydsl

// HasParentQuery matches child documents whose parent document of the
// specified type matches the given query. It corresponds to the OpenSearch
// has_parent query in JSON DSL:
//
//	{"has_parent": {"parent_type": "question", "query": {...}, "score": true}}
//
// Key parameters:
//   - ParentType: the parent document type (join relation name).
//   - Query: the query executed against the parent document.
//   - Score: when true, the parent's relevance score is propagated to the
//     matching child document.
//   - InnerHit: optional [InnerHit] to return the matched parent document.
//   - IgnoreUnmapped: when true, unmapped parent types are skipped instead
//     of failing the query.
//
// Use NewHasParentQuery(parentType, query) to create an instance.
type HasParentQuery struct {
	Query          Query
	ParentType     string
	Boost          *float64
	Score          *bool
	QueryName      string
	InnerHit       *InnerHit
	IgnoreUnmapped *bool
}

// NewHasParentQuery creates a HasParentQuery for the given parent type and inner query.
func NewHasParentQuery(parentType string, query Query) HasParentQuery {
	return HasParentQuery{ParentType: parentType, Query: query}
}

func (q HasParentQuery) Source() (any, error) {
	src, err := q.Query.Source()
	if err != nil {
		return nil, err
	}
	body := map[string]any{"query": src, "parent_type": q.ParentType}
	if q.Boost != nil {
		body["boost"] = *q.Boost
	}
	if q.Score != nil {
		body["score"] = *q.Score
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
	if q.IgnoreUnmapped != nil {
		body["ignore_unmapped"] = *q.IgnoreUnmapped
	}
	return map[string]any{"has_parent": body}, nil
}
