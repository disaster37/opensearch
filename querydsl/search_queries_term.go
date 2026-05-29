package querydsl

// TermQuery finds documents that contain the exact term specified in the
// inverted index. Unlike MatchQuery, no analysis is performed on the query
// value. It corresponds to the OpenSearch term query in JSON DSL:
//
//	{"term": {"field": "value"}}
//	{"term": {"field": {"value": "x", "boost": 1.5}}}
//
// The compact form is used when no optional parameters (boost, case_insensitive,
// _name) are set.
//
// Use NewTermQuery(field, value) to create an instance with the required
// field and value.
type TermQuery struct {
	Field           string
	Value           any
	Boost           *float64
	CaseInsensitive *bool
	QueryName       string
}

// NewTermQuery creates a TermQuery for the given field and exact-match value.
func NewTermQuery(field string, value any) TermQuery {
	return TermQuery{Field: field, Value: value}
}

func (q TermQuery) Source() (any, error) {
	if q.Boost == nil && q.CaseInsensitive == nil && q.QueryName == "" {
		return map[string]any{"term": map[string]any{q.Field: q.Value}}, nil
	}
	sub := map[string]any{"value": q.Value}
	if q.Boost != nil {
		sub["boost"] = *q.Boost
	}
	if q.CaseInsensitive != nil {
		sub["case_insensitive"] = *q.CaseInsensitive
	}
	if q.QueryName != "" {
		sub["_name"] = q.QueryName
	}
	return map[string]any{"term": map[string]any{q.Field: sub}}, nil
}
