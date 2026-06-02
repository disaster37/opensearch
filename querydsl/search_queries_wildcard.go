package querydsl

type wildcardInner struct {
	Value           string   `json:"value"`
	Boost           *float64 `json:"boost,omitempty"`
	Rewrite         string   `json:"rewrite,omitempty"`
	QueryName       string   `json:"_name,omitempty"`
	CaseInsensitive *bool    `json:"case_insensitive,omitempty"`
}

// WildcardQuery matches documents using a wildcard pattern. Supported
// wildcard operators are * (matches any character sequence) and ?
// (matches any single character). It corresponds to the OpenSearch wildcard
// query in JSON DSL:
//
//	{"wildcard": {"field": {"value": "ki*y"}}}
//
// Note: wildcard queries are not analyzed. The pattern is matched literally
// against the indexed terms. Avoid leading wildcards (e.g., *foo) as they
// require a full index scan and can be slow.
//
// Use NewWildcardQuery(field, wildcard) to create an instance with the
// required field and pattern.
type WildcardQuery struct {
	Field string
	wildcardInner
}

// NewWildcardQuery creates a WildcardQuery for the given field and pattern.
func NewWildcardQuery(field, wildcard string) *WildcardQuery {
	return &WildcardQuery{Field: field, wildcardInner: wildcardInner{Value: wildcard}}
}

// WithBoost sets the boost factor for this query.
func (q *WildcardQuery) WithBoost(boost float64) *WildcardQuery {
	q.Boost = &boost
	return q
}

// WithRewrite sets the rewrite method used to score wildcard matching terms.
func (q *WildcardQuery) WithRewrite(rewrite string) *WildcardQuery {
	q.Rewrite = rewrite
	return q
}

// WithQueryName sets the query name for identification in search responses.
func (q *WildcardQuery) WithQueryName(queryName string) *WildcardQuery {
	q.QueryName = queryName
	return q
}

// WithCaseInsensitive sets whether the wildcard match is case-insensitive.
func (q *WildcardQuery) WithCaseInsensitive(caseInsensitive bool) *WildcardQuery {
	q.CaseInsensitive = &caseInsensitive
	return q
}

func (q WildcardQuery) Source() (any, error) {
	inner, _ := marshalStruct(q.wildcardInner)
	return map[string]any{"wildcard": map[string]any{q.Field: inner}}, nil
}
