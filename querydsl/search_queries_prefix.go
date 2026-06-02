package querydsl

type prefixInner struct {
	Value           string   `json:"value"`
	Boost           *float64 `json:"boost,omitempty"`
	Rewrite         string   `json:"rewrite,omitempty"`
	CaseInsensitive *bool    `json:"case_insensitive,omitempty"`
	QueryName       string   `json:"_name,omitempty"`
}

// PrefixQuery matches documents whose field values begin with the given prefix.
// It corresponds to the OpenSearch prefix query in JSON DSL:
//
//	{"prefix": {"field": "pre"}}
//	{"prefix": {"field": {"value": "pre", "boost": 1.5}}}
//
// The compact form is used when no optional parameters (boost, rewrite,
// case_insensitive, _name) are set.
//
// Note: prefix queries are not analyzed. The prefix is matched literally
// against the indexed terms. For analyzed queries, use [MatchBoolPrefixQuery] instead.
//
// Use NewPrefixQuery(field, prefix) to create an instance with the required
// field and prefix value.
type PrefixQuery struct {
	Field string
	prefixInner
}

// NewPrefixQuery creates a PrefixQuery for the given field and prefix value.
func NewPrefixQuery(field, prefix string) *PrefixQuery {
	return &PrefixQuery{Field: field, prefixInner: prefixInner{Value: prefix}}
}

// WithBoost sets the boost factor for this query.
func (q *PrefixQuery) WithBoost(boost float64) *PrefixQuery {
	q.Boost = &boost
	return q
}

// WithRewrite sets the rewrite method used to score prefix matching terms.
func (q *PrefixQuery) WithRewrite(rewrite string) *PrefixQuery {
	q.Rewrite = rewrite
	return q
}

// WithCaseInsensitive sets whether the prefix match is case-insensitive.
func (q *PrefixQuery) WithCaseInsensitive(caseInsensitive bool) *PrefixQuery {
	q.CaseInsensitive = &caseInsensitive
	return q
}

// WithQueryName sets the query name for identification in search responses.
func (q *PrefixQuery) WithQueryName(queryName string) *PrefixQuery {
	q.QueryName = queryName
	return q
}

func (q PrefixQuery) Source() (any, error) {
	if q.Boost == nil && q.Rewrite == "" && q.QueryName == "" && q.CaseInsensitive == nil {
		return map[string]any{"prefix": map[string]any{q.Field: q.Value}}, nil
	}
	inner, _ := marshalStruct(q.prefixInner)
	return map[string]any{"prefix": map[string]any{q.Field: inner}}, nil
}
