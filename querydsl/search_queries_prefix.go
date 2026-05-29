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
func NewPrefixQuery(field, prefix string) PrefixQuery {
	return PrefixQuery{Field: field, prefixInner: prefixInner{Value: prefix}}
}

func (q PrefixQuery) Source() (any, error) {
	if q.Boost == nil && q.Rewrite == "" && q.QueryName == "" && q.CaseInsensitive == nil {
		return map[string]any{"prefix": map[string]any{q.Field: q.Value}}, nil
	}
	inner, _ := marshalStruct(q.prefixInner)
	return map[string]any{"prefix": map[string]any{q.Field: inner}}, nil
}
