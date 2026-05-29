package querydsl

type regexpInner struct {
	Value                 string   `json:"value"`
	Flags                 string   `json:"flags,omitempty"`
	Boost                 *float64 `json:"boost,omitempty"`
	Rewrite               string   `json:"rewrite,omitempty"`
	CaseInsensitive       *bool    `json:"case_insensitive,omitempty"`
	MaxDeterminizedStates *int     `json:"max_determinized_states,omitempty"`
	QueryName             string   `json:"name,omitempty"`
}

// RegexpQuery matches documents whose field values match the given regular
// expression. It corresponds to the OpenSearch regexp query in JSON DSL:
//
//	{"regexp": {"field": {"value": "[a-z]+\\d{3}"}}}
//
// Optional parameters include Flags, Boost, Rewrite, CaseInsensitive, and
// MaxDeterminizedStates. Use MaxDeterminizedStates to limit the complexity
// of the expanded automaton and prevent expensive queries.
//
// Use NewRegexpQuery(field, regexp) to create an instance with the required
// field and regular expression.
type RegexpQuery struct {
	Field string
	regexpInner
}

// NewRegexpQuery creates a RegexpQuery for the given field and regular expression.
func NewRegexpQuery(field, regexp string) RegexpQuery {
	return RegexpQuery{Field: field, regexpInner: regexpInner{Value: regexp}}
}

func (q RegexpQuery) Source() (any, error) {
	inner, _ := marshalStruct(q.regexpInner)
	return map[string]any{"regexp": map[string]any{q.Field: inner}}, nil
}
