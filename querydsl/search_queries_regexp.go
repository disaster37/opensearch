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
func NewRegexpQuery(field, regexp string) *RegexpQuery {
	return &RegexpQuery{Field: field, regexpInner: regexpInner{Value: regexp}}
}

// WithFlags sets the regular expression flags.
func (q *RegexpQuery) WithFlags(flags string) *RegexpQuery {
	q.Flags = flags
	return q
}

// WithBoost sets the boost factor for this query.
func (q *RegexpQuery) WithBoost(boost float64) *RegexpQuery {
	q.Boost = &boost
	return q
}

// WithRewrite sets the rewrite method used to score regexp matching terms.
func (q *RegexpQuery) WithRewrite(rewrite string) *RegexpQuery {
	q.Rewrite = rewrite
	return q
}

// WithCaseInsensitive sets whether the regexp match is case-insensitive.
func (q *RegexpQuery) WithCaseInsensitive(caseInsensitive bool) *RegexpQuery {
	q.CaseInsensitive = &caseInsensitive
	return q
}

// WithMaxDeterminizedStates sets the maximum number of automaton states required for the query.
func (q *RegexpQuery) WithMaxDeterminizedStates(maxDeterminizedStates int) *RegexpQuery {
	q.MaxDeterminizedStates = &maxDeterminizedStates
	return q
}

// WithQueryName sets the query name for identification in search responses.
func (q *RegexpQuery) WithQueryName(queryName string) *RegexpQuery {
	q.QueryName = queryName
	return q
}

func (q RegexpQuery) Source() (any, error) {
	inner, _ := marshalStruct(q.regexpInner)
	return map[string]any{"regexp": map[string]any{q.Field: inner}}, nil
}
