package querydsl

type fuzzyInner struct {
	Value          any      `json:"value"`
	Boost          *float64 `json:"boost,omitempty"`
	Fuzziness      any      `json:"fuzziness,omitempty"`
	PrefixLength   *int     `json:"prefix_length,omitempty"`
	MaxExpansions  *int     `json:"max_expansions,omitempty"`
	Transpositions *bool    `json:"transpositions,omitempty"`
	Rewrite        string   `json:"rewrite,omitempty"`
	QueryName      string   `json:"_name,omitempty"`
}

// FuzzyQuery matches documents containing terms within a specified edit
// distance of the given value, enabling typographical error tolerance.
// It corresponds to the OpenSearch fuzzy query in JSON DSL:
//
//	{"fuzzy": {"field": {"value": "kibana", "fuzziness": "AUTO"}}}
//
// Key parameters:
//   - Field: the indexed field to search.
//   - Value: the term to search for (any type).
//   - Fuzziness: the maximum Levenshtein edit distance ("AUTO", "0", "1", "2").
//   - PrefixLength: number of leading characters that must match exactly.
//   - MaxExpansions: maximum number of term variants to expand to.
//   - Transpositions: whether character swaps count as a single edit.
//
// Use NewFuzzyQuery(field, value) to create an instance with the required
// field and search value.
type FuzzyQuery struct {
	Field string
	fuzzyInner
}

// NewFuzzyQuery creates a FuzzyQuery for the given field and search value.
func NewFuzzyQuery(field string, value any) FuzzyQuery {
	return FuzzyQuery{Field: field, fuzzyInner: fuzzyInner{Value: value}}
}

func (q FuzzyQuery) Source() (any, error) {
	inner, _ := marshalStruct(q.fuzzyInner)
	return map[string]any{"fuzzy": map[string]any{q.Field: inner}}, nil
}
