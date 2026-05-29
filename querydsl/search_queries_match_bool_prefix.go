package querydsl

type MatchBoolPrefixQuery struct {
	Field               string
	Query               any
	Analyzer            string
	MinimumShouldMatch  string
	Operator            string
	Fuzziness           string
	PrefixLength        *int
	MaxExpansions       *int
	FuzzyTranspositions *bool
	FuzzyRewrite        string
	Boost               *float64
}

func NewMatchBoolPrefixQuery(field string, query any) MatchBoolPrefixQuery {
	return MatchBoolPrefixQuery{Field: field, Query: query}
}

func (q MatchBoolPrefixQuery) Source() (any, error) {
	body, _ := marshalStruct(q)
	m, _ := body.(map[string]any)
	delete(m, "field")
	return map[string]any{"match_bool_prefix": map[string]any{q.Field: m}}, nil
}
