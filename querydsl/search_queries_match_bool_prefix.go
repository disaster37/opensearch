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

func NewMatchBoolPrefixQuery(field string, query any) *MatchBoolPrefixQuery {
	return &MatchBoolPrefixQuery{Field: field, Query: query}
}

// WithAnalyzer sets the analyzer used to analyze the query text.
func (q *MatchBoolPrefixQuery) WithAnalyzer(analyzer string) *MatchBoolPrefixQuery {
	q.Analyzer = analyzer
	return q
}

// WithMinimumShouldMatch sets the minimum number of optional clauses that must match.
func (q *MatchBoolPrefixQuery) WithMinimumShouldMatch(minimumShouldMatch string) *MatchBoolPrefixQuery {
	q.MinimumShouldMatch = minimumShouldMatch
	return q
}

// WithOperator sets the boolean logic used to interpret query terms ("or" or "and").
func (q *MatchBoolPrefixQuery) WithOperator(operator string) *MatchBoolPrefixQuery {
	q.Operator = operator
	return q
}

// WithFuzziness sets the maximum edit distance for fuzzy matching.
func (q *MatchBoolPrefixQuery) WithFuzziness(fuzziness string) *MatchBoolPrefixQuery {
	q.Fuzziness = fuzziness
	return q
}

// WithPrefixLength sets the number of leading characters that must match exactly for fuzzy matching.
func (q *MatchBoolPrefixQuery) WithPrefixLength(prefixLength int) *MatchBoolPrefixQuery {
	q.PrefixLength = &prefixLength
	return q
}

// WithMaxExpansions sets the maximum number of terms the query can expand to.
func (q *MatchBoolPrefixQuery) WithMaxExpansions(maxExpansions int) *MatchBoolPrefixQuery {
	q.MaxExpansions = &maxExpansions
	return q
}

// WithFuzzyTranspositions sets whether transpositions count as a single edit for fuzzy matching.
func (q *MatchBoolPrefixQuery) WithFuzzyTranspositions(fuzzyTranspositions bool) *MatchBoolPrefixQuery {
	q.FuzzyTranspositions = &fuzzyTranspositions
	return q
}

// WithFuzzyRewrite sets the rewrite method used to score fuzzy matching terms.
func (q *MatchBoolPrefixQuery) WithFuzzyRewrite(fuzzyRewrite string) *MatchBoolPrefixQuery {
	q.FuzzyRewrite = fuzzyRewrite
	return q
}

// WithBoost sets the boost factor for this query.
func (q *MatchBoolPrefixQuery) WithBoost(boost float64) *MatchBoolPrefixQuery {
	q.Boost = &boost
	return q
}

func (q MatchBoolPrefixQuery) Source() (any, error) {
	body, _ := marshalStruct(q)
	m, _ := body.(map[string]any)
	delete(m, "field")
	return map[string]any{"match_bool_prefix": map[string]any{q.Field: m}}, nil
}
