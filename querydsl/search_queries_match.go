package querydsl

// MatchQuery is a full-text query that analyzes the provided query text
// and constructs a query from the result. It is the standard query for
// performing full-text search in OpenSearch, corresponding to the JSON DSL:
//
//	{"match": {"field": "query text"}}
//	{"match": {"field": {"query": "query text", "operator": "and"}}}
//
// When only a single query value is provided and no extra parameters are
// set, the compact form (first example) is emitted. Otherwise the full
// object form with additional parameters is used.
//
// Use NewMatchQuery(field, query) to create an instance with the required
// field and query value, then chain option methods (e.g. Operator, Fuzziness,
// Boost) before calling Source.
type MatchQuery struct {
	Field               string
	Query               any      `json:"query,omitempty"`
	Analyzer            string   `json:"analyzer,omitempty"`
	Operator            string   `json:"operator,omitempty"`
	Fuzziness           string   `json:"fuzziness,omitempty"`
	PrefixLength        *int     `json:"prefix_length,omitempty"`
	MaxExpansions       *int     `json:"max_expansions,omitempty"`
	MinimumShouldMatch  string   `json:"minimum_should_match,omitempty"`
	FuzzyRewrite        string   `json:"fuzzy_rewrite,omitempty"`
	Lenient             *bool    `json:"lenient,omitempty"`
	FuzzyTranspositions *bool    `json:"fuzzy_transpositions,omitempty"`
	ZeroTermsQuery      string   `json:"zero_terms_query,omitempty"`
	CutoffFrequency     *float64 `json:"cutoff_frequency,omitempty"`
	Boost               *float64 `json:"boost,omitempty"`
	QueryName           string   `json:"_name,omitempty"`
}

// NewMatchQuery creates a MatchQuery for the given field and query value.
// The query value is typically a string, but any type accepted by OpenSearch
// is accepted here (int, float, bool, etc.).
func NewMatchQuery(field string, query any) *MatchQuery {
	return &MatchQuery{Field: field, Query: query}
}

// WithAnalyzer sets the analyzer used to analyze the query text.
func (q *MatchQuery) WithAnalyzer(analyzer string) *MatchQuery {
	q.Analyzer = analyzer
	return q
}

// WithOperator sets the boolean logic used to interpret query terms ("or" or "and").
func (q *MatchQuery) WithOperator(operator string) *MatchQuery {
	q.Operator = operator
	return q
}

// WithFuzziness sets the maximum edit distance for fuzzy matching (e.g., "AUTO", "1", "2").
func (q *MatchQuery) WithFuzziness(fuzziness string) *MatchQuery {
	q.Fuzziness = fuzziness
	return q
}

// WithPrefixLength sets the number of leading characters that must match exactly for fuzzy matching.
func (q *MatchQuery) WithPrefixLength(prefixLength int) *MatchQuery {
	q.PrefixLength = &prefixLength
	return q
}

// WithMaxExpansions sets the maximum number of terms the query can expand to.
func (q *MatchQuery) WithMaxExpansions(maxExpansions int) *MatchQuery {
	q.MaxExpansions = &maxExpansions
	return q
}

// WithMinimumShouldMatch sets the minimum number of optional clauses that must match.
func (q *MatchQuery) WithMinimumShouldMatch(minimumShouldMatch string) *MatchQuery {
	q.MinimumShouldMatch = minimumShouldMatch
	return q
}

// WithFuzzyRewrite sets the rewrite method used to score fuzzy matching terms.
func (q *MatchQuery) WithFuzzyRewrite(fuzzyRewrite string) *MatchQuery {
	q.FuzzyRewrite = fuzzyRewrite
	return q
}

// WithLenient sets whether format-based errors are ignored (e.g., querying a numeric field with text).
func (q *MatchQuery) WithLenient(lenient bool) *MatchQuery {
	q.Lenient = &lenient
	return q
}

// WithFuzzyTranspositions sets whether transpositions count as a single edit for fuzzy matching.
func (q *MatchQuery) WithFuzzyTranspositions(fuzzyTranspositions bool) *MatchQuery {
	q.FuzzyTranspositions = &fuzzyTranspositions
	return q
}

// WithZeroTermsQuery sets the behavior when the analyzer removes all tokens ("none" or "all").
func (q *MatchQuery) WithZeroTermsQuery(zeroTermsQuery string) *MatchQuery {
	q.ZeroTermsQuery = zeroTermsQuery
	return q
}

// WithCutoffFrequency sets the cutoff frequency for high-frequency terms.
func (q *MatchQuery) WithCutoffFrequency(cutoffFrequency float64) *MatchQuery {
	q.CutoffFrequency = &cutoffFrequency
	return q
}

// WithBoost sets the boost factor for this query.
func (q *MatchQuery) WithBoost(boost float64) *MatchQuery {
	q.Boost = &boost
	return q
}

// WithQueryName sets the query name for identification in search responses.
func (q *MatchQuery) WithQueryName(queryName string) *MatchQuery {
	q.QueryName = queryName
	return q
}

func (q MatchQuery) Source() (any, error) {
	body, _ := marshalStruct(q)
	m, _ := body.(map[string]any)
	delete(m, "Field")
	if len(m) == 1 {
		if v, ok := m["query"]; ok {
			return map[string]any{"match": map[string]any{q.Field: v}}, nil
		}
	}
	return map[string]any{"match": map[string]any{q.Field: m}}, nil
}
