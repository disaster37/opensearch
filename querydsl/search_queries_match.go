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
func NewMatchQuery(field string, query any) MatchQuery {
	return MatchQuery{Field: field, Query: query}
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
