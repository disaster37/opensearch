package querydsl

// MatchPhrasePrefixQuery matches documents that contain the words of a phrase in order,
// with the last term treated as a prefix. This is useful for autocomplete functionality.
//
// Typical use: search-as-you-type suggestions like "quick brown fo" matching "quick brown fox".
//
// JSON DSL output:
//
//	{
//	  "match_phrase_prefix": {
//	    "field_name": {
//	      "query": "quick brown fo"
//	    }
//	  }
//	}
type MatchPhrasePrefixQuery struct {
	Field         string
	Query         any
	Analyzer      string
	Slop          *int
	MaxExpansions *int
	Boost         *float64
	QueryName     string
}

// NewMatchPhrasePrefixQuery creates a new MatchPhrasePrefixQuery for the given field and value.
func NewMatchPhrasePrefixQuery(field string, value any) *MatchPhrasePrefixQuery {
	return &MatchPhrasePrefixQuery{Field: field, Query: value}
}

// WithAnalyzer sets the analyzer used to analyze the query text.
func (q *MatchPhrasePrefixQuery) WithAnalyzer(analyzer string) *MatchPhrasePrefixQuery {
	q.Analyzer = analyzer
	return q
}

// WithSlop sets the maximum number of positions allowed between matching tokens.
func (q *MatchPhrasePrefixQuery) WithSlop(slop int) *MatchPhrasePrefixQuery {
	q.Slop = &slop
	return q
}

// WithMaxExpansions sets the maximum number of terms the last token can expand to.
func (q *MatchPhrasePrefixQuery) WithMaxExpansions(maxExpansions int) *MatchPhrasePrefixQuery {
	q.MaxExpansions = &maxExpansions
	return q
}

// WithBoost sets the boost factor for this query.
func (q *MatchPhrasePrefixQuery) WithBoost(boost float64) *MatchPhrasePrefixQuery {
	q.Boost = &boost
	return q
}

// WithQueryName sets the query name for identification in search responses.
func (q *MatchPhrasePrefixQuery) WithQueryName(queryName string) *MatchPhrasePrefixQuery {
	q.QueryName = queryName
	return q
}

func (q MatchPhrasePrefixQuery) Source() (any, error) {
	body, _ := marshalStruct(q)
	m, _ := body.(map[string]any)
	delete(m, "field")
	return map[string]any{"match_phrase_prefix": map[string]any{q.Field: m}}, nil
}
