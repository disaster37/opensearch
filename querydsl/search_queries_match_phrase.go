package querydsl

// MatchPhraseQuery matches documents that contain an exact phrase.
// Unlike [MatchQuery], it does not reorder tokens and matches them in sequence.
//
// Typical use: searching for exact strings like "the quick brown fox".
//
// JSON DSL output:
//
//	{
//	  "match_phrase": {
//	    "field_name": {
//	      "query": "search text"
//	    }
//	  }
//	}
type MatchPhraseQuery struct {
	Field          string
	Query          any
	Analyzer       string
	Slop           *int
	Boost          *float64
	QueryName      string
	ZeroTermsQuery string
}

// NewMatchPhraseQuery creates a new MatchPhraseQuery for the given field and value.
func NewMatchPhraseQuery(field string, value any) *MatchPhraseQuery {
	return &MatchPhraseQuery{Field: field, Query: value}
}

// WithAnalyzer sets the analyzer used to analyze the query text.
func (q *MatchPhraseQuery) WithAnalyzer(analyzer string) *MatchPhraseQuery {
	q.Analyzer = analyzer
	return q
}

// WithSlop sets the maximum number of positions allowed between matching tokens.
func (q *MatchPhraseQuery) WithSlop(slop int) *MatchPhraseQuery {
	q.Slop = &slop
	return q
}

// WithBoost sets the boost factor for this query.
func (q *MatchPhraseQuery) WithBoost(boost float64) *MatchPhraseQuery {
	q.Boost = &boost
	return q
}

// WithQueryName sets the query name for identification in search responses.
func (q *MatchPhraseQuery) WithQueryName(queryName string) *MatchPhraseQuery {
	q.QueryName = queryName
	return q
}

// WithZeroTermsQuery sets the behavior when the analyzer removes all tokens ("none" or "all").
func (q *MatchPhraseQuery) WithZeroTermsQuery(zeroTermsQuery string) *MatchPhraseQuery {
	q.ZeroTermsQuery = zeroTermsQuery
	return q
}

func (q MatchPhraseQuery) Source() (any, error) {
	body, _ := marshalStruct(q)
	m, _ := body.(map[string]any)
	delete(m, "field")
	return map[string]any{"match_phrase": map[string]any{q.Field: m}}, nil
}
