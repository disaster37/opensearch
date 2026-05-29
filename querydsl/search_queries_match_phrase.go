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
func NewMatchPhraseQuery(field string, value any) MatchPhraseQuery {
	return MatchPhraseQuery{Field: field, Query: value}
}

func (q MatchPhraseQuery) Source() (any, error) {
	body, _ := marshalStruct(q)
	m, _ := body.(map[string]any)
	delete(m, "field")
	return map[string]any{"match_phrase": map[string]any{q.Field: m}}, nil
}
