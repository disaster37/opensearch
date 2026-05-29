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
func NewMatchPhrasePrefixQuery(field string, value any) MatchPhrasePrefixQuery {
	return MatchPhrasePrefixQuery{Field: field, Query: value}
}

func (q MatchPhrasePrefixQuery) Source() (any, error) {
	body, _ := marshalStruct(q)
	m, _ := body.(map[string]any)
	delete(m, "field")
	return map[string]any{"match_phrase_prefix": map[string]any{q.Field: m}}, nil
}
