package querydsl

// MatchAllQuery is the simplest query: it matches every document in the
// index, assigning a relevance score of 1.0 (or the optional Boost value).
// It corresponds to the OpenSearch match_all query in JSON DSL:
//
//	{"match_all": {}}
//	{"match_all": {"boost": 1.2}}
//
// Use NewMatchAllQuery() to create an instance. It is commonly used as the
// default query when no other query is specified, or as a placeholder in
// compound queries.
type MatchAllQuery struct {
	Boost     *float64 `json:"boost,omitempty"`
	QueryName string   `json:"_name,omitempty"`
}

// NewMatchAllQuery creates a MatchAllQuery that matches every document.
func NewMatchAllQuery() *MatchAllQuery {
	return &MatchAllQuery{}
}

// WithBoost sets the boost factor for this query.
func (q *MatchAllQuery) WithBoost(boost float64) *MatchAllQuery {
	q.Boost = &boost
	return q
}

// WithQueryName sets the query name for identification in search responses.
func (q *MatchAllQuery) WithQueryName(queryName string) *MatchAllQuery {
	q.QueryName = queryName
	return q
}

func (q MatchAllQuery) Source() (any, error) {
	body, _ := marshalStruct(q)
	return map[string]any{"match_all": body}, nil
}
