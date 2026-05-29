package querydsl

// MatchNoneQuery is the inverse of MatchAllQuery: it matches no documents.
// It corresponds to the OpenSearch match_none query in JSON DSL:
//
//	{"match_none": {}}
//
// Use NewMatchNoneQuery() to create an instance. It is typically used as a
// placeholder or in compound queries where a no-op clause is needed.
type MatchNoneQuery struct {
	QueryName string `json:"_name,omitempty"`
}

// NewMatchNoneQuery creates a MatchNoneQuery that matches no documents.
func NewMatchNoneQuery() MatchNoneQuery {
	return MatchNoneQuery{}
}

func (q MatchNoneQuery) Source() (any, error) {
	body, _ := marshalStruct(q)
	return map[string]any{"match_none": body}, nil
}
