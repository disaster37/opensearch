package querydsl

// IdsQuery matches documents based on their _id values. It corresponds to
// the OpenSearch ids query in JSON DSL:
//
//	{"ids": {"values": ["1", "2", "3"]}}
//
// Use NewIdsQuery(values...) to create an instance for one or more document
// IDs.
type IdsQuery struct {
	Values    []string `json:"values"`
	Boost     *float64 `json:"boost,omitempty"`
	QueryName string   `json:"_name,omitempty"`
}

// NewIdsQuery creates an IdsQuery that matches documents whose _id is one
// of the provided string values.
func NewIdsQuery(values ...string) *IdsQuery {
	return &IdsQuery{Values: values}
}

// WithBoost sets the boost factor for this query.
func (q *IdsQuery) WithBoost(boost float64) *IdsQuery {
	q.Boost = &boost
	return q
}

// WithQueryName sets the query name for identification in search responses.
func (q *IdsQuery) WithQueryName(queryName string) *IdsQuery {
	q.QueryName = queryName
	return q
}

func (q IdsQuery) Source() (any, error) {
	body, _ := marshalStruct(q)
	return map[string]any{"ids": body}, nil
}
