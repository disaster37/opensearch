package querydsl

// ExistsQuery matches documents where at least one non-null value is
// indexed in the specified field. It corresponds to the OpenSearch exists
// query in JSON DSL:
//
//	{"exists": {"field": "field_name"}}
//
// Use NewExistsQuery(field) to create an instance for the given field name.
//
// See: https://opensearch.org/docs/latest/query-dsl/term/exists/
type ExistsQuery struct {
	Field     string `json:"field"`
	QueryName string `json:"_name,omitempty"`
}

// NewExistsQuery creates an ExistsQuery that matches documents having at
// least one non-null value in the given field.
func NewExistsQuery(field string) *ExistsQuery {
	return &ExistsQuery{Field: field}
}

// WithQueryName sets the query name for identification in search responses.
func (q *ExistsQuery) WithQueryName(queryName string) *ExistsQuery {
	q.QueryName = queryName
	return q
}

func (q ExistsQuery) Source() (any, error) {
	body, _ := marshalStruct(q)
	return map[string]any{"exists": body}, nil
}
