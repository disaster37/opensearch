package querydsl

// TermsQuery filters documents that contain any of the specified terms in
// a given field. It is the multi-value equivalent of TermQuery and
// corresponds to the OpenSearch terms query in JSON DSL:
//
//	{"terms": {"field": ["value1", "value2"]}}
//	{"terms": {"field": {"index": "...", "id": "...", "path": "..."}}}  // terms lookup
//
// Use NewTermsQuery(field, values...) for general term values or
// NewTermsQueryFromStrings(field, values...) when you have a []string.
type TermsQuery struct {
	Field       string
	Values      []any
	TermsLookup *TermsLookup
	Boost       *float64
	QueryName   string
}

// NewTermsQuery creates a TermsQuery for the given field and one or more
// term values of any type.
func NewTermsQuery(field string, values ...any) *TermsQuery {
	return &TermsQuery{Field: field, Values: values}
}

// NewTermsQueryFromStrings creates a TermsQuery for the given field using
// string values, converting them to the []any representation required
// by the JSON DSL.
func NewTermsQueryFromStrings(field string, values ...string) *TermsQuery {
	vs := make([]any, len(values))
	for i, v := range values {
		vs[i] = v
	}
	return &TermsQuery{Field: field, Values: vs}
}

// WithTermsLookup sets a terms lookup to fetch term values from another document.
func (q *TermsQuery) WithTermsLookup(lookup *TermsLookup) *TermsQuery {
	q.TermsLookup = lookup
	return q
}

// WithBoost sets the boost factor for this query.
func (q *TermsQuery) WithBoost(boost float64) *TermsQuery {
	q.Boost = &boost
	return q
}

// WithQueryName sets the query name for identification in search responses.
func (q *TermsQuery) WithQueryName(queryName string) *TermsQuery {
	q.QueryName = queryName
	return q
}

func (q TermsQuery) Source() (any, error) {
	params := map[string]any{}
	if q.TermsLookup != nil {
		src, _ := q.TermsLookup.Source()
		params[q.Field] = src
	} else {
		params[q.Field] = q.Values
		if q.Boost != nil {
			params["boost"] = *q.Boost
		}
		if q.QueryName != "" {
			params["_name"] = q.QueryName
		}
	}
	return map[string]any{"terms": params}, nil
}
