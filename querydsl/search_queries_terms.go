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
//
// Since OpenSearch 3.6.0 the values may be base64-encoded Roaring bitmaps
// (32-bit for integer fields, 64-bit for long fields) when ValueType is
// set to "bitmap" (PR #20606, "bitmap64").
type TermsQuery struct {
	Field       string
	Values      []any
	TermsLookup *TermsLookup
	// ValueType sets the value type hint for the terms values, e.g. "bitmap"
	// to interpret values as base64-encoded serialized Roaring bitmaps
	// (32-bit for integer fields, 64-bit for long fields since OpenSearch 3.6.0).
	ValueType string
	Boost     *float64
	QueryName string
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

// WithValueType sets the value type hint for the terms values, e.g. "bitmap"
// to interpret values as base64-encoded serialized Roaring bitmaps
// (32-bit for integer fields, 64-bit for long fields since OpenSearch 3.6.0).
func (q *TermsQuery) WithValueType(valueType string) *TermsQuery {
	q.ValueType = valueType
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
	if q.ValueType != "" {
		params["value_type"] = q.ValueType
	}
	return map[string]any{"terms": params}, nil
}
