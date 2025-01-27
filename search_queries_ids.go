// Copyright 2012-present Oliver Eilhard. All rights reserved.
// Use of this source code is governed by a MIT-license.
// See http://olivere.mit-license.org/license.txt for details.

package opensearch

// IdsQuery filters documents that only have the provided ids.
// Note, this query uses the _uid field.
//
// For more details, see
// https://www.opensearch.co/guide/en/opensearchsearch/reference/7.6/query-dsl-ids-query.html
type IdsQuery struct {
	types     []string
	values    []string
	boost     *float64
	queryName string
}

// NewIdsQuery creates and initializes a new ids query.
//
// Notice that types are in the process of being removed.
// You should filter on a field instead.
func NewIdsQuery(types ...string) *IdsQuery {
	return &IdsQuery{
		types:  types,
		values: make([]string, 0),
	}
}

// Ids adds ids to the filter.
func (q *IdsQuery) Ids(ids ...string) *IdsQuery {
	q.values = append(q.values, ids...)
	return q
}

// Boost sets the boost for this query.
func (q *IdsQuery) Boost(boost float64) *IdsQuery {
	q.boost = &boost
	return q
}

// QueryName sets the query name for the filter.
func (q *IdsQuery) QueryName(queryName string) *IdsQuery {
	q.queryName = queryName
	return q
}

// Source returns JSON for the function score query.
func (q *IdsQuery) Source() (interface{}, error) {
	// {
	//	"ids" : {
	//		"type" : "my_type",
	//		"values" : ["1", "4", "100"]
	//	}
	// }

	source := make(map[string]interface{})
	query := make(map[string]interface{})
	source["ids"] = query

	// type(s)
	if len(q.types) == 1 {
		query["type"] = q.types[0]
	} else if len(q.types) > 1 {
		query["types"] = q.types
	}

	// values
	query["values"] = q.values

	if q.boost != nil {
		query["boost"] = *q.boost
	}
	if q.queryName != "" {
		query["_name"] = q.queryName
	}

	return source, nil
}
