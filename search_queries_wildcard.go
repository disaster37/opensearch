// Copyright 2012-present Oliver Eilhard. All rights reserved.
// Use of this source code is governed by a MIT-license.
// See http://olivere.mit-license.org/license.txt for details.

package opensearch

// WildcardQuery matches documents that have fields matching a wildcard
// expression (not analyzed). Supported wildcards are *, which matches
// any character sequence (including the empty one), and ?, which matches
// any single character. Note this query can be slow, as it needs to iterate
// over many terms. In order to prevent extremely slow wildcard queries,
// a wildcard term should not start with one of the wildcards * or ?.
// The wildcard query maps to Lucene WildcardQuery.
//
// For more details, see
// https://www.opensearch.co/guide/en/opensearchsearch/reference/7.x/query-dsl-wildcard-query.html
type WildcardQuery struct {
	name            string
	wildcard        string
	boost           *float64
	rewrite         string
	queryName       string
	caseInsensitive *bool
}

// NewWildcardQuery creates and initializes a new WildcardQuery.
func NewWildcardQuery(name, wildcard string) *WildcardQuery {
	return &WildcardQuery{
		name:     name,
		wildcard: wildcard,
	}
}

// Boost sets the boost for this query.
func (q *WildcardQuery) Boost(boost float64) *WildcardQuery {
	q.boost = &boost
	return q
}

func (q *WildcardQuery) Rewrite(rewrite string) *WildcardQuery {
	q.rewrite = rewrite
	return q
}

// QueryName sets the name of this query.
func (q *WildcardQuery) QueryName(queryName string) *WildcardQuery {
	q.queryName = queryName
	return q
}

// CaseInsensitive sets case insensitive matching of this query.
func (q *WildcardQuery) CaseInsensitive(caseInsensitive bool) *WildcardQuery {
	q.caseInsensitive = &caseInsensitive
	return q
}

// Source returns the JSON serializable body of this query.
func (q *WildcardQuery) Source() (interface{}, error) {
	// {
	//	"wildcard" : {
	//		"user" : {
	//      "value" : "ki*y",
	//      "boost" : 1.0,
	//      "case_insensitive" : true
	//    }
	// }

	source := make(map[string]interface{})

	query := make(map[string]interface{})
	source["wildcard"] = query

	wq := make(map[string]interface{})
	query[q.name] = wq

	wq["value"] = q.wildcard

	if q.boost != nil {
		wq["boost"] = *q.boost
	}
	if q.rewrite != "" {
		wq["rewrite"] = q.rewrite
	}
	if q.queryName != "" {
		wq["_name"] = q.queryName
	}
	if q.caseInsensitive != nil {
		wq["case_insensitive"] = *q.caseInsensitive
	}

	return source, nil
}
