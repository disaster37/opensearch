// Copyright 2012-present Oliver Eilhard. All rights reserved.
// Use of this source code is governed by a MIT-license.
// See http://olivere.mit-license.org/license.txt for details.

package opensearch

// MatchPhraseQuery analyzes the text and creates a phrase query out of
// the analyzed text.
//
// For more details, see
// https://www.opensearch.co/guide/en/opensearchsearch/reference/7.0/query-dsl-match-query-phrase.html
type MatchPhraseQuery struct {
	name           string
	value          interface{}
	analyzer       string
	slop           *int
	boost          *float64
	queryName      string
	zeroTermsQuery string
}

// NewMatchPhraseQuery creates and initializes a new MatchPhraseQuery.
func NewMatchPhraseQuery(name string, value interface{}) *MatchPhraseQuery {
	return &MatchPhraseQuery{name: name, value: value}
}

// Analyzer explicitly sets the analyzer to use. It defaults to use explicit
// mapping config for the field, or, if not set, the default search analyzer.
func (q *MatchPhraseQuery) Analyzer(analyzer string) *MatchPhraseQuery {
	q.analyzer = analyzer
	return q
}

// Slop sets the phrase slop if evaluated to a phrase query type.
func (q *MatchPhraseQuery) Slop(slop int) *MatchPhraseQuery {
	q.slop = &slop
	return q
}

// ZeroTermsQuery can be "all" or "none".
func (q *MatchPhraseQuery) ZeroTermsQuery(zeroTermsQuery string) *MatchPhraseQuery {
	q.zeroTermsQuery = zeroTermsQuery
	return q
}

// Boost sets the boost to apply to this query.
func (q *MatchPhraseQuery) Boost(boost float64) *MatchPhraseQuery {
	q.boost = &boost
	return q
}

// QueryName sets the query name for the filter that can be used when
// searching for matched filters per hit.
func (q *MatchPhraseQuery) QueryName(queryName string) *MatchPhraseQuery {
	q.queryName = queryName
	return q
}

// Source returns JSON for the function score query.
func (q *MatchPhraseQuery) Source() (interface{}, error) {
	// {"match_phrase":{"name":{"query":"value","analyzer":"my_analyzer"}}}
	source := make(map[string]interface{})

	match := make(map[string]interface{})
	source["match_phrase"] = match

	query := make(map[string]interface{})
	match[q.name] = query

	query["query"] = q.value

	if q.analyzer != "" {
		query["analyzer"] = q.analyzer
	}
	if q.slop != nil {
		query["slop"] = *q.slop
	}
	if q.zeroTermsQuery != "" {
		query["zero_terms_query"] = q.zeroTermsQuery
	}
	if q.boost != nil {
		query["boost"] = *q.boost
	}
	if q.queryName != "" {
		query["_name"] = q.queryName
	}

	return source, nil
}
