// Copyright 2012-present Oliver Eilhard. All rights reserved.
// Use of this source code is governed by a MIT-license.
// See http://olivere.mit-license.org/license.txt for details.

package opensearch

// CommonTermsQuery is a modern alternative to stopwords
// which improves the precision and recall of search results
// (by taking stopwords into account), without sacrificing performance.
// For more details, see:
// https://www.opensearch.co/guide/en/opensearchsearch/reference/7.4/query-dsl-common-terms-query.html
//
// Deprecated: Use Match query instead (7.3.0+), which skips blocks of
// documents efficiently, without any configuration, provided that the
// total number of hits is not tracked.
type CommonTermsQuery struct {
	Query
	name                       string
	text                       interface{}
	cutoffFreq                 *float64
	highFreq                   *float64
	highFreqOp                 string
	highFreqMinimumShouldMatch string
	lowFreq                    *float64
	lowFreqOp                  string
	lowFreqMinimumShouldMatch  string
	analyzer                   string
	boost                      *float64
	queryName                  string
}

// NewCommonTermsQuery creates and initializes a new common terms query.
//
// Deprecated: Common Terms Query was deprecated in >= 7.3.0. See https://www.opensearch.co/guide/en/opensearchsearch/reference/current/query-dsl-common-terms-query.html.
func NewCommonTermsQuery(name string, text interface{}) *CommonTermsQuery {
	return &CommonTermsQuery{name: name, text: text}
}

func (q *CommonTermsQuery) CutoffFrequency(f float64) *CommonTermsQuery {
	q.cutoffFreq = &f
	return q
}

func (q *CommonTermsQuery) HighFreq(f float64) *CommonTermsQuery {
	q.highFreq = &f
	return q
}

func (q *CommonTermsQuery) HighFreqOperator(op string) *CommonTermsQuery {
	q.highFreqOp = op
	return q
}

func (q *CommonTermsQuery) HighFreqMinimumShouldMatch(minShouldMatch string) *CommonTermsQuery {
	q.highFreqMinimumShouldMatch = minShouldMatch
	return q
}

func (q *CommonTermsQuery) LowFreq(f float64) *CommonTermsQuery {
	q.lowFreq = &f
	return q
}

func (q *CommonTermsQuery) LowFreqOperator(op string) *CommonTermsQuery {
	q.lowFreqOp = op
	return q
}

func (q *CommonTermsQuery) LowFreqMinimumShouldMatch(minShouldMatch string) *CommonTermsQuery {
	q.lowFreqMinimumShouldMatch = minShouldMatch
	return q
}

func (q *CommonTermsQuery) Analyzer(analyzer string) *CommonTermsQuery {
	q.analyzer = analyzer
	return q
}

func (q *CommonTermsQuery) Boost(boost float64) *CommonTermsQuery {
	q.boost = &boost
	return q
}

func (q *CommonTermsQuery) QueryName(queryName string) *CommonTermsQuery {
	q.queryName = queryName
	return q
}

// Creates the query source for the common query.
func (q *CommonTermsQuery) Source() (interface{}, error) {
	//  {
	//    "common": {
	//      "body": {
	//        "query":            "this is bonsai cool",
	//        "cutoff_frequency": 0.001
	//      }
	//    }
	//  }
	source := make(map[string]interface{})
	body := make(map[string]interface{})
	query := make(map[string]interface{})

	source["common"] = body
	body[q.name] = query
	query["query"] = q.text

	if q.cutoffFreq != nil {
		query["cutoff_frequency"] = *q.cutoffFreq
	}
	if q.highFreq != nil {
		query["high_freq"] = *q.highFreq
	}
	if q.highFreqOp != "" {
		query["high_freq_operator"] = q.highFreqOp
	}
	if q.lowFreq != nil {
		query["low_freq"] = *q.lowFreq
	}
	if q.lowFreqOp != "" {
		query["low_freq_operator"] = q.lowFreqOp
	}
	if q.lowFreqMinimumShouldMatch != "" || q.highFreqMinimumShouldMatch != "" {
		mm := make(map[string]interface{})
		if q.lowFreqMinimumShouldMatch != "" {
			mm["low_freq"] = q.lowFreqMinimumShouldMatch
		}
		if q.highFreqMinimumShouldMatch != "" {
			mm["high_freq"] = q.highFreqMinimumShouldMatch
		}
		query["minimum_should_match"] = mm
	}
	if q.analyzer != "" {
		query["analyzer"] = q.analyzer
	}
	if q.boost != nil {
		query["boost"] = *q.boost
	}
	if q.queryName != "" {
		query["_name"] = q.queryName
	}

	return source, nil
}
