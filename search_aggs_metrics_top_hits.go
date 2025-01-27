// Copyright 2012-present Oliver Eilhard. All rights reserved.
// Use of this source code is governed by a MIT-license.
// See http://olivere.mit-license.org/license.txt for details.

package opensearch

// TopHitsAggregation keeps track of the most relevant document
// being aggregated. This aggregator is intended to be used as a
// sub aggregator, so that the top matching documents
// can be aggregated per bucket.
//
// It can effectively be used to group result sets by certain fields via
// a bucket aggregator. One or more bucket aggregators determines by
// which properties a result set get sliced into.
//
// See: https://www.opensearch.co/guide/en/opensearchsearch/reference/7.0/search-aggregations-metrics-top-hits-aggregation.html
type TopHitsAggregation struct {
	searchSource *SearchSource
}

func NewTopHitsAggregation() *TopHitsAggregation {
	return &TopHitsAggregation{
		searchSource: NewSearchSource(),
	}
}

func (a *TopHitsAggregation) SearchSource(searchSource *SearchSource) *TopHitsAggregation {
	a.searchSource = searchSource
	if a.searchSource == nil {
		a.searchSource = NewSearchSource()
	}
	return a
}

func (a *TopHitsAggregation) From(from int) *TopHitsAggregation {
	a.searchSource = a.searchSource.From(from)
	return a
}

func (a *TopHitsAggregation) Size(size int) *TopHitsAggregation {
	a.searchSource = a.searchSource.Size(size)
	return a
}

func (a *TopHitsAggregation) TrackScores(trackScores bool) *TopHitsAggregation {
	a.searchSource = a.searchSource.TrackScores(trackScores)
	return a
}

func (a *TopHitsAggregation) Explain(explain bool) *TopHitsAggregation {
	a.searchSource = a.searchSource.Explain(explain)
	return a
}

func (a *TopHitsAggregation) Version(version bool) *TopHitsAggregation {
	a.searchSource = a.searchSource.Version(version)
	return a
}

func (a *TopHitsAggregation) NoStoredFields() *TopHitsAggregation {
	a.searchSource = a.searchSource.NoStoredFields()
	return a
}

func (a *TopHitsAggregation) FetchSource(fetchSource bool) *TopHitsAggregation {
	a.searchSource = a.searchSource.FetchSource(fetchSource)
	return a
}

func (a *TopHitsAggregation) FetchSourceContext(fetchSourceContext *FetchSourceContext) *TopHitsAggregation {
	a.searchSource = a.searchSource.FetchSourceContext(fetchSourceContext)
	return a
}

func (a *TopHitsAggregation) DocvalueFields(docvalueFields ...string) *TopHitsAggregation {
	a.searchSource = a.searchSource.DocvalueFields(docvalueFields...)
	return a
}

func (a *TopHitsAggregation) DocvalueFieldsWithFormat(docvalueFields ...DocvalueField) *TopHitsAggregation {
	a.searchSource = a.searchSource.DocvalueFieldsWithFormat(docvalueFields...)
	return a
}

func (a *TopHitsAggregation) DocvalueField(docvalueField string) *TopHitsAggregation {
	a.searchSource = a.searchSource.DocvalueField(docvalueField)
	return a
}

func (a *TopHitsAggregation) DocvalueFieldWithFormat(docvalueField DocvalueField) *TopHitsAggregation {
	a.searchSource = a.searchSource.DocvalueFieldWithFormat(docvalueField)
	return a
}

func (a *TopHitsAggregation) ScriptFields(scriptFields ...*ScriptField) *TopHitsAggregation {
	a.searchSource = a.searchSource.ScriptFields(scriptFields...)
	return a
}

func (a *TopHitsAggregation) ScriptField(scriptField *ScriptField) *TopHitsAggregation {
	a.searchSource = a.searchSource.ScriptField(scriptField)
	return a
}

func (a *TopHitsAggregation) Sort(field string, ascending bool) *TopHitsAggregation {
	a.searchSource = a.searchSource.Sort(field, ascending)
	return a
}

func (a *TopHitsAggregation) SortWithInfo(info SortInfo) *TopHitsAggregation {
	a.searchSource = a.searchSource.SortWithInfo(info)
	return a
}

func (a *TopHitsAggregation) SortBy(sorter ...Sorter) *TopHitsAggregation {
	a.searchSource = a.searchSource.SortBy(sorter...)
	return a
}

func (a *TopHitsAggregation) Highlight(highlight *Highlight) *TopHitsAggregation {
	a.searchSource = a.searchSource.Highlight(highlight)
	return a
}

func (a *TopHitsAggregation) Highlighter() *Highlight {
	return a.searchSource.Highlighter()
}

func (a *TopHitsAggregation) Source() (interface{}, error) {
	// Example:
	// {
	//   "aggs": {
	//       "top_tag_hits": {
	//           "top_hits": {
	//               "sort": [
	//                   {
	//                       "last_activity_date": {
	//                           "order": "desc"
	//                       }
	//                   }
	//               ],
	//               "_source": {
	//                   "include": [
	//                       "title"
	//                   ]
	//               },
	//               "size" : 1
	//           }
	//       }
	//   }
	// }
	// This method returns only the { "top_hits" : { ... } } part.

	source := make(map[string]interface{})
	src, err := a.searchSource.Source()
	if err != nil {
		return nil, err
	}
	source["top_hits"] = src
	return source, nil
}
