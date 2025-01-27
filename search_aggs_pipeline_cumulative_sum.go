// Copyright 2012-present Oliver Eilhard. All rights reserved.
// Use of this source code is governed by a MIT-license.
// See http://olivere.mit-license.org/license.txt for details.

package opensearch

// CumulativeSumAggregation is a parent pipeline aggregation which calculates
// the cumulative sum of a specified metric in a parent histogram (or date_histogram)
// aggregation. The specified metric must be numeric and the enclosing
// histogram must have min_doc_count set to 0 (default for histogram aggregations).
//
// For more details, see
// https://www.opensearch.co/guide/en/opensearchsearch/reference/7.0/search-aggregations-pipeline-cumulative-sum-aggregation.html
type CumulativeSumAggregation struct {
	format string

	meta         map[string]interface{}
	bucketsPaths []string
}

// NewCumulativeSumAggregation creates and initializes a new CumulativeSumAggregation.
func NewCumulativeSumAggregation() *CumulativeSumAggregation {
	return &CumulativeSumAggregation{
		bucketsPaths: make([]string, 0),
	}
}

// Format to use on the output of this aggregation.
func (a *CumulativeSumAggregation) Format(format string) *CumulativeSumAggregation {
	a.format = format
	return a
}

// Meta sets the meta data to be included in the aggregation response.
func (a *CumulativeSumAggregation) Meta(metaData map[string]interface{}) *CumulativeSumAggregation {
	a.meta = metaData
	return a
}

// BucketsPath sets the paths to the buckets to use for this pipeline aggregator.
func (a *CumulativeSumAggregation) BucketsPath(bucketsPaths ...string) *CumulativeSumAggregation {
	a.bucketsPaths = append(a.bucketsPaths, bucketsPaths...)
	return a
}

// Source returns the a JSON-serializable interface.
func (a *CumulativeSumAggregation) Source() (interface{}, error) {
	source := make(map[string]interface{})
	params := make(map[string]interface{})
	source["cumulative_sum"] = params

	if a.format != "" {
		params["format"] = a.format
	}

	// Add buckets paths
	switch len(a.bucketsPaths) {
	case 0:
	case 1:
		params["buckets_path"] = a.bucketsPaths[0]
	default:
		params["buckets_path"] = a.bucketsPaths
	}

	// Add Meta data if available
	if len(a.meta) > 0 {
		source["meta"] = a.meta
	}

	return source, nil
}
