// Copyright 2012-present Oliver Eilhard. All rights reserved.
// Use of this source code is governed by a MIT-license.
// See http://olivere.mit-license.org/license.txt for details.

package opensearch

// MinBucketAggregation is a sibling pipeline aggregation which identifies
// the bucket(s) with the maximum value of a specified metric in a sibling
// aggregation and outputs both the value and the key(s) of the bucket(s).
// The specified metric must be numeric and the sibling aggregation must
// be a multi-bucket aggregation.
//
// For more details, see
// https://www.opensearch.co/guide/en/opensearchsearch/reference/7.0/search-aggregations-pipeline-min-bucket-aggregation.html
type MinBucketAggregation struct {
	format    string
	gapPolicy string

	meta         map[string]interface{}
	bucketsPaths []string
}

// NewMinBucketAggregation creates and initializes a new MinBucketAggregation.
func NewMinBucketAggregation() *MinBucketAggregation {
	return &MinBucketAggregation{
		bucketsPaths: make([]string, 0),
	}
}

// Format to use on the output of this aggregation.
func (a *MinBucketAggregation) Format(format string) *MinBucketAggregation {
	a.format = format
	return a
}

// GapPolicy defines what should be done when a gap in the series is discovered.
// Valid values include "insert_zeros" or "skip". Default is "insert_zeros".
func (a *MinBucketAggregation) GapPolicy(gapPolicy string) *MinBucketAggregation {
	a.gapPolicy = gapPolicy
	return a
}

// GapInsertZeros inserts zeros for gaps in the series.
func (a *MinBucketAggregation) GapInsertZeros() *MinBucketAggregation {
	a.gapPolicy = "insert_zeros"
	return a
}

// GapSkip skips gaps in the series.
func (a *MinBucketAggregation) GapSkip() *MinBucketAggregation {
	a.gapPolicy = "skip"
	return a
}

// Meta sets the meta data to be included in the aggregation response.
func (a *MinBucketAggregation) Meta(metaData map[string]interface{}) *MinBucketAggregation {
	a.meta = metaData
	return a
}

// BucketsPath sets the paths to the buckets to use for this pipeline aggregator.
func (a *MinBucketAggregation) BucketsPath(bucketsPaths ...string) *MinBucketAggregation {
	a.bucketsPaths = append(a.bucketsPaths, bucketsPaths...)
	return a
}

// Source returns the a JSON-serializable interface.
func (a *MinBucketAggregation) Source() (interface{}, error) {
	source := make(map[string]interface{})
	params := make(map[string]interface{})
	source["min_bucket"] = params

	if a.format != "" {
		params["format"] = a.format
	}
	if a.gapPolicy != "" {
		params["gap_policy"] = a.gapPolicy
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
