// Copyright 2012-present Oliver Eilhard. All rights reserved.
// Use of this source code is governed by a MIT-license.
// See http://olivere.mit-license.org/license.txt for details.

package opensearch

// SerialDiffAggregation implements serial differencing.
// Serial differencing is a technique where values in a time series are
// subtracted from itself at different time lags or periods.
//
// For more details, see
// https://www.opensearch.co/guide/en/opensearchsearch/reference/7.0/search-aggregations-pipeline-serialdiff-aggregation.html
type SerialDiffAggregation struct {
	format    string
	gapPolicy string
	lag       *int

	meta         map[string]interface{}
	bucketsPaths []string
}

// NewSerialDiffAggregation creates and initializes a new SerialDiffAggregation.
func NewSerialDiffAggregation() *SerialDiffAggregation {
	return &SerialDiffAggregation{
		bucketsPaths: make([]string, 0),
	}
}

// Format to use on the output of this aggregation.
func (a *SerialDiffAggregation) Format(format string) *SerialDiffAggregation {
	a.format = format
	return a
}

// GapPolicy defines what should be done when a gap in the series is discovered.
// Valid values include "insert_zeros" or "skip". Default is "insert_zeros".
func (a *SerialDiffAggregation) GapPolicy(gapPolicy string) *SerialDiffAggregation {
	a.gapPolicy = gapPolicy
	return a
}

// GapInsertZeros inserts zeros for gaps in the series.
func (a *SerialDiffAggregation) GapInsertZeros() *SerialDiffAggregation {
	a.gapPolicy = "insert_zeros"
	return a
}

// GapSkip skips gaps in the series.
func (a *SerialDiffAggregation) GapSkip() *SerialDiffAggregation {
	a.gapPolicy = "skip"
	return a
}

// Lag specifies the historical bucket to subtract from the current value.
// E.g. a lag of 7 will subtract the current value from the value 7 buckets
// ago. Lag must be a positive, non-zero integer.
func (a *SerialDiffAggregation) Lag(lag int) *SerialDiffAggregation {
	a.lag = &lag
	return a
}

// Meta sets the meta data to be included in the aggregation response.
func (a *SerialDiffAggregation) Meta(metaData map[string]interface{}) *SerialDiffAggregation {
	a.meta = metaData
	return a
}

// BucketsPath sets the paths to the buckets to use for this pipeline aggregator.
func (a *SerialDiffAggregation) BucketsPath(bucketsPaths ...string) *SerialDiffAggregation {
	a.bucketsPaths = append(a.bucketsPaths, bucketsPaths...)
	return a
}

// Source returns the a JSON-serializable interface.
func (a *SerialDiffAggregation) Source() (interface{}, error) {
	source := make(map[string]interface{})
	params := make(map[string]interface{})
	source["serial_diff"] = params

	if a.format != "" {
		params["format"] = a.format
	}
	if a.gapPolicy != "" {
		params["gap_policy"] = a.gapPolicy
	}
	if a.lag != nil {
		params["lag"] = *a.lag
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
