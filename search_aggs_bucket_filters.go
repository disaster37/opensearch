// Copyright 2012-present Oliver Eilhard. All rights reserved.
// Use of this source code is governed by a MIT-license.
// See http://olivere.mit-license.org/license.txt for details.

package opensearch

import "errors"

// FiltersAggregation defines a multi bucket aggregations where each bucket
// is associated with a filter. Each bucket will collect all documents that
// match its associated filter.
//
// Notice that the caller has to decide whether to add filters by name
// (using FilterWithName) or unnamed filters (using Filter or Filters). One cannot
// use both named and unnamed filters.
//
// For details, see
// https://www.opensearch.co/guide/en/opensearchsearch/reference/7.0/search-aggregations-bucket-filters-aggregation.html
type FiltersAggregation struct {
	unnamedFilters  []Query
	namedFilters    map[string]Query
	otherBucket     *bool
	otherBucketKey  string
	subAggregations map[string]Aggregation
	meta            map[string]interface{}
}

// NewFiltersAggregation initializes a new FiltersAggregation.
func NewFiltersAggregation() *FiltersAggregation {
	return &FiltersAggregation{
		unnamedFilters:  make([]Query, 0),
		namedFilters:    make(map[string]Query),
		subAggregations: make(map[string]Aggregation),
	}
}

// Filter adds an unnamed filter. Notice that you can
// either use named or unnamed filters, but not both.
func (a *FiltersAggregation) Filter(filter Query) *FiltersAggregation {
	a.unnamedFilters = append(a.unnamedFilters, filter)
	return a
}

// Filters adds one or more unnamed filters. Notice that you can
// either use named or unnamed filters, but not both.
func (a *FiltersAggregation) Filters(filters ...Query) *FiltersAggregation {
	if len(filters) > 0 {
		a.unnamedFilters = append(a.unnamedFilters, filters...)
	}
	return a
}

// FilterWithName adds a filter with a specific name. Notice that you can
// either use named or unnamed filters, but not both.
func (a *FiltersAggregation) FilterWithName(name string, filter Query) *FiltersAggregation {
	a.namedFilters[name] = filter
	return a
}

// OtherBucket indicates whether to include a bucket for documents not
// matching any filter.
func (a *FiltersAggregation) OtherBucket(otherBucket bool) *FiltersAggregation {
	a.otherBucket = &otherBucket
	return a
}

// OtherBucketKey sets the key to use for the bucket for documents not
// matching any filter.
func (a *FiltersAggregation) OtherBucketKey(key string) *FiltersAggregation {
	a.otherBucketKey = key
	return a
}

// SubAggregation adds a sub-aggregation to this aggregation.
func (a *FiltersAggregation) SubAggregation(name string, subAggregation Aggregation) *FiltersAggregation {
	a.subAggregations[name] = subAggregation
	return a
}

// Meta sets the meta data to be included in the aggregation response.
func (a *FiltersAggregation) Meta(metaData map[string]interface{}) *FiltersAggregation {
	a.meta = metaData
	return a
}

// Source returns the a JSON-serializable interface.
// If the aggregation is invalid, an error is returned. This may e.g. happen
// if you mixed named and unnamed filters.
func (a *FiltersAggregation) Source() (interface{}, error) {
	// Example:
	//	{
	//  "aggs" : {
	//    "messages" : {
	//      "filters" : {
	//        "filters" : {
	//          "errors" :   { "term" : { "body" : "error"   }},
	//          "warnings" : { "term" : { "body" : "warning" }}
	//        }
	//      }
	//    }
	//  }
	//	}
	// This method returns only the (outer) { "filters" : {} } part.

	source := make(map[string]interface{})
	filters := make(map[string]interface{})
	source["filters"] = filters

	if len(a.unnamedFilters) > 0 && len(a.namedFilters) > 0 {
		return nil, errors.New("opensearch: use either named or unnamed filters with FiltersAggregation but not both")
	}

	if len(a.unnamedFilters) > 0 {
		arr := make([]interface{}, len(a.unnamedFilters))
		for i, filter := range a.unnamedFilters {
			src, err := filter.Source()
			if err != nil {
				return nil, err
			}
			arr[i] = src
		}
		filters["filters"] = arr
	} else {
		dict := make(map[string]interface{})
		for key, filter := range a.namedFilters {
			src, err := filter.Source()
			if err != nil {
				return nil, err
			}
			dict[key] = src
		}
		filters["filters"] = dict
	}

	if v := a.otherBucket; v != nil {
		filters["other_bucket"] = *v
	}
	if v := a.otherBucketKey; v != "" {
		filters["other_bucket_key"] = v
	}

	// AggregationBuilder (SubAggregations)
	if len(a.subAggregations) > 0 {
		aggsMap := make(map[string]interface{})
		source["aggregations"] = aggsMap
		for name, aggregate := range a.subAggregations {
			src, err := aggregate.Source()
			if err != nil {
				return nil, err
			}
			aggsMap[name] = src
		}
	}

	// Add Meta data if available
	if len(a.meta) > 0 {
		source["meta"] = a.meta
	}

	return source, nil
}
