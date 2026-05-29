package querydsl

// SignificantTextAggregation returns interesting or unusual occurrences
// of free-text terms in a set.
// See: https://www.opensearch.co/guide/en/opensearchsearch/reference/7.0/search-aggregations-bucket-significanttext-aggregation.html
type SignificantTextAggregation struct {
	FieldVal              string                          `json:"field,omitempty"`
	SubAggs               map[string]Aggregation          `json:"-"`
	Meta                  map[string]any                  `json:"meta,omitempty"`
	SourceFieldNames      []string                        `json:"source_field_names,omitempty"`
	FilterDuplicateText   *bool                           `json:"filter_duplicate_text,omitempty"`
	Filter                Query                           `json:"-"`
	IncludeExclude        *TermsAggregationIncludeExclude `json:"-"`
	BucketCountThresholds *BucketCountThresholds          `json:"-"`
	Heuristic             SignificanceHeuristic           `json:"-"`
}

func NewSignificantTextAggregation() SignificantTextAggregation {
	return SignificantTextAggregation{}
}

func (a SignificantTextAggregation) SubAggregation(name string, subAggregation Aggregation) SignificantTextAggregation {
	if a.SubAggs == nil {
		a.SubAggs = make(map[string]Aggregation)
	}
	a.SubAggs[name] = subAggregation
	return a
}

func (a SignificantTextAggregation) WithMeta(metaData map[string]any) SignificantTextAggregation {
	a.Meta = metaData
	return a
}

func (a SignificantTextAggregation) BackgroundFilter(filter Query) SignificantTextAggregation {
	a.Filter = filter
	return a
}

func (a SignificantTextAggregation) SignificanceHeuristic(heuristic SignificanceHeuristic) SignificantTextAggregation {
	a.Heuristic = heuristic
	return a
}

func (a SignificantTextAggregation) MinDocCount(minDocCount int64) SignificantTextAggregation {
	if a.BucketCountThresholds == nil {
		a.BucketCountThresholds = &BucketCountThresholds{}
	}
	a.BucketCountThresholds.MinDocCount = &minDocCount
	return a
}

func (a SignificantTextAggregation) ShardMinDocCount(shardMinDocCount int64) SignificantTextAggregation {
	if a.BucketCountThresholds == nil {
		a.BucketCountThresholds = &BucketCountThresholds{}
	}
	a.BucketCountThresholds.ShardMinDocCount = &shardMinDocCount
	return a
}

func (a SignificantTextAggregation) WithSize(size int) SignificantTextAggregation {
	if a.BucketCountThresholds == nil {
		a.BucketCountThresholds = &BucketCountThresholds{}
	}
	a.BucketCountThresholds.RequiredSize = &size
	return a
}

func (a SignificantTextAggregation) WithShardSize(shardSize int) SignificantTextAggregation {
	if a.BucketCountThresholds == nil {
		a.BucketCountThresholds = &BucketCountThresholds{}
	}
	a.BucketCountThresholds.ShardSize = &shardSize
	return a
}

func (a SignificantTextAggregation) Include(regexp string) SignificantTextAggregation {
	if a.IncludeExclude == nil {
		a.IncludeExclude = &TermsAggregationIncludeExclude{}
	}
	a.IncludeExclude.Include = regexp
	return a
}

func (a SignificantTextAggregation) IncludeValues(values ...any) SignificantTextAggregation {
	if a.IncludeExclude == nil {
		a.IncludeExclude = &TermsAggregationIncludeExclude{}
	}
	a.IncludeExclude.IncludeValues = append(a.IncludeExclude.IncludeValues, values...)
	return a
}

func (a SignificantTextAggregation) Exclude(regexp string) SignificantTextAggregation {
	if a.IncludeExclude == nil {
		a.IncludeExclude = &TermsAggregationIncludeExclude{}
	}
	a.IncludeExclude.Exclude = regexp
	return a
}

func (a SignificantTextAggregation) ExcludeValues(values ...any) SignificantTextAggregation {
	if a.IncludeExclude == nil {
		a.IncludeExclude = &TermsAggregationIncludeExclude{}
	}
	a.IncludeExclude.ExcludeValues = append(a.IncludeExclude.ExcludeValues, values...)
	return a
}

func (a SignificantTextAggregation) Partition(p int) SignificantTextAggregation {
	if a.IncludeExclude == nil {
		a.IncludeExclude = &TermsAggregationIncludeExclude{}
	}
	a.IncludeExclude.Partition = p
	return a
}

func (a SignificantTextAggregation) NumPartitions(n int) SignificantTextAggregation {
	if a.IncludeExclude == nil {
		a.IncludeExclude = &TermsAggregationIncludeExclude{}
	}
	a.IncludeExclude.NumPartitions = n
	return a
}

func (a SignificantTextAggregation) SetIncludeExclude(includeExclude *TermsAggregationIncludeExclude) SignificantTextAggregation {
	a.IncludeExclude = includeExclude
	return a
}

func (a SignificantTextAggregation) Source() (any, error) {
	body := make(map[string]any)

	if a.FieldVal != "" {
		body["field"] = a.FieldVal
	}
	if a.BucketCountThresholds != nil {
		if a.BucketCountThresholds.RequiredSize != nil {
			body["size"] = *a.BucketCountThresholds.RequiredSize
		}
		if a.BucketCountThresholds.ShardSize != nil {
			body["shard_size"] = *a.BucketCountThresholds.ShardSize
		}
		if a.BucketCountThresholds.MinDocCount != nil {
			body["min_doc_count"] = *a.BucketCountThresholds.MinDocCount
		}
		if a.BucketCountThresholds.ShardMinDocCount != nil {
			body["shard_min_doc_count"] = *a.BucketCountThresholds.ShardMinDocCount
		}
	}
	if a.Filter != nil {
		src, err := a.Filter.Source()
		if err != nil {
			return nil, err
		}
		body["background_filter"] = src
	}
	if a.Heuristic != nil {
		name := a.Heuristic.Name()
		src, err := a.Heuristic.Source()
		if err != nil {
			return nil, err
		}
		body[name] = src
	}

	if ie := a.IncludeExclude; ie != nil {
		if ie.Include != "" {
			body["include"] = ie.Include
		} else if len(ie.IncludeValues) > 0 {
			body["include"] = ie.IncludeValues
		} else if ie.NumPartitions > 0 {
			inc := make(map[string]any)
			inc["partition"] = ie.Partition
			inc["num_partitions"] = ie.NumPartitions
			body["include"] = inc
		}
		if ie.Exclude != "" {
			body["exclude"] = ie.Exclude
		} else if len(ie.ExcludeValues) > 0 {
			body["exclude"] = ie.ExcludeValues
		}
	}

	return sourceAgg("significant_text", body, a.SubAggs, a.Meta, nil)
}
