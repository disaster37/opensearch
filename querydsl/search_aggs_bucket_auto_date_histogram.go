package querydsl

// AutoDateHistogramAggregation is a multi-bucket aggregation similar to the
// histogram except it can only be applied on date values, and the buckets num can bin pointed.
// See: https://www.opensearch.co/guide/en/opensearchsearch/reference/7.3/search-aggregations-bucket-autodatehistogram-aggregation.html
type AutoDateHistogramAggregation struct {
	Field           string                 `json:"field,omitempty"`
	Script          *Script                `json:"-"`
	Missing         any                    `json:"missing,omitempty"`
	Buckets         *int                   `json:"buckets,omitempty"`
	MinDocCount     *int64                 `json:"min_doc_count,omitempty"`
	TimeZone        string                 `json:"time_zone,omitempty"`
	Format          string                 `json:"format,omitempty"`
	MinimumInterval string                 `json:"minimum_interval,omitempty"`
	SubAggs         map[string]Aggregation `json:"-"`
	Meta            map[string]any         `json:"meta,omitempty"`
}

func NewAutoDateHistogramAggregation() AutoDateHistogramAggregation {
	return AutoDateHistogramAggregation{}
}

func (a AutoDateHistogramAggregation) WithBuckets(buckets int) AutoDateHistogramAggregation {
	a.Buckets = &buckets
	return a
}

func (a AutoDateHistogramAggregation) WithMinDocCount(minDocCount int64) AutoDateHistogramAggregation {
	a.MinDocCount = &minDocCount
	return a
}

func (a AutoDateHistogramAggregation) SubAggregation(name string, subAggregation Aggregation) AutoDateHistogramAggregation {
	if a.SubAggs == nil {
		a.SubAggs = make(map[string]Aggregation)
	}
	a.SubAggs[name] = subAggregation
	return a
}

func (a AutoDateHistogramAggregation) WithMeta(metaData map[string]any) AutoDateHistogramAggregation {
	a.Meta = metaData
	return a
}

func (a AutoDateHistogramAggregation) Source() (any, error) {
	body := make(map[string]any)

	if a.Field != "" {
		body["field"] = a.Field
	}
	if a.Missing != nil {
		body["missing"] = a.Missing
	}
	if a.Buckets != nil {
		body["buckets"] = *a.Buckets
	}
	if a.MinDocCount != nil {
		body["min_doc_count"] = *a.MinDocCount
	}
	if a.TimeZone != "" {
		body["time_zone"] = a.TimeZone
	}
	if a.Format != "" {
		body["format"] = a.Format
	}
	if a.MinimumInterval != "" {
		body["minimum_interval"] = a.MinimumInterval
	}

	return sourceAgg("auto_date_histogram", body, a.SubAggs, a.Meta, a.Script)
}
