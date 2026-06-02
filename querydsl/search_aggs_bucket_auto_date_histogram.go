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

func NewAutoDateHistogramAggregation() *AutoDateHistogramAggregation {
	return &AutoDateHistogramAggregation{}
}

// WithField sets the field for the aggregation.
func (a *AutoDateHistogramAggregation) WithField(field string) *AutoDateHistogramAggregation {
	a.Field = field
	return a
}

// WithScript sets the script for the aggregation.
func (a *AutoDateHistogramAggregation) WithScript(script *Script) *AutoDateHistogramAggregation {
	a.Script = script
	return a
}

// WithMissing sets the missing value substituted for documents without a value.
func (a *AutoDateHistogramAggregation) WithMissing(missing any) *AutoDateHistogramAggregation {
	a.Missing = missing
	return a
}

// WithBuckets sets the target number of buckets.
func (a *AutoDateHistogramAggregation) WithBuckets(buckets int) *AutoDateHistogramAggregation {
	a.Buckets = &buckets
	return a
}

// WithMinDocCount sets the minimum document count threshold for a bucket to be returned.
func (a *AutoDateHistogramAggregation) WithMinDocCount(minDocCount int64) *AutoDateHistogramAggregation {
	a.MinDocCount = &minDocCount
	return a
}

// WithTimeZone sets the time zone used for computing bucket boundaries.
func (a *AutoDateHistogramAggregation) WithTimeZone(timeZone string) *AutoDateHistogramAggregation {
	a.TimeZone = timeZone
	return a
}

// WithFormat sets the date format for bucket keys.
func (a *AutoDateHistogramAggregation) WithFormat(format string) *AutoDateHistogramAggregation {
	a.Format = format
	return a
}

// WithMinimumInterval sets the minimum interval for bucket sizing.
func (a *AutoDateHistogramAggregation) WithMinimumInterval(minimumInterval string) *AutoDateHistogramAggregation {
	a.MinimumInterval = minimumInterval
	return a
}

// SubAggregation adds a sub-aggregation.
func (a *AutoDateHistogramAggregation) SubAggregation(name string, subAggregation Aggregation) *AutoDateHistogramAggregation {
	if a.SubAggs == nil {
		a.SubAggs = make(map[string]Aggregation)
	}
	a.SubAggs[name] = subAggregation
	return a
}

// WithMeta sets meta data for the aggregation.
func (a *AutoDateHistogramAggregation) WithMeta(metaData map[string]any) *AutoDateHistogramAggregation {
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
