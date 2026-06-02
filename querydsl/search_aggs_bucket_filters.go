package querydsl

// FiltersAggregation produces multiple named or unnamed buckets, each backed
// by a separate query filter. Unlike FilterAggregation (single filter), this
// allows comparing metrics across several filter criteria in one request.
// An optional "other" bucket collects documents that match none of the filters.
//
// Typical use: compare order counts for "new" vs "returning" customers.
//
// JSON output shape (named):
//
//	{"filters": {"filters": {"new_users": {"term": {"type": "new"}}, "returning": {"term": {"type": "old"}}}}, "other_bucket": true}
type FiltersAggregation struct {
	UnnamedFilters []Query
	NamedFilters   map[string]Query
	OtherBucket    *bool
	OtherBucketKey string
	SubAggs        map[string]Aggregation
	Meta           map[string]any
}

// NewFiltersAggregation returns a FiltersAggregation initialized with an
// empty named-filters map.
func NewFiltersAggregation() *FiltersAggregation {
	return &FiltersAggregation{
		NamedFilters: map[string]Query{},
	}
}

// WithNamedFilter adds a named filter to the aggregation.
func (a *FiltersAggregation) WithNamedFilter(name string, q Query) *FiltersAggregation {
	if a.NamedFilters == nil {
		a.NamedFilters = map[string]Query{}
	}
	a.NamedFilters[name] = q
	return a
}

// WithUnnamedFilter appends an unnamed filter to the aggregation.
func (a *FiltersAggregation) WithUnnamedFilter(q Query) *FiltersAggregation {
	a.UnnamedFilters = append(a.UnnamedFilters, q)
	return a
}

// WithOtherBucket sets whether to include an "other" bucket for unmatched documents.
func (a *FiltersAggregation) WithOtherBucket(otherBucket bool) *FiltersAggregation {
	a.OtherBucket = &otherBucket
	return a
}

// WithOtherBucketKey sets the key for the "other" bucket.
func (a *FiltersAggregation) WithOtherBucketKey(key string) *FiltersAggregation {
	a.OtherBucketKey = key
	return a
}

// WithSubAggregation adds a sub-aggregation.
func (a *FiltersAggregation) WithSubAggregation(name string, sub Aggregation) *FiltersAggregation {
	if a.SubAggs == nil {
		a.SubAggs = map[string]Aggregation{}
	}
	a.SubAggs[name] = sub
	return a
}

// WithMeta sets the meta data for the aggregation.
func (a *FiltersAggregation) WithMeta(meta map[string]any) *FiltersAggregation {
	a.Meta = meta
	return a
}

func (a FiltersAggregation) Source() (any, error) {
	body := map[string]any{}

	if len(a.NamedFilters) > 0 {
		filters := map[string]any{}
		for name, q := range a.NamedFilters {
			src, err := q.Source()
			if err != nil {
				return nil, err
			}
			filters[name] = src
		}
		body["filters"] = filters
	} else if len(a.UnnamedFilters) > 0 {
		filters := make([]any, len(a.UnnamedFilters))
		for i, q := range a.UnnamedFilters {
			src, err := q.Source()
			if err != nil {
				return nil, err
			}
			filters[i] = src
		}
		body["filters"] = filters
	}

	if a.OtherBucket != nil {
		body["other_bucket"] = *a.OtherBucket
	}
	if a.OtherBucketKey != "" {
		body["other_bucket_key"] = a.OtherBucketKey
	}
	return sourceAgg("filters", body, a.SubAggs, a.Meta, nil)
}
