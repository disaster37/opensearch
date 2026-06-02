package querydsl

// TermsAggregation buckets documents by unique values of a field.
// Each distinct value produces one bucket; buckets are ordered by doc count
// (descending) by default.
//
// Typical use: top-N facets, category aggregations.
//
// JSON output shape:
//
//	{"terms": {"field": "genre", "size": 10}}
type TermsAggregation struct {
	Field                 string                          `json:"field,omitempty"`
	Script                *Script                         `json:"-"`
	Missing               any                             `json:"-"`
	SubAggs               map[string]Aggregation          `json:"-"`
	Meta                  map[string]any                  `json:"-"`
	Size                  *int                            `json:"-"`
	ShardSize             *int                            `json:"-"`
	RequiredSize          *int                            `json:"-"`
	MinDocCount           *int                            `json:"-"`
	ShardMinDocCount      *int                            `json:"-"`
	ValueType             string                          `json:"value_type,omitempty"`
	IncludeExclude        *TermsAggregationIncludeExclude `json:"-"`
	ExecutionHint         string                          `json:"execution_hint,omitempty"`
	CollectionMode        string                          `json:"collect_mode,omitempty"`
	ShowTermDocCountError *bool                           `json:"-"`
	Order                 []TermsOrder                    `json:"-"`
}

// NewTermsAggregation returns a zero-value TermsAggregation.
func NewTermsAggregation() *TermsAggregation { return &TermsAggregation{} }

// WithField sets the field to aggregate on.
func (a *TermsAggregation) WithField(field string) *TermsAggregation {
	a.Field = field
	return a
}

// WithScript sets the script used to compute bucket keys.
func (a *TermsAggregation) WithScript(script *Script) *TermsAggregation {
	a.Script = script
	return a
}

// WithMissing sets the value used for documents without the field.
func (a *TermsAggregation) WithMissing(missing any) *TermsAggregation {
	a.Missing = missing
	return a
}

// WithSubAggregation adds a sub-aggregation.
func (a *TermsAggregation) WithSubAggregation(name string, sub Aggregation) *TermsAggregation {
	if a.SubAggs == nil {
		a.SubAggs = map[string]Aggregation{}
	}
	a.SubAggs[name] = sub
	return a
}

// WithMeta sets the meta data for the aggregation.
func (a *TermsAggregation) WithMeta(meta map[string]any) *TermsAggregation {
	a.Meta = meta
	return a
}

// WithSize sets the number of term buckets to return.
func (a *TermsAggregation) WithSize(size int) *TermsAggregation {
	a.Size = &size
	return a
}

// WithRequiredSize sets the required number of term buckets.
func (a *TermsAggregation) WithRequiredSize(size int) *TermsAggregation {
	a.RequiredSize = &size
	return a
}

// WithShardSize sets the number of term buckets to fetch per shard.
func (a *TermsAggregation) WithShardSize(size int) *TermsAggregation {
	a.ShardSize = &size
	return a
}

// WithMinDocCount sets the minimum document count for a bucket to be returned.
func (a *TermsAggregation) WithMinDocCount(count int) *TermsAggregation {
	a.MinDocCount = &count
	return a
}

// WithShardMinDocCount sets the shard-level minimum document count.
func (a *TermsAggregation) WithShardMinDocCount(count int) *TermsAggregation {
	a.ShardMinDocCount = &count
	return a
}

// WithInclude sets a regexp pattern for included term values.
func (a *TermsAggregation) WithInclude(regexp string) *TermsAggregation {
	if a.IncludeExclude == nil {
		a.IncludeExclude = &TermsAggregationIncludeExclude{}
	}
	a.IncludeExclude.Include = regexp
	return a
}

// WithIncludeValues sets an explicit list of values to include.
func (a *TermsAggregation) WithIncludeValues(values ...any) *TermsAggregation {
	if a.IncludeExclude == nil {
		a.IncludeExclude = &TermsAggregationIncludeExclude{}
	}
	a.IncludeExclude.IncludeValues = append(a.IncludeExclude.IncludeValues, values...)
	return a
}

// WithExclude sets a regexp pattern for excluded term values.
func (a *TermsAggregation) WithExclude(regexp string) *TermsAggregation {
	if a.IncludeExclude == nil {
		a.IncludeExclude = &TermsAggregationIncludeExclude{}
	}
	a.IncludeExclude.Exclude = regexp
	return a
}

// WithExcludeValues sets an explicit list of values to exclude.
func (a *TermsAggregation) WithExcludeValues(values ...any) *TermsAggregation {
	if a.IncludeExclude == nil {
		a.IncludeExclude = &TermsAggregationIncludeExclude{}
	}
	a.IncludeExclude.ExcludeValues = append(a.IncludeExclude.ExcludeValues, values...)
	return a
}

// WithPartition sets the partition number for partitioned term filtering.
func (a *TermsAggregation) WithPartition(p int) *TermsAggregation {
	if a.IncludeExclude == nil {
		a.IncludeExclude = &TermsAggregationIncludeExclude{}
	}
	a.IncludeExclude.Partition = p
	return a
}

// WithNumPartitions sets the total number of partitions for partitioned term filtering.
func (a *TermsAggregation) WithNumPartitions(n int) *TermsAggregation {
	if a.IncludeExclude == nil {
		a.IncludeExclude = &TermsAggregationIncludeExclude{}
	}
	a.IncludeExclude.NumPartitions = n
	return a
}

// WithIncludeExclude sets the include/exclude filter directly.
func (a *TermsAggregation) WithIncludeExclude(ie *TermsAggregationIncludeExclude) *TermsAggregation {
	a.IncludeExclude = ie
	return a
}

// WithValueType sets the value type hint for the aggregation.
func (a *TermsAggregation) WithValueType(vt string) *TermsAggregation {
	a.ValueType = vt
	return a
}

// OrderBy appends an ordering criterion by field name.
func (a *TermsAggregation) OrderBy(field string, asc bool) *TermsAggregation {
	a.Order = append(a.Order, TermsOrder{Field: field, Ascending: asc})
	return a
}

// OrderByCount orders buckets by document count.
func (a *TermsAggregation) OrderByCount(asc bool) *TermsAggregation {
	a.Order = append(a.Order, TermsOrder{Field: "_count", Ascending: asc})
	return a
}

// OrderByCountAsc orders buckets by document count ascending.
func (a *TermsAggregation) OrderByCountAsc() *TermsAggregation {
	return a.OrderByCount(true)
}

// OrderByCountDesc orders buckets by document count descending.
func (a *TermsAggregation) OrderByCountDesc() *TermsAggregation {
	return a.OrderByCount(false)
}

// OrderByTerm orders buckets by term value.
func (a *TermsAggregation) OrderByTerm(asc bool) *TermsAggregation {
	a.Order = append(a.Order, TermsOrder{Field: "_term", Ascending: asc})
	return a
}

// OrderByTermAsc orders buckets by term value ascending.
func (a *TermsAggregation) OrderByTermAsc() *TermsAggregation {
	return a.OrderByTerm(true)
}

// OrderByTermDesc orders buckets by term value descending.
func (a *TermsAggregation) OrderByTermDesc() *TermsAggregation {
	return a.OrderByTerm(false)
}

// OrderByKey orders buckets by key.
func (a *TermsAggregation) OrderByKey(asc bool) *TermsAggregation {
	a.Order = append(a.Order, TermsOrder{Field: "_key", Ascending: asc})
	return a
}

// OrderByKeyAsc orders buckets by key ascending.
func (a *TermsAggregation) OrderByKeyAsc() *TermsAggregation {
	return a.OrderByKey(true)
}

// OrderByKeyDesc orders buckets by key descending.
func (a *TermsAggregation) OrderByKeyDesc() *TermsAggregation {
	return a.OrderByKey(false)
}

// OrderByAggregation orders by a sub-aggregation metric.
func (a *TermsAggregation) OrderByAggregation(aggName string, asc bool) *TermsAggregation {
	a.Order = append(a.Order, TermsOrder{Field: aggName, Ascending: asc})
	return a
}

// OrderByAggregationAndMetric orders by a sub-aggregation metric path.
func (a *TermsAggregation) OrderByAggregationAndMetric(aggName, metric string, asc bool) *TermsAggregation {
	a.Order = append(a.Order, TermsOrder{Field: aggName + "." + metric, Ascending: asc})
	return a
}

// WithExecutionHint sets the execution hint for the aggregation.
func (a *TermsAggregation) WithExecutionHint(hint string) *TermsAggregation {
	a.ExecutionHint = hint
	return a
}

// WithCollectionMode sets the collection mode (breadth_first or depth_first).
func (a *TermsAggregation) WithCollectionMode(mode string) *TermsAggregation {
	a.CollectionMode = mode
	return a
}

// WithShowTermDocCountError sets whether per-bucket doc count errors are shown.
func (a *TermsAggregation) WithShowTermDocCountError(show bool) *TermsAggregation {
	a.ShowTermDocCountError = &show
	return a
}

func (a TermsAggregation) Source() (any, error) {
	body := map[string]any{}

	if a.Field != "" {
		body["field"] = a.Field
	}
	if a.Missing != nil {
		body["missing"] = a.Missing
	}

	if a.Size != nil && *a.Size >= 0 {
		body["size"] = *a.Size
	}
	if a.ShardSize != nil && *a.ShardSize >= 0 {
		body["shard_size"] = *a.ShardSize
	}
	if a.RequiredSize != nil && *a.RequiredSize >= 0 {
		body["required_size"] = *a.RequiredSize
	}
	if a.MinDocCount != nil && *a.MinDocCount >= 0 {
		body["min_doc_count"] = *a.MinDocCount
	}
	if a.ShardMinDocCount != nil && *a.ShardMinDocCount >= 0 {
		body["shard_min_doc_count"] = *a.ShardMinDocCount
	}
	if a.ShowTermDocCountError != nil {
		body["show_term_doc_count_error"] = *a.ShowTermDocCountError
	}
	if a.CollectionMode != "" {
		body["collect_mode"] = a.CollectionMode
	}
	if a.ValueType != "" {
		body["value_type"] = a.ValueType
	}
	if len(a.Order) > 0 {
		orderSlice := make([]any, len(a.Order))
		for i, o := range a.Order {
			orderSlice[i], _ = o.Source()
		}
		body["order"] = orderSlice
	}

	if ie := a.IncludeExclude; ie != nil {
		if err := ie.MergeInto(body); err != nil {
			return nil, err
		}
	}

	if a.ExecutionHint != "" {
		body["execution_hint"] = a.ExecutionHint
	}

	return sourceAgg("terms", body, a.SubAggs, a.Meta, a.Script)
}

// TermsAggregationIncludeExclude controls which term values are included in
// or excluded from a TermsAggregation. Matching can be done by regular
// expression, by an explicit list of values, or by partition-based filtering.
type TermsAggregationIncludeExclude struct {
	Include       string
	Exclude       string
	IncludeValues []any
	ExcludeValues []any
	Partition     int
	NumPartitions int
}

func (ie *TermsAggregationIncludeExclude) Source() (any, error) {
	source := map[string]any{}
	if ie.Include != "" {
		source["include"] = ie.Include
	} else if len(ie.IncludeValues) > 0 {
		source["include"] = ie.IncludeValues
	} else if ie.NumPartitions > 0 {
		source["include"] = map[string]any{
			"partition":      ie.Partition,
			"num_partitions": ie.NumPartitions,
		}
	}
	if ie.Exclude != "" {
		source["exclude"] = ie.Exclude
	} else if len(ie.ExcludeValues) > 0 {
		source["exclude"] = ie.ExcludeValues
	}
	return source, nil
}

func (ie *TermsAggregationIncludeExclude) MergeInto(source map[string]any) error {
	values, _ := ie.Source()
	mv := values.(map[string]any)
	for k, v := range mv {
		source[k] = v
	}
	return nil
}

// TermsOrder specifies how buckets in a terms aggregation are sorted.
type TermsOrder struct {
	Field     string
	Ascending bool
}

func (o *TermsOrder) Source() (any, error) {
	src := map[string]string{o.Field: "desc"}
	if o.Ascending {
		src[o.Field] = "asc"
	}
	return src, nil
}
