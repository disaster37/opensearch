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
func NewTermsAggregation() TermsAggregation { return TermsAggregation{} }

func (a TermsAggregation) WithField(field string) TermsAggregation {
	a.Field = field
	return a
}

func (a TermsAggregation) WithScript(script *Script) TermsAggregation {
	a.Script = script
	return a
}

func (a TermsAggregation) WithMissing(missing any) TermsAggregation {
	a.Missing = missing
	return a
}

func (a TermsAggregation) WithSubAggregation(name string, sub Aggregation) TermsAggregation {
	if a.SubAggs == nil {
		a.SubAggs = map[string]Aggregation{}
	}
	a.SubAggs[name] = sub
	return a
}

func (a TermsAggregation) WithMeta(meta map[string]any) TermsAggregation {
	a.Meta = meta
	return a
}

func (a TermsAggregation) WithSize(size int) TermsAggregation {
	a.Size = &size
	return a
}

func (a TermsAggregation) WithRequiredSize(size int) TermsAggregation {
	a.RequiredSize = &size
	return a
}

func (a TermsAggregation) WithShardSize(size int) TermsAggregation {
	a.ShardSize = &size
	return a
}

func (a TermsAggregation) WithMinDocCount(count int) TermsAggregation {
	a.MinDocCount = &count
	return a
}

func (a TermsAggregation) WithShardMinDocCount(count int) TermsAggregation {
	a.ShardMinDocCount = &count
	return a
}

func (a TermsAggregation) WithInclude(regexp string) TermsAggregation {
	if a.IncludeExclude == nil {
		a.IncludeExclude = &TermsAggregationIncludeExclude{}
	}
	a.IncludeExclude.Include = regexp
	return a
}

func (a TermsAggregation) WithIncludeValues(values ...any) TermsAggregation {
	if a.IncludeExclude == nil {
		a.IncludeExclude = &TermsAggregationIncludeExclude{}
	}
	a.IncludeExclude.IncludeValues = append(a.IncludeExclude.IncludeValues, values...)
	return a
}

func (a TermsAggregation) WithExclude(regexp string) TermsAggregation {
	if a.IncludeExclude == nil {
		a.IncludeExclude = &TermsAggregationIncludeExclude{}
	}
	a.IncludeExclude.Exclude = regexp
	return a
}

func (a TermsAggregation) WithExcludeValues(values ...any) TermsAggregation {
	if a.IncludeExclude == nil {
		a.IncludeExclude = &TermsAggregationIncludeExclude{}
	}
	a.IncludeExclude.ExcludeValues = append(a.IncludeExclude.ExcludeValues, values...)
	return a
}

func (a TermsAggregation) WithPartition(p int) TermsAggregation {
	if a.IncludeExclude == nil {
		a.IncludeExclude = &TermsAggregationIncludeExclude{}
	}
	a.IncludeExclude.Partition = p
	return a
}

func (a TermsAggregation) WithNumPartitions(n int) TermsAggregation {
	if a.IncludeExclude == nil {
		a.IncludeExclude = &TermsAggregationIncludeExclude{}
	}
	a.IncludeExclude.NumPartitions = n
	return a
}

func (a TermsAggregation) WithIncludeExclude(ie *TermsAggregationIncludeExclude) TermsAggregation {
	a.IncludeExclude = ie
	return a
}

func (a TermsAggregation) WithValueType(vt string) TermsAggregation {
	a.ValueType = vt
	return a
}

func (a TermsAggregation) OrderBy(field string, asc bool) TermsAggregation {
	a.Order = append(a.Order, TermsOrder{Field: field, Ascending: asc})
	return a
}

func (a TermsAggregation) OrderByCount(asc bool) TermsAggregation {
	a.Order = append(a.Order, TermsOrder{Field: "_count", Ascending: asc})
	return a
}

func (a TermsAggregation) OrderByCountAsc() TermsAggregation {
	return a.OrderByCount(true)
}

func (a TermsAggregation) OrderByCountDesc() TermsAggregation {
	return a.OrderByCount(false)
}

func (a TermsAggregation) OrderByTerm(asc bool) TermsAggregation {
	a.Order = append(a.Order, TermsOrder{Field: "_term", Ascending: asc})
	return a
}

func (a TermsAggregation) OrderByTermAsc() TermsAggregation {
	return a.OrderByTerm(true)
}

func (a TermsAggregation) OrderByTermDesc() TermsAggregation {
	return a.OrderByTerm(false)
}

func (a TermsAggregation) OrderByKey(asc bool) TermsAggregation {
	a.Order = append(a.Order, TermsOrder{Field: "_key", Ascending: asc})
	return a
}

func (a TermsAggregation) OrderByKeyAsc() TermsAggregation {
	return a.OrderByKey(true)
}

func (a TermsAggregation) OrderByKeyDesc() TermsAggregation {
	return a.OrderByKey(false)
}

func (a TermsAggregation) OrderByAggregation(aggName string, asc bool) TermsAggregation {
	a.Order = append(a.Order, TermsOrder{Field: aggName, Ascending: asc})
	return a
}

func (a TermsAggregation) OrderByAggregationAndMetric(aggName, metric string, asc bool) TermsAggregation {
	a.Order = append(a.Order, TermsOrder{Field: aggName + "." + metric, Ascending: asc})
	return a
}

func (a TermsAggregation) WithExecutionHint(hint string) TermsAggregation {
	a.ExecutionHint = hint
	return a
}

func (a TermsAggregation) WithCollectionMode(mode string) TermsAggregation {
	a.CollectionMode = mode
	return a
}

func (a TermsAggregation) WithShowTermDocCountError(show bool) TermsAggregation {
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
		ie.MergeInto(body)
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
