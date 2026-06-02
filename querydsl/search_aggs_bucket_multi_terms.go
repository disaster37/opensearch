package querydsl

// MultiTermsAggregation is a multi-bucket value source based aggregation
// where buckets are dynamically built - one per unique set of values.
// See: https://www.opensearch.co/guide/en/opensearchsearch/reference/7.13/search-aggregations-bucket-multi-terms-aggregation.html
type MultiTermsAggregation struct {
	MultiTermsData        []MultiTerm            `json:"-"`
	SubAggs               map[string]Aggregation `json:"-"`
	Meta                  map[string]any         `json:"meta,omitempty"`
	Size                  *int                   `json:"size,omitempty"`
	ShardSize             *int                   `json:"shard_size,omitempty"`
	MinDocCount           *int                   `json:"min_doc_count,omitempty"`
	ShardMinDocCount      *int                   `json:"shard_min_doc_count,omitempty"`
	CollectionMode        string                 `json:"collect_mode,omitempty"`
	ShowTermDocCountError *bool                  `json:"show_term_doc_count_error,omitempty"`
	OrderData             []MultiTermsOrder      `json:"-"`
}

func NewMultiTermsAggregation() *MultiTermsAggregation {
	return &MultiTermsAggregation{}
}

// Terms adds field names as simple multi-terms.
func (a *MultiTermsAggregation) Terms(fields ...string) *MultiTermsAggregation {
	for _, field := range fields {
		a.MultiTermsData = append(a.MultiTermsData, MultiTerm{Field: field})
	}
	return a
}

// MultiTerms appends MultiTerm entries to the aggregation.
func (a *MultiTermsAggregation) MultiTerms(multiTerms ...MultiTerm) *MultiTermsAggregation {
	a.MultiTermsData = append(a.MultiTermsData, multiTerms...)
	return a
}

// SubAggregation adds a sub-aggregation.
func (a *MultiTermsAggregation) SubAggregation(name string, subAggregation Aggregation) *MultiTermsAggregation {
	if a.SubAggs == nil {
		a.SubAggs = make(map[string]Aggregation)
	}
	a.SubAggs[name] = subAggregation
	return a
}

// WithMeta sets meta data for the aggregation.
func (a *MultiTermsAggregation) WithMeta(metaData map[string]any) *MultiTermsAggregation {
	a.Meta = metaData
	return a
}

// WithSize sets the number of term buckets to return.
func (a *MultiTermsAggregation) WithSize(size int) *MultiTermsAggregation {
	a.Size = &size
	return a
}

// WithShardSize sets the number of term buckets to fetch per shard.
func (a *MultiTermsAggregation) WithShardSize(shardSize int) *MultiTermsAggregation {
	a.ShardSize = &shardSize
	return a
}

// WithMinDocCount sets the minimum document count for a bucket to be included.
func (a *MultiTermsAggregation) WithMinDocCount(minDocCount int) *MultiTermsAggregation {
	a.MinDocCount = &minDocCount
	return a
}

// WithShardMinDocCount sets the minimum shard document count for a bucket to be included.
func (a *MultiTermsAggregation) WithShardMinDocCount(shardMinDocCount int) *MultiTermsAggregation {
	a.ShardMinDocCount = &shardMinDocCount
	return a
}

// Order appends an ordering criterion.
func (a *MultiTermsAggregation) Order(order string, asc bool) *MultiTermsAggregation {
	a.OrderData = append(a.OrderData, MultiTermsOrder{Field: order, Ascending: asc})
	return a
}

// OrderByCount orders buckets by document count.
func (a *MultiTermsAggregation) OrderByCount(asc bool) *MultiTermsAggregation {
	a.OrderData = append(a.OrderData, MultiTermsOrder{Field: "_count", Ascending: asc})
	return a
}

// OrderByCountAsc orders buckets by document count ascending.
func (a *MultiTermsAggregation) OrderByCountAsc() *MultiTermsAggregation {
	return a.OrderByCount(true)
}

// OrderByCountDesc orders buckets by document count descending.
func (a *MultiTermsAggregation) OrderByCountDesc() *MultiTermsAggregation {
	return a.OrderByCount(false)
}

// OrderByKey orders buckets by key.
func (a *MultiTermsAggregation) OrderByKey(asc bool) *MultiTermsAggregation {
	a.OrderData = append(a.OrderData, MultiTermsOrder{Field: "_key", Ascending: asc})
	return a
}

// OrderByKeyAsc orders buckets by key ascending.
func (a *MultiTermsAggregation) OrderByKeyAsc() *MultiTermsAggregation {
	return a.OrderByKey(true)
}

// OrderByKeyDesc orders buckets by key descending.
func (a *MultiTermsAggregation) OrderByKeyDesc() *MultiTermsAggregation {
	return a.OrderByKey(false)
}

// OrderByAggregation orders by a sub-aggregation metric.
func (a *MultiTermsAggregation) OrderByAggregation(aggName string, asc bool) *MultiTermsAggregation {
	a.OrderData = append(a.OrderData, MultiTermsOrder{Field: aggName, Ascending: asc})
	return a
}

// OrderByAggregationAndMetric orders by a sub-aggregation metric path.
func (a *MultiTermsAggregation) OrderByAggregationAndMetric(aggName, metric string, asc bool) *MultiTermsAggregation {
	a.OrderData = append(a.OrderData, MultiTermsOrder{Field: aggName + "." + metric, Ascending: asc})
	return a
}

// WithCollectionMode sets the collection mode (breadth_first or depth_first).
func (a *MultiTermsAggregation) WithCollectionMode(collectionMode string) *MultiTermsAggregation {
	a.CollectionMode = collectionMode
	return a
}

// WithShowTermDocCountError sets whether per-bucket doc count errors are shown.
func (a *MultiTermsAggregation) WithShowTermDocCountError(showTermDocCountError bool) *MultiTermsAggregation {
	a.ShowTermDocCountError = &showTermDocCountError
	return a
}

func (a MultiTermsAggregation) Source() (any, error) {
	body := make(map[string]any)

	terms := make([]any, len(a.MultiTermsData))
	for i := range a.MultiTermsData {
		terms[i], _ = a.MultiTermsData[i].Source()
	}
	body["terms"] = terms

	if a.Size != nil && *a.Size >= 0 {
		body["size"] = *a.Size
	}
	if a.ShardSize != nil && *a.ShardSize >= 0 {
		body["shard_size"] = *a.ShardSize
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
	if len(a.OrderData) > 0 {
		var orderSlice []any
		for _, order := range a.OrderData {
			src, _ := order.Source()
			orderSlice = append(orderSlice, src)
		}
		body["order"] = orderSlice
	}

	return sourceAgg("multi_terms", body, a.SubAggs, a.Meta, nil)
}

// MultiTermsOrder specifies a single order field for a multi terms aggregation.
type MultiTermsOrder struct {
	Field     string
	Ascending bool
}

func (order MultiTermsOrder) Source() (any, error) {
	source := make(map[string]string)
	if order.Ascending {
		source[order.Field] = "asc"
	} else {
		source[order.Field] = "desc"
	}
	return source, nil
}

// MultiTerm specifies a single term field for a multi terms aggregation.
type MultiTerm struct {
	Field   string
	Missing any
}

func (term MultiTerm) Source() (any, error) {
	source := make(map[string]any)
	source["field"] = term.Field
	if term.Missing != nil {
		source["missing"] = term.Missing
	}
	return source, nil
}
