package querydsl

// HistogramAggregation buckets numeric values into fixed-width intervals.
// Documents are assigned to buckets by rounding their value down to the
// nearest multiple of Interval. Supports extended bounds, offset, and
// min_doc_count filtering.
//
// Typical use: price distribution histograms, age distribution.
//
// JSON output shape:
//
//	{"histogram": {"field": "price", "interval": 50, "min_doc_count": 1}}
type HistogramAggregation struct {
	Field       string
	Script      *Script
	Missing     any
	SubAggs     map[string]Aggregation
	Meta        map[string]any
	Interval    float64
	Order       string
	OrderAsc    bool
	MinDocCount *int64
	MinBounds   *float64
	MaxBounds   *float64
	Offset      *float64
}

// NewHistogramAggregation returns a zero-value HistogramAggregation.
func NewHistogramAggregation() *HistogramAggregation {
	return &HistogramAggregation{}
}

func (a *HistogramAggregation) Field_(v string) *HistogramAggregation   { a.Field = v; return a }
func (a *HistogramAggregation) Script_(v *Script) *HistogramAggregation { a.Script = v; return a }
func (a *HistogramAggregation) Missing_(v any) *HistogramAggregation    { a.Missing = v; return a }
func (a *HistogramAggregation) SubAggregation(name string, sub Aggregation) *HistogramAggregation {
	if a.SubAggs == nil {
		a.SubAggs = make(map[string]Aggregation)
	}
	a.SubAggs[name] = sub
	return a
}

func (a *HistogramAggregation) Meta_(v map[string]any) *HistogramAggregation { a.Meta = v; return a }

func (a *HistogramAggregation) Interval_(v float64) *HistogramAggregation { a.Interval = v; return a }

func (a *HistogramAggregation) MinDocCount_(v int64) *HistogramAggregation {
	a.MinDocCount = &v
	return a
}
func (a *HistogramAggregation) Offset_(v float64) *HistogramAggregation { a.Offset = &v; return a }
func (a *HistogramAggregation) ExtendedBounds(min, max float64) *HistogramAggregation {
	a.MinBounds = &min
	a.MaxBounds = &max
	return a
}

func (a *HistogramAggregation) ExtendedBoundsMin(min float64) *HistogramAggregation {
	a.MinBounds = &min
	return a
}

func (a *HistogramAggregation) ExtendedBoundsMax(max float64) *HistogramAggregation {
	a.MaxBounds = &max
	return a
}

func (a *HistogramAggregation) Order_(order string, asc bool) *HistogramAggregation {
	a.Order = order
	a.OrderAsc = asc
	return a
}

func (a *HistogramAggregation) OrderByCount(asc bool) *HistogramAggregation {
	return a.Order_("_count", asc)
}

func (a *HistogramAggregation) OrderByCountAsc() *HistogramAggregation {
	return a.OrderByCount(true)
}

func (a *HistogramAggregation) OrderByCountDesc() *HistogramAggregation {
	return a.OrderByCount(false)
}

func (a *HistogramAggregation) OrderByKey(asc bool) *HistogramAggregation {
	return a.Order_("_key", asc)
}

func (a *HistogramAggregation) OrderByKeyAsc() *HistogramAggregation {
	return a.OrderByKey(true)
}

func (a *HistogramAggregation) OrderByKeyDesc() *HistogramAggregation {
	return a.OrderByKey(false)
}

func (a *HistogramAggregation) OrderByAggregation(aggName string, asc bool) *HistogramAggregation {
	return a.Order_(aggName, asc)
}

func (a *HistogramAggregation) OrderByAggregationAndMetric(aggName, metric string, asc bool) *HistogramAggregation {
	return a.Order_(aggName+"."+metric, asc)
}

func (a HistogramAggregation) Source() (any, error) {
	body := map[string]any{}

	if a.Field != "" {
		body["field"] = a.Field
	}
	if a.Missing != nil {
		body["missing"] = a.Missing
	}

	body["interval"] = a.Interval

	if a.Order != "" {
		dir := "desc"
		if a.OrderAsc {
			dir = "asc"
		}
		body["order"] = map[string]any{a.Order: dir}
	}
	if a.Offset != nil {
		body["offset"] = *a.Offset
	}
	if a.MinDocCount != nil {
		body["min_doc_count"] = *a.MinDocCount
	}
	if a.MinBounds != nil || a.MaxBounds != nil {
		bounds := map[string]any{}
		if a.MinBounds != nil {
			bounds["min"] = a.MinBounds
		}
		if a.MaxBounds != nil {
			bounds["max"] = a.MaxBounds
		}
		body["extended_bounds"] = bounds
	}

	return sourceAgg("histogram", body, a.SubAggs, a.Meta, a.Script)
}
