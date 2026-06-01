package querydsl

// DateHistogramAggregation buckets date/time values into fixed or calendar
// intervals (e.g. "1d", "month"). Each bucket represents a time window and
// contains all documents whose field value falls within that window.
// Supports time zone, offset, extended bounds, and keyed output.
//
// Typical use: time-series charts, monthly/weekly roll-ups.
//
// JSON output shape:
//
//	{"date_histogram": {"field": "timestamp", "calendar_interval": "month", "time_zone": "UTC"}}
type DateHistogramAggregation struct {
	Field             string
	Script            *Script
	Missing           any
	SubAggs           map[string]Aggregation
	Meta              map[string]any
	Interval          string
	FixedInterval     string
	CalendarInterval  string
	Order             string
	OrderAsc          bool
	MinDocCount       *int64
	ExtendedBoundsMin any
	ExtendedBoundsMax any
	TimeZone          string
	Format            string
	Offset            string
	Keyed             *bool
}

// NewDateHistogramAggregation returns a zero-value DateHistogramAggregation.
func NewDateHistogramAggregation() DateHistogramAggregation {
	return DateHistogramAggregation{}
}

func (a *DateHistogramAggregation) Field_(v string) *DateHistogramAggregation { a.Field = v; return a }

func (a *DateHistogramAggregation) Script_(v *Script) *DateHistogramAggregation {
	a.Script = v
	return a
}

func (a *DateHistogramAggregation) Missing_(v any) *DateHistogramAggregation { a.Missing = v; return a }

func (a *DateHistogramAggregation) SubAggregation(name string, sub Aggregation) *DateHistogramAggregation {
	if a.SubAggs == nil {
		a.SubAggs = make(map[string]Aggregation)
	}
	a.SubAggs[name] = sub
	return a
}

func (a *DateHistogramAggregation) Meta_(v map[string]any) *DateHistogramAggregation {
	a.Meta = v
	return a
}

func (a *DateHistogramAggregation) Interval_(v string) *DateHistogramAggregation {
	a.Interval = v
	return a
}

func (a *DateHistogramAggregation) FixedInterval_(v string) *DateHistogramAggregation {
	a.FixedInterval = v
	return a
}

func (a *DateHistogramAggregation) CalendarInterval_(v string) *DateHistogramAggregation {
	a.CalendarInterval = v
	return a
}

func (a *DateHistogramAggregation) MinDocCount_(v int64) *DateHistogramAggregation {
	a.MinDocCount = &v
	return a
}

func (a *DateHistogramAggregation) TimeZone_(v string) *DateHistogramAggregation {
	a.TimeZone = v
	return a
}

func (a *DateHistogramAggregation) Format_(v string) *DateHistogramAggregation {
	a.Format = v
	return a
}

func (a *DateHistogramAggregation) Offset_(v string) *DateHistogramAggregation {
	a.Offset = v
	return a
}

func (a *DateHistogramAggregation) Keyed_(v bool) *DateHistogramAggregation { a.Keyed = &v; return a }

func (a *DateHistogramAggregation) ExtendedBounds(min, max any) *DateHistogramAggregation {
	a.ExtendedBoundsMin = min
	a.ExtendedBoundsMax = max
	return a
}

func (a *DateHistogramAggregation) Order_(order string, asc bool) *DateHistogramAggregation {
	a.Order = order
	a.OrderAsc = asc
	return a
}

func (a *DateHistogramAggregation) OrderByCount(asc bool) *DateHistogramAggregation {
	return a.Order_("_count", asc)
}

func (a *DateHistogramAggregation) OrderByCountAsc() *DateHistogramAggregation {
	return a.OrderByCount(true)
}

func (a *DateHistogramAggregation) OrderByCountDesc() *DateHistogramAggregation {
	return a.OrderByCount(false)
}

func (a *DateHistogramAggregation) OrderByKey(asc bool) *DateHistogramAggregation {
	return a.Order_("_key", asc)
}

func (a *DateHistogramAggregation) OrderByKeyAsc() *DateHistogramAggregation {
	return a.OrderByKey(true)
}

func (a *DateHistogramAggregation) OrderByKeyDesc() *DateHistogramAggregation {
	return a.OrderByKey(false)
}

func (a *DateHistogramAggregation) OrderByAggregation(aggName string, asc bool) *DateHistogramAggregation {
	return a.Order_(aggName, asc)
}

func (a *DateHistogramAggregation) OrderByAggregationAndMetric(aggName, metric string, asc bool) *DateHistogramAggregation {
	return a.Order_(aggName+"."+metric, asc)
}

func (a DateHistogramAggregation) Source() (any, error) {
	body := map[string]any{}

	if a.Field != "" {
		body["field"] = a.Field
	}
	if a.Missing != nil {
		body["missing"] = a.Missing
	}
	if a.Interval != "" {
		body["interval"] = a.Interval
	}
	if a.FixedInterval != "" {
		body["fixed_interval"] = a.FixedInterval
	}
	if a.CalendarInterval != "" {
		body["calendar_interval"] = a.CalendarInterval
	}
	if a.MinDocCount != nil {
		body["min_doc_count"] = *a.MinDocCount
	}
	if a.Order != "" {
		dir := "desc"
		if a.OrderAsc {
			dir = "asc"
		}
		body["order"] = map[string]any{a.Order: dir}
	}
	if a.TimeZone != "" {
		body["time_zone"] = a.TimeZone
	}
	if a.Offset != "" {
		body["offset"] = a.Offset
	}
	if a.Format != "" {
		body["format"] = a.Format
	}
	if a.ExtendedBoundsMin != nil || a.ExtendedBoundsMax != nil {
		bounds := map[string]any{}
		if a.ExtendedBoundsMin != nil {
			bounds["min"] = a.ExtendedBoundsMin
		}
		if a.ExtendedBoundsMax != nil {
			bounds["max"] = a.ExtendedBoundsMax
		}
		body["extended_bounds"] = bounds
	}
	if a.Keyed != nil {
		body["keyed"] = *a.Keyed
	}

	return sourceAgg("date_histogram", body, a.SubAggs, a.Meta, a.Script)
}
