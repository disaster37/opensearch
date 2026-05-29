package querydsl

// DateRangeAggregation buckets date/time values into user-defined ranges.
// Unlike RangeAggregation, this variant is designed for date fields and
// supports a time zone and date format. Each range entry produces one bucket
// containing all documents whose date value falls within that interval.
//
// Typical use: documents created in the last week, this month, or last year.
//
// JSON output shape:
//
//	{"date_range": {"field": "created", "format": "MM-yyyy", "ranges": [{"from": "now-1M", "to": "now"}]}}
type DateRangeAggregation struct {
	Field    string                      `json:"field,omitempty"`
	Script   *Script                     `json:"-"`
	Keyed    *bool                       `json:"-"`
	Unmapped *bool                       `json:"-"`
	TimeZone string                      `json:"time_zone,omitempty"`
	Format   string                      `json:"format,omitempty"`
	Ranges   []DateRangeAggregationEntry `json:"-"`
	SubAggs  map[string]Aggregation      `json:"-"`
	Meta     map[string]any              `json:"-"`
}

// DateRangeAggregationEntry defines a single bucket in a date range
// aggregation, with an optional key, lower bound (From), and upper bound (To).
type DateRangeAggregationEntry struct {
	Key  string
	From any
	To   any
}

// NewDateRangeAggregation returns a zero-value DateRangeAggregation.
func NewDateRangeAggregation() DateRangeAggregation { return DateRangeAggregation{} }

func (a DateRangeAggregation) WithField(v string) DateRangeAggregation    { a.Field = v; return a }
func (a DateRangeAggregation) WithScript(v *Script) DateRangeAggregation  { a.Script = v; return a }
func (a DateRangeAggregation) WithKeyed(v bool) DateRangeAggregation      { a.Keyed = &v; return a }
func (a DateRangeAggregation) WithUnmapped(v bool) DateRangeAggregation   { a.Unmapped = &v; return a }
func (a DateRangeAggregation) WithTimeZone(v string) DateRangeAggregation { a.TimeZone = v; return a }
func (a DateRangeAggregation) WithFormat(v string) DateRangeAggregation   { a.Format = v; return a }

func (a DateRangeAggregation) WithSubAggregation(name string, sub Aggregation) DateRangeAggregation {
	if a.SubAggs == nil {
		a.SubAggs = map[string]Aggregation{}
	}
	a.SubAggs[name] = sub
	return a
}

func (a DateRangeAggregation) WithMeta(v map[string]any) DateRangeAggregation { a.Meta = v; return a }

func (a DateRangeAggregation) AddRange(from, to any) DateRangeAggregation {
	a.Ranges = append(a.Ranges, DateRangeAggregationEntry{From: from, To: to})
	return a
}

func (a DateRangeAggregation) AddRangeWithKey(key string, from, to any) DateRangeAggregation {
	a.Ranges = append(a.Ranges, DateRangeAggregationEntry{Key: key, From: from, To: to})
	return a
}

func (a DateRangeAggregation) AddUnboundedTo(from any) DateRangeAggregation {
	a.Ranges = append(a.Ranges, DateRangeAggregationEntry{From: from})
	return a
}

func (a DateRangeAggregation) AddUnboundedToWithKey(key string, from any) DateRangeAggregation {
	a.Ranges = append(a.Ranges, DateRangeAggregationEntry{Key: key, From: from})
	return a
}

func (a DateRangeAggregation) AddUnboundedFrom(to any) DateRangeAggregation {
	a.Ranges = append(a.Ranges, DateRangeAggregationEntry{To: to})
	return a
}

func (a DateRangeAggregation) AddUnboundedFromWithKey(key string, to any) DateRangeAggregation {
	a.Ranges = append(a.Ranges, DateRangeAggregationEntry{Key: key, To: to})
	return a
}

func (a DateRangeAggregation) Lt(to any) DateRangeAggregation {
	a.Ranges = append(a.Ranges, DateRangeAggregationEntry{To: to})
	return a
}

func (a DateRangeAggregation) LtWithKey(key string, to any) DateRangeAggregation {
	a.Ranges = append(a.Ranges, DateRangeAggregationEntry{Key: key, To: to})
	return a
}

func (a DateRangeAggregation) Between(from, to any) DateRangeAggregation {
	a.Ranges = append(a.Ranges, DateRangeAggregationEntry{From: from, To: to})
	return a
}

func (a DateRangeAggregation) BetweenWithKey(key string, from, to any) DateRangeAggregation {
	a.Ranges = append(a.Ranges, DateRangeAggregationEntry{Key: key, From: from, To: to})
	return a
}

func (a DateRangeAggregation) Gt(from any) DateRangeAggregation {
	a.Ranges = append(a.Ranges, DateRangeAggregationEntry{From: from})
	return a
}

func (a DateRangeAggregation) GtWithKey(key string, from any) DateRangeAggregation {
	a.Ranges = append(a.Ranges, DateRangeAggregationEntry{Key: key, From: from})
	return a
}

func (a DateRangeAggregation) Source() (any, error) {
	body := map[string]any{}
	if a.Field != "" {
		body["field"] = a.Field
	}
	if a.Keyed != nil {
		body["keyed"] = *a.Keyed
	}
	if a.Unmapped != nil {
		body["unmapped"] = *a.Unmapped
	}
	if a.TimeZone != "" {
		body["time_zone"] = a.TimeZone
	}
	if a.Format != "" {
		body["format"] = a.Format
	}

	ranges := make([]any, 0, len(a.Ranges))
	for _, ent := range a.Ranges {
		r := map[string]any{}
		if ent.Key != "" {
			r["key"] = ent.Key
		}
		if ent.From != nil {
			if v := rangeBoundValue(ent.From); v != nil {
				r["from"] = v
			}
		}
		if ent.To != nil {
			if v := rangeBoundValue(ent.To); v != nil {
				r["to"] = v
			}
		}
		ranges = append(ranges, r)
	}
	body["ranges"] = ranges

	return sourceAgg("date_range", body, a.SubAggs, a.Meta, a.Script)
}
