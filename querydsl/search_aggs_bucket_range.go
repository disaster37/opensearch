package querydsl

import "time"

// RangeAggregation buckets numeric or date values into user-defined ranges.
// Each range produces one bucket; a document may appear in multiple buckets
// if its value matches more than one range. Open-ended ranges (from only, to
// only) are supported.
//
// Typical use: price brackets (0–50, 50–100, 100+), age groups.
//
// JSON output shape:
//
//	{"range": {"field": "price", "ranges": [{"to": 50}, {"from": 50, "to": 100}, {"from": 100}]}}
type RangeAggregation struct {
	FieldVal string  `json:"field,omitempty"`
	Script   *Script `json:"-"`
	Missing  any     `json:"missing,omitempty"`
	Keyed    *bool   `json:"keyed,omitempty"`
	Unmapped *bool   `json:"unmapped,omitempty"`
	Ranges   []RangeAggregationEntry
	SubAggs  map[string]Aggregation `json:"-"`
	Meta     map[string]any         `json:"meta,omitempty"`
}

// RangeAggregationEntry defines a single bucket in a range aggregation,
// with optional key, lower bound (From), and upper bound (To).
type RangeAggregationEntry struct {
	Key  string `json:"key,omitempty"`
	From any    `json:"from,omitempty"`
	To   any    `json:"to,omitempty"`
}

// NewRangeAggregation returns a zero-value RangeAggregation.
func NewRangeAggregation() *RangeAggregation { return &RangeAggregation{} }

// WithField sets the field to aggregate on.
func (a *RangeAggregation) WithField(field string) *RangeAggregation {
	a.FieldVal = field
	return a
}

// WithScript sets the script used to compute values.
func (a *RangeAggregation) WithScript(script *Script) *RangeAggregation {
	a.Script = script
	return a
}

// WithMissing sets the value to use for documents missing the field.
func (a *RangeAggregation) WithMissing(missing any) *RangeAggregation {
	a.Missing = missing
	return a
}

// WithKeyed sets whether buckets are returned as a keyed object.
func (a *RangeAggregation) WithKeyed(keyed bool) *RangeAggregation {
	a.Keyed = &keyed
	return a
}

// WithUnmapped sets whether unmapped fields are treated as missing.
func (a *RangeAggregation) WithUnmapped(unmapped bool) *RangeAggregation {
	a.Unmapped = &unmapped
	return a
}

// WithSubAggregation adds a sub-aggregation.
func (a *RangeAggregation) WithSubAggregation(name string, sub Aggregation) *RangeAggregation {
	if a.SubAggs == nil {
		a.SubAggs = map[string]Aggregation{}
	}
	a.SubAggs[name] = sub
	return a
}

// WithMeta sets the meta data for the aggregation.
func (a *RangeAggregation) WithMeta(meta map[string]any) *RangeAggregation {
	a.Meta = meta
	return a
}

func (a *RangeAggregation) AddRange(from, to any) *RangeAggregation {
	a.Ranges = append(a.Ranges, RangeAggregationEntry{From: from, To: to})
	return a
}

func (a *RangeAggregation) AddRangeWithKey(key string, from, to any) *RangeAggregation {
	a.Ranges = append(a.Ranges, RangeAggregationEntry{Key: key, From: from, To: to})
	return a
}

func (a *RangeAggregation) AddUnboundedTo(from any) *RangeAggregation {
	a.Ranges = append(a.Ranges, RangeAggregationEntry{From: from})
	return a
}

func (a *RangeAggregation) AddUnboundedToWithKey(key string, from any) *RangeAggregation {
	a.Ranges = append(a.Ranges, RangeAggregationEntry{Key: key, From: from})
	return a
}

func (a *RangeAggregation) AddUnboundedFrom(to any) *RangeAggregation {
	a.Ranges = append(a.Ranges, RangeAggregationEntry{To: to})
	return a
}

func (a *RangeAggregation) AddUnboundedFromWithKey(key string, to any) *RangeAggregation {
	a.Ranges = append(a.Ranges, RangeAggregationEntry{Key: key, To: to})
	return a
}

func (a *RangeAggregation) Lt(to any) *RangeAggregation {
	a.Ranges = append(a.Ranges, RangeAggregationEntry{To: to})
	return a
}

func (a *RangeAggregation) LtWithKey(key string, to any) *RangeAggregation {
	a.Ranges = append(a.Ranges, RangeAggregationEntry{Key: key, To: to})
	return a
}

func (a *RangeAggregation) Between(from, to any) *RangeAggregation {
	a.Ranges = append(a.Ranges, RangeAggregationEntry{From: from, To: to})
	return a
}

func (a *RangeAggregation) BetweenWithKey(key string, from, to any) *RangeAggregation {
	a.Ranges = append(a.Ranges, RangeAggregationEntry{Key: key, From: from, To: to})
	return a
}

func (a *RangeAggregation) Gt(from any) *RangeAggregation {
	a.Ranges = append(a.Ranges, RangeAggregationEntry{From: from})
	return a
}

func (a *RangeAggregation) GtWithKey(key string, from any) *RangeAggregation {
	a.Ranges = append(a.Ranges, RangeAggregationEntry{Key: key, From: from})
	return a
}

func rangeBoundValue(v any) any {
	switch x := v.(type) {
	case int, int16, int32, int64, float32, float64:
		return x
	case *int, *int16, *int32, *int64, *float32, *float64:
		return x
	case time.Time:
		return x.Format(time.RFC3339)
	case *time.Time:
		return x.Format(time.RFC3339)
	case string:
		return x
	case *string:
		return x
	}
	return nil
}

func (a RangeAggregation) Source() (any, error) {
	body := map[string]any{}
	if a.FieldVal != "" {
		body["field"] = a.FieldVal
	}
	if a.Missing != nil {
		body["missing"] = a.Missing
	}
	if a.Keyed != nil {
		body["keyed"] = *a.Keyed
	}
	if a.Unmapped != nil {
		body["unmapped"] = *a.Unmapped
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

	return sourceAgg("range", body, a.SubAggs, a.Meta, a.Script)
}
