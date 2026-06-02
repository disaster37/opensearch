package querydsl

type IPRangeAggregation struct {
	Field   string                    `json:"field,omitempty"`
	Keyed   *bool                     `json:"-"`
	Entries []IPRangeAggregationEntry `json:"-"`
	SubAggs map[string]Aggregation    `json:"-"`
	Meta    map[string]any            `json:"-"`
}

type IPRangeAggregationEntry struct {
	Key  string
	Mask string
	From string
	To   string
}

func NewIPRangeAggregation() *IPRangeAggregation { return &IPRangeAggregation{} }

// WithField sets the IP field to aggregate on.
func (a *IPRangeAggregation) WithField(v string) *IPRangeAggregation { a.Field = v; return a }

// WithKeyed sets whether buckets are returned as a keyed object.
func (a *IPRangeAggregation) WithKeyed(v bool) *IPRangeAggregation { a.Keyed = &v; return a }

// WithSubAggregation adds a sub-aggregation.
func (a *IPRangeAggregation) WithSubAggregation(name string, sub Aggregation) *IPRangeAggregation {
	if a.SubAggs == nil {
		a.SubAggs = map[string]Aggregation{}
	}
	a.SubAggs[name] = sub
	return a
}

// WithMeta sets the meta data for the aggregation.
func (a *IPRangeAggregation) WithMeta(v map[string]any) *IPRangeAggregation { a.Meta = v; return a }

func (a *IPRangeAggregation) AddMaskRange(mask string) *IPRangeAggregation {
	a.Entries = append(a.Entries, IPRangeAggregationEntry{Mask: mask})
	return a
}

func (a *IPRangeAggregation) AddMaskRangeWithKey(key, mask string) *IPRangeAggregation {
	a.Entries = append(a.Entries, IPRangeAggregationEntry{Key: key, Mask: mask})
	return a
}

func (a *IPRangeAggregation) AddRange(from, to string) *IPRangeAggregation {
	a.Entries = append(a.Entries, IPRangeAggregationEntry{From: from, To: to})
	return a
}

func (a *IPRangeAggregation) AddRangeWithKey(key, from, to string) *IPRangeAggregation {
	a.Entries = append(a.Entries, IPRangeAggregationEntry{Key: key, From: from, To: to})
	return a
}

func (a *IPRangeAggregation) AddUnboundedTo(from string) *IPRangeAggregation {
	a.Entries = append(a.Entries, IPRangeAggregationEntry{From: from})
	return a
}

func (a *IPRangeAggregation) AddUnboundedToWithKey(key, from string) *IPRangeAggregation {
	a.Entries = append(a.Entries, IPRangeAggregationEntry{Key: key, From: from})
	return a
}

func (a *IPRangeAggregation) AddUnboundedFrom(to string) *IPRangeAggregation {
	a.Entries = append(a.Entries, IPRangeAggregationEntry{To: to})
	return a
}

func (a *IPRangeAggregation) AddUnboundedFromWithKey(key, to string) *IPRangeAggregation {
	a.Entries = append(a.Entries, IPRangeAggregationEntry{Key: key, To: to})
	return a
}

func (a *IPRangeAggregation) Lt(to string) *IPRangeAggregation {
	a.Entries = append(a.Entries, IPRangeAggregationEntry{To: to})
	return a
}

func (a *IPRangeAggregation) LtWithKey(key, to string) *IPRangeAggregation {
	a.Entries = append(a.Entries, IPRangeAggregationEntry{Key: key, To: to})
	return a
}

func (a *IPRangeAggregation) Between(from, to string) *IPRangeAggregation {
	a.Entries = append(a.Entries, IPRangeAggregationEntry{From: from, To: to})
	return a
}

func (a *IPRangeAggregation) BetweenWithKey(key, from, to string) *IPRangeAggregation {
	a.Entries = append(a.Entries, IPRangeAggregationEntry{Key: key, From: from, To: to})
	return a
}

func (a *IPRangeAggregation) Gt(from string) *IPRangeAggregation {
	a.Entries = append(a.Entries, IPRangeAggregationEntry{From: from})
	return a
}

func (a *IPRangeAggregation) GtWithKey(key, from string) *IPRangeAggregation {
	a.Entries = append(a.Entries, IPRangeAggregationEntry{Key: key, From: from})
	return a
}

func (a IPRangeAggregation) Source() (any, error) {
	body := map[string]any{}
	if a.Field != "" {
		body["field"] = a.Field
	}
	if a.Keyed != nil {
		body["keyed"] = *a.Keyed
	}

	ranges := make([]any, 0, len(a.Entries))
	for _, ent := range a.Entries {
		r := map[string]any{}
		if ent.Key != "" {
			r["key"] = ent.Key
		}
		if ent.Mask != "" {
			r["mask"] = ent.Mask
		} else {
			if ent.From != "" {
				r["from"] = ent.From
			}
			if ent.To != "" {
				r["to"] = ent.To
			}
		}
		ranges = append(ranges, r)
	}
	body["ranges"] = ranges

	return sourceAgg("ip_range", body, a.SubAggs, a.Meta, nil)
}
