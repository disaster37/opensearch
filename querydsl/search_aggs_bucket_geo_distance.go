package querydsl

// GeoDistanceAggregation is a multi-bucket aggregation that works on geo_point fields
// and conceptually works very similar to the range aggregation.
// See: https://www.opensearch.co/guide/en/opensearchsearch/reference/7.0/search-aggregations-bucket-geodistance-aggregation.html
type GeoDistanceAggregation struct {
	Field        string                 `json:"field,omitempty"`
	Unit         string                 `json:"unit,omitempty"`
	DistanceType string                 `json:"distance_type,omitempty"`
	Origin       string                 `json:"origin,omitempty"`
	Ranges       []GeoDistanceRange     `json:"ranges"`
	SubAggs      map[string]Aggregation `json:"-"`
	Meta         map[string]any         `json:"meta,omitempty"`
}

type GeoDistanceRange struct {
	Key  string `json:"key,omitempty"`
	From any    `json:"from,omitempty"`
	To   any    `json:"to,omitempty"`
}

func NewGeoDistanceAggregation() GeoDistanceAggregation {
	return GeoDistanceAggregation{}
}

func (a GeoDistanceAggregation) SubAggregation(name string, subAggregation Aggregation) GeoDistanceAggregation {
	if a.SubAggs == nil {
		a.SubAggs = make(map[string]Aggregation)
	}
	a.SubAggs[name] = subAggregation
	return a
}

func (a GeoDistanceAggregation) AddRange(from, to any) GeoDistanceAggregation {
	a.Ranges = append(a.Ranges, GeoDistanceRange{From: from, To: to})
	return a
}

func (a GeoDistanceAggregation) AddRangeWithKey(key string, from, to any) GeoDistanceAggregation {
	a.Ranges = append(a.Ranges, GeoDistanceRange{Key: key, From: from, To: to})
	return a
}

func (a GeoDistanceAggregation) AddUnboundedTo(from float64) GeoDistanceAggregation {
	a.Ranges = append(a.Ranges, GeoDistanceRange{From: from, To: nil})
	return a
}

func (a GeoDistanceAggregation) AddUnboundedToWithKey(key string, from float64) GeoDistanceAggregation {
	a.Ranges = append(a.Ranges, GeoDistanceRange{Key: key, From: from, To: nil})
	return a
}

func (a GeoDistanceAggregation) AddUnboundedFrom(to float64) GeoDistanceAggregation {
	a.Ranges = append(a.Ranges, GeoDistanceRange{From: nil, To: to})
	return a
}

func (a GeoDistanceAggregation) AddUnboundedFromWithKey(key string, to float64) GeoDistanceAggregation {
	a.Ranges = append(a.Ranges, GeoDistanceRange{Key: key, From: nil, To: to})
	return a
}

func (a GeoDistanceAggregation) Between(from, to any) GeoDistanceAggregation {
	a.Ranges = append(a.Ranges, GeoDistanceRange{From: from, To: to})
	return a
}

func (a GeoDistanceAggregation) BetweenWithKey(key string, from, to any) GeoDistanceAggregation {
	a.Ranges = append(a.Ranges, GeoDistanceRange{Key: key, From: from, To: to})
	return a
}

func geoRangeBound(v any) any {
	switch x := v.(type) {
	case int, int16, int32, int64, float32, float64:
		return x
	case *int, *int16, *int32, *int64, *float32, *float64:
		return x
	case string:
		return x
	case *string:
		return x
	}
	return nil
}

func (a GeoDistanceAggregation) Source() (any, error) {
	body := make(map[string]any)

	if a.Field != "" {
		body["field"] = a.Field
	}
	if a.Unit != "" {
		body["unit"] = a.Unit
	}
	if a.DistanceType != "" {
		body["distance_type"] = a.DistanceType
	}
	if a.Origin != "" {
		body["origin"] = a.Origin
	}

	ranges := make([]any, 0, len(a.Ranges))
	for _, ent := range a.Ranges {
		r := make(map[string]any)
		if ent.Key != "" {
			r["key"] = ent.Key
		}
		if ent.From != nil {
			if v := geoRangeBound(ent.From); v != nil {
				r["from"] = v
			}
		}
		if ent.To != nil {
			if v := geoRangeBound(ent.To); v != nil {
				r["to"] = v
			}
		}
		ranges = append(ranges, r)
	}
	body["ranges"] = ranges

	return sourceAgg("geo_distance", body, a.SubAggs, a.Meta, nil)
}
