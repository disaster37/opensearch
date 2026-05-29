package querydsl

// GeoHashGridAggregation is a multi-bucket aggregation that works on geo_point fields
// and groups points by geohash cells.
// See: https://www.opensearch.co/guide/en/opensearchsearch/reference/7.0/search-aggregations-bucket-geohashgrid-aggregation.html
type GeoHashGridAggregation struct {
	GeoHashField string                 `json:"field,omitempty"`
	Precision    any                    `json:"precision,omitempty"`
	Size         *int                   `json:"size,omitempty"`
	ShardSize    *int                   `json:"shard_size,omitempty"`
	SubAggs      map[string]Aggregation `json:"-"`
	Meta         map[string]any         `json:"meta,omitempty"`
}

func NewGeoHashGridAggregation() GeoHashGridAggregation {
	return GeoHashGridAggregation{}
}

func (a GeoHashGridAggregation) SubAggregation(name string, subAggregation Aggregation) GeoHashGridAggregation {
	if a.SubAggs == nil {
		a.SubAggs = make(map[string]Aggregation)
	}
	a.SubAggs[name] = subAggregation
	return a
}

func (a GeoHashGridAggregation) Source() (any, error) {
	body := make(map[string]any)

	if a.GeoHashField != "" {
		body["field"] = a.GeoHashField
	}
	if a.Precision != nil {
		body["precision"] = a.Precision
	}
	if a.Size != nil {
		body["size"] = *a.Size
	}
	if a.ShardSize != nil {
		body["shard_size"] = *a.ShardSize
	}

	return sourceAgg("geohash_grid", body, a.SubAggs, a.Meta, nil)
}
