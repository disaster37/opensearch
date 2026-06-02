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

func NewGeoHashGridAggregation() *GeoHashGridAggregation {
	return &GeoHashGridAggregation{}
}

// WithField sets the geo_point field to aggregate on.
func (a *GeoHashGridAggregation) WithField(field string) *GeoHashGridAggregation {
	a.GeoHashField = field
	return a
}

// WithPrecision sets the geohash precision level.
func (a *GeoHashGridAggregation) WithPrecision(precision any) *GeoHashGridAggregation {
	a.Precision = precision
	return a
}

// WithSize sets the maximum number of buckets to return.
func (a *GeoHashGridAggregation) WithSize(size int) *GeoHashGridAggregation {
	a.Size = &size
	return a
}

// WithShardSize sets the maximum number of buckets to collect per shard.
func (a *GeoHashGridAggregation) WithShardSize(shardSize int) *GeoHashGridAggregation {
	a.ShardSize = &shardSize
	return a
}

// WithMeta sets the meta data for the aggregation.
func (a *GeoHashGridAggregation) WithMeta(meta map[string]any) *GeoHashGridAggregation {
	a.Meta = meta
	return a
}

// SubAggregation adds a sub-aggregation.
func (a *GeoHashGridAggregation) SubAggregation(name string, subAggregation Aggregation) *GeoHashGridAggregation {
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
