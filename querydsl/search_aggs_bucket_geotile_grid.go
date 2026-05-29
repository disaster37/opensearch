package querydsl

import "errors"

// GeoTileGridAggregation is a multi-bucket aggregation that works on geo_point fields
// and groups points by geotile cells.
// See: https://www.opensearch.co/guide/en/opensearchsearch/reference/7.0/search-aggregations-bucket-geotilegrid-aggregation.html
type GeoTileGridAggregation struct {
	Field     string                 `json:"field"`
	Precision *int                   `json:"precision,omitempty"`
	Size      *int                   `json:"size,omitempty"`
	ShardSize *int                   `json:"shard_size,omitempty"`
	Bounds    *BoundingBox           `json:"bounds,omitempty"`
	SubAggs   map[string]Aggregation `json:"-"`
	Meta      map[string]any         `json:"meta,omitempty"`
}

// NewGeoTileGridAggregation creates a new GeoTileGridAggregation.
func NewGeoTileGridAggregation() GeoTileGridAggregation {
	return GeoTileGridAggregation{}
}

func (a GeoTileGridAggregation) WithPrecision(precision int) GeoTileGridAggregation {
	a.Precision = &precision
	return a
}

func (a GeoTileGridAggregation) WithSize(size int) GeoTileGridAggregation {
	a.Size = &size
	return a
}

func (a GeoTileGridAggregation) WithShardSize(shardSize int) GeoTileGridAggregation {
	a.ShardSize = &shardSize
	return a
}

func (a GeoTileGridAggregation) WithBounds(bounds BoundingBox) GeoTileGridAggregation {
	a.Bounds = &bounds
	return a
}

func (a GeoTileGridAggregation) SubAggregation(name string, subAggregation Aggregation) GeoTileGridAggregation {
	if a.SubAggs == nil {
		a.SubAggs = make(map[string]Aggregation)
	}
	a.SubAggs[name] = subAggregation
	return a
}

func (a GeoTileGridAggregation) WithMeta(metaData map[string]any) GeoTileGridAggregation {
	a.Meta = metaData
	return a
}

// Source returns a JSON-serializable interface.
func (a GeoTileGridAggregation) Source() (any, error) {
	if a.Field == "" {
		return nil, errors.New("opensearch: 'field' is a mandatory parameter")
	}

	body := make(map[string]any)
	body["field"] = a.Field

	if a.Precision != nil {
		body["precision"] = *a.Precision
	}
	if a.Size != nil {
		body["size"] = *a.Size
	}
	if a.ShardSize != nil {
		body["shard_size"] = *a.ShardSize
	}
	if a.Bounds != nil {
		body["bounds"] = *a.Bounds
	}

	return sourceAgg("geotile_grid", body, a.SubAggs, a.Meta, nil)
}

// BoundingBox bounding box
type BoundingBox struct {
	TopLeft     GeoPoint `json:"top_left"`
	BottomRight GeoPoint `json:"bottom_right"`
}
