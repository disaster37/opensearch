package querydsl

type GeoDistanceQuery struct {
	Field        string
	Lat          float64
	Lon          float64
	GeoHash      string
	Distance     string
	DistanceType string
	QueryName    string
}

func NewGeoDistanceQuery(field string) *GeoDistanceQuery {
	return &GeoDistanceQuery{Field: field}
}

// WithGeoHash sets the geohash for the center point instead of lat/lon.
func (q *GeoDistanceQuery) WithGeoHash(geoHash string) *GeoDistanceQuery {
	q.GeoHash = geoHash
	return q
}

// WithDistance sets the distance radius for the query (e.g., "12km").
func (q *GeoDistanceQuery) WithDistance(distance string) *GeoDistanceQuery {
	q.Distance = distance
	return q
}

// WithDistanceType sets the distance calculation type (arc or plane).
func (q *GeoDistanceQuery) WithDistanceType(distanceType string) *GeoDistanceQuery {
	q.DistanceType = distanceType
	return q
}

// WithQueryName sets the query name used for matched_filters per hit.
func (q *GeoDistanceQuery) WithQueryName(name string) *GeoDistanceQuery {
	q.QueryName = name
	return q
}

func (q GeoDistanceQuery) Point(lat, lon float64) GeoDistanceQuery {
	q.Lat = lat
	q.Lon = lon
	return q
}

func (q GeoDistanceQuery) FromGeoPoint(point *GeoPoint) GeoDistanceQuery {
	q.Lat = point.Lat
	q.Lon = point.Lon
	return q
}

func (q GeoDistanceQuery) Source() (any, error) {
	params := map[string]any{}
	if q.GeoHash != "" {
		params[q.Field] = q.GeoHash
	} else {
		params[q.Field] = map[string]any{"lat": q.Lat, "lon": q.Lon}
	}
	if q.Distance != "" {
		params["distance"] = q.Distance
	}
	if q.DistanceType != "" {
		params["distance_type"] = q.DistanceType
	}
	if q.QueryName != "" {
		params["_name"] = q.QueryName
	}
	return map[string]any{"geo_distance": params}, nil
}
