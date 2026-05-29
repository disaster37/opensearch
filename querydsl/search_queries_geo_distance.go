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

func NewGeoDistanceQuery(field string) GeoDistanceQuery {
	return GeoDistanceQuery{Field: field}
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
