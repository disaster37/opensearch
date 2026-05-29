package querydsl

type GeoPolygonQuery struct {
	Field     string
	Points    []*GeoPoint
	QueryName string
}

func NewGeoPolygonQuery(field string) GeoPolygonQuery {
	return GeoPolygonQuery{Field: field}
}

func (q GeoPolygonQuery) AddPoint(lat, lon float64) GeoPolygonQuery {
	q.Points = append(q.Points, GeoPointFromLatLon(lat, lon))
	return q
}

func (q GeoPolygonQuery) Source() (any, error) {
	points := make([]any, len(q.Points))
	for i, p := range q.Points {
		points[i] = p.Source()
	}
	polygon := map[string]any{"points": points}
	params := map[string]any{q.Field: polygon}
	if q.QueryName != "" {
		params["_name"] = q.QueryName
	}
	return map[string]any{"geo_polygon": params}, nil
}
