package querydsl

import "fmt"

type DistanceFeatureQuery struct {
	Field     string
	Pivot     string
	Origin    any
	Boost     *float64
	QueryName string
}

func NewDistanceFeatureQuery(field string, origin any, pivot string) *DistanceFeatureQuery {
	return &DistanceFeatureQuery{Field: field, Origin: origin, Pivot: pivot}
}

// WithBoost sets the boost factor for this query.
func (q *DistanceFeatureQuery) WithBoost(boost float64) *DistanceFeatureQuery {
	q.Boost = &boost
	return q
}

// WithQueryName sets the query name used for matched_filters per hit.
func (q *DistanceFeatureQuery) WithQueryName(name string) *DistanceFeatureQuery {
	q.QueryName = name
	return q
}

func (q DistanceFeatureQuery) Source() (any, error) {
	var origin any
	switch v := q.Origin.(type) {
	case string:
		origin = v
	case *GeoPoint:
		origin = v.Source()
	case GeoPoint:
		origin = v.Source()
	default:
		return nil, fmt.Errorf("DistanceFeatureQuery: unable to serialize Origin from type %T", v)
	}
	body := map[string]any{"field": q.Field, "pivot": q.Pivot, "origin": origin}
	if q.Boost != nil {
		body["boost"] = *q.Boost
	}
	if q.QueryName != "" {
		body["_name"] = q.QueryName
	}
	return map[string]any{"distance_feature": body}, nil
}
