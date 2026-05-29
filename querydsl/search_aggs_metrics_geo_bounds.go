package querydsl

type GeoBoundsAggregation struct {
	Field         string
	Script        *Script
	WrapLongitude *bool
	SubAggs       map[string]Aggregation
	Meta          map[string]any
}

func NewGeoBoundsAggregation() GeoBoundsAggregation { return GeoBoundsAggregation{} }

func (a GeoBoundsAggregation) Source() (any, error) {
	body := map[string]any{}
	if a.Field != "" {
		body["field"] = a.Field
	}
	if a.WrapLongitude != nil {
		body["wrap_longitude"] = *a.WrapLongitude
	}
	return sourceAgg("geo_bounds", body, a.SubAggs, a.Meta, a.Script)
}
