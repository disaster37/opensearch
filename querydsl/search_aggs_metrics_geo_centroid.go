package querydsl

type GeoCentroidAggregation struct {
	Field   string
	Script  *Script
	SubAggs map[string]Aggregation
	Meta    map[string]any
}

func NewGeoCentroidAggregation() GeoCentroidAggregation { return GeoCentroidAggregation{} }

func (a GeoCentroidAggregation) Source() (any, error) {
	body := map[string]any{}
	if a.Field != "" {
		body["field"] = a.Field
	}
	return sourceAgg("geo_centroid", body, a.SubAggs, a.Meta, a.Script)
}
