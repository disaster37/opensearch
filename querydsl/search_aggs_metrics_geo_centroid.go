package querydsl

// GeoCentroidAggregation computes the weighted centroid from all coordinate
// values for geo_point fields across the aggregated documents.
type GeoCentroidAggregation struct {
	Field   string
	Script  *Script
	SubAggs map[string]Aggregation
	Meta    map[string]any
}

// NewGeoCentroidAggregation returns a new GeoCentroidAggregation with default settings.
func NewGeoCentroidAggregation() *GeoCentroidAggregation { return &GeoCentroidAggregation{} }

// WithField sets the geo_point field to compute the centroid on.
func (a *GeoCentroidAggregation) WithField(field string) *GeoCentroidAggregation {
	a.Field = field
	return a
}

// WithScript sets the script used to compute per-document geo values.
func (a *GeoCentroidAggregation) WithScript(script *Script) *GeoCentroidAggregation {
	a.Script = script
	return a
}

// WithSubAggs sets the sub-aggregations.
func (a *GeoCentroidAggregation) WithSubAggs(subAggs map[string]Aggregation) *GeoCentroidAggregation {
	a.SubAggs = subAggs
	return a
}

// WithMeta sets the meta data for the aggregation.
func (a *GeoCentroidAggregation) WithMeta(meta map[string]any) *GeoCentroidAggregation {
	a.Meta = meta
	return a
}

func (a GeoCentroidAggregation) Source() (any, error) {
	body := map[string]any{}
	if a.Field != "" {
		body["field"] = a.Field
	}
	return sourceAgg("geo_centroid", body, a.SubAggs, a.Meta, a.Script)
}
