package querydsl

// GeoBoundsAggregation computes the geographic bounding box containing all
// geo_point values for a field across the aggregated documents.
type GeoBoundsAggregation struct {
	Field         string
	Script        *Script
	WrapLongitude *bool
	SubAggs       map[string]Aggregation
	Meta          map[string]any
}

// NewGeoBoundsAggregation returns a new GeoBoundsAggregation with default settings.
func NewGeoBoundsAggregation() *GeoBoundsAggregation { return &GeoBoundsAggregation{} }

// WithField sets the geo_point field to compute bounds on.
func (a *GeoBoundsAggregation) WithField(field string) *GeoBoundsAggregation {
	a.Field = field
	return a
}

// WithScript sets the script used to compute per-document geo values.
func (a *GeoBoundsAggregation) WithScript(script *Script) *GeoBoundsAggregation {
	a.Script = script
	return a
}

// WithWrapLongitude sets whether the bounding box is allowed to overlap the international date line.
func (a *GeoBoundsAggregation) WithWrapLongitude(v bool) *GeoBoundsAggregation {
	a.WrapLongitude = &v
	return a
}

// WithSubAggs sets the sub-aggregations.
func (a *GeoBoundsAggregation) WithSubAggs(subAggs map[string]Aggregation) *GeoBoundsAggregation {
	a.SubAggs = subAggs
	return a
}

// WithMeta sets the meta data for the aggregation.
func (a *GeoBoundsAggregation) WithMeta(meta map[string]any) *GeoBoundsAggregation {
	a.Meta = meta
	return a
}

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
