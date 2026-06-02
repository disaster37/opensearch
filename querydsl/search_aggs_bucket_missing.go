package querydsl

// MissingAggregation produces a single bucket of documents that have no value
// for the specified field (i.e. the field is null or not present). Sub-aggregations
// are computed only on the documents within this bucket.
//
// Typical use: count documents where "email" is not set.
//
// JSON output shape:
//
//	{"missing": {"field": "email"}}
type MissingAggregation struct {
	Field   string
	SubAggs map[string]Aggregation
	Meta    map[string]any
}

// NewMissingAggregation returns a zero-value MissingAggregation.
func NewMissingAggregation() *MissingAggregation { return &MissingAggregation{} }

// WithField sets the field for which missing values are counted.
func (a *MissingAggregation) WithField(field string) *MissingAggregation {
	a.Field = field
	return a
}

// WithSubAggregation adds a sub-aggregation.
func (a *MissingAggregation) WithSubAggregation(name string, sub Aggregation) *MissingAggregation {
	if a.SubAggs == nil {
		a.SubAggs = map[string]Aggregation{}
	}
	a.SubAggs[name] = sub
	return a
}

// WithMeta sets the meta data for the aggregation.
func (a *MissingAggregation) WithMeta(meta map[string]any) *MissingAggregation {
	a.Meta = meta
	return a
}

func (a MissingAggregation) Source() (any, error) {
	body := map[string]any{}
	if a.Field != "" {
		body["field"] = a.Field
	}
	return sourceAgg("missing", body, a.SubAggs, a.Meta, nil)
}
