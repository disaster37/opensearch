package querydsl

// MaxAggregation computes the maximum value of a numeric field across all
// matching documents. Returns null if no documents have a value for the field.
//
// JSON output shape:
//
//	{"max": {"field": "price"}}
type MaxAggregation struct {
	Field   string
	Script  *Script
	Format  string
	Missing any
	SubAggs map[string]Aggregation
	Meta    map[string]any
}

// NewMaxAggregation returns a new MaxAggregation with default settings.
func NewMaxAggregation() *MaxAggregation { return &MaxAggregation{} }

// WithField sets the field to compute the maximum on.
func (a *MaxAggregation) WithField(field string) *MaxAggregation {
	a.Field = field
	return a
}

// WithScript sets the script used to compute per-document values.
func (a *MaxAggregation) WithScript(script *Script) *MaxAggregation {
	a.Script = script
	return a
}

// WithFormat sets the numeric format for the output value.
func (a *MaxAggregation) WithFormat(format string) *MaxAggregation {
	a.Format = format
	return a
}

// WithMissing sets the value to use for documents missing the field.
func (a *MaxAggregation) WithMissing(missing any) *MaxAggregation {
	a.Missing = missing
	return a
}

// WithSubAggs sets the sub-aggregations.
func (a *MaxAggregation) WithSubAggs(subAggs map[string]Aggregation) *MaxAggregation {
	a.SubAggs = subAggs
	return a
}

// WithMeta sets the meta data for the aggregation.
func (a *MaxAggregation) WithMeta(meta map[string]any) *MaxAggregation {
	a.Meta = meta
	return a
}

func (a MaxAggregation) Source() (any, error) {
	body := map[string]any{}
	if a.Field != "" {
		body["field"] = a.Field
	}
	if a.Format != "" {
		body["format"] = a.Format
	}
	if a.Missing != nil {
		body["missing"] = a.Missing
	}
	return sourceAgg("max", body, a.SubAggs, a.Meta, a.Script)
}
