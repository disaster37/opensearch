package querydsl

// MinAggregation computes the minimum value of a numeric field across all
// matching documents. Returns null if no documents have a value for the field.
//
// JSON output shape:
//
//	{"min": {"field": "price"}}
type MinAggregation struct {
	Field   string
	Script  *Script
	Format  string
	Missing any
	SubAggs map[string]Aggregation
	Meta    map[string]any
}

// NewMinAggregation returns a new MinAggregation with default settings.
func NewMinAggregation() *MinAggregation { return &MinAggregation{} }

// WithField sets the field to compute the minimum on.
func (a *MinAggregation) WithField(field string) *MinAggregation {
	a.Field = field
	return a
}

// WithScript sets the script used to compute per-document values.
func (a *MinAggregation) WithScript(script *Script) *MinAggregation {
	a.Script = script
	return a
}

// WithFormat sets the numeric format for the output value.
func (a *MinAggregation) WithFormat(format string) *MinAggregation {
	a.Format = format
	return a
}

// WithMissing sets the value to use for documents missing the field.
func (a *MinAggregation) WithMissing(missing any) *MinAggregation {
	a.Missing = missing
	return a
}

// WithSubAggs sets the sub-aggregations.
func (a *MinAggregation) WithSubAggs(subAggs map[string]Aggregation) *MinAggregation {
	a.SubAggs = subAggs
	return a
}

// WithMeta sets the meta data for the aggregation.
func (a *MinAggregation) WithMeta(meta map[string]any) *MinAggregation {
	a.Meta = meta
	return a
}

func (a MinAggregation) Source() (any, error) {
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
	return sourceAgg("min", body, a.SubAggs, a.Meta, a.Script)
}
