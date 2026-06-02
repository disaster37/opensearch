package querydsl

// SumAggregation computes the sum of all values in a numeric field across
// matching documents. Returns 0 when no documents have a value for the field.
//
// JSON output shape:
//
//	{"sum": {"field": "price"}}
type SumAggregation struct {
	Field   string
	Script  *Script
	Format  string
	Missing any
	SubAggs map[string]Aggregation
	Meta    map[string]any
}

// NewSumAggregation returns a new SumAggregation with default settings.
func NewSumAggregation() *SumAggregation { return &SumAggregation{} }

// WithField sets the field to compute the sum on.
func (a *SumAggregation) WithField(field string) *SumAggregation {
	a.Field = field
	return a
}

// WithScript sets the script used to compute per-document values.
func (a *SumAggregation) WithScript(script *Script) *SumAggregation {
	a.Script = script
	return a
}

// WithFormat sets the numeric format for the output value.
func (a *SumAggregation) WithFormat(format string) *SumAggregation {
	a.Format = format
	return a
}

// WithMissing sets the value to use for documents missing the field.
func (a *SumAggregation) WithMissing(missing any) *SumAggregation {
	a.Missing = missing
	return a
}

// WithSubAggs sets the sub-aggregations.
func (a *SumAggregation) WithSubAggs(subAggs map[string]Aggregation) *SumAggregation {
	a.SubAggs = subAggs
	return a
}

// WithMeta sets the meta data for the aggregation.
func (a *SumAggregation) WithMeta(meta map[string]any) *SumAggregation {
	a.Meta = meta
	return a
}

func (a SumAggregation) Source() (any, error) {
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
	return sourceAgg("sum", body, a.SubAggs, a.Meta, a.Script)
}
