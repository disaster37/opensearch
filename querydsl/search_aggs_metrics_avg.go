package querydsl

// AvgAggregation computes the average value of a numeric field across all
// matching documents. Returns null if no documents have a value for the field.
//
// JSON output shape:
//
//	{"avg": {"field": "price"}}
type AvgAggregation struct {
	Field   string
	Script  *Script
	Format  string
	Missing any
	SubAggs map[string]Aggregation
	Meta    map[string]any
}

// NewAvgAggregation returns a new AvgAggregation with default settings.
func NewAvgAggregation() *AvgAggregation { return &AvgAggregation{} }

// WithField sets the field to compute the average on.
func (a *AvgAggregation) WithField(field string) *AvgAggregation {
	a.Field = field
	return a
}

// WithScript sets the script used to compute per-document values.
func (a *AvgAggregation) WithScript(script *Script) *AvgAggregation {
	a.Script = script
	return a
}

// WithFormat sets the numeric format for the output value.
func (a *AvgAggregation) WithFormat(format string) *AvgAggregation {
	a.Format = format
	return a
}

// WithMissing sets the value to use for documents missing the field.
func (a *AvgAggregation) WithMissing(missing any) *AvgAggregation {
	a.Missing = missing
	return a
}

// WithSubAggs sets the sub-aggregations.
func (a *AvgAggregation) WithSubAggs(subAggs map[string]Aggregation) *AvgAggregation {
	a.SubAggs = subAggs
	return a
}

// WithMeta sets the meta data for the aggregation.
func (a *AvgAggregation) WithMeta(meta map[string]any) *AvgAggregation {
	a.Meta = meta
	return a
}

func (a AvgAggregation) Source() (any, error) {
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
	return sourceAgg("avg", body, a.SubAggs, a.Meta, a.Script)
}
