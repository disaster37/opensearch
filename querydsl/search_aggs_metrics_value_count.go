package querydsl

// ValueCountAggregation counts the number of values that are extracted from
// the aggregated documents. Useful for counting documents that have a specific
// field or determining how many values a scripted expression produces.
//
// JSON output shape:
//
//	{"value_count": {"field": "field_name"}}
type ValueCountAggregation struct {
	Field   string
	Script  *Script
	Format  string
	SubAggs map[string]Aggregation
	Meta    map[string]any
}

// NewValueCountAggregation returns a new ValueCountAggregation with default settings.
func NewValueCountAggregation() *ValueCountAggregation { return &ValueCountAggregation{} }

// WithField sets the field to count values on.
func (a *ValueCountAggregation) WithField(field string) *ValueCountAggregation {
	a.Field = field
	return a
}

// WithScript sets the script used to compute per-document values.
func (a *ValueCountAggregation) WithScript(script *Script) *ValueCountAggregation {
	a.Script = script
	return a
}

// WithFormat sets the numeric format for the output value.
func (a *ValueCountAggregation) WithFormat(format string) *ValueCountAggregation {
	a.Format = format
	return a
}

// WithSubAggs sets the sub-aggregations.
func (a *ValueCountAggregation) WithSubAggs(subAggs map[string]Aggregation) *ValueCountAggregation {
	a.SubAggs = subAggs
	return a
}

// WithMeta sets the meta data for the aggregation.
func (a *ValueCountAggregation) WithMeta(meta map[string]any) *ValueCountAggregation {
	a.Meta = meta
	return a
}

func (a ValueCountAggregation) Source() (any, error) {
	body := map[string]any{}
	if a.Field != "" {
		body["field"] = a.Field
	}
	if a.Format != "" {
		body["format"] = a.Format
	}
	return sourceAgg("value_count", body, a.SubAggs, a.Meta, a.Script)
}
