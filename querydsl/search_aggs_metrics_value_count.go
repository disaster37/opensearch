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
func NewValueCountAggregation() ValueCountAggregation { return ValueCountAggregation{} }

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
