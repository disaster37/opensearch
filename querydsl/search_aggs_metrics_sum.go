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
func NewSumAggregation() SumAggregation { return SumAggregation{} }

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
