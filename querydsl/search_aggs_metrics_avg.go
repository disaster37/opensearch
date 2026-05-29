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
func NewAvgAggregation() AvgAggregation { return AvgAggregation{} }

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
