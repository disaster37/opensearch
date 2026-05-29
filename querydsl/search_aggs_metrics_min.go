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
func NewMinAggregation() MinAggregation { return MinAggregation{} }

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
