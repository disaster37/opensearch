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
func NewMaxAggregation() MaxAggregation { return MaxAggregation{} }

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
