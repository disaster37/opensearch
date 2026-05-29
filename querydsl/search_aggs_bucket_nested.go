package querydsl

// NestedAggregation changes the aggregation context to a nested object path.
// It produces a single bucket of all parent documents whose nested objects
// exist at the given path. Sub-aggregations are computed on the nested objects
// rather than the parent documents.
//
// Typical use: aggregate on fields inside a nested array (e.g. line items in
// an order document).
//
// JSON output shape:
//
//	{"nested": {"path": "line_items"}}
type NestedAggregation struct {
	Path    string
	SubAggs map[string]Aggregation
	Meta    map[string]any
}

// NewNestedAggregation returns a zero-value NestedAggregation.
func NewNestedAggregation() NestedAggregation { return NestedAggregation{} }

func (a NestedAggregation) Source() (any, error) {
	body := map[string]any{"path": a.Path}
	return sourceAgg("nested", body, a.SubAggs, a.Meta, nil)
}
