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
func NewNestedAggregation() *NestedAggregation { return &NestedAggregation{} }

// WithPath sets the nested object path.
func (a *NestedAggregation) WithPath(path string) *NestedAggregation {
	a.Path = path
	return a
}

// WithSubAggregation adds a sub-aggregation.
func (a *NestedAggregation) WithSubAggregation(name string, sub Aggregation) *NestedAggregation {
	if a.SubAggs == nil {
		a.SubAggs = map[string]Aggregation{}
	}
	a.SubAggs[name] = sub
	return a
}

// WithMeta sets the meta data for the aggregation.
func (a *NestedAggregation) WithMeta(meta map[string]any) *NestedAggregation {
	a.Meta = meta
	return a
}

func (a NestedAggregation) Source() (any, error) {
	body := map[string]any{"path": a.Path}
	return sourceAgg("nested", body, a.SubAggs, a.Meta, nil)
}
