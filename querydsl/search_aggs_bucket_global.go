package querydsl

// GlobalAggregation produces a single bucket containing every document in the
// index, ignoring any top-level query filter. It is commonly used to compute
// aggregations across the full dataset while the surrounding query is narrow.
//
// Typical use: show global stats alongside filtered results.
//
// JSON output shape:
//
//	{"global": {}}
type GlobalAggregation struct {
	SubAggs map[string]Aggregation
	Meta    map[string]any
}

// NewGlobalAggregation returns a zero-value GlobalAggregation.
func NewGlobalAggregation() GlobalAggregation { return GlobalAggregation{} }

func (a GlobalAggregation) Source() (any, error) {
	return sourceAgg("global", map[string]any{}, a.SubAggs, a.Meta, nil)
}
