package querydsl

type ChildrenAggregation struct {
	Type    string
	SubAggs map[string]Aggregation
	Meta    map[string]any
}

func NewChildrenAggregation() *ChildrenAggregation { return &ChildrenAggregation{} }

// WithType sets the child document type for the aggregation.
func (a *ChildrenAggregation) WithType(t string) *ChildrenAggregation {
	a.Type = t
	return a
}

// WithSubAggregation adds a sub-aggregation.
func (a *ChildrenAggregation) WithSubAggregation(name string, sub Aggregation) *ChildrenAggregation {
	if a.SubAggs == nil {
		a.SubAggs = map[string]Aggregation{}
	}
	a.SubAggs[name] = sub
	return a
}

// WithMeta sets the meta data for the aggregation.
func (a *ChildrenAggregation) WithMeta(meta map[string]any) *ChildrenAggregation {
	a.Meta = meta
	return a
}

func (a ChildrenAggregation) Source() (any, error) {
	body := map[string]any{"type": a.Type}
	return sourceAgg("children", body, a.SubAggs, a.Meta, nil)
}
