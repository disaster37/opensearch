package querydsl

type ChildrenAggregation struct {
	Type    string
	SubAggs map[string]Aggregation
	Meta    map[string]any
}

func NewChildrenAggregation() ChildrenAggregation { return ChildrenAggregation{} }

func (a ChildrenAggregation) Source() (any, error) {
	body := map[string]any{"type": a.Type}
	return sourceAgg("children", body, a.SubAggs, a.Meta, nil)
}
