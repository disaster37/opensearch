package querydsl

type ReverseNestedAggregation struct {
	Path    string
	SubAggs map[string]Aggregation
	Meta    map[string]any
}

func NewReverseNestedAggregation() ReverseNestedAggregation { return ReverseNestedAggregation{} }

func (a ReverseNestedAggregation) Source() (any, error) {
	body := map[string]any{}
	if a.Path != "" {
		body["path"] = a.Path
	}
	return sourceAgg("reverse_nested", body, a.SubAggs, a.Meta, nil)
}
