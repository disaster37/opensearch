package querydsl

type ReverseNestedAggregation struct {
	Path    string
	SubAggs map[string]Aggregation
	Meta    map[string]any
}

func NewReverseNestedAggregation() *ReverseNestedAggregation { return &ReverseNestedAggregation{} }

// WithPath sets the path to reverse to; leave empty to go back to the root.
func (a *ReverseNestedAggregation) WithPath(path string) *ReverseNestedAggregation {
	a.Path = path
	return a
}

// WithSubAggregation adds a sub-aggregation.
func (a *ReverseNestedAggregation) WithSubAggregation(name string, sub Aggregation) *ReverseNestedAggregation {
	if a.SubAggs == nil {
		a.SubAggs = map[string]Aggregation{}
	}
	a.SubAggs[name] = sub
	return a
}

// WithMeta sets the meta data for the aggregation.
func (a *ReverseNestedAggregation) WithMeta(meta map[string]any) *ReverseNestedAggregation {
	a.Meta = meta
	return a
}

func (a ReverseNestedAggregation) Source() (any, error) {
	body := map[string]any{}
	if a.Path != "" {
		body["path"] = a.Path
	}
	return sourceAgg("reverse_nested", body, a.SubAggs, a.Meta, nil)
}
