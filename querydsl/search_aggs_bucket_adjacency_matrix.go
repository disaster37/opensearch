package querydsl

type AdjacencyMatrixAggregation struct {
	Filters map[string]Query
	SubAggs map[string]Aggregation
	Meta    map[string]any
}

func NewAdjacencyMatrixAggregation() *AdjacencyMatrixAggregation {
	return &AdjacencyMatrixAggregation{Filters: map[string]Query{}}
}

// WithFilters sets the named filters map for the adjacency matrix aggregation.
func (a *AdjacencyMatrixAggregation) WithFilters(filters map[string]Query) *AdjacencyMatrixAggregation {
	a.Filters = filters
	return a
}

// WithFilter adds a named filter to the adjacency matrix aggregation.
func (a *AdjacencyMatrixAggregation) WithFilter(name string, q Query) *AdjacencyMatrixAggregation {
	if a.Filters == nil {
		a.Filters = map[string]Query{}
	}
	a.Filters[name] = q
	return a
}

// WithSubAggregation adds a sub-aggregation.
func (a *AdjacencyMatrixAggregation) WithSubAggregation(name string, sub Aggregation) *AdjacencyMatrixAggregation {
	if a.SubAggs == nil {
		a.SubAggs = map[string]Aggregation{}
	}
	a.SubAggs[name] = sub
	return a
}

// WithMeta sets the meta data for the aggregation.
func (a *AdjacencyMatrixAggregation) WithMeta(meta map[string]any) *AdjacencyMatrixAggregation {
	a.Meta = meta
	return a
}

func (a AdjacencyMatrixAggregation) Source() (any, error) {
	body := map[string]any{}
	if len(a.Filters) > 0 {
		f := map[string]any{}
		for name, q := range a.Filters {
			src, err := q.Source()
			if err != nil {
				return nil, err
			}
			f[name] = src
		}
		body["filters"] = f
	}
	return sourceAgg("adjacency_matrix", body, a.SubAggs, a.Meta, nil)
}
