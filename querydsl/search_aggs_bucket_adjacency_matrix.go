package querydsl

type AdjacencyMatrixAggregation struct {
	Filters map[string]Query
	SubAggs map[string]Aggregation
	Meta    map[string]any
}

func NewAdjacencyMatrixAggregation() AdjacencyMatrixAggregation {
	return AdjacencyMatrixAggregation{Filters: map[string]Query{}}
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
