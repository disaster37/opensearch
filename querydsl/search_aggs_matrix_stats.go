package querydsl

type MatrixStatsAggregation struct {
	Fields    []string
	Missing   any
	Format    string
	ValueType any
	Mode      string
	Script    *Script
	SubAggs   map[string]Aggregation
	Meta      map[string]any
}

func NewMatrixStatsAggregation() MatrixStatsAggregation { return MatrixStatsAggregation{} }

func (a MatrixStatsAggregation) Source() (any, error) {
	body := map[string]any{"fields": a.Fields}
	if a.Missing != nil {
		body["missing"] = a.Missing
	}
	if a.Format != "" {
		body["format"] = a.Format
	}
	if a.ValueType != nil {
		body["value_type"] = a.ValueType
	}
	if a.Mode != "" {
		body["mode"] = a.Mode
	}
	return sourceAgg("matrix_stats", body, a.SubAggs, a.Meta, a.Script)
}
