package querydsl

// MatrixStatsAggregation computes statistics over a set of document fields
// and produces a matrix of results. Supports optional weighting by another field.
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

// NewMatrixStatsAggregation returns a new MatrixStatsAggregation with default settings.
func NewMatrixStatsAggregation() *MatrixStatsAggregation { return &MatrixStatsAggregation{} }

// WithFields sets the list of fields to compute matrix stats on.
func (a *MatrixStatsAggregation) WithFields(fields []string) *MatrixStatsAggregation {
	a.Fields = fields
	return a
}

// WithMissing sets the value to use for documents missing any of the fields.
func (a *MatrixStatsAggregation) WithMissing(missing any) *MatrixStatsAggregation {
	a.Missing = missing
	return a
}

// WithFormat sets the numeric format for the output values.
func (a *MatrixStatsAggregation) WithFormat(format string) *MatrixStatsAggregation {
	a.Format = format
	return a
}

// WithValueType sets the value type hint for the aggregation.
func (a *MatrixStatsAggregation) WithValueType(valueType any) *MatrixStatsAggregation {
	a.ValueType = valueType
	return a
}

// WithMode sets the multi-value mode (e.g. "avg", "min", "max", "sum", "median").
func (a *MatrixStatsAggregation) WithMode(mode string) *MatrixStatsAggregation {
	a.Mode = mode
	return a
}

// WithScript sets the script used to compute per-document values.
func (a *MatrixStatsAggregation) WithScript(script *Script) *MatrixStatsAggregation {
	a.Script = script
	return a
}

// WithSubAggs sets the sub-aggregations.
func (a *MatrixStatsAggregation) WithSubAggs(subAggs map[string]Aggregation) *MatrixStatsAggregation {
	a.SubAggs = subAggs
	return a
}

// WithMeta sets the meta data for the aggregation.
func (a *MatrixStatsAggregation) WithMeta(meta map[string]any) *MatrixStatsAggregation {
	a.Meta = meta
	return a
}

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
