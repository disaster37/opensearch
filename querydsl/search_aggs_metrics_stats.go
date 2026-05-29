package querydsl

// StatsAggregation computes statistics (count, sum, min, max, and avg) over
// numeric values extracted from the aggregated documents. Returns all stats
// in a single pass, which is more efficient than running individual metric
// aggregations.
//
// JSON output shape:
//
//	{"stats": {"field": "grade"}}
type StatsAggregation struct {
	Field   string
	Script  *Script
	Format  string
	Missing any
	SubAggs map[string]Aggregation
	Meta    map[string]any
}

// NewStatsAggregation returns a new StatsAggregation with default settings.
func NewStatsAggregation() StatsAggregation { return StatsAggregation{} }

func (a StatsAggregation) Source() (any, error) {
	body := map[string]any{}
	if a.Field != "" {
		body["field"] = a.Field
	}
	if a.Format != "" {
		body["format"] = a.Format
	}
	if a.Missing != nil {
		body["missing"] = a.Missing
	}
	return sourceAgg("stats", body, a.SubAggs, a.Meta, a.Script)
}
