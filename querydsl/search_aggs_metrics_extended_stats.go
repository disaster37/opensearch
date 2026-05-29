package querydsl

// ExtendedStatsAggregation extends StatsAggregation with additional statistics
// including sum of squares, variance, and standard deviation. The Sigma
// parameter controls the standard deviation bounds displayed in the response.
//
// JSON output shape:
//
//	{"extended_stats": {"field": "grade"}}
type ExtendedStatsAggregation struct {
	Field   string
	Script  *Script
	Format  string
	Missing any
	Sigma   *float64
	SubAggs map[string]Aggregation
	Meta    map[string]any
}

// NewExtendedStatsAggregation returns a new ExtendedStatsAggregation with default settings.
func NewExtendedStatsAggregation() ExtendedStatsAggregation { return ExtendedStatsAggregation{} }

func (a ExtendedStatsAggregation) Source() (any, error) {
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
	if a.Sigma != nil {
		body["sigma"] = *a.Sigma
	}
	return sourceAgg("extended_stats", body, a.SubAggs, a.Meta, a.Script)
}
