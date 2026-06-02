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
func NewStatsAggregation() *StatsAggregation { return &StatsAggregation{} }

// WithField sets the field to compute stats on.
func (a *StatsAggregation) WithField(field string) *StatsAggregation {
	a.Field = field
	return a
}

// WithScript sets the script used to compute per-document values.
func (a *StatsAggregation) WithScript(script *Script) *StatsAggregation {
	a.Script = script
	return a
}

// WithFormat sets the numeric format for the output values.
func (a *StatsAggregation) WithFormat(format string) *StatsAggregation {
	a.Format = format
	return a
}

// WithMissing sets the value to use for documents missing the field.
func (a *StatsAggregation) WithMissing(missing any) *StatsAggregation {
	a.Missing = missing
	return a
}

// WithSubAggs sets the sub-aggregations.
func (a *StatsAggregation) WithSubAggs(subAggs map[string]Aggregation) *StatsAggregation {
	a.SubAggs = subAggs
	return a
}

// WithMeta sets the meta data for the aggregation.
func (a *StatsAggregation) WithMeta(meta map[string]any) *StatsAggregation {
	a.Meta = meta
	return a
}

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
