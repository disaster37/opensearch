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
func NewExtendedStatsAggregation() *ExtendedStatsAggregation { return &ExtendedStatsAggregation{} }

// WithField sets the field to compute extended stats on.
func (a *ExtendedStatsAggregation) WithField(field string) *ExtendedStatsAggregation {
	a.Field = field
	return a
}

// WithScript sets the script used to compute per-document values.
func (a *ExtendedStatsAggregation) WithScript(script *Script) *ExtendedStatsAggregation {
	a.Script = script
	return a
}

// WithFormat sets the numeric format for the output values.
func (a *ExtendedStatsAggregation) WithFormat(format string) *ExtendedStatsAggregation {
	a.Format = format
	return a
}

// WithMissing sets the value to use for documents missing the field.
func (a *ExtendedStatsAggregation) WithMissing(missing any) *ExtendedStatsAggregation {
	a.Missing = missing
	return a
}

// WithSigma sets the number of standard deviations for the bounds output.
func (a *ExtendedStatsAggregation) WithSigma(v float64) *ExtendedStatsAggregation {
	a.Sigma = &v
	return a
}

// WithSubAggs sets the sub-aggregations.
func (a *ExtendedStatsAggregation) WithSubAggs(subAggs map[string]Aggregation) *ExtendedStatsAggregation {
	a.SubAggs = subAggs
	return a
}

// WithMeta sets the meta data for the aggregation.
func (a *ExtendedStatsAggregation) WithMeta(meta map[string]any) *ExtendedStatsAggregation {
	a.Meta = meta
	return a
}

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
