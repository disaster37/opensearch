package querydsl

// PercentileRanksAggregation computes the percentile ranks of one or more
// numeric values over a numeric field. Returns the percentile rank of each
// given value within the distribution of field values.
type PercentileRanksAggregation struct {
	Field       string
	Script      *Script
	Format      string
	Missing     any
	Values      []float64
	Compression *float64
	Estimator   string
	SubAggs     map[string]Aggregation
	Meta        map[string]any
}

// NewPercentileRanksAggregation returns a new PercentileRanksAggregation with default settings.
func NewPercentileRanksAggregation() *PercentileRanksAggregation {
	return &PercentileRanksAggregation{}
}

// WithField sets the field to compute percentile ranks on.
func (a *PercentileRanksAggregation) WithField(field string) *PercentileRanksAggregation {
	a.Field = field
	return a
}

// WithScript sets the script used to compute per-document values.
func (a *PercentileRanksAggregation) WithScript(script *Script) *PercentileRanksAggregation {
	a.Script = script
	return a
}

// WithFormat sets the numeric format for the output values.
func (a *PercentileRanksAggregation) WithFormat(format string) *PercentileRanksAggregation {
	a.Format = format
	return a
}

// WithMissing sets the value to use for documents missing the field.
func (a *PercentileRanksAggregation) WithMissing(missing any) *PercentileRanksAggregation {
	a.Missing = missing
	return a
}

// WithValues sets the numeric values for which to compute percentile ranks.
func (a *PercentileRanksAggregation) WithValues(values []float64) *PercentileRanksAggregation {
	a.Values = values
	return a
}

// WithCompression sets the compression factor for the TDigest algorithm.
func (a *PercentileRanksAggregation) WithCompression(v float64) *PercentileRanksAggregation {
	a.Compression = &v
	return a
}

// WithEstimator sets the estimator method (e.g. "tdigest" or "hdr").
func (a *PercentileRanksAggregation) WithEstimator(estimator string) *PercentileRanksAggregation {
	a.Estimator = estimator
	return a
}

// WithSubAggs sets the sub-aggregations.
func (a *PercentileRanksAggregation) WithSubAggs(subAggs map[string]Aggregation) *PercentileRanksAggregation {
	a.SubAggs = subAggs
	return a
}

// WithMeta sets the meta data for the aggregation.
func (a *PercentileRanksAggregation) WithMeta(meta map[string]any) *PercentileRanksAggregation {
	a.Meta = meta
	return a
}

func (a PercentileRanksAggregation) Source() (any, error) {
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
	if len(a.Values) > 0 {
		body["values"] = a.Values
	}
	if a.Compression != nil {
		body["compression"] = *a.Compression
	}
	if a.Estimator != "" {
		body["estimator"] = a.Estimator
	}
	return sourceAgg("percentile_ranks", body, a.SubAggs, a.Meta, a.Script)
}
