package querydsl

// MedianAbsoluteDeviationAggregation computes the median absolute deviation
// of a numeric field, a measure of variability that is robust to outliers.
type MedianAbsoluteDeviationAggregation struct {
	Field       string
	Script      *Script
	Format      string
	Missing     any
	Compression *float64
	SubAggs     map[string]Aggregation
	Meta        map[string]any
}

// NewMedianAbsoluteDeviationAggregation returns a new MedianAbsoluteDeviationAggregation with default settings.
func NewMedianAbsoluteDeviationAggregation() *MedianAbsoluteDeviationAggregation {
	return &MedianAbsoluteDeviationAggregation{}
}

// WithField sets the field to compute the median absolute deviation on.
func (a *MedianAbsoluteDeviationAggregation) WithField(field string) *MedianAbsoluteDeviationAggregation {
	a.Field = field
	return a
}

// WithScript sets the script used to compute per-document values.
func (a *MedianAbsoluteDeviationAggregation) WithScript(script *Script) *MedianAbsoluteDeviationAggregation {
	a.Script = script
	return a
}

// WithFormat sets the numeric format for the output value.
func (a *MedianAbsoluteDeviationAggregation) WithFormat(format string) *MedianAbsoluteDeviationAggregation {
	a.Format = format
	return a
}

// WithMissing sets the value to use for documents missing the field.
func (a *MedianAbsoluteDeviationAggregation) WithMissing(missing any) *MedianAbsoluteDeviationAggregation {
	a.Missing = missing
	return a
}

// WithCompression sets the compression factor for the TDigest algorithm used internally.
func (a *MedianAbsoluteDeviationAggregation) WithCompression(v float64) *MedianAbsoluteDeviationAggregation {
	a.Compression = &v
	return a
}

// WithSubAggs sets the sub-aggregations.
func (a *MedianAbsoluteDeviationAggregation) WithSubAggs(subAggs map[string]Aggregation) *MedianAbsoluteDeviationAggregation {
	a.SubAggs = subAggs
	return a
}

// WithMeta sets the meta data for the aggregation.
func (a *MedianAbsoluteDeviationAggregation) WithMeta(meta map[string]any) *MedianAbsoluteDeviationAggregation {
	a.Meta = meta
	return a
}

func (a MedianAbsoluteDeviationAggregation) Source() (any, error) {
	body := map[string]any{}
	if a.Field != "" {
		body["field"] = a.Field
	}
	if a.Compression != nil {
		body["compression"] = *a.Compression
	}
	if a.Format != "" {
		body["format"] = a.Format
	}
	if a.Missing != nil {
		body["missing"] = a.Missing
	}
	return sourceAgg("median_absolute_deviation", body, a.SubAggs, a.Meta, a.Script)
}
