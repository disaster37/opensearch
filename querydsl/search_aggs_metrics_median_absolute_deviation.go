package querydsl

type MedianAbsoluteDeviationAggregation struct {
	Field       string
	Script      *Script
	Format      string
	Missing     any
	Compression *float64
	SubAggs     map[string]Aggregation
	Meta        map[string]any
}

func NewMedianAbsoluteDeviationAggregation() MedianAbsoluteDeviationAggregation {
	return MedianAbsoluteDeviationAggregation{}
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
