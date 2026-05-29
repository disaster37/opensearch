package querydsl

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

func NewPercentileRanksAggregation() PercentileRanksAggregation {
	return PercentileRanksAggregation{}
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
