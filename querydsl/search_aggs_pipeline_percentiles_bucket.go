package querydsl

type PercentilesBucketAggregation struct {
	Format       string
	GapPolicy    string
	Percents     []float64
	BucketsPaths []string
	Meta         map[string]any
}

func NewPercentilesBucketAggregation() PercentilesBucketAggregation {
	return PercentilesBucketAggregation{}
}

func (a PercentilesBucketAggregation) Source() (any, error) {
	body := map[string]any{}
	if a.Format != "" {
		body["format"] = a.Format
	}
	if a.GapPolicy != "" {
		body["gap_policy"] = a.GapPolicy
	}
	if len(a.Percents) > 0 {
		body["percents"] = a.Percents
	}
	if bp := pipelineBucketsPath(a.BucketsPaths); bp != nil {
		body["buckets_path"] = bp
	}
	return sourcePipeline("percentiles_bucket", body, a.Meta, nil)
}
