package querydsl

type ExtendedStatsBucketAggregation struct {
	Format       string
	GapPolicy    string
	Sigma        *float32
	BucketsPaths []string
	Meta         map[string]any
}

func NewExtendedStatsBucketAggregation() ExtendedStatsBucketAggregation {
	return ExtendedStatsBucketAggregation{}
}

func (a ExtendedStatsBucketAggregation) Source() (any, error) {
	body := map[string]any{}
	if a.Format != "" {
		body["format"] = a.Format
	}
	if a.GapPolicy != "" {
		body["gap_policy"] = a.GapPolicy
	}
	if a.Sigma != nil && *a.Sigma >= 0 {
		body["sigma"] = *a.Sigma
	}
	if bp := pipelineBucketsPath(a.BucketsPaths); bp != nil {
		body["buckets_path"] = bp
	}
	return sourcePipeline("extended_stats_bucket", body, a.Meta, nil)
}
