package querydsl

type PercentilesBucketAggregation struct {
	Format       string
	GapPolicy    string
	Percents     []float64
	BucketsPaths []string
	Meta         map[string]any
}

func NewPercentilesBucketAggregation() *PercentilesBucketAggregation {
	return &PercentilesBucketAggregation{}
}

// WithFormat sets the format for the percentiles bucket aggregation.
func (a *PercentilesBucketAggregation) WithFormat(format string) *PercentilesBucketAggregation {
	a.Format = format
	return a
}

// WithGapPolicy sets the gap policy for the percentiles bucket aggregation.
func (a *PercentilesBucketAggregation) WithGapPolicy(policy string) *PercentilesBucketAggregation {
	a.GapPolicy = policy
	return a
}

// WithPercents sets the percentiles for the percentiles bucket aggregation.
func (a *PercentilesBucketAggregation) WithPercents(percents ...float64) *PercentilesBucketAggregation {
	a.Percents = percents
	return a
}

// WithBucketsPaths sets the buckets paths for the percentiles bucket aggregation.
func (a *PercentilesBucketAggregation) WithBucketsPaths(paths ...string) *PercentilesBucketAggregation {
	a.BucketsPaths = paths
	return a
}

// WithMeta sets the meta for the percentiles bucket aggregation.
func (a *PercentilesBucketAggregation) WithMeta(meta map[string]any) *PercentilesBucketAggregation {
	a.Meta = meta
	return a
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
