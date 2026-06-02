package querydsl

type SumBucketAggregation struct {
	Format       string
	GapPolicy    string
	BucketsPaths []string
	Meta         map[string]any
}

func NewSumBucketAggregation() *SumBucketAggregation { return &SumBucketAggregation{} }

// WithFormat sets the format for the sum bucket aggregation.
func (a *SumBucketAggregation) WithFormat(format string) *SumBucketAggregation {
	a.Format = format
	return a
}

// WithGapPolicy sets the gap policy for the sum bucket aggregation.
func (a *SumBucketAggregation) WithGapPolicy(policy string) *SumBucketAggregation {
	a.GapPolicy = policy
	return a
}

// WithBucketsPaths sets the buckets paths for the sum bucket aggregation.
func (a *SumBucketAggregation) WithBucketsPaths(paths ...string) *SumBucketAggregation {
	a.BucketsPaths = paths
	return a
}

// WithMeta sets the meta for the sum bucket aggregation.
func (a *SumBucketAggregation) WithMeta(meta map[string]any) *SumBucketAggregation {
	a.Meta = meta
	return a
}

func (a SumBucketAggregation) Source() (any, error) {
	body := map[string]any{}
	if a.Format != "" {
		body["format"] = a.Format
	}
	if a.GapPolicy != "" {
		body["gap_policy"] = a.GapPolicy
	}
	if bp := pipelineBucketsPath(a.BucketsPaths); bp != nil {
		body["buckets_path"] = bp
	}
	return sourcePipeline("sum_bucket", body, a.Meta, nil)
}
