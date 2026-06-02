package querydsl

type MaxBucketAggregation struct {
	Format       string
	GapPolicy    string
	BucketsPaths []string
	Meta         map[string]any
}

func NewMaxBucketAggregation() *MaxBucketAggregation { return &MaxBucketAggregation{} }

// WithFormat sets the format for the max bucket aggregation.
func (a *MaxBucketAggregation) WithFormat(format string) *MaxBucketAggregation {
	a.Format = format
	return a
}

// WithGapPolicy sets the gap policy for the max bucket aggregation.
func (a *MaxBucketAggregation) WithGapPolicy(policy string) *MaxBucketAggregation {
	a.GapPolicy = policy
	return a
}

// WithBucketsPaths sets the buckets paths for the max bucket aggregation.
func (a *MaxBucketAggregation) WithBucketsPaths(paths ...string) *MaxBucketAggregation {
	a.BucketsPaths = paths
	return a
}

// WithMeta sets the meta for the max bucket aggregation.
func (a *MaxBucketAggregation) WithMeta(meta map[string]any) *MaxBucketAggregation {
	a.Meta = meta
	return a
}

func (a MaxBucketAggregation) Source() (any, error) {
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
	return sourcePipeline("max_bucket", body, a.Meta, nil)
}
