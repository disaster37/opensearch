package querydsl

type AvgBucketAggregation struct {
	Format       string
	GapPolicy    string
	BucketsPaths []string
	Meta         map[string]any
}

func NewAvgBucketAggregation() *AvgBucketAggregation { return &AvgBucketAggregation{} }

// WithFormat sets the format for the avg bucket aggregation.
func (a *AvgBucketAggregation) WithFormat(format string) *AvgBucketAggregation {
	a.Format = format
	return a
}

// WithGapPolicy sets the gap policy for the avg bucket aggregation.
func (a *AvgBucketAggregation) WithGapPolicy(policy string) *AvgBucketAggregation {
	a.GapPolicy = policy
	return a
}

// WithBucketsPaths sets the buckets paths for the avg bucket aggregation.
func (a *AvgBucketAggregation) WithBucketsPaths(paths ...string) *AvgBucketAggregation {
	a.BucketsPaths = paths
	return a
}

// WithMeta sets the meta for the avg bucket aggregation.
func (a *AvgBucketAggregation) WithMeta(meta map[string]any) *AvgBucketAggregation {
	a.Meta = meta
	return a
}

func (a AvgBucketAggregation) Source() (any, error) {
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
	return sourcePipeline("avg_bucket", body, a.Meta, nil)
}
