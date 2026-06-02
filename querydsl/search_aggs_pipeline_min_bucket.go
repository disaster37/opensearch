package querydsl

type MinBucketAggregation struct {
	Format       string
	GapPolicy    string
	BucketsPaths []string
	Meta         map[string]any
}

func NewMinBucketAggregation() *MinBucketAggregation { return &MinBucketAggregation{} }

// WithFormat sets the format for the min bucket aggregation.
func (a *MinBucketAggregation) WithFormat(format string) *MinBucketAggregation {
	a.Format = format
	return a
}

// WithGapPolicy sets the gap policy for the min bucket aggregation.
func (a *MinBucketAggregation) WithGapPolicy(policy string) *MinBucketAggregation {
	a.GapPolicy = policy
	return a
}

// WithBucketsPaths sets the buckets paths for the min bucket aggregation.
func (a *MinBucketAggregation) WithBucketsPaths(paths ...string) *MinBucketAggregation {
	a.BucketsPaths = paths
	return a
}

// WithMeta sets the meta for the min bucket aggregation.
func (a *MinBucketAggregation) WithMeta(meta map[string]any) *MinBucketAggregation {
	a.Meta = meta
	return a
}

func (a MinBucketAggregation) Source() (any, error) {
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
	return sourcePipeline("min_bucket", body, a.Meta, nil)
}
