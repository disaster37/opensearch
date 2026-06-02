package querydsl

type StatsBucketAggregation struct {
	Format       string
	GapPolicy    string
	BucketsPaths []string
	Meta         map[string]any
}

func NewStatsBucketAggregation() *StatsBucketAggregation { return &StatsBucketAggregation{} }

// WithFormat sets the format for the stats bucket aggregation.
func (a *StatsBucketAggregation) WithFormat(format string) *StatsBucketAggregation {
	a.Format = format
	return a
}

// WithGapPolicy sets the gap policy for the stats bucket aggregation.
func (a *StatsBucketAggregation) WithGapPolicy(policy string) *StatsBucketAggregation {
	a.GapPolicy = policy
	return a
}

// WithBucketsPaths sets the buckets paths for the stats bucket aggregation.
func (a *StatsBucketAggregation) WithBucketsPaths(paths ...string) *StatsBucketAggregation {
	a.BucketsPaths = paths
	return a
}

// WithMeta sets the meta for the stats bucket aggregation.
func (a *StatsBucketAggregation) WithMeta(meta map[string]any) *StatsBucketAggregation {
	a.Meta = meta
	return a
}

func (a StatsBucketAggregation) Source() (any, error) {
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
	return sourcePipeline("stats_bucket", body, a.Meta, nil)
}
