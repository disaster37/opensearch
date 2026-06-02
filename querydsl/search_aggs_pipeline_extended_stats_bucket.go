package querydsl

type ExtendedStatsBucketAggregation struct {
	Format       string
	GapPolicy    string
	Sigma        *float32
	BucketsPaths []string
	Meta         map[string]any
}

func NewExtendedStatsBucketAggregation() *ExtendedStatsBucketAggregation {
	return &ExtendedStatsBucketAggregation{}
}

// WithFormat sets the format for the extended stats bucket aggregation.
func (a *ExtendedStatsBucketAggregation) WithFormat(format string) *ExtendedStatsBucketAggregation {
	a.Format = format
	return a
}

// WithGapPolicy sets the gap policy for the extended stats bucket aggregation.
func (a *ExtendedStatsBucketAggregation) WithGapPolicy(policy string) *ExtendedStatsBucketAggregation {
	a.GapPolicy = policy
	return a
}

// WithSigma sets the sigma value for the extended stats bucket aggregation.
func (a *ExtendedStatsBucketAggregation) WithSigma(sigma float32) *ExtendedStatsBucketAggregation {
	a.Sigma = &sigma
	return a
}

// WithBucketsPaths sets the buckets paths for the extended stats bucket aggregation.
func (a *ExtendedStatsBucketAggregation) WithBucketsPaths(paths ...string) *ExtendedStatsBucketAggregation {
	a.BucketsPaths = paths
	return a
}

// WithMeta sets the meta for the extended stats bucket aggregation.
func (a *ExtendedStatsBucketAggregation) WithMeta(meta map[string]any) *ExtendedStatsBucketAggregation {
	a.Meta = meta
	return a
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
