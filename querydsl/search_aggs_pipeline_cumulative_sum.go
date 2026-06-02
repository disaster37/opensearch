package querydsl

type CumulativeSumAggregation struct {
	Format       string
	BucketsPaths []string
	Meta         map[string]any
}

func NewCumulativeSumAggregation() *CumulativeSumAggregation { return &CumulativeSumAggregation{} }

// WithFormat sets the format for the cumulative sum aggregation.
func (a *CumulativeSumAggregation) WithFormat(format string) *CumulativeSumAggregation {
	a.Format = format
	return a
}

// WithBucketsPaths sets the buckets paths for the cumulative sum aggregation.
func (a *CumulativeSumAggregation) WithBucketsPaths(paths ...string) *CumulativeSumAggregation {
	a.BucketsPaths = paths
	return a
}

// WithMeta sets the meta for the cumulative sum aggregation.
func (a *CumulativeSumAggregation) WithMeta(meta map[string]any) *CumulativeSumAggregation {
	a.Meta = meta
	return a
}

func (a CumulativeSumAggregation) Source() (any, error) {
	body := map[string]any{}
	if a.Format != "" {
		body["format"] = a.Format
	}
	if bp := pipelineBucketsPath(a.BucketsPaths); bp != nil {
		body["buckets_path"] = bp
	}
	return sourcePipeline("cumulative_sum", body, a.Meta, nil)
}
