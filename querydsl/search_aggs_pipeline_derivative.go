package querydsl

type DerivativeAggregation struct {
	Format       string
	GapPolicy    string
	Unit         string
	BucketsPaths []string
	Meta         map[string]any
}

func NewDerivativeAggregation() *DerivativeAggregation { return &DerivativeAggregation{} }

// WithFormat sets the format for the derivative aggregation.
func (a *DerivativeAggregation) WithFormat(format string) *DerivativeAggregation {
	a.Format = format
	return a
}

// WithGapPolicy sets the gap policy for the derivative aggregation.
func (a *DerivativeAggregation) WithGapPolicy(policy string) *DerivativeAggregation {
	a.GapPolicy = policy
	return a
}

// WithUnit sets the unit for the derivative aggregation.
func (a *DerivativeAggregation) WithUnit(unit string) *DerivativeAggregation {
	a.Unit = unit
	return a
}

// WithBucketsPaths sets the buckets paths for the derivative aggregation.
func (a *DerivativeAggregation) WithBucketsPaths(paths ...string) *DerivativeAggregation {
	a.BucketsPaths = paths
	return a
}

// WithMeta sets the meta for the derivative aggregation.
func (a *DerivativeAggregation) WithMeta(meta map[string]any) *DerivativeAggregation {
	a.Meta = meta
	return a
}

func (a DerivativeAggregation) Source() (any, error) {
	body := map[string]any{}
	if a.Format != "" {
		body["format"] = a.Format
	}
	if a.GapPolicy != "" {
		body["gap_policy"] = a.GapPolicy
	}
	if a.Unit != "" {
		body["unit"] = a.Unit
	}
	if bp := pipelineBucketsPath(a.BucketsPaths); bp != nil {
		body["buckets_path"] = bp
	}
	return sourcePipeline("derivative", body, a.Meta, nil)
}
