package querydsl

type SerialDiffAggregation struct {
	Format       string
	GapPolicy    string
	Lag          *int
	BucketsPaths []string
	Meta         map[string]any
}

func NewSerialDiffAggregation() *SerialDiffAggregation { return &SerialDiffAggregation{} }

// WithFormat sets the format for the serial diff aggregation.
func (a *SerialDiffAggregation) WithFormat(format string) *SerialDiffAggregation {
	a.Format = format
	return a
}

// WithGapPolicy sets the gap policy for the serial diff aggregation.
func (a *SerialDiffAggregation) WithGapPolicy(policy string) *SerialDiffAggregation {
	a.GapPolicy = policy
	return a
}

// WithLag sets the lag value for the serial diff aggregation.
func (a *SerialDiffAggregation) WithLag(lag int) *SerialDiffAggregation {
	a.Lag = &lag
	return a
}

// WithBucketsPaths sets the buckets paths for the serial diff aggregation.
func (a *SerialDiffAggregation) WithBucketsPaths(paths ...string) *SerialDiffAggregation {
	a.BucketsPaths = paths
	return a
}

// WithMeta sets the meta for the serial diff aggregation.
func (a *SerialDiffAggregation) WithMeta(meta map[string]any) *SerialDiffAggregation {
	a.Meta = meta
	return a
}

func (a SerialDiffAggregation) Source() (any, error) {
	body := map[string]any{}
	if a.Format != "" {
		body["format"] = a.Format
	}
	if a.GapPolicy != "" {
		body["gap_policy"] = a.GapPolicy
	}
	if a.Lag != nil {
		body["lag"] = *a.Lag
	}
	if bp := pipelineBucketsPath(a.BucketsPaths); bp != nil {
		body["buckets_path"] = bp
	}
	return sourcePipeline("serial_diff", body, a.Meta, nil)
}
