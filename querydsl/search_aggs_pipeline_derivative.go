package querydsl

type DerivativeAggregation struct {
	Format       string
	GapPolicy    string
	Unit         string
	BucketsPaths []string
	Meta         map[string]any
}

func NewDerivativeAggregation() DerivativeAggregation { return DerivativeAggregation{} }

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
