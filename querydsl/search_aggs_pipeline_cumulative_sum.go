package querydsl

type CumulativeSumAggregation struct {
	Format       string
	BucketsPaths []string
	Meta         map[string]any
}

func NewCumulativeSumAggregation() CumulativeSumAggregation { return CumulativeSumAggregation{} }

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
