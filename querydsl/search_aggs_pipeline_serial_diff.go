package querydsl

type SerialDiffAggregation struct {
	Format       string
	GapPolicy    string
	Lag          *int
	BucketsPaths []string
	Meta         map[string]any
}

func NewSerialDiffAggregation() SerialDiffAggregation { return SerialDiffAggregation{} }

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
