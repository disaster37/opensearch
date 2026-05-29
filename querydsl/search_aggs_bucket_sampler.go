package querydsl

type SamplerAggregation struct {
	ShardSize int
	SubAggs   map[string]Aggregation
	Meta      map[string]any
}

func NewSamplerAggregation() SamplerAggregation { return SamplerAggregation{ShardSize: -1} }

func (a SamplerAggregation) Source() (any, error) {
	body := map[string]any{}
	if a.ShardSize != -1 {
		body["shard_size"] = a.ShardSize
	}
	return sourceAgg("sampler", body, a.SubAggs, a.Meta, nil)
}
