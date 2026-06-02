package querydsl

type SamplerAggregation struct {
	ShardSize int
	SubAggs   map[string]Aggregation
	Meta      map[string]any
}

func NewSamplerAggregation() *SamplerAggregation { return &SamplerAggregation{ShardSize: -1} }

// WithShardSize sets the maximum number of documents to sample per shard.
func (a *SamplerAggregation) WithShardSize(shardSize int) *SamplerAggregation {
	a.ShardSize = shardSize
	return a
}

// WithSubAggregation adds a sub-aggregation.
func (a *SamplerAggregation) WithSubAggregation(name string, sub Aggregation) *SamplerAggregation {
	if a.SubAggs == nil {
		a.SubAggs = map[string]Aggregation{}
	}
	a.SubAggs[name] = sub
	return a
}

// WithMeta sets the meta data for the aggregation.
func (a *SamplerAggregation) WithMeta(meta map[string]any) *SamplerAggregation {
	a.Meta = meta
	return a
}

func (a SamplerAggregation) Source() (any, error) {
	body := map[string]any{}
	if a.ShardSize != -1 {
		body["shard_size"] = a.ShardSize
	}
	return sourceAgg("sampler", body, a.SubAggs, a.Meta, nil)
}
