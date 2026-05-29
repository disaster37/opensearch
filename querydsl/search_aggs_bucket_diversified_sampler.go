package querydsl

type DiversifiedSamplerAggregation struct {
	Field           string
	Script          *Script
	ShardSize       int
	MaxDocsPerValue int
	ExecutionHint   string
	SubAggs         map[string]Aggregation
	Meta            map[string]any
}

func NewDiversifiedSamplerAggregation() DiversifiedSamplerAggregation {
	return DiversifiedSamplerAggregation{ShardSize: -1, MaxDocsPerValue: -1}
}

func (a DiversifiedSamplerAggregation) Source() (any, error) {
	body := map[string]any{}
	if a.Field != "" {
		body["field"] = a.Field
	}
	if a.ShardSize != -1 {
		body["shard_size"] = a.ShardSize
	}
	if a.MaxDocsPerValue != -1 {
		body["max_docs_per_value"] = a.MaxDocsPerValue
	}
	if a.ExecutionHint != "" {
		body["execution_hint"] = a.ExecutionHint
	}
	return sourceAgg("diversified_sampler", body, a.SubAggs, a.Meta, a.Script)
}
