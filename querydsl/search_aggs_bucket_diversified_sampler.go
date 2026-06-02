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

func NewDiversifiedSamplerAggregation() *DiversifiedSamplerAggregation {
	return &DiversifiedSamplerAggregation{ShardSize: -1, MaxDocsPerValue: -1}
}

// WithField sets the field used for diversification.
func (a *DiversifiedSamplerAggregation) WithField(field string) *DiversifiedSamplerAggregation {
	a.Field = field
	return a
}

// WithScript sets the script used for diversification.
func (a *DiversifiedSamplerAggregation) WithScript(script *Script) *DiversifiedSamplerAggregation {
	a.Script = script
	return a
}

// WithShardSize sets the maximum number of documents to collect per shard.
func (a *DiversifiedSamplerAggregation) WithShardSize(shardSize int) *DiversifiedSamplerAggregation {
	a.ShardSize = shardSize
	return a
}

// WithMaxDocsPerValue sets the maximum number of documents per unique value.
func (a *DiversifiedSamplerAggregation) WithMaxDocsPerValue(maxDocsPerValue int) *DiversifiedSamplerAggregation {
	a.MaxDocsPerValue = maxDocsPerValue
	return a
}

// WithExecutionHint sets the execution hint for the aggregation.
func (a *DiversifiedSamplerAggregation) WithExecutionHint(hint string) *DiversifiedSamplerAggregation {
	a.ExecutionHint = hint
	return a
}

// WithSubAggregation adds a sub-aggregation.
func (a *DiversifiedSamplerAggregation) WithSubAggregation(name string, sub Aggregation) *DiversifiedSamplerAggregation {
	if a.SubAggs == nil {
		a.SubAggs = map[string]Aggregation{}
	}
	a.SubAggs[name] = sub
	return a
}

// WithMeta sets the meta data for the aggregation.
func (a *DiversifiedSamplerAggregation) WithMeta(meta map[string]any) *DiversifiedSamplerAggregation {
	a.Meta = meta
	return a
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
