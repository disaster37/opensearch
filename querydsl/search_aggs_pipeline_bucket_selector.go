package querydsl

type BucketSelectorAggregation struct {
	Format          string
	GapPolicy       string
	Script          *Script
	BucketsPathsMap map[string]string
	Meta            map[string]any
}

func NewBucketSelectorAggregation() *BucketSelectorAggregation {
	return &BucketSelectorAggregation{BucketsPathsMap: map[string]string{}}
}

// WithFormat sets the format for the bucket selector aggregation.
func (a *BucketSelectorAggregation) WithFormat(format string) *BucketSelectorAggregation {
	a.Format = format
	return a
}

// WithGapPolicy sets the gap policy for the bucket selector aggregation.
func (a *BucketSelectorAggregation) WithGapPolicy(policy string) *BucketSelectorAggregation {
	a.GapPolicy = policy
	return a
}

// WithScript sets the script for the bucket selector aggregation.
func (a *BucketSelectorAggregation) WithScript(script *Script) *BucketSelectorAggregation {
	a.Script = script
	return a
}

// WithBucketsPathsMap sets the buckets paths map for the bucket selector aggregation.
func (a *BucketSelectorAggregation) WithBucketsPathsMap(m map[string]string) *BucketSelectorAggregation {
	a.BucketsPathsMap = m
	return a
}

// WithMeta sets the meta for the bucket selector aggregation.
func (a *BucketSelectorAggregation) WithMeta(meta map[string]any) *BucketSelectorAggregation {
	a.Meta = meta
	return a
}

func (a BucketSelectorAggregation) Source() (any, error) {
	body := map[string]any{}
	if a.Format != "" {
		body["format"] = a.Format
	}
	if a.GapPolicy != "" {
		body["gap_policy"] = a.GapPolicy
	}
	if len(a.BucketsPathsMap) > 0 {
		body["buckets_path"] = a.BucketsPathsMap
	}
	return sourcePipeline("bucket_selector", body, a.Meta, a.Script)
}
