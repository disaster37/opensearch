package querydsl

type BucketScriptAggregation struct {
	Format          string
	GapPolicy       string
	Script          *Script
	BucketsPathsMap map[string]string
	Meta            map[string]any
}

func NewBucketScriptAggregation() *BucketScriptAggregation {
	return &BucketScriptAggregation{BucketsPathsMap: map[string]string{}}
}

// WithFormat sets the format for the bucket script aggregation.
func (a *BucketScriptAggregation) WithFormat(format string) *BucketScriptAggregation {
	a.Format = format
	return a
}

// WithGapPolicy sets the gap policy for the bucket script aggregation.
func (a *BucketScriptAggregation) WithGapPolicy(policy string) *BucketScriptAggregation {
	a.GapPolicy = policy
	return a
}

// WithScript sets the script for the bucket script aggregation.
func (a *BucketScriptAggregation) WithScript(script *Script) *BucketScriptAggregation {
	a.Script = script
	return a
}

// WithBucketsPathsMap sets the buckets paths map for the bucket script aggregation.
func (a *BucketScriptAggregation) WithBucketsPathsMap(m map[string]string) *BucketScriptAggregation {
	a.BucketsPathsMap = m
	return a
}

// WithMeta sets the meta for the bucket script aggregation.
func (a *BucketScriptAggregation) WithMeta(meta map[string]any) *BucketScriptAggregation {
	a.Meta = meta
	return a
}

func (a BucketScriptAggregation) Source() (any, error) {
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
	return sourcePipeline("bucket_script", body, a.Meta, a.Script)
}
