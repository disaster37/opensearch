package querydsl

type BucketScriptAggregation struct {
	Format          string
	GapPolicy       string
	Script          *Script
	BucketsPathsMap map[string]string
	Meta            map[string]any
}

func NewBucketScriptAggregation() BucketScriptAggregation {
	return BucketScriptAggregation{BucketsPathsMap: map[string]string{}}
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
