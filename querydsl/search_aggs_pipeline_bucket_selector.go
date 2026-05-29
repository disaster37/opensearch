package querydsl

type BucketSelectorAggregation struct {
	Format          string
	GapPolicy       string
	Script          *Script
	BucketsPathsMap map[string]string
	Meta            map[string]any
}

func NewBucketSelectorAggregation() BucketSelectorAggregation {
	return BucketSelectorAggregation{BucketsPathsMap: map[string]string{}}
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
