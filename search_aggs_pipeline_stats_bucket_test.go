// Copyright 2012-present Oliver Eilhard. All rights reserved.
// Use of this source code is governed by a MIT-license.
// See http://olivere.mit-license.org/license.txt for details.

package opensearch

import (
	"encoding/json"
	"testing"
)

func TestStatsBucketAggregation(t *testing.T) {
	agg := NewStatsBucketAggregation().BucketsPath("the_sum").GapPolicy("skip")
	src, err := agg.Source()
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(src)
	if err != nil {
		t.Fatalf("marshaling to JSON failed: %v", err)
	}
	got := string(data)
	expected := `{"stats_bucket":{"buckets_path":"the_sum","gap_policy":"skip"}}`
	if got != expected {
		t.Errorf("expected\n%s\n,got:\n%s", expected, got)
	}
}
