// Copyright 2012-present Oliver Eilhard. All rights reserved.
// Use of this source code is governed by a MIT-license.
// See http://olivere.mit-license.org/license.txt for details.

package opensearch

import (
	"encoding/json"
	"testing"
)

func TestSamplerAggregation(t *testing.T) {
	keywordsAgg := NewSignificantTermsAggregation().Field("text")
	agg := NewSamplerAggregation().
		ShardSize(200).
		SubAggregation("keywords", keywordsAgg)
	src, err := agg.Source()
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(src)
	if err != nil {
		t.Fatalf("marshaling to JSON failed: %v", err)
	}
	got := string(data)
	expected := `{"aggregations":{"keywords":{"significant_terms":{"field":"text"}}},"sampler":{"shard_size":200}}`
	if got != expected {
		t.Errorf("expected\n%s\n,got:\n%s", expected, got)
	}
}
