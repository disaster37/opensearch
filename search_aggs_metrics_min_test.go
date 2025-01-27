// Copyright 2012-present Oliver Eilhard. All rights reserved.
// Use of this source code is governed by a MIT-license.
// See http://olivere.mit-license.org/license.txt for details.

package opensearch

import (
	"encoding/json"
	"testing"
)

func TestMinAggregation(t *testing.T) {
	agg := NewMinAggregation().Field("price")
	src, err := agg.Source()
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(src)
	if err != nil {
		t.Fatalf("marshaling to JSON failed: %v", err)
	}
	got := string(data)
	expected := `{"min":{"field":"price"}}`
	if got != expected {
		t.Errorf("expected\n%s\n,got:\n%s", expected, got)
	}
}

func TestMinAggregationWithOptions(t *testing.T) {
	agg := NewMinAggregation().
		Field("price").
		Format("00000.00").
		Missing(1.2)
	src, err := agg.Source()
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(src)
	if err != nil {
		t.Fatalf("marshaling to JSON failed: %v", err)
	}
	got := string(data)
	expected := `{"min":{"field":"price","format":"00000.00","missing":1.2}}`
	if got != expected {
		t.Errorf("expected\n%s\n,got:\n%s", expected, got)
	}
}

func TestMinAggregationWithMetaData(t *testing.T) {
	agg := NewMinAggregation().Field("price").Meta(map[string]interface{}{"name": "Oliver"})
	src, err := agg.Source()
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(src)
	if err != nil {
		t.Fatalf("marshaling to JSON failed: %v", err)
	}
	got := string(data)
	expected := `{"meta":{"name":"Oliver"},"min":{"field":"price"}}`
	if got != expected {
		t.Errorf("expected\n%s\n,got:\n%s", expected, got)
	}
}
