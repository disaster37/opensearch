// Copyright 2012-present Oliver Eilhard. All rights reserved.
// Use of this source code is governed by a MIT-license.
// See http://olivere.mit-license.org/license.txt for details.

package opensearch

import (
	"encoding/json"
	"testing"
)

func TestHasChildQuery(t *testing.T) {
	q := NewHasChildQuery("blog_tag", NewTermQuery("tag", "something")).ScoreMode("min")
	src, err := q.Source()
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(src)
	if err != nil {
		t.Fatalf("marshaling to JSON failed: %v", err)
	}
	got := string(data)
	expected := `{"has_child":{"query":{"term":{"tag":"something"}},"score_mode":"min","type":"blog_tag"}}`
	if got != expected {
		t.Errorf("expected\n%s\n,got:\n%s", expected, got)
	}
}

func TestHasChildQueryWithInnerHit(t *testing.T) {
	q := NewHasChildQuery("blog_tag", NewTermQuery("tag", "something"))
	q = q.InnerHit(NewInnerHit().Name("comments"))
	src, err := q.Source()
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(src)
	if err != nil {
		t.Fatalf("marshaling to JSON failed: %v", err)
	}
	got := string(data)
	expected := `{"has_child":{"inner_hits":{"name":"comments"},"query":{"term":{"tag":"something"}},"type":"blog_tag"}}`
	if got != expected {
		t.Errorf("expected\n%s\n,got:\n%s", expected, got)
	}
}
