// Copyright 2012-present Oliver Eilhard. All rights reserved.
// Use of this source code is governed by a MIT-license.
// See http://olivere.mit-license.org/license.txt for details.

package opensearch

import (
	"encoding/json"
	_ "net/http"
	"testing"
)

func TestSearchRequestIndex(t *testing.T) {
	builder := NewSearchRequest().Index("test")
	data, err := json.Marshal(builder.header())
	if err != nil {
		t.Fatalf("marshaling to JSON failed: %v", err)
	}
	got := string(data)
	expected := `{"index":"test"}`
	if got != expected {
		t.Errorf("expected\n%s\n,got:\n%s", expected, got)
	}
}

func TestSearchRequestIndices(t *testing.T) {
	builder := NewSearchRequest().Index("test", "test2")
	data, err := json.Marshal(builder.header())
	if err != nil {
		t.Fatalf("marshaling to JSON failed: %v", err)
	}
	got := string(data)
	expected := `{"indices":["test","test2"]}`
	if got != expected {
		t.Errorf("expected\n%s\n,got:\n%s", expected, got)
	}
}

func TestSearchRequestHasIndices(t *testing.T) {
	builder := NewSearchRequest()
	if builder.HasIndices() {
		t.Errorf("expected HasIndices to return true; got %v", builder.HasIndices())
	}
	builder = builder.Index("test", "test2")
	if !builder.HasIndices() {
		t.Errorf("expected HasIndices to return false; got %v", builder.HasIndices())
	}
}

func TestSearchRequestIgnoreUnavailable(t *testing.T) {
	builder := NewSearchRequest().Index("test").IgnoreUnavailable(true)
	data, err := json.Marshal(builder.header())
	if err != nil {
		t.Fatalf("marshaling to JSON failed: %v", err)
	}
	got := string(data)
	expected := `{"ignore_unavailable":true,"index":"test"}`
	if got != expected {
		t.Errorf("expected\n%s\n,got:\n%s", expected, got)
	}
}
