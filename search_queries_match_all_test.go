// Copyright 2012-present Oliver Eilhard. All rights reserved.
// Use of this source code is governed by a MIT-license.
// See http://olivere.mit-license.org/license.txt for details.

package opensearch

import (
	"encoding/json"
	"testing"
)

func TestMatchAllQuery(t *testing.T) {
	q := NewMatchAllQuery()
	src, err := q.Source()
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(src)
	if err != nil {
		t.Fatalf("marshaling to JSON failed: %v", err)
	}
	got := string(data)
	expected := `{"match_all":{}}`
	if got != expected {
		t.Errorf("expected\n%s\n,got:\n%s", expected, got)
	}
}

func TestMatchAllQueryWithBoost(t *testing.T) {
	q := NewMatchAllQuery().Boost(3.14)
	src, err := q.Source()
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(src)
	if err != nil {
		t.Fatalf("marshaling to JSON failed: %v", err)
	}
	got := string(data)
	expected := `{"match_all":{"boost":3.14}}`
	if got != expected {
		t.Errorf("expected\n%s\n,got:\n%s", expected, got)
	}
}

func TestMatchAllQueryWithQueryName(t *testing.T) {
	q := NewMatchAllQuery().QueryName("qname")
	src, err := q.Source()
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(src)
	if err != nil {
		t.Fatalf("marshaling to JSON failed: %v", err)
	}
	got := string(data)
	expected := `{"match_all":{"_name":"qname"}}`
	if got != expected {
		t.Errorf("expected\n%s\n,got:\n%s", expected, got)
	}
}

func TestMatchAllMarshalJSON(t *testing.T) {
	in := NewMatchAllQuery().Boost(3.14)
	data, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	got := string(data)
	expected := `{"match_all":{"boost":3.14}}`
	if got != expected {
		t.Errorf("expected\n%s\n,got:\n%s", expected, got)
	}
}
