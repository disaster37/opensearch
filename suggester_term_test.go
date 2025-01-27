// Copyright 2012-present Oliver Eilhard. All rights reserved.
// Use of this source code is governed by a MIT-license.
// See http://olivere.mit-license.org/license.txt for details.

package opensearch

import (
	"encoding/json"
	"testing"
)

func TestTermSuggesterSource(t *testing.T) {
	s := NewTermSuggester("name").
		Text("n").
		Field("suggest")
	src, err := s.Source(true)
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(src)
	if err != nil {
		t.Fatalf("marshaling to JSON failed: %v", err)
	}
	got := string(data)
	expected := `{"name":{"text":"n","term":{"field":"suggest"}}}`
	if got != expected {
		t.Errorf("expected\n%s\n,got:\n%s", expected, got)
	}
}

func TestTermSuggesterWithPrefixLengthSource(t *testing.T) {
	s := NewTermSuggester("name").
		Text("n").
		Field("suggest").
		PrefixLength(0)
	src, err := s.Source(true)
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(src)
	if err != nil {
		t.Fatalf("marshaling to JSON failed: %v", err)
	}
	got := string(data)
	expected := `{"name":{"text":"n","term":{"field":"suggest","prefix_length":0}}}`
	if got != expected {
		t.Errorf("expected\n%s\n,got:\n%s", expected, got)
	}
}
