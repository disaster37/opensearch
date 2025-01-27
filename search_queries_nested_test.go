// Copyright 2012-present Oliver Eilhard. All rights reserved.
// Use of this source code is governed by a MIT-license.
// See http://olivere.mit-license.org/license.txt for details.

package opensearch

import (
	"encoding/json"
	"testing"
)

func TestNestedQuery(t *testing.T) {
	bq := NewBoolQuery()
	bq = bq.Must(NewTermQuery("obj1.name", "blue"))
	bq = bq.Must(NewRangeQuery("obj1.count").Gt(5))
	q := NewNestedQuery("obj1", bq).QueryName("qname")
	src, err := q.Source()
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(src)
	if err != nil {
		t.Fatalf("marshaling to JSON failed: %v", err)
	}
	got := string(data)
	expected := `{"nested":{"_name":"qname","path":"obj1","query":{"bool":{"must":[{"term":{"obj1.name":"blue"}},{"range":{"obj1.count":{"from":5,"include_lower":false,"include_upper":true,"to":null}}}]}}}}`
	if got != expected {
		t.Errorf("expected\n%s\n,got:\n%s", expected, got)
	}
}

func TestNestedQueryWithInnerHit(t *testing.T) {
	bq := NewBoolQuery()
	bq = bq.Must(NewTermQuery("obj1.name", "blue"))
	bq = bq.Must(NewRangeQuery("obj1.count").Gt(5))
	q := NewNestedQuery("obj1", bq)
	q = q.QueryName("qname")
	q = q.InnerHit(NewInnerHit().Name("comments").Query(NewTermQuery("user", "olivere")))
	src, err := q.Source()
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(src)
	if err != nil {
		t.Fatalf("marshaling to JSON failed: %v", err)
	}
	got := string(data)
	expected := `{"nested":{"_name":"qname","inner_hits":{"name":"comments","query":{"term":{"user":"olivere"}}},"path":"obj1","query":{"bool":{"must":[{"term":{"obj1.name":"blue"}},{"range":{"obj1.count":{"from":5,"include_lower":false,"include_upper":true,"to":null}}}]}}}}`
	if got != expected {
		t.Errorf("expected\n%s\n,got:\n%s", expected, got)
	}
}

func TestNestedQueryWithIgnoreUnmapped(t *testing.T) {
	tests := []struct {
		query    *BoolQuery
		expected string
	}{
		{
			NewBoolQuery().Must(NewNestedQuery("path", NewTermQuery("test", "test"))),
			`{"bool":{"must":{"nested":{"path":"path","query":{"term":{"test":"test"}}}}}}`,
		},
		{
			NewBoolQuery().Must(NewNestedQuery("path", NewTermQuery("test", "test")).IgnoreUnmapped(true)),
			`{"bool":{"must":{"nested":{"ignore_unmapped":true,"path":"path","query":{"term":{"test":"test"}}}}}}`,
		},
		{
			NewBoolQuery().Must(NewNestedQuery("path", NewTermQuery("test", "test")).IgnoreUnmapped(false)),
			`{"bool":{"must":{"nested":{"ignore_unmapped":false,"path":"path","query":{"term":{"test":"test"}}}}}}`,
		},
	}
	for _, test := range tests {
		src, err := test.query.Source()
		if err != nil {
			t.Fatal(err)
		}
		data, err := json.Marshal(src)
		if err != nil {
			t.Fatalf("marshaling to JSON failed: %v", err)
		}
		got := string(data)
		if got != test.expected {
			t.Errorf("expected\n%s\n,got:\n%s", test.expected, got)
		}
	}
}
