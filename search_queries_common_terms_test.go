// Copyright 2012-present Oliver Eilhard. All rights reserved.
// Use of this source code is governed by a MIT-license.
// See http://olivere.mit-license.org/license.txt for details.

package opensearch

import (
	"context"
	"encoding/json"
	_ "net/http"
	"testing"

	"github.com/hashicorp/go-version"
)

func TestCommonTermsQuery(t *testing.T) {
	q := NewCommonTermsQuery("message", "Golang").CutoffFrequency(0.001)
	src, err := q.Source()
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(src)
	if err != nil {
		t.Fatalf("marshaling to JSON failed: %v", err)
	}
	got := string(data)
	expected := `{"common":{"message":{"cutoff_frequency":0.001,"query":"Golang"}}}`
	if got != expected {
		t.Errorf("expected\n%s\n,got:\n%s", expected, got)
	}
}

func TestSearchQueriesCommonTermsQuery(t *testing.T) {
	client := setupTestClientAndCreateIndex(t)

	// Deprecated in >= 7.3.0
	// https://www.opensearch.co/guide/en/opensearchsearch/reference/current/query-dsl-common-terms-query.html
	esversion, err := client.OpensearchVersion("https://opensearch.svc:9200")
	if err != nil {
		t.Fatal(err)
	}
	currentVersion, err := version.NewVersion(esversion)
	if err != nil {
		t.Fatal(err)
	}
	maxVersion, err := version.NewVersion("2.0.0")
	if err != nil {
		t.Fatal(err)
	}
	if currentVersion.GreaterThanOrEqual(maxVersion) {
		t.Skipf("Opensearch versions >= 2.0.0 deprecated Common Terms Query. "+
			"See https://www.opensearch.co/guide/en/opensearchsearch/reference/current/query-dsl-common-terms-query.html. "+
			"You are running Opensearch %v.", esversion)
	}

	tweet1 := tweet{User: "olivere", Message: "Welcome to Golang and Opensearch."}
	tweet2 := tweet{User: "olivere", Message: "Another unrelated topic."}
	tweet3 := tweet{User: "sandrae", Message: "Cycling is fun."}

	// Add all documents
	_, err = client.Index().Index(testIndexName).Id("1").BodyJson(&tweet1).Do(context.TODO())
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.Index().Index(testIndexName).Id("2").BodyJson(&tweet2).Do(context.TODO())
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.Index().Index(testIndexName).Id("3").BodyJson(&tweet3).Do(context.TODO())
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.Refresh().Index(testIndexName).Do(context.TODO())
	if err != nil {
		t.Fatal(err)
	}

	// Common terms query
	q := NewCommonTermsQuery("message", "Golang")
	searchResult, err := client.Search().Index(testIndexName).Query(q).Do(context.TODO())
	if err != nil {
		t.Fatal(err)
	}
	if searchResult.Hits == nil {
		t.Errorf("expected SearchResult.Hits != nil; got nil")
	}
	if searchResult.TotalHits() != 1 {
		t.Errorf("expected SearchResult.TotalHits() = %d; got %d", 1, searchResult.TotalHits())
	}
	if len(searchResult.Hits.Hits) != 1 {
		t.Errorf("expected len(SearchResult.Hits.Hits) = %d; got %d", 1, len(searchResult.Hits.Hits))
	}

	for _, hit := range searchResult.Hits.Hits {
		if hit.Index != testIndexName {
			t.Errorf("expected SearchResult.Hits.Hit.Index = %q; got %q", testIndexName, hit.Index)
		}
		item := make(map[string]interface{})
		err := json.Unmarshal(hit.Source, &item)
		if err != nil {
			t.Fatal(err)
		}
	}
}
