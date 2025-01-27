// Copyright 2012-present Oliver Eilhard. All rights reserved.
// Use of this source code is governed by a MIT-license.
// See http://olivere.mit-license.org/license.txt for details.

package opensearch

import (
	"context"
	"encoding/json"
	"testing"
)

func TestSimpleQueryStringQuery(t *testing.T) {
	q := NewSimpleQueryStringQuery(`"fried eggs" +(eggplant | potato) -frittata`)
	src, err := q.Source()
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(src)
	if err != nil {
		t.Fatalf("marshaling to JSON failed: %v", err)
	}
	got := string(data)
	expected := `{"simple_query_string":{"query":"\"fried eggs\" +(eggplant | potato) -frittata"}}`
	if got != expected {
		t.Errorf("expected\n%s\n,got:\n%s", expected, got)
	}
}

func TestSimpleQueryStringQueryExec(t *testing.T) {
	// client := setupTestClientAndCreateIndexAndLog(t, SetTraceLog(log.New(os.Stdout, "", 0)))
	client := setupTestClientAndCreateIndex(t)

	tweet1 := tweet{User: "olivere", Message: "Welcome to Golang and Opensearch."}
	tweet2 := tweet{User: "olivere", Message: "Another unrelated topic."}
	tweet3 := tweet{User: "sandrae", Message: "Cycling is fun."}

	// Add all documents
	_, err := client.Index().Index(testIndexName).Id("1").BodyJson(&tweet1).Do(context.TODO())
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

	// Match all should return all documents
	searchResult, err := client.Search().
		Index(testIndexName).
		Query(NewSimpleQueryStringQuery("+Golang +Opensearch")).
		Do(context.TODO())
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
