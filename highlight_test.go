// Copyright 2012-present Oliver Eilhard. All rights reserved.
// Use of this source code is governed by a MIT-license.
// See http://olivere.mit-license.org/license.txt for details.

package opensearch

import (
	"context"
	"encoding/json"
	"testing"
)

func TestHighlighterField(t *testing.T) {
	field := NewHighlighterField("grade")
	src, err := field.Source()
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(src)
	if err != nil {
		t.Fatalf("marshaling to JSON failed: %v", err)
	}
	got := string(data)
	expected := `{}`
	if got != expected {
		t.Errorf("expected\n%s\n,got:\n%s", expected, got)
	}
}

func TestHighlighterFieldWithOptions(t *testing.T) {
	field := NewHighlighterField("grade").FragmentSize(2).NumOfFragments(1)
	src, err := field.Source()
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(src)
	if err != nil {
		t.Fatalf("marshaling to JSON failed: %v", err)
	}
	got := string(data)
	expected := `{"fragment_size":2,"number_of_fragments":1}`
	if got != expected {
		t.Errorf("expected\n%s\n,got:\n%s", expected, got)
	}
}

func TestHighlightWithStringField(t *testing.T) {
	builder := NewHighlight().Field("grade")
	src, err := builder.Source()
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(src)
	if err != nil {
		t.Fatalf("marshaling to JSON failed: %v", err)
	}
	got := string(data)
	expected := `{"fields":{"grade":{}}}`
	if got != expected {
		t.Errorf("expected\n%s\n,got:\n%s", expected, got)
	}
}

func TestHighlightWithFields(t *testing.T) {
	gradeField := NewHighlighterField("grade")
	builder := NewHighlight().Fields(gradeField)
	src, err := builder.Source()
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(src)
	if err != nil {
		t.Fatalf("marshaling to JSON failed: %v", err)
	}
	got := string(data)
	expected := `{"fields":{"grade":{}}}`
	if got != expected {
		t.Errorf("expected\n%s\n,got:\n%s", expected, got)
	}
}

func TestHighlightWithMultipleFields(t *testing.T) {
	gradeField := NewHighlighterField("grade")
	colorField := NewHighlighterField("color")
	builder := NewHighlight().Fields(gradeField, colorField)
	src, err := builder.Source()
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(src)
	if err != nil {
		t.Fatalf("marshaling to JSON failed: %v", err)
	}
	got := string(data)
	expected := `{"fields":{"color":{},"grade":{}}}`
	if got != expected {
		t.Errorf("expected\n%s\n,got:\n%s", expected, got)
	}
}

func TestHighlighterWithExplicitFieldOrder(t *testing.T) {
	gradeField := NewHighlighterField("grade").FragmentSize(2)
	colorField := NewHighlighterField("color").FragmentSize(2).NumOfFragments(1)
	builder := NewHighlight().Fields(gradeField, colorField).UseExplicitFieldOrder(true)
	src, err := builder.Source()
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(src)
	if err != nil {
		t.Fatalf("marshaling to JSON failed: %v", err)
	}
	got := string(data)
	expected := `{"fields":[{"grade":{"fragment_size":2}},{"color":{"fragment_size":2,"number_of_fragments":1}}]}`
	if got != expected {
		t.Errorf("expected\n%s\n,got:\n%s", expected, got)
	}
}

func TestHighlightWithBoundarySettings(t *testing.T) {
	builder := NewHighlight().
		BoundaryChars(" \t\r").
		BoundaryScannerType("word")
	src, err := builder.Source()
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(src)
	if err != nil {
		t.Fatalf("marshaling to JSON failed: %v", err)
	}
	got := string(data)
	expected := `{"boundary_chars":" \t\r","boundary_scanner":"word"}`
	if got != expected {
		t.Errorf("expected\n%s\n,got:\n%s", expected, got)
	}
}

func TestHighlightWithTermQuery(t *testing.T) {
	client := setupTestClientAndCreateIndex(t) //, SetTraceLog(log.New(os.Stdout, "", 0)))

	tweet1 := tweet{User: "olivere", Message: "Welcome to Golang and Opensearch."}
	tweet2 := tweet{User: "olivere", Message: "Another unrelated topic."}
	tweet3 := tweet{User: "sandrae", Message: "Cycling is fun to do."}

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

	// Specify highlighter
	hl := NewHighlight()
	hl = hl.Fields(NewHighlighterField("message"))
	hl = hl.PreTags("<em>").PostTags("</em>")

	// Match all should return all documents
	query := NewPrefixQuery("message", "golang")
	searchResult, err := client.Search().
		Index(testIndexName).
		Highlight(hl).
		Query(query).
		Pretty(true).
		Do(context.TODO())
	if err != nil {
		t.Fatal(err)
	}
	if searchResult.Hits == nil {
		t.Fatalf("expected SearchResult.Hits != nil; got nil")
	}
	if searchResult.TotalHits() != 1 {
		t.Fatalf("expected SearchResult.TotalHits() = %d; got %d", 1, searchResult.TotalHits())
	}
	if len(searchResult.Hits.Hits) != 1 {
		t.Fatalf("expected len(SearchResult.Hits.Hits) = %d; got %d", 1, len(searchResult.Hits.Hits))
	}

	hit := searchResult.Hits.Hits[0]
	var tw tweet
	if err := json.Unmarshal(hit.Source, &tw); err != nil {
		t.Fatal(err)
	}
	if len(hit.Highlight) == 0 {
		t.Fatal("expected hit to have a highlight; got nil")
	}
	if hl, found := hit.Highlight["message"]; found {
		if len(hl) != 1 {
			t.Fatalf("expected to have one highlight for field \"message\"; got %d", len(hl))
		}
		expected := "Welcome to <em>Golang</em> and Opensearch."
		if hl[0] != expected {
			t.Errorf("expected to have highlight \"%s\"; got \"%s\"", expected, hl[0])
		}
	} else {
		t.Fatal("expected to have a highlight on field \"message\"; got none")
	}
}
