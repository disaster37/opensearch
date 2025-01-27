// Copyright 2012-present Oliver Eilhard. All rights reserved.
// Use of this source code is governed by a MIT-license.
// See http://olivere.mit-license.org/license.txt for details.

package opensearch

import (
	"context"
	"testing"
	"time"
)

func TestTermVectorsBuildURL(t *testing.T) {
	client := setupTestClientAndCreateIndex(t)

	tests := []struct {
		Index    string
		Type     string
		Id       string
		Expected string
	}{
		{
			"twitter",
			"_doc",
			"",
			"/twitter/_doc/_termvectors",
		},
		{
			"twitter",
			"",
			"",
			"/twitter/_termvectors",
		},
		{
			"twitter",
			"_doc",
			"1",
			"/twitter/_doc/1/_termvectors",
		},
		{
			"twitter",
			"",
			"1",
			"/twitter/_termvectors/1",
		},
	}

	for _, test := range tests {
		builder := client.TermVectors(test.Index).Type(test.Type)
		if test.Id != "" {
			builder = builder.Id(test.Id)
		}
		path, _, err := builder.buildURL()
		if err != nil {
			t.Fatal(err)
		}
		if path != test.Expected {
			t.Errorf("expected %q; got: %q", test.Expected, path)
		}
	}
}

func TestTermVectorsWithId(t *testing.T) {
	client := setupTestClientAndCreateIndex(t)

	tweet1 := tweet{User: "olivere", Message: "Welcome to Golang and Opensearch."}

	// Add a document
	indexResult, err := client.Index().
		Index(testIndexName).
		Id("1").
		BodyJson(&tweet1).
		Refresh("true").
		Do(context.TODO())
	if err != nil {
		t.Fatal(err)
	}
	if indexResult == nil {
		t.Errorf("expected result to be != nil; got: %v", indexResult)
	}

	// TermVectors by specifying ID
	field := "Message"
	result, err := client.TermVectors(testIndexName).
		Id("1").
		Fields(field).
		FieldStatistics(true).
		TermStatistics(true).
		Do(context.TODO())
	if err != nil {
		t.Fatal(err)
	}
	if result == nil {
		t.Fatal("expected to return information and statistics")
	}
	if !result.Found {
		t.Errorf("expected found to be %v; got: %v", true, result.Found)
	}
}

func TestTermVectorsWithDoc(t *testing.T) {
	client := setupTestClientAndCreateIndex(t)

	// Travis lags sometimes
	if isCI() {
		time.Sleep(2 * time.Second)
	}

	// TermVectors by specifying Doc
	doc := map[string]interface{}{
		"fullname": "John Doe",
		"text":     "twitter test test test",
	}
	perFieldAnalyzer := map[string]string{
		"fullname": "keyword",
	}

	result, err := client.TermVectors(testIndexName).
		Doc(doc).
		PerFieldAnalyzer(perFieldAnalyzer).
		FieldStatistics(true).
		TermStatistics(true).
		Do(context.TODO())
	if err != nil {
		t.Fatal(err)
	}
	if result == nil {
		t.Fatal("expected to return information and statistics")
	}
	if !result.Found {
		t.Errorf("expected found to be %v; got: %v", true, result.Found)
	}
}

func TestTermVectorsWithFilter(t *testing.T) {
	client := setupTestClientAndCreateIndex(t)

	// Travis lags sometimes
	if isCI() {
		time.Sleep(2 * time.Second)
	}

	// TermVectors by specifying Doc
	doc := map[string]interface{}{
		"fullname": "John Doe",
		"text":     "twitter test test test",
	}
	perFieldAnalyzer := map[string]string{
		"fullname": "keyword",
	}

	result, err := client.TermVectors(testIndexName).
		Doc(doc).
		PerFieldAnalyzer(perFieldAnalyzer).
		FieldStatistics(true).
		TermStatistics(true).
		Filter(NewTermvectorsFilterSettings().MinTermFreq(1)).
		Do(context.TODO())
	if err != nil {
		t.Fatal(err)
	}
	if result == nil {
		t.Fatal("expected to return information and statistics")
	}
	if !result.Found {
		t.Errorf("expected found to be %v; got: %v", true, result.Found)
	}
}
