// Copyright 2012-present Oliver Eilhard. All rights reserved.
// Use of this source code is governed by a MIT-license.
// See http://olivere.mit-license.org/license.txt for details.

package opensearch

import (
	"context"
	"testing"
)

func TestIndicesDatastream(t *testing.T) {
	client := setupTestClient(t)

	// Create template for datastream index
	createTemplate, err := client.IndexPutIndexTemplate(testDatastreamIndexName).
		BodyString(`{
			"index_patterns": ["` + testDatastreamIndexName + `"],
			"data_stream": {}
		}`).Do(context.TODO())
	if err != nil {
		t.Fatal(err)
	}
	if !createTemplate.Acknowledged {
		t.Errorf("expected IndicesPutIndexTemplateResult.Acknowledged %v; got %v", true, createTemplate.Acknowledged)
	}

	// Create datastream index
	createDS, err := client.CreateDataStreamIndex(testDatastreamIndexName).Do(context.TODO())
	if err != nil {
		t.Fatal(err)
	}
	if !createDS.Acknowledged {
		t.Errorf("expected IndicesDataStreamCreateResult.Acknowledged %v; got %v", true, createDS.Acknowledged)
	}

	// Check if datastream index exists
	datastreamIndexes, err := client.GetDataStreamIndex(testDatastreamIndexName).Do(context.TODO())
	if err != nil {
		t.Fatal(err)
	}
	if len(datastreamIndexes.Datastreams) == 0 {
		t.Fatalf("datastream index %s should exist, but doesn't\n", testDatastreamIndexName)
	}

	// Delete datastream index
	deleteDS, err := client.DeleteDataStreamIndex(testDatastreamIndexName).Do(context.TODO())
	if err != nil {
		t.Fatal(err)
	}
	if !deleteDS.Acknowledged {
		t.Errorf("expected IndicesDatastreamDeleteResponse.Acknowledged %v; got %v", true, deleteDS.Acknowledged)
	}

	// Check if datastream index exists
	datastreamIndexes, err = client.GetDataStreamIndex(testDatastreamIndexName).Do(context.TODO())
	if err != nil {
		t.Fatal(err)
	}
	if len(datastreamIndexes.Datastreams) > 0 {
		t.Fatalf("datastream index %s should not exist, but does\n", testDatastreamIndexName)
	}
}
