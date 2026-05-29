package opensearch_test

import (
	"context"
	"fmt"
	"log"
	"testing"

	opensearch "github.com/disaster37/opensearch/v3"
	"github.com/disaster37/opensearch/v3/api"
	"github.com/sirupsen/logrus"
)

func TestExamples(t *testing.T) {
}

func ExampleNew() {
	logger := logrus.NewEntry(logrus.StandardLogger())

	client, err := opensearch.New(&opensearch.Config{
		URL:      "https://localhost:9200",
		Username: "admin",
		Password: "admin",
	}, logger)
	if err != nil {
		log.Fatal(err)
	}

	health, err := client.Cluster().Health(
		context.Background(),
		nil,
	)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("cluster %s: %s\n", health.ClusterName, health.Status)
}

func ExampleIsNotFound() {
	logger := logrus.NewEntry(logrus.StandardLogger())
	client, err := opensearch.New(&opensearch.Config{
		URL: "http://localhost:9200",
	}, logger)
	if err != nil {
		log.Fatal(err)
	}

	_, err = client.Document().Get(context.Background(), &api.GetRequest{
		Index: "my-index",
		Id:    "missing-id",
	})
	if err != nil {
		if opensearch.IsNotFound(err) {
			fmt.Println("document not found; creating fresh one")
		} else if opensearch.IsConflict(err) {
			fmt.Println("version conflict: refetch and retry")
		} else {
			log.Fatal(err)
		}
	}
}

func ExampleDocumentVersion() {
	logger := logrus.NewEntry(logrus.StandardLogger())
	client, err := opensearch.New(&opensearch.Config{
		URL: "http://localhost:9200",
	}, logger)
	if err != nil {
		log.Fatal(err)
	}

	doc, err := client.Document().Get(context.Background(), &api.GetRequest{
		Index: "my-index",
		Id:    "doc-1",
	})
	if err != nil {
		log.Fatal(err)
	}

	newSource := map[string]any{"name": "updated"}

	if doc.SeqNo == nil || doc.PrimaryTerm == nil {
		log.Fatal("document has no seq_no/primary_term; refetch")
	}
	_, err = client.Document().Update(
		context.Background(),
		&api.UpdateRequest{
			Index: "my-index",
			Id:    "doc-1",
			Body:  newSource,
			Params: map[string]string{
				"if_seq_no":       fmt.Sprintf("%d", *doc.SeqNo),
				"if_primary_term": fmt.Sprintf("%d", *doc.PrimaryTerm),
			},
		},
	)
	if err != nil {
		if opensearch.IsConflict(err) {
			fmt.Println("conflict; retry the read-modify-write cycle")
		} else {
			log.Fatal(err)
		}
	}
}
