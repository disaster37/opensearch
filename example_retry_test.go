package opensearch_test

import (
	"fmt"
	"log"
	"time"

	"github.com/disaster37/opensearch/v4"
	"github.com/sirupsen/logrus"
)

func ExampleNew_withRetry() {
	logger := logrus.NewEntry(logrus.New())

	// Example: Configure client with retry for long-running queries like PIT searches
	client, err := opensearch.New(&opensearch.Config{
		URL:      "https://localhost:9200",
		Username: "admin",
		Password: "admin",
		RetryCount: 3,
		RetryWaitTime: 500 * time.Millisecond,
		RetryMaxWaitTime: 5 * time.Second,
		RetryConditions: opensearch.PITSearchRetryConditions(),
	}, logger)
	if err != nil {
		log.Fatal(err)
	}

	// The client will now automatically retry failed requests up to 3 times
	// with exponential backoff between 500ms and 5s, including OpenSearch-specific
	// transient errors like search_phase_execution_exception.
	
	_ = client
	fmt.Println("Client configured with retry settings")
	// Output:
	// Client configured with retry settings
}

func ExampleNew_defaultRetry() {
	logger := logrus.NewEntry(logrus.New())

	// Example: Use default retry conditions (network errors, 429, 5xx)
	client, err := opensearch.New(&opensearch.Config{
		URL:      "https://localhost:9200",
		Username: "admin", 
		Password: "admin",
		RetryCount: 2,
		RetryWaitTime: 100 * time.Millisecond,
		RetryMaxWaitTime: 2 * time.Second,
		RetryConditions: opensearch.DefaultRetryConditions(),
	}, logger)
	if err != nil {
		log.Fatal(err)
	}

	_ = client
	fmt.Println("Client with default retry conditions")
	// Output:
	// Client with default retry conditions
}