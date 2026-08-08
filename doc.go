// Package opensearch provides a Go client for the OpenSearch REST API.
//
// This client provides a type-safe, ergonomic interface for interacting with
// OpenSearch clusters. It supports all major OpenSearch features including:
//
//   - Document CRUD operations
//   - Full-text search and complex queries
//   - Aggregations and analytics
//   - Index lifecycle management
//   - Cross-cluster replication
//   - Security and access control
//   - And many more features
//
// # Quick Start
//
// Create a client:
//
//	import (
//	    "github.com/disaster37/opensearch/v4"
//	    "github.com/sirupsen/logrus"
//	)
//
//	func main() {
//	    logger := logrus.NewEntry(logrus.StandardLogger())
//
//	    client, err := opensearch.New(&opensearch.Config{
//	        URL:           "https://localhost:9200",
//	        Username:      "admin",
//	        Password:      "admin",
//	        TLSSkipVerify: true, // For development only
//	    }, logger)
//	    if err != nil {
//	        log.Fatal(err)
//	    }
//
//	    // Use the client to interact with OpenSearch
//	    // ...
//	}
//
// # Package Organization
//
// The client is organized into several sub-packages:
//
//   - types: Common types and error handling
//   - api: Service interfaces for OpenSearch REST APIs
//   - querydsl: Query and aggregation builder DSL
//
// Most users will only need to import the root package. The sub-packages are
// automatically re-exported for convenience.
//
// # Service Access
//
// Access OpenSearch features through service methods on the Client interface:
//
//	// Document operations
//	client.Document().Index(ctx, &api.IndexRequest{
//		Index: "my-index", Id: "doc-id", Body: doc,
//	})
//	client.Document().Get(ctx, &api.GetRequest{
//		Index: "my-index", Id: "doc-id",
//	})
//	client.Document().Delete(ctx, &api.DeleteRequest{
//		Index: "my-index", Id: "doc-id",
//	})
//
//	// Search operations
//	client.Search().Search(ctx, &api.SearchRequest{
//		Indices: []string{"my-index"}, Body: query,
//	})
//	client.Search().Count(ctx, []string{"my-index"}, query)
//
//	// Index operations
//	client.Indices().Create(ctx, "my-index", nil)
//	client.Indices().Delete(ctx, []string{"my-index"})
//
//	// Cluster operations
//	client.Cluster().Health(ctx, []string{"my-index"})
//	client.Cluster().Stats(ctx, nil)
//
// # Using the Query DSL
//
// For complex queries, use the querydsl sub-package to build type-safe queries:
//
//	import "github.com/disaster37/opensearch/v4/querydsl"
//
//	// Build a bool query
//	query := querydsl.NewBoolQuery().
//	    Must(querydsl.NewMatchQuery("status", "published")).
//	    Filter(querydsl.NewRangeQuery("date").Gte("2024-01-01")).
//	    Should(querydsl.NewTermQuery("featured", true))
//
//	// Execute the search
//	src, _ := query.Source()
//	result, err := client.Search().Search(ctx, &api.SearchRequest{
//	    Indices: []string{"my-index"}, Body: src},
//	)
//
// # Error Handling
//
// All operations can return an OpenSearchError. Use the helper functions to
// check for specific error conditions:
//
//	result, err := client.Document().Get(ctx, &api.GetRequest{
//	    Index: "my-index", Id: "doc-id",
//	})
//	if err != nil {
//	    if os.IsNotFound(err) {
//	        log.Println("Document not found")
//	    } else if os.IsConflict(err) {
//	        log.Println("Version conflict")
//	    } else {
//	        log.Fatal(err)
//	    }
//	}
//
// # Type Aliases
//
// For convenience, common types from the types sub-package are re-exported
// in the root package:
//
//   - OpenSearchError
//   - AcknowledgedResponse
//   - ShardsInfo
//   - BroadcastResponse
//   - DocumentVersion
//
// You can use these directly without importing the types package:
//
//	params := &api.IndexParams{
//	    IfSeqNo:       ptr(&version.SeqNo),
//	    IfPrimaryTerm: ptr(&version.PrimaryTerm),
//	}
//	client.Document().Index(ctx, &api.IndexRequest{
//	    Index: "my-index", Id: "doc-id", Body: doc, Params: params,
//	})
package opensearch
