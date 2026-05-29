// Package types provides the common types and error handling used throughout
// the OpenSearch client.
//
// This package contains the foundational types that are shared across all
// service implementations in the api package and query builders in the
// querydsl package.
//
// # Error Handling
//
// All API methods return *OpenSearchError when an error occurs. Use the
// IsNotFound and IsConflict helpers to check for specific error conditions:
//
//	result, err := client.Document().Get(ctx, "my-index", "doc-id")
//	if types.IsNotFound(err) {
//	    log.Println("Document not found")
//	} else if err != nil {
//	    log.Fatal(err)
//	}
//
// # Common Response Types
//
// Many operations return AcknowledgedResponse or include ShardsInfo in their
// responses:
//
//	// Create an index
//	resp, err := client.Indices().Create(ctx, "my-index", nil)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	if !resp.Acknowledged {
//	    log.Println("Index creation not acknowledged")
//	}
//
// # Versioning and Concurrency
//
// Use DocumentVersion to implement optimistic concurrency control:
//
//	version := &types.DocumentVersion{
//	    SeqNo:       int64Ptr(0),
//	    PrimaryTerm: int64Ptr(1),
//	}
//	_, err := client.Document().Index(ctx, "my-index", "doc-id", doc, version)
package types
