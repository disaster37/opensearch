package opensearch

import "github.com/disaster37/opensearch/v3/types"

// IsNotFound returns true when err is an [OpenSearchError] with status 404.
// Use this to branch on "document or index not found" cases without
// inspecting the error details manually:
//
//	res, err := client.Document().Get(ctx, "my-index", "doc-id", nil)
//	if err != nil {
//	    if opensearch.IsNotFound(err) {
//	        log.Printf("document missing; creating fresh copy")
//	        // insert new document
//	    } else {
//	        return err
//	    }
//	}
//
// _ = res
func IsNotFound(err error) bool {
	return types.IsNotFound(err)
}

// IsConflict returns true when err is an [OpenSearchError] with status 409.
// This typically indicates a version conflict during an optimistic
// concurrency operation (see [DocumentVersion]).
func IsConflict(err error) bool {
	return types.IsConflict(err)
}
