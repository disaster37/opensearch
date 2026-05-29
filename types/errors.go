package types

import "fmt"

// Error returns a human-readable string combining the HTTP status code,
// error reason, and error type from the OpenSearch response.
//
// Example output: "opensearch: Error 404: no such index [my-index] [type=index_not_found_exception]"
func (e *OpenSearchError) Error() string {
	if e.Details != nil && e.Details.Reason != "" {
		return fmt.Sprintf("opensearch: Error %d: %s [type=%s]", e.Status, e.Details.Reason, e.Details.Type)
	}
	return fmt.Sprintf("opensearch: Error %d", e.Status)
}

// StatusCode returns the HTTP status code reported by OpenSearch (e.g., 400, 404, 500).
func (e *OpenSearchError) StatusCode() int {
	return e.Status
}

// IsNotFound reports whether err is an [OpenSearchError] with HTTP status 404.
// Use it to branch on "resource not found" conditions without type assertions:
//
//	_, err := client.Document().Get(ctx, "my-index", "missing-id", nil)
//	if types.IsNotFound(err) {
//	    log.Println("document not found; creating it")
//	    // insert fresh document
//	}
func IsNotFound(err error) bool {
	if e, ok := err.(*OpenSearchError); ok {
		return e.Status == 404
	}
	return false
}

// IsConflict reports whether err is an [OpenSearchError] with HTTP status 409.
// This typically indicates a version conflict during an optimistic
// concurrency operation (see [DocumentVersion]).
//
//	_, err := client.Document().Update(ctx, "idx", "id", doc, version, nil)
//	if types.IsConflict(err) {
//	    // someone else modified the document; refetch and retry
//	}
func IsConflict(err error) bool {
	if e, ok := err.(*OpenSearchError); ok {
		return e.Status == 409
	}
	return false
}
