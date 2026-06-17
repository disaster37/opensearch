package types

import "fmt"

// Error returns a human-readable string combining the HTTP status code,
// error reason, and error type from the OpenSearch response.
// When a nested cause is available (via caused_by, root_cause, or failed_shards),
// it is appended to help diagnose the root problem.
//
// Example output: "opensearch: Error 400: all shards failed [type=search_phase_execution_exception]; caused by: [type=query_shard_exception] failed to create query: ..."
func (e *OpenSearchError) Error() string {
	if e.Details == nil || e.Details.Reason == "" {
		return fmt.Sprintf("opensearch: Error %d", e.Status)
	}
	msg := fmt.Sprintf("opensearch: Error %d: %s [type=%s]", e.Status, e.Details.Reason, e.Details.Type)
	if cause := causeString(e.Details); cause != "" {
		msg += "; caused by: " + cause
	}
	return msg
}

// causeString extracts the most informative cause from an OpenSearchErrorDetails,
// checking caused_by, root_cause, and failed_shards in that priority order.
// Returns an empty string when no distinct cause is found.
func causeString(d *OpenSearchErrorDetails) string {
	// 1. caused_by map
	if len(d.CausedBy) > 0 {
		reason, _ := d.CausedBy["reason"].(string)
		typ, _ := d.CausedBy["type"].(string)
		if reason != "" && reason != d.Reason {
			if typ != "" {
				return fmt.Sprintf("[type=%s] %s", typ, reason)
			}
			return reason
		}
	}

	// 2. root_cause — first entry with a reason different from the top-level reason
	for _, rc := range d.RootCause {
		if rc == nil || rc.Reason == "" || rc.Reason == d.Reason {
			continue
		}
		if rc.Type != "" {
			return fmt.Sprintf("[type=%s] %s", rc.Type, rc.Reason)
		}
		return rc.Reason
	}

	// 3. failed_shards — first shard whose nested reason object is non-empty
	for _, shard := range d.FailedShards {
		if shard == nil {
			continue
		}
		// reason may be a nested map[string]any or a plain string
		switch rv := shard["reason"].(type) {
		case map[string]any:
			reason, _ := rv["reason"].(string)
			typ, _ := rv["type"].(string)
			if reason != "" {
				if typ != "" {
					return fmt.Sprintf("[type=%s] %s", typ, reason)
				}
				return reason
			}
		case string:
			if rv != "" {
				return rv
			}
		}
	}

	return ""
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
