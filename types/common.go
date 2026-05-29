package types

// CommonParams are the query parameters common to all OpenSearch API requests
// (pretty, human, error_trace, filter_path). These are forwarded by all service
// methods and encoded into the request URL.
type CommonParams struct {
	// Pretty formats the JSON response with indentation (adds ?pretty=true).
	Pretty *bool
	// Human returns human-readable values (adds ?human=true).
	Human *bool
	// ErrorTrace includes the stack trace for errors (adds ?error_trace=true).
	ErrorTrace *bool
	// FilterPath is a comma-separated list of response fields to include.
	// Useful for reducing network payload.
	FilterPath []string
}

// AcknowledgedResponse is the standard response for operations that request
// cluster acknowledgment. Most index creation, deletion, and mapping
// operations return this type.
//
// The Acknowledged field indicates whether all cluster nodes applied the change.
// ShardsAcknowledged indicates whether the requisite number of shard copies
// were started before the operation timed out.
//
// Example:
//
//	resp, err := client.Indices().Create(ctx, "my-index", settings)
//	if err != nil {
//	    return err
//	}
//	if !resp.Acknowledged {
//	    log.Println("cluster did not acknowledge index creation; check health")
//	}
type AcknowledgedResponse struct {
	Acknowledged       bool   `json:"acknowledged"`
	ShardsAcknowledged bool   `json:"shards_acknowledged,omitempty"`
	Index              string `json:"index,omitempty"`
}

// ShardsInfo represents the "_shards" section of most OpenSearch responses.
// It aggregates shard-level success and failure counts for the operation.
type ShardsInfo struct {
	// Total is the total number of shard copies this operation was applied to.
	Total int `json:"total"`
	// Successful is the number of shard copies that applied successfully.
	Successful int `json:"successful"`
	// Failed is the number of shard copies that failed.
	Failed int `json:"failed"`
	// Failures contains the per-shard failure details. Empty when Failed is 0.
	Failures []*ShardOperationFailedException `json:"failures,omitempty"`
	// Skipped is the number of shard copies skipped (e.g., on unfetched data).
	Skipped int `json:"skipped,omitempty"`
}

// ShardOperationFailedException describes a single shard failure within
// a [ShardsInfo] response. Contains the shard identifier, affected index,
// and a structured reason map with type/stack information.
type ShardOperationFailedException struct {
	Shard   int            `json:"shard,omitempty"`
	Index   string         `json:"index,omitempty"`
	Status  string         `json:"status,omitempty"`
	Reason  map[string]any `json:"reason,omitempty"`
	Node    string         `json:"_node,omitempty"`
	Primary bool           `json:"primary,omitempty"`
}

// BroadcastResponse is the common response for operations applied across all
// shards of a set of indices (flush, refresh, forcemerge, etc.).
type BroadcastResponse struct {
	Shards     *ShardsInfo                      `json:"_shards,omitempty"`
	Total      int                              `json:"total"`
	Successful int                              `json:"successful"`
	Failed     int                              `json:"failed"`
	Failures   []*ShardOperationFailedException `json:"failures,omitempty"`
}

// FailedNodeException represents a failure that occurred on a specific node
// during a cluster-wide operation (e.g., node info or stats). The embedded
// [OpenSearchErrorDetails] carries the structured error; NodeId identifies
// the affected node.
type FailedNodeException struct {
	*OpenSearchErrorDetails
	NodeId string `json:"node_id"`
}

// OpenSearchError is the structured error returned by OpenSearch when a
// request fails. Status is the HTTP status code (e.g., 400, 404, 409, 500)
// and Details holds the typed payload.
//
// Use [IsNotFound] and [IsConflict] helpers for common cases, or type-assert:
//
//	var osErr *OpenSearchError
//	if errors.As(err, &osErr) {
//	    log.Printf("OpenSearch %d: %s", osErr.Status, osErr.Details.Reason)
//	}
type OpenSearchError struct {
	Status  int                     `json:"status"`
	Details *OpenSearchErrorDetails `json:"error,omitempty"`
}

// OpenSearchErrorDetails is the structured payload inside an [OpenSearchError].
// For scripting errors, ScriptStack contains the script source and Position
// points to the offending token.
type OpenSearchErrorDetails struct {
	// Type is a short error classification (e.g., "illegal_argument_exception").
	Type string `json:"type"`
	// Reason is a human-readable description of what went wrong.
	Reason string `json:"reason"`
	// ResourceType is the kind of resource affected (e.g., "index_or_data_stream").
	ResourceType string `json:"resource.type,omitempty"`
	// ResourceId is the name/id of the affected resource.
	ResourceId string `json:"resource.id,omitempty"`
	// Index is the index name, if applicable.
	Index string `json:"index,omitempty"`
	// Phase is the search phase in which the error occurred.
	Phase string `json:"phase,omitempty"`
	// Grouped indicates whether this error was grouped with others.
	Grouped bool `json:"grouped,omitempty"`
	// CausedBy is a nested map with the upstream cause of this error.
	CausedBy map[string]any `json:"caused_by,omitempty"`
	// RootCause contains a list of the most immediate causes (for multi-shard failures).
	RootCause []*OpenSearchErrorDetails `json:"root_cause,omitempty"`
	// Suppressed contains additional suppressed errors (rare; scripting failures).
	Suppressed []*OpenSearchErrorDetails `json:"suppressed,omitempty"`
	// FailedShards lists per-shard failure details when shards failed.
	FailedShards []map[string]any `json:"failed_shards,omitempty"`
	// Header contains request headers OpenSearch included in the error response.
	Header map[string]any `json:"header,omitempty"`
	// ScriptStack contains the offending script source (line-by-line).
	ScriptStack []string `json:"script_stack,omitempty"`
	// Script is the script source that failed.
	Script string `json:"script,omitempty"`
	// Lang is the script language (e.g., "painless").
	Lang string `json:"lang,omitempty"`
	// Position points to the offending token in the script (scripting errors only).
	Position *ScriptErrorPosition `json:"position,omitempty"`
}

// ScriptErrorPosition describes where in a script an error occurred.
// Used inside [OpenSearchErrorDetails] when a scripting failure is detected.
type ScriptErrorPosition struct {
	Offset int `json:"offset"`
	Start  int `json:"start"`
	End    int `json:"end"`
}

// ErrorDetails is an alias for [OpenSearchErrorDetails] for backward
// compatibility with earlier versions of this client.
//
// Deprecated: use [OpenSearchErrorDetails] directly.
type ErrorDetails = OpenSearchErrorDetails

// DocumentVersion carries the seq_no and primary_term used for optimistic
// concurrency control. Pass to document write operations (index, update,
// delete) to fail the operation if the document has been modified since it
// was last fetched.
//
// Example:
//
//	doc, err := client.Document().Get(ctx, "my-index", "doc-id", nil)
//	// ... modify doc.Source ...
//	_, err = client.Document().Update(ctx, "my-index", "doc-id", modified,
//	    &types.DocumentVersion{
//	        SeqNo:       doc.SeqNo,
//	        PrimaryTerm: doc.PrimaryTerm,
//	    }, nil)
type DocumentVersion struct {
	SeqNo       *int64
	PrimaryTerm *int64
}

// ListResponse is a generic list-response wrapper used by plugin services
// such as ISM, SM, Alerting, and Transform.
type ListResponse[T any] struct {
	Items []T `json:"items"`
	Total int `json:"total"`
}
