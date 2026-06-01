package opensearch

import "github.com/disaster37/opensearch/v4/types"

// CommonParams are the query parameters common to all OpenSearch API requests.
// They are forwarded to all service methods via [DocumentService], [SearchService], etc.
type CommonParams = types.CommonParams

// AcknowledgedResponse is the common response for operations that are simply
// acknowledged or rejected. Returned by most indices and cluster operations.
type AcknowledgedResponse = types.AcknowledgedResponse

// ShardsInfo represents the "_shards" section of most OpenSearch responses.
// Reports the number of successful, failed, and pending shard operations.
type ShardsInfo = types.ShardsInfo

// ShardOperationFailedException describes a single shard failure in a
// [ShardsInfo] response.
type ShardOperationFailedException = types.ShardOperationFailedException

// BroadcastResponse is the common response for broadcast-style operations
// (operations applied to all shards in a set of indices).
type BroadcastResponse = types.BroadcastResponse

// FailedNodeException represents a failure that occurred on a specific
// node during a multi-node operation (e.g., [NodesService.Info]).
type FailedNodeException = types.FailedNodeException

// OpenSearchError is the structured error returned by OpenSearch when a
// request fails. Use [IsNotFound] and [IsConflict] for common checks.
type OpenSearchError = types.OpenSearchError

// OpenSearchErrorDetails contains the detailed error payload of an
// [OpenSearchError], including root causes and stack traces for scripting errors.
type OpenSearchErrorDetails = types.OpenSearchErrorDetails

// ScriptErrorPosition describes where in a script an error occurred
// (used inside [OpenSearchErrorDetails] for scripting errors).
type ScriptErrorPosition = types.ScriptErrorPosition

// ErrorDetails is an alias for [OpenSearchErrorDetails] for backward compatibility.
type ErrorDetails = types.ErrorDetails

// DocumentVersion carries the seq_no and primary_term used for optimistic
// concurrency control in document write operations.
//
// Pass a [DocumentVersion] to [DocumentService.Index], [DocumentService.Update],
// or [DocumentService.Delete] to fail the operation if the document has been
// modified since it was last fetched.
type DocumentVersion = types.DocumentVersion

// UnixMilliTime is a [time.Time] that serializes to/from Unix milliseconds.
// Used by the CCR and plugin APIs to represent timestamps.
type UnixMilliTime = types.UnixMilliTime

// ListResponse is a generic list-response wrapper used by plugin services
// (ISM, SM, Alerting, Transform).
type ListResponse[T any] = types.ListResponse[T]
