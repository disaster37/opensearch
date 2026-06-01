package api

import (
	json "github.com/goccy/go-json"
	"net/http"

	"github.com/disaster37/opensearch/v4/types"
)

// IndexResponse represents the result of an index (add/update) document operation.
// It contains the document metadata assigned by OpenSearch, including the index name,
// document ID, version, sequence number, and shard acknowledgment details.
type IndexResponse struct {
	// Index is the name of the index the document was written to.
	Index string `json:"_index,omitempty"`
	// Type is the document type (deprecated; always "_doc" in modern OpenSearch).
	Type string `json:"_type,omitempty"`
	// Id is the document ID, either provided or server-generated.
	Id string `json:"_id,omitempty"`
	// Version is the document version after the operation.
	Version int64 `json:"_version,omitempty"`
	// Result indicates the outcome of the operation (e.g. "created", "updated").
	Result string `json:"result,omitempty"`
	// Shards provides shard-level acknowledgment of the write.
	Shards *types.ShardsInfo `json:"_shards,omitempty"`
	// SeqNo is the sequence number assigned to this operation.
	SeqNo int64 `json:"_seq_no,omitempty"`
	// PrimaryTerm is the primary term of the shard that processed this operation.
	PrimaryTerm int64 `json:"_primary_term,omitempty"`
	// Status is the HTTP status code returned by the server.
	Status int `json:"status,omitempty"`
	// ForcedRefresh indicates whether the index was force-refreshed as part of this operation.
	ForcedRefresh bool `json:"forced_refresh,omitempty"`
}

// GetResult represents the result of a get document operation.
// It contains the document source, metadata, and a flag indicating whether the document was found.
type GetResult struct {
	// Index is the index the document belongs to.
	Index string `json:"_index"`
	// Id is the document ID.
	Id string `json:"_id"`
	// Uid is the legacy unique identifier for the document.
	Uid string `json:"_uid"`
	// Routing is the custom routing value for this document, if any.
	Routing string `json:"_routing"`
	// Parent is the parent document ID, if applicable (deprecated).
	Parent string `json:"_parent"`
	// Version is the current version of the document; nil if not found.
	Version *int64 `json:"_version"`
	// SeqNo is the sequence number of the document; nil if not found.
	SeqNo *int64 `json:"_seq_no"`
	// PrimaryTerm is the primary term of the shard; nil if not found.
	PrimaryTerm *int64 `json:"_primary_term"`
	// Source is the raw JSON source of the document.
	Source json.RawMessage `json:"_source,omitempty"`
	// Found indicates whether the document was found.
	Found bool `json:"found,omitempty"`
	// Fields contains stored fields returned when requested via the stored_fields parameter.
	Fields map[string]any `json:"fields,omitempty"`
	// Error contains error details if the document retrieval failed.
	Error *types.OpenSearchErrorDetails `json:"error,omitempty"`
}

// DeleteResponse represents the result of a delete document operation.
type DeleteResponse struct {
	// Index is the name of the index the document was deleted from.
	Index string `json:"_index,omitempty"`
	// Id is the deleted document's ID.
	Id string `json:"_id,omitempty"`
	// Version is the document version after deletion.
	Version int64 `json:"_version,omitempty"`
	// Result indicates the outcome of the operation (e.g. "deleted", "not_found").
	Result string `json:"result,omitempty"`
	// Shards provides shard-level acknowledgment of the delete.
	Shards *types.ShardsInfo `json:"_shards,omitempty"`
	// SeqNo is the sequence number assigned to this delete operation.
	SeqNo int64 `json:"_seq_no,omitempty"`
	// PrimaryTerm is the primary term of the shard that processed this operation.
	PrimaryTerm int64 `json:"_primary_term,omitempty"`
	// Status is the HTTP status code returned by the server.
	Status int `json:"status,omitempty"`
	// ForcedRefresh indicates whether the index was force-refreshed as part of this operation.
	ForcedRefresh bool `json:"forced_refresh,omitempty"`
}

// UpdateResponse represents the result of an update document operation.
// When the update request includes "detect_noop": false or requests the updated source,
// the GetResult field is populated.
type UpdateResponse struct {
	// Index is the name of the index the document was updated in.
	Index string `json:"_index,omitempty"`
	// Type is the document type (deprecated).
	Type string `json:"_type,omitempty"`
	// Id is the updated document's ID.
	Id string `json:"_id,omitempty"`
	// Version is the document version after the update.
	Version int64 `json:"_version,omitempty"`
	// Result indicates the outcome of the operation (e.g. "updated", "noop").
	Result string `json:"result,omitempty"`
	// Shards provides shard-level acknowledgment of the update.
	Shards *types.ShardsInfo `json:"_shards,omitempty"`
	// SeqNo is the sequence number assigned to this update operation.
	SeqNo int64 `json:"_seq_no,omitempty"`
	// PrimaryTerm is the primary term of the shard that processed this operation.
	PrimaryTerm int64 `json:"_primary_term,omitempty"`
	// Status is the HTTP status code returned by the server.
	Status int `json:"status,omitempty"`
	// ForcedRefresh indicates whether the index was force-refreshed as part of this operation.
	ForcedRefresh bool `json:"forced_refresh,omitempty"`
	// GetResult contains the updated document source when requested.
	GetResult *GetResult `json:"get,omitempty"`
}

// BulkResponse represents the result of a bulk API request.
// Each item in Items is keyed by the operation type ("index", "create", "update", "delete").
type BulkResponse struct {
	// Took is the total time in milliseconds for the bulk operation.
	Took int `json:"took,omitempty"`
	// Errors indicates whether any individual operation within the bulk request failed.
	Errors bool `json:"errors,omitempty"`
	// Items contains the per-operation results, each keyed by operation type.
	Items []map[string]*BulkResponseItem `json:"items,omitempty"`
}

// BulkResponseItem represents the result of a single operation within a bulk request.
type BulkResponseItem struct {
	// Index is the name of the index the operation targeted.
	Index string `json:"_index,omitempty"`
	// Type is the document type (deprecated).
	Type string `json:"_type,omitempty"`
	// Id is the document ID affected by this operation.
	Id string `json:"_id,omitempty"`
	// Version is the document version after the operation.
	Version int64 `json:"_version,omitempty"`
	// Result indicates the outcome (e.g. "created", "updated", "deleted", "not_found").
	Result string `json:"result,omitempty"`
	// Shards provides shard-level acknowledgment.
	Shards *types.ShardsInfo `json:"_shards,omitempty"`
	// SeqNo is the sequence number assigned to this operation.
	SeqNo int64 `json:"_seq_no,omitempty"`
	// PrimaryTerm is the primary term of the shard that processed this operation.
	PrimaryTerm int64 `json:"_primary_term,omitempty"`
	// Status is the HTTP status code for this individual operation.
	Status int `json:"status,omitempty"`
	// ForcedRefresh indicates whether a refresh was forced for this operation.
	ForcedRefresh bool `json:"forced_refresh,omitempty"`
	// Error contains error details if this particular operation failed.
	Error *types.OpenSearchErrorDetails `json:"error,omitempty"`
	// GetResult contains the updated source when requested for an update operation.
	GetResult *GetResult `json:"get,omitempty"`
}

// MgetResponse represents the result of a multi-get (mget) request.
// It contains one GetResult per requested document, in the same order as the input items.
type MgetResponse struct {
	// Docs contains the result for each requested document.
	Docs []*GetResult `json:"docs,omitempty"`
}

// MultiGetItem identifies a single document to retrieve in a MultiGet request.
type MultiGetItem struct {
	// Index is the name of the index containing the document.
	Index string `json:"_index"`
	// Id is the document ID to retrieve.
	Id string `json:"_id"`
}

// BulkIndexByScrollResponse represents the result of delete-by-query, update-by-query,
// and reindex operations. It includes counts of affected documents and any failures encountered.
type BulkIndexByScrollResponse struct {
	// Header contains the HTTP response headers from OpenSearch.
	Header http.Header `json:"-"`
	// Took is the total time in milliseconds the operation took.
	Took int64 `json:"took"`
	// SliceId identifies the slice this response belongs to when using sliced scroll.
	SliceId *int64 `json:"slice_id,omitempty"`
	// TimedOut indicates whether the operation timed out.
	TimedOut bool `json:"timed_out"`
	// Total is the number of documents processed by the operation.
	Total int64 `json:"total"`
	// Updated is the number of documents successfully updated (update-by-query).
	Updated int64 `json:"updated,omitempty"`
	// Created is the number of documents created during reindex.
	Created int64 `json:"created,omitempty"`
	// Deleted is the number of documents successfully deleted (delete-by-query).
	Deleted int64 `json:"deleted"`
	// Batches is the number of scroll responses pulled back during the operation.
	Batches int64 `json:"batches"`
	// VersionConflicts is the number of version conflicts encountered.
	VersionConflicts int64 `json:"version_conflicts"`
	// Noops is the number of documents skipped because no changes were needed.
	Noops int64 `json:"noops"`
	// Retries tracks the number of retry attempts made for bulk and search sub-operations.
	Retries struct {
		Bulk   int64 `json:"bulk"`
		Search int64 `json:"search"`
	} `json:"retries,omitempty"`
	// Throttled is the duration string the request was throttled (e.g. "0s").
	Throttled string `json:"throttled"`
	// ThrottledMillis is the time in milliseconds the request was throttled.
	ThrottledMillis int64 `json:"throttled_millis"`
	// RequestsPerSecond is the effective rate limiting value in requests per second.
	RequestsPerSecond float64 `json:"requests_per_second"`
	// Canceled is a non-empty string describing why the operation was canceled, if applicable.
	Canceled string `json:"canceled,omitempty"`
	// ThrottledUntil is a timestamp string indicating when throttling ends.
	ThrottledUntil string `json:"throttled_until"`
	// ThrottledUntilMillis is the remaining throttle time in milliseconds.
	ThrottledUntilMillis int64 `json:"throttled_until_millis"`
	// Failures contains details of individual document-level failures.
	Failures []bulkIndexByScrollResponseFailure `json:"failures"`
}

// bulkIndexByScrollResponseFailure describes a single failure within a bulk-by-scroll operation.
type bulkIndexByScrollResponseFailure struct {
	Index  string `json:"index,omitempty"`
	Type   string `json:"type,omitempty"`
	Id     string `json:"id,omitempty"`
	Status int    `json:"status,omitempty"`
	Shard  int    `json:"shard,omitempty"`
	Node   int    `json:"node,omitempty"`
}

// TokenInfo describes a single token occurrence within a term vector response,
// including its position and character offsets in the original text.
type TokenInfo struct {
	// StartOffset is the character offset where the token starts.
	StartOffset int64 `json:"start_offset"`
	// EndOffset is the character offset where the token ends (exclusive).
	EndOffset int64 `json:"end_offset"`
	// Position is the ordinal position of the token in the field.
	Position int64 `json:"position"`
	// Payload is an optional payload associated with the token.
	Payload string `json:"payload"`
}

// TermsInfo provides per-term statistics within a field's term vector,
// including document frequency, term frequency, total term frequency, and token positions.
type TermsInfo struct {
	// DocFreq is the number of documents containing this term.
	DocFreq int64 `json:"doc_freq"`
	// Score is the relevance score for this term, if computed.
	Score float64 `json:"score"`
	// TermFreq is the number of times this term appears in the document field.
	TermFreq int64 `json:"term_freq"`
	// Ttf is the total term frequency across all documents in the index.
	Ttf int64 `json:"ttf"`
	// Tokens lists each token occurrence with position and offset information.
	Tokens []TokenInfo `json:"tokens"`
}

// FieldStatistics contains aggregate statistics for all terms in a field.
type FieldStatistics struct {
	// DocCount is the number of documents that have at least one term in this field.
	DocCount int64 `json:"doc_count"`
	// SumDocFreq is the sum of document frequencies for all terms in this field.
	SumDocFreq int64 `json:"sum_doc_freq"`
	// SumTtf is the sum of total term frequencies for all terms in this field.
	SumTtf int64 `json:"sum_ttf"`
}

// TermVectorsFieldInfo holds the term vector data for a single field,
// including field-level statistics and per-term information.
type TermVectorsFieldInfo struct {
	// FieldStatistics contains aggregate statistics across all terms in this field.
	FieldStatistics FieldStatistics `json:"field_statistics"`
	// Terms maps each term string to its statistics within this field.
	Terms map[string]TermsInfo `json:"terms"`
}

// TermvectorsResponse represents the result of a term vectors request for a single document.
type TermvectorsResponse struct {
	// Index is the index containing the document.
	Index string `json:"_index"`
	// Type is the document type (deprecated).
	Type string `json:"_type"`
	// Id is the document ID.
	Id string `json:"_id,omitempty"`
	// Version is the version of the document.
	Version int `json:"_version"`
	// Found indicates whether the document was found.
	Found bool `json:"found"`
	// Took is the time in milliseconds the request took.
	Took int64 `json:"took"`
	// TermVectors maps field names to their respective term vector data.
	TermVectors map[string]TermVectorsFieldInfo `json:"term_vectors"`
}

// MultiTermvectorResponse represents the result of a multi-term-vectors request.
// It contains one TermvectorsResponse per requested document.
type MultiTermvectorResponse struct {
	// Docs contains the term vector result for each requested document.
	Docs []*TermvectorsResponse `json:"docs"`
}

// ExplainResponse represents the result of an explain API request.
// It describes whether a document matched a query and provides the scoring explanation.
type ExplainResponse struct {
	// Index is the index containing the explained document.
	Index string `json:"_index"`
	// Type is the document type (deprecated).
	Type string `json:"_type"`
	// Id is the explained document's ID.
	Id string `json:"_id"`
	// Matched indicates whether the document matched the provided query.
	Matched bool `json:"matched"`
	// Explanation is a nested structure describing the scoring breakdown for the match.
	Explanation map[string]any `json:"explanation"`
}
