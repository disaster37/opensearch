package api

// TransformJobBase represents the base definition of a Transform job,
// including source/target indices, schedule, data selection query, groups, and aggregations.
type TransformJobBase struct {
	Enabled            *bool          `json:"enabled,omitempty"`
	Continuous         *bool          `json:"continuous,omitempty"`
	Schedule           map[string]any `json:"schedule,omitempty"`
	Description        *string        `json:"description,omitempty"`
	MetadataId         *string        `json:"metadata_id,omitempty"`
	SourceIndex        string         `json:"source_index,omitempty"`
	TargetIndex        string         `json:"target_index,omitempty"`
	DataSelectionQuery any            `json:"data_selection_query,omitempty"`
	PageSize           int64          `json:"page_size,omitempty"`
	Groups             []any          `json:"groups,omitempty"`
	SourceField        string         `json:"source_field,omitempty"`
	Aggregations       any            `json:"aggregations,omitempty"`
}

// TransformPutJob wraps a Transform job for the PUT API request body.
type TransformPutJob struct {
	Transform TransformJobBase `json:"transform"`
}

// TransformGetJobResponse represents the full response from the Transform GET job API,
// including the document ID, version, sequence number, primary term, and the job definition.
type TransformGetJobResponse struct {
	Id             string           `json:"_id"`
	Version        int64            `json:"_version"`
	SequenceNumber int64            `json:"_seq_no"`
	PrimaryTerm    int64            `json:"_primary_term"`
	Transform      TransformJobBase `json:"transform"`
}

// TransformDeleteJobResponse represents the response from the Transform delete job API,
// indicating whether the deletion was successful and any per-item results.
type TransformDeleteJobResponse struct {
	Took   int64            `json:"took,omitempty"`
	Errors bool             `json:"errors,omitempty"`
	Items  []map[string]any `json:"items,omitempty"`
}

// TransformSearchJobResponse represents the response from the Transform search API,
// containing the total count and list of matching transform jobs.
type TransformSearchJobResponse struct {
	TotalTransforms int64                     `json:"total_transforms"`
	Transforms      []TransformGetJobResponse `json:"transforms"`
}

// TransformExplainJob represents the execution status and statistics for a Transform job,
// including metadata, status, failure reason, and processing stats.
type TransformExplainJob struct {
	MetadataId        string                  `json:"metadata_id"`
	TransformMetadata map[string]any          `json:"transform_metadata"`
	TransformId       string                  `json:"transform_id"`
	LastUpdatedAt     int64                   `json:"last_updated_at"`
	Status            string                  `json:"status"`
	FailureReason     string                  `json:"failure_reason"`
	Stats             TransformExplainJobStat `json:"stats"`
}

// TransformExplainJobStat contains processing statistics for a Transform job,
// including pages processed, documents processed/indexed, and time spent indexing/searching.
type TransformExplainJobStat struct {
	PagesProcessed     int64 `json:"pages_processed"`
	DocumentsProcessed int64 `json:"documents_processed"`
	DocumentsIndexed   int64 `json:"documents_indexed"`
	IndexTimeInMillis  int64 `json:"index_time_in_millis"`
	SearchTimeInMillis int64 `json:"search_time_in_millis"`
}

// TransformPreviewJobResponse represents the response from the Transform preview API,
// containing sample transformed documents without persisting to the target index.
type TransformPreviewJobResponse struct {
	Documents []map[string]any `json:"documents"`
}

// TransformStartJobResponse represents the response from the Transform start job API,
// indicating whether the start request was acknowledged.
type TransformStartJobResponse struct {
	Acknowledged bool `json:"acknowledged,omitempty"`
}

// TransformStopJobResponse represents the response from the Transform stop job API,
// indicating whether the stop request was acknowledged.
type TransformStopJobResponse struct {
	Acknowledged bool `json:"acknowledged,omitempty"`
}
