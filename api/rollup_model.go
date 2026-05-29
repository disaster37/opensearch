package api

// RollupGetResponse represents a single rollup job as returned by the Rollup GET API.
type RollupGetResponse struct {
	Id             string        `json:"_id,omitempty"`
	Version        int64         `json:"_version,omitempty"`
	SequenceNumber int64         `json:"_seq_no,omitempty"`
	PrimaryTerm    int64         `json:"_primary_term,omitempty"`
	Rollup         RollupJobBase `json:"rollup"`
}

// RollupJobBase represents the definition of a rollup job.
type RollupJobBase struct {
	RollupId    string           `json:"rollup_id,omitempty"`
	Enabled     *bool            `json:"enabled,omitempty"`
	Schedule    map[string]any   `json:"schedule,omitempty"`
	Description *string          `json:"description,omitempty"`
	SourceIndex string           `json:"source_index,omitempty"`
	TargetIndex string           `json:"target_index,omitempty"`
	Dimensions  []map[string]any `json:"dimensions,omitempty"`
	Metrics     []map[string]any `json:"metrics,omitempty"`
}

// RollupExplainResponse represents explain information for a rollup job.
type RollupExplainResponse struct {
	Metadata       map[string]any `json:"metadata,omitempty"`
	RollupMetadata map[string]any `json:"rollup_metadata,omitempty"`
}
