package api

// TieringStatusResponse is the response from GET /{index}/_tier
// (OpenSearch 3.7.0+, gated by the writable_warm_index feature flag).
type TieringStatusResponse struct {
	TieringStatus *TieringStatus `json:"tiering_status"`
}

// TieringStatus describes the tiering state of a single index.
type TieringStatus struct {
	Index            string                   `json:"index"`
	State            string                   `json:"state"`
	Source           string                   `json:"source"`
	Target           string                   `json:"target"`
	StartTime        int64                    `json:"start_time"`
	ShardLevelStatus *TieringShardLevelStatus `json:"shard_level_status"`
}

// TieringShardLevelStatus summarizes the per-shard progress of an ongoing
// tiering operation. Only present when detailed=true and relocations are
// ongoing.
type TieringShardLevelStatus struct {
	Pending               int                   `json:"pending"`
	Running               int                   `json:"running"`
	Succeeded             int                   `json:"succeeded"`
	Total                 int                   `json:"total"`
	ShardRelocationStatus []TieringOngoingShard `json:"shard_relocation_status"`
}

// TieringOngoingShard describes a single ongoing shard relocation.
type TieringOngoingShard struct {
	SourceShardId    int    `json:"source_shard_id"`
	RelocatingNodeId string `json:"relocating_node_id"`
}

// TieringListEntry is one row of GET /_tier/all (format=json).
type TieringListEntry struct {
	Index  string `json:"index"`
	State  string `json:"state"`
	Source string `json:"source"`
	Target string `json:"target"`
}
