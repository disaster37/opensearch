package api

import (
	"time"

	"github.com/disaster37/opensearch/v4/types"
)

// SnapshotShardFailure describes a single shard that failed during a snapshot
// operation, including the index, shard ID, failure reason, and node involved.
type SnapshotShardFailure struct {
	Index     string `json:"index"`
	IndexUUID string `json:"index_uuid"`
	ShardID   int    `json:"shard_id"`
	Reason    string `json:"reason"`
	NodeID    string `json:"node_id"`
	Status    string `json:"status"`
}

// Snapshot represents a single OpenSearch snapshot with its metadata,
// timing, included indices, shard statistics, and any shard-level failures.
// Key fields include Snapshot (name), State, StartTime, EndTime, and Shards.
type Snapshot struct {
	Snapshot          string                 `json:"snapshot"`
	UUID              string                 `json:"uuid"`
	VersionID         int                    `json:"version_id"`
	Version           string                 `json:"version"`
	Indices           []string               `json:"indices"`
	State             string                 `json:"state"`
	Reason            string                 `json:"reason"`
	StartTime         time.Time              `json:"start_time"`
	StartTimeInMillis int64                  `json:"start_time_in_millis"`
	EndTime           time.Time              `json:"end_time"`
	EndTimeInMillis   int64                  `json:"end_time_in_millis"`
	DurationInMillis  int64                  `json:"duration_in_millis"`
	Failures          []SnapshotShardFailure `json:"failures"`
	Shards            *types.ShardsInfo      `json:"shards"`
}

// SnapshotCreateResponse represents the result of creating a snapshot.
// When the snapshot is taken synchronously, Snapshot contains the full
// snapshot details; for async operations, Accepted is set to true.
type SnapshotCreateResponse struct {
	Accepted *bool     `json:"accepted"`
	Snapshot *Snapshot `json:"snapshot"`
}

// SnapshotGetResponse represents the result of retrieving snapshot information.
// It contains a list of Snapshot objects matching the request.
type SnapshotGetResponse struct {
	Snapshots []*Snapshot `json:"snapshots"`
}

// SnapshotDeleteResponse represents the result of deleting a snapshot.
// The Acknowledged field indicates whether the delete was accepted.
type SnapshotDeleteResponse struct {
	Acknowledged bool `json:"acknowledged"`
}

// SnapshotStatusResponse represents the result of a snapshot status request.
// It contains a list of SnapshotStatus objects for in-progress snapshots.
type SnapshotStatusResponse struct {
	Snapshots []SnapshotStatus `json:"snapshots"`
}

// SnapshotStatus represents the detailed status of a single in-progress
// snapshot, including its state, shard statistics, file/size progress,
// per-index status, and any failures encountered.
type SnapshotStatus struct {
	Snapshot           string                         `json:"snapshot"`
	Repository         string                         `json:"repository"`
	UUID               string                         `json:"uuid"`
	State              string                         `json:"state"`
	IncludeGlobalState bool                           `json:"include_global_state"`
	ShardsStats        SnapshotShardsStats            `json:"shards_stats"`
	Stats              SnapshotStats                  `json:"stats"`
	Indices            map[string]SnapshotIndexStatus `json:"indices"`
	Failures           []SnapshotFailure              `json:"failures,omitempty"`
	StartTime          time.Time                      `json:"start_time,omitempty"`
	EndTime            time.Time                      `json:"end_time,omitempty"`
}

// SnapshotFailure describes a single failure that occurred during a
// snapshot operation, including the affected node, index, and reason.
type SnapshotFailure struct {
	NodeID string `json:"node_id,omitempty"`
	Indice string `json:"index,omitempty"`
	Reason string `json:"reason,omitempty"`
	Status string `json:"status,omitempty"`
}

// SnapshotShardsStats tracks the number of shards in each snapshot phase:
// initializing, started, finalizing, done, and failed.
type SnapshotShardsStats struct {
	Initializing int `json:"initializing"`
	Started      int `json:"started"`
	Finalizing   int `json:"finalizing"`
	Done         int `json:"done"`
	Failed       int `json:"failed"`
	Total        int `json:"total"`
}

// SnapshotStats tracks file and size statistics for a snapshot operation,
// including incremental, processed, and total counts along with timing.
type SnapshotStats struct {
	Incremental struct {
		FileCount   int    `json:"file_count"`
		Size        string `json:"size"`
		SizeInBytes int64  `json:"size_in_bytes"`
	} `json:"incremental"`

	Processed struct {
		FileCount   int    `json:"file_count"`
		Size        string `json:"size"`
		SizeInBytes int64  `json:"size_in_bytes"`
	} `json:"processed"`

	Total struct {
		FileCount   int    `json:"file_count"`
		Size        string `json:"size"`
		SizeInBytes int64  `json:"size_in_bytes"`
	} `json:"total"`

	StartTime         string `json:"start_time"`
	StartTimeInMillis int64  `json:"start_time_in_millis"`

	Time         string `json:"time"`
	TimeInMillis int64  `json:"time_in_millis"`

	NumberOfFiles  int `json:"number_of_files"`
	ProcessedFiles int `json:"processed_files"`

	TotalSize        string `json:"total_size"`
	TotalSizeInBytes int64  `json:"total_size_in_bytes"`
}

// SnapshotIndexStatus represents the snapshot status for a single index,
// including aggregate shard statistics, file progress, and per-shard details.
type SnapshotIndexStatus struct {
	ShardsStats SnapshotShardsStats                 `json:"shards_stats"`
	Stats       SnapshotStats                       `json:"stats"`
	Shards      map[string]SnapshotIndexShardStatus `json:"shards"`
}

// SnapshotIndexShardStatus represents the snapshot status for a single
// shard within an index, including its current stage, transfer stats,
// assigned node, and failure reason if applicable.
type SnapshotIndexShardStatus struct {
	Stage  string        `json:"stage"`
	Stats  SnapshotStats `json:"stats"`
	Node   string        `json:"node"`
	Reason string        `json:"reason"`
}

// SnapshotRestoreResponse represents the result of a snapshot restore operation.
// When the restore is synchronous, Snapshot contains the restore info;
// for async operations, Accepted is set to true.
type SnapshotRestoreResponse struct {
	Accepted *bool        `json:"accepted"`
	Snapshot *RestoreInfo `json:"snapshot"`
}

// RestoreInfo describes a completed restore operation, including the snapshot
// name, restored indices, and shard-level restoration results.
type RestoreInfo struct {
	Snapshot string           `json:"snapshot"`
	Indices  []string         `json:"indices"`
	Shards   types.ShardsInfo `json:"shards"`
}

// SnapshotCreateRepositoryResponse represents the acknowledgment response
// from creating or updating a snapshot repository.
type SnapshotCreateRepositoryResponse struct {
	Acknowledged       bool   `json:"acknowledged"`
	ShardsAcknowledged bool   `json:"shards_acknowledged"`
	Index              string `json:"index,omitempty"`
}

// SnapshotGetRepositoryResponse represents a registered snapshot repository,
// containing its storage backend type and configuration settings.
type SnapshotGetRepositoryResponse struct {
	Type     string         `json:"type"`
	Settings map[string]any `json:"settings,omitempty"`
}

// SnapshotRepositoryMetaData holds the type and settings for a snapshot
// repository definition, identical in structure to SnapshotGetRepositoryResponse.
type SnapshotRepositoryMetaData struct {
	Type     string         `json:"type"`
	Settings map[string]any `json:"settings,omitempty"`
}

// SnapshotDeleteRepositoryResponse represents the acknowledgment response
// from unregistering a snapshot repository.
type SnapshotDeleteRepositoryResponse struct {
	Acknowledged       bool   `json:"acknowledged"`
	ShardsAcknowledged bool   `json:"shards_acknowledged"`
	Index              string `json:"index,omitempty"`
}

// SnapshotVerifyRepositoryResponse represents the result of verifying a
// snapshot repository. It maps node IDs to the node names that successfully
// verified access to the repository.
type SnapshotVerifyRepositoryResponse struct {
	Nodes map[string]*SnapshotVerifyRepositoryNode `json:"nodes"`
}

// SnapshotVerifyRepositoryNode represents a single node that participated
// in a repository verification, identified by its name.
type SnapshotVerifyRepositoryNode struct {
	Name string `json:"name"`
}

// SnapshotCleanupRepositoryResponse represents the result of cleaning up a snapshot repository.
type SnapshotCleanupRepositoryResponse struct {
	Results *SnapshotCleanupResults `json:"results,omitempty"`
}

// SnapshotCleanupResults contains counts of deleted bytes and blobs.
type SnapshotCleanupResults struct {
	DeletedBytes int64 `json:"deleted_bytes"`
	DeletedBlobs int64 `json:"deleted_blobs"`
}
