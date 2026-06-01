package api

import "github.com/disaster37/opensearch/v4/types"

// CcrAutoFollowStatusResponse represents the aggregate response from the CCR auto-follow status API,
// containing counts of successful/failed replication starts and per-rule statistics.
type CcrAutoFollowStatusResponse struct {
	NumSuccessStartReplications int64                 `json:"num_success_start_replications"`
	NumFailedStartReplications  int64                 `json:"num_failed_start_replications"`
	NumFailedLeaderCalls        int64                 `json:"num_failed_leader_calls"`
	FailedIndices               []string              `json:"failed_indices"`
	AutofollowStats             []CcrAutoFollowStatus `json:"autofollow_stats"`
}

// CcrAutoFollowStatus represents the status of a single auto-follow rule,
// including success/failure counts, failed indices, and last execution time.
type CcrAutoFollowStatus struct {
	Name                        string              `json:"name"`
	Pattern                     string              `json:"pattern"`
	NumSuccessStartReplications int64               `json:"num_success_start_replications"`
	NumFailedStartReplications  int64               `json:"num_failed_start_replications"`
	NumFailedLeaderCalls        int64               `json:"num_failed_leader_calls"`
	FailedIndices               []string            `json:"failed_indices"`
	LastExecutionTime           types.UnixMilliTime `json:"last_execution_time"`
}

// CcrAutoFollowRule represents an auto-follow rule for cross-cluster replication,
// defining the leader cluster alias, rule name, index pattern, and role mappings.
type CcrAutoFollowRule struct {
	LeaderAlias string          `json:"leader_alias"`
	Name        string          `json:"name"`
	Pattern     string          `json:"pattern"`
	UseRoles    CcrRuleUseRoles `json:"use_roles"`
}

// CcrPostAutoFollowResponse represents the response from the CCR auto-follow creation API,
// indicating whether the auto-follow rule was acknowledged.
type CcrPostAutoFollowResponse struct {
	Acknowledged bool `json:"acknowledged"`
}

// CcrDeleteAutoFollowRequest represents the request body for deleting an auto-follow rule.
type CcrDeleteAutoFollowRequest struct {
	LeaderAlias string `json:"leader_alias"`
	Name        string `json:"name"`
}

// CcrDeleteAutoFollowResponse represents the response from the CCR auto-follow deletion API,
// indicating whether the deletion was acknowledged.
type CcrDeleteAutoFollowResponse struct {
	Acknowledged bool `json:"acknowledged"`
}

// CcrRuleUseRoles defines the role mappings used for cross-cluster replication
// between the leader and follower clusters.
type CcrRuleUseRoles struct {
	LeaderClusterRole   string `json:"leader_cluster_role"`
	FollowerClusterRole string `json:"follower_cluster_role"`
}

// CcrRule represents a single cross-cluster replication rule,
// specifying the leader cluster alias, leader index, and role mappings.
type CcrRule struct {
	LeaderAlias string          `json:"leader_alias"`
	LeaderIndex string          `json:"leader_index"`
	UseRoles    CcrRuleUseRoles `json:"use_roles"`
}

// CcrStartRuleResponse represents the response from the CCR start replication rule API,
// indicating whether the start request was acknowledged.
type CcrStartRuleResponse struct {
	Acknowledged bool `json:"acknowledged"`
}

// CcrStopRuleResponse represents the response from the CCR stop replication rule API,
// indicating whether the stop request was acknowledged.
type CcrStopRuleResponse struct {
	Acknowledged bool `json:"acknowledged"`
}

// CcrPauseRuleResponse represents the response from the CCR pause replication rule API,
// indicating whether the pause request was acknowledged.
type CcrPauseRuleResponse struct {
	Acknowledged bool `json:"acknowledged"`
}

// CcrResumeRuleResponse represents the response from the CCR resume replication rule API,
// indicating whether the resume request was acknowledged.
type CcrResumeRuleResponse struct {
	Acknowledged bool `json:"acknowledged"`
}

// CcrStatusRuleResponse represents the response from the CCR replication rule status API,
// containing the current replication status, leader/follower index info, and syncing details.
type CcrStatusRuleResponse struct {
	Status         string                `json:"status"`
	Reason         string                `json:"reason"`
	LeaderAlias    string                `json:"leader_alias"`
	LeaderIndex    string                `json:"leader_index"`
	FollowerIndex  string                `json:"follower_index"`
	SyncingDetails CcrRuleSyncingDetails `json:"syncing_details"`
}

// CcrRuleSyncingDetails contains the sequence number checkpoint information
// for a cross-cluster replication rule, tracking sync progress between leader and follower.
type CcrRuleSyncingDetails struct {
	LeaderCheckpoint   int64 `json:"leader_checkpoint"`
	FollowerCheckpoint int64 `json:"follower_checkpoint"`
	SeqNumber          int64 `json:"seq_no"`
}

// CcrFollowerStatsResponse represents the aggregate follower statistics from the CCR plugin,
// including counts of syncing, bootstrapping, paused, and failed indices, as well as per-index stats.
type CcrFollowerStatsResponse struct {
	CcrStatusFollowerState
	NumSyncingIndices       int64                             `json:"num_syncing_indices"`
	NumBootstrappingIndices int64                             `json:"num_bootstrapping_indices"`
	NumPausedIndices        int64                             `json:"num_paused_indices"`
	NumFailedIndices        int64                             `json:"num_failed_indices"`
	NumShardTasks           int64                             `json:"num_shard_tasks"`
	NumIndexTasks           int64                             `json:"num_index_tasks"`
	IndexStats              map[string]CcrStatusFollowerState `json:"index_stats"`
}

// CcrStatusFollowerState contains the replication statistics for a follower index,
// including operations read/written, failed/throttled requests, and checkpoint positions.
type CcrStatusFollowerState struct {
	OperationsWritten      int64 `json:"operations_written"`
	OperationsRead         int64 `json:"operations_read"`
	FailedReadRequests     int64 `json:"failed_read_requests"`
	ThrottledReadRequests  int64 `json:"throttled_read_requests"`
	FailedWriteRequests    int64 `json:"failed_write_requests"`
	ThrottledWriteRequests int64 `json:"throttled_write_requests"`
	FollowerCheckpoint     int64 `json:"follower_checkpoint"`
	LeaderCheckpoint       int64 `json:"leader_checkpoint"`
	TotalWriteTimeMillis   int64 `json:"total_write_time_millis"`
}

// CcrLeaderStatsResponse represents the aggregate leader statistics from the CCR plugin,
// including the count of replicated indices and per-index leader stats.
type CcrLeaderStatsResponse struct {
	CcrStatusLeaderState
	NumReplicatedIndices int64                           `json:"num_replicated_indices"`
	IndexStats           map[string]CcrStatusLeaderState `json:"index_stats"`
}

// CcrStatusLeaderState contains the replication statistics for a leader index,
// including operations read, translog/lucene read sizes, and read time metrics.
type CcrStatusLeaderState struct {
	OperationsRead              int64 `json:"operations_read"`
	TranslogSizeBytes           int64 `json:"translog_size_bytes"`
	OperationsReadLucene        int64 `json:"operations_read_lucene"`
	OperationsReadTranslog      int64 `json:"operations_read_translog"`
	TotalReadTimeLuceneMillis   int64 `json:"total_read_time_lucene_millis"`
	TotalReadTimeTranslogMillis int64 `json:"total_read_time_translog_millis"`
	BytesRead                   int64 `json:"bytes_read"`
}

// CcrUpdateRuleResponse represents the response from updating a CCR replication rule.
type CcrUpdateRuleResponse struct {
	Acknowledged bool   `json:"acknowledged,omitempty"`
	Status       string `json:"status,omitempty"`
}
