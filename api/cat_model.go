package api

// CatAliasesResponse is the response type returned by the cat aliases API.
// Each row represents a single alias-to-index mapping with optional filter
// and routing configuration.
type CatAliasesResponse []CatAliasesResponseRow

// CatAliasesResponseRow represents a single row in the cat aliases output,
// containing the alias name, target index, filter expression, routing settings,
// and whether this alias points to the write index.
type CatAliasesResponseRow struct {
	Alias         string `json:"alias"`
	Index         string `json:"index"`
	Filter        string `json:"filter"`
	RoutingIndex  string `json:"routing.index"`
	RoutingSearch string `json:"routing.search"`
	IsWriteIndex  string `json:"is_write_index"`
}

// CatAllocationResponse is the response type returned by the cat allocation API.
// Each row represents a single node's shard allocation and disk usage information.
type CatAllocationResponse []CatAllocationResponseRow

// CatAllocationResponseRow represents a single row in the cat allocation output,
// containing shard count, disk usage (indices, used, available, total), percentage,
// and node identification details.
type CatAllocationResponseRow struct {
	Shards      int    `json:"shards,string"`
	DiskIndices string `json:"disk.indices"`
	DiskUsed    string `json:"disk.used"`
	DiskAvail   string `json:"disk.avail"`
	DiskTotal   string `json:"disk.total"`
	DiskPercent int    `json:"disk.percent,string"`
	Host        string `json:"host"`
	IP          string `json:"ip"`
	Node        string `json:"node"`
}

// CatCountResponse is the response type returned by the cat count API.
// Each row contains a document count snapshot for the cluster or specific indices.
type CatCountResponse []CatCountResponseRow

// CatCountResponseRow represents a single row in the cat count output,
// containing the epoch timestamp and total document count.
type CatCountResponseRow struct {
	Epoch     int64  `json:"epoch,string"`
	Timestamp string `json:"timestamp"`
	Count     int    `json:"count,string"`
}

// CatFielddataResponse is the response type returned by the cat fielddata API.
// Each row represents fielddata memory usage for a specific field on a specific node.
type CatFielddataResponse []CatFielddataResponseRow

// CatFielddataResponseRow represents a single row in the cat fielddata output,
// containing node identification, field name, and memory size used.
type CatFielddataResponseRow struct {
	Id    string `json:"id"`
	Host  string `json:"host"`
	IP    string `json:"ip"`
	Node  string `json:"node"`
	Field string `json:"field"`
	Size  string `json:"size"`
}

// CatHealthResponse is the response type returned by the cat health API.
// It contains a compact cluster health overview.
type CatHealthResponse []CatHealthResponseRow

// CatHealthResponseRow represents a single row in the cat health output,
// containing cluster name, status, node/shard counts, pending tasks,
// and the active shard percentage.
type CatHealthResponseRow struct {
	Epoch                    int64  `json:"epoch,string"`
	Timestamp                string `json:"timestamp"`
	Cluster                  string `json:"cluster"`
	Status                   string `json:"status"`
	NodeTotal                int    `json:"node.total,string"`
	NodeData                 int    `json:"node.data,string"`
	Shards                   int    `json:"shards,string"`
	Pri                      int    `json:"pri,string"`
	Relo                     int    `json:"relo,string"`
	Init                     int    `json:"init,string"`
	Unassign                 int    `json:"unassign,string"`
	PendingTasks             int    `json:"pending_tasks,string"`
	MaxTaskWaitTime          string `json:"max_task_wait_time"`
	ActiveShardsPercent      string `json:"active_shards_percent"`
	DiscoveredClusterManager string `json:"discovered_cluster_manager"`
}

// CatIndicesResponse is the response type returned by the cat indices API.
// Each row contains detailed information about a single index.
type CatIndicesResponse []CatIndicesResponseRow

// CatIndicesResponseRow represents a single row in the cat indices output,
// containing health, status, shard/replica counts, document counts, store sizes,
// fielddata, cache, search, indexing, merge, refresh, flush, and segment metrics.
type CatIndicesResponseRow struct {
	Health                            string `json:"health"`
	Status                            string `json:"status"`
	Index                             string `json:"index"`
	UUID                              string `json:"uuid"`
	Pri                               int    `json:"pri,string"`
	Rep                               int    `json:"rep,string"`
	DocsCount                         int    `json:"docs.count,string"`
	DocsDeleted                       int    `json:"docs.deleted,string"`
	CreationDate                      int64  `json:"creation.date,string"`
	CreationDateString                string `json:"creation.date.string"`
	StoreSize                         string `json:"store.size"`
	PriStoreSize                      string `json:"pri.store.size"`
	CompletionSize                    string `json:"completion.size"`
	PriCompletionSize                 string `json:"pri.completion.size"`
	FielddataMemorySize               string `json:"fielddata.memory_size"`
	PriFielddataMemorySize            string `json:"pri.fielddata.memory_size"`
	FielddataEvictions                int    `json:"fielddata.evictions,string"`
	PriFielddataEvictions             int    `json:"pri.fielddata.evictions,string"`
	QueryCacheMemorySize              string `json:"query_cache.memory_size"`
	PriQueryCacheMemorySize           string `json:"pri.query_cache.memory_size"`
	QueryCacheEvictions               int    `json:"query_cache.evictions,string"`
	PriQueryCacheEvictions            int    `json:"pri.query_cache.evictions,string"`
	RequestCacheMemorySize            string `json:"request_cache.memory_size"`
	PriRequestCacheMemorySize         string `json:"pri.request_cache.memory_size"`
	RequestCacheEvictions             int    `json:"request_cache.evictions,string"`
	PriRequestCacheEvictions          int    `json:"pri.request_cache.evictions,string"`
	RequestCacheHitCount              int    `json:"request_cache.hit_count,string"`
	PriRequestCacheHitCount           int    `json:"pri.request_cache.hit_count,string"`
	RequestCacheMissCount             int    `json:"request_cache.miss_count,string"`
	PriRequestCacheMissCount          int    `json:"pri.request_cache.miss_count,string"`
	FlushTotal                        int    `json:"flush.total,string"`
	PriFlushTotal                     int    `json:"pri.flush.total,string"`
	FlushTotalTime                    string `json:"flush.total_time"`
	PriFlushTotalTime                 string `json:"pri.flush.total_time"`
	GetCurrent                        int    `json:"get.current,string"`
	PriGetCurrent                     int    `json:"pri.get.current,string"`
	GetTime                           string `json:"get.time"`
	PriGetTime                        string `json:"pri.get.time"`
	GetTotal                          int    `json:"get.total,string"`
	PriGetTotal                       int    `json:"pri.get.total,string"`
	GetExistsTime                     string `json:"get.exists_time"`
	PriGetExistsTime                  string `json:"pri.get.exists_time"`
	GetExistsTotal                    int    `json:"get.exists_total,string"`
	PriGetExistsTotal                 int    `json:"pri.get.exists_total,string"`
	GetMissingTime                    string `json:"get.missing_time"`
	PriGetMissingTime                 string `json:"pri.get.missing_time"`
	GetMissingTotal                   int    `json:"get.missing_total,string"`
	PriGetMissingTotal                int    `json:"pri.get.missing_total,string"`
	IndexingDeleteCurrent             int    `json:"indexing.delete_current,string"`
	PriIndexingDeleteCurrent          int    `json:"pri.indexing.delete_current,string"`
	IndexingDeleteTime                string `json:"indexing.delete_time"`
	PriIndexingDeleteTime             string `json:"pri.indexing.delete_time"`
	IndexingDeleteTotal               int    `json:"indexing.delete_total,string"`
	PriIndexingDeleteTotal            int    `json:"pri.indexing.delete_total,string"`
	IndexingIndexCurrent              int    `json:"indexing.index_current,string"`
	PriIndexingIndexCurrent           int    `json:"pri.indexing.index_current,string"`
	IndexingIndexTime                 string `json:"indexing.index_time"`
	PriIndexingIndexTime              string `json:"pri.indexing.index_time"`
	IndexingIndexTotal                int    `json:"indexing.index_total,string"`
	PriIndexingIndexTotal             int    `json:"pri.indexing.index_total,string"`
	IndexingIndexFailed               int    `json:"indexing.index_failed,string"`
	PriIndexingIndexFailed            int    `json:"pri.indexing.index_failed,string"`
	MergesCurrent                     int    `json:"merges.current,string"`
	PriMergesCurrent                  int    `json:"pri.merges.current,string"`
	MergesCurrentDocs                 int    `json:"merges.current_docs,string"`
	PriMergesCurrentDocs              int    `json:"pri.merges.current_docs,string"`
	MergesCurrentSize                 string `json:"merges.current_size"`
	PriMergesCurrentSize              string `json:"pri.merges.current_size"`
	MergesTotal                       int    `json:"merges.total,string"`
	PriMergesTotal                    int    `json:"pri.merges.total,string"`
	MergesTotalDocs                   int    `json:"merges.total_docs,string"`
	PriMergesTotalDocs                int    `json:"pri.merges.total_docs,string"`
	MergesTotalSize                   string `json:"merges.total_size"`
	PriMergesTotalSize                string `json:"pri.merges.total_size"`
	MergesTotalTime                   string `json:"merges.total_time"`
	PriMergesTotalTime                string `json:"pri.merges.total_time"`
	MergesWarmerTotalInvocations      int    `json:"merges.warmer.total_invocations,string"`
	PriMergesWarmerTotalInvocations   int    `json:"pri.merges.warmer.total_invocations,string"`
	MergesWarmerTotalTime             string `json:"merges.warmer.total_time"`
	PriMergesWarmerTotalTime          string `json:"pri.merges.warmer.total_time"`
	MergesWarmerOngoingCount          int    `json:"merges.warmer.ongoing_count,string"`
	PriMergesWarmerOngoingCount       int    `json:"pri.merges.warmer.ongoing_count,string"`
	MergesWarmerTotalBytesReceived    string `json:"merges.warmer.total_bytes_received"`
	PriMergesWarmerTotalBytesReceived string `json:"pri.merges.warmer.total_bytes_received"`
	MergesWarmerTotalBytesSent        string `json:"merges.warmer.total_bytes_sent"`
	PriMergesWarmerTotalBytesSent     string `json:"pri.merges.warmer.total_bytes_sent"`
	MergesWarmerTotalReceiveTime      string `json:"merges.warmer.total_receive_time"`
	PriMergesWarmerTotalReceiveTime   string `json:"pri.merges.warmer.total_receive_time"`
	MergesWarmerTotalFailureCount     int    `json:"merges.warmer.total_failure_count,string"`
	PriMergesWarmerTotalFailureCount  int    `json:"pri.merges.warmer.total_failure_count,string"`
	MergesWarmerTotalSendTime         string `json:"merges.warmer.total_send_time"`
	PriMergesWarmerTotalSendTime      string `json:"pri.merges.warmer.total_send_time"`
	RefreshTotal                      int    `json:"refresh.total,string"`
	PriRefreshTotal                   int    `json:"pri.refresh.total,string"`
	RefreshExternalTotal              int    `json:"refresh.external_total,string"`
	PriRefreshExternalTotal           int    `json:"pri.refresh.external_total,string"`
	RefreshTime                       string `json:"refresh.time"`
	PriRefreshTime                    string `json:"pri.refresh.time"`
	RefreshExternalTime               string `json:"refresh.external_time"`
	PriRefreshExternalTime            string `json:"pri.refresh.external_time"`
	RefreshListeners                  int    `json:"refresh.listeners,string"`
	PriRefreshListeners               int    `json:"pri.refresh.listeners,string"`
	SearchFetchCurrent                int    `json:"search.fetch_current,string"`
	PriSearchFetchCurrent             int    `json:"pri.search.fetch_current,string"`
	SearchFetchTime                   string `json:"search.fetch_time"`
	PriSearchFetchTime                string `json:"pri.search.fetch_time"`
	SearchFetchTotal                  int    `json:"search.fetch_total,string"`
	PriSearchFetchTotal               int    `json:"pri.search.fetch_total,string"`
	SearchOpenContexts                int    `json:"search.open_contexts,string"`
	PriSearchOpenContexts             int    `json:"pri.search.open_contexts,string"`
	SearchQueryCurrent                int    `json:"search.query_current,string"`
	PriSearchQueryCurrent             int    `json:"pri.search.query_current,string"`
	SearchQueryTime                   string `json:"search.query_time"`
	PriSearchQueryTime                string `json:"pri.search.query_time"`
	SearchQueryTotal                  int    `json:"search.query_total,string"`
	PriSearchQueryTotal               int    `json:"pri.search.query_total,string"`
	SearchQueryFailed                 int    `json:"search.query_failed,string"`
	PriSearchQueryFailed              int    `json:"pri.search.query_failed,string"`
	SearchConcurrentQueryCurrent      string `json:"search.concurrent_query_current"`
	PriSearchConcurrentQueryCurrent   string `json:"pri.search.concurrent_query_current"`
	SearchConcurrentQueryTime         string `json:"search.concurrent_query_time"`
	PriSearchConcurrentQueryTime      string `json:"pri.search.concurrent_query_time"`
	SearchConcurrentQueryTotal        string `json:"search.concurrent_query_total"`
	PriSearchConcurrentQueryTotal     string `json:"pri.search.concurrent_query_total"`
	SearchConcurrentAvgSliceCount     string `json:"search.concurrent_avg_slice_count"`
	PriSearchConcurrentAvgSliceCount  string `json:"pri.search.concurrent_avg_slice_count"`
	SearchStartreeQueryCurrent        int    `json:"search.startree_query_current,string"`
	PriSearchStartreeQueryCurrent     int    `json:"pri.search.startree.query_current,string"`
	SearchStartreeQueryTime           string `json:"search.startree_query_time"`
	PriSearchStartreeQueryTime        string `json:"pri.search.startree.query_time"`
	SearchStartreeQueryFailed         int    `json:"search.startree_query_failed,string"`
	PriSearchStartreeQueryFailed      int    `json:"pri.search.startree_query_failed,string"`
	SearchStartreeQueryTotal          int    `json:"search.startree_query_total,string"`
	PriSearchStartreeQueryTotal       int    `json:"pri.search.startree.query_total,string"`
	SearchScrollCurrent               int    `json:"search.scroll_current,string"`
	PriSearchScrollCurrent            int    `json:"pri.search.scroll_current,string"`
	SearchScrollTime                  string `json:"search.scroll_time"`
	PriSearchScrollTime               string `json:"pri.search.scroll_time"`
	SearchScrollTotal                 int    `json:"search.scroll_total,string"`
	PriSearchScrollTotal              int    `json:"pri.search.scroll_total,string"`
	SearchPointInTimeCurrent          string `json:"search.point_in_time_current"`
	PriSearchPointInTimeCurrent       string `json:"pri.search.point_in_time_current"`
	SearchPointInTimeTime             string `json:"search.point_in_time_time"`
	PriSearchPointInTimeTime          string `json:"pri.search.point_in_time_time"`
	SearchPointInTimeTotal            string `json:"search.point_in_time_total"`
	PriSearchPointInTimeTotal         string `json:"pri.search.point_in_time_total"`
	SearchThrottled                   bool   `json:"search.throttled,string"`
	SegmentsCount                     int    `json:"segments.count,string"`
	PriSegmentsCount                  int    `json:"pri.segments.count,string"`
	SegmentsMemory                    string `json:"segments.memory"`
	PriSegmentsMemory                 string `json:"pri.segments.memory"`
	SegmentsIndexWriterMemory         string `json:"segments.index_writer_memory"`
	PriSegmentsIndexWriterMemory      string `json:"pri.segments.index_writer_memory"`
	SegmentsVersionMapMemory          string `json:"segments.version_map_memory"`
	PriSegmentsVersionMapMemory       string `json:"pri.segments.version_map_memory"`
	SegmentsFixedBitsetMemory         string `json:"segments.fixed_bitset_memory"`
	PriSegmentsFixedBitsetMemory      string `json:"pri.segments.fixed_bitset_memory"`
	WarmerCurrent                     int    `json:"warmer.current,string"`
	PriWarmerCurrent                  int    `json:"pri.warmer.current,string"`
	WarmerTotal                       int    `json:"warmer.total,string"`
	PriWarmerTotal                    int    `json:"pri.warmer.total,string"`
	WarmerTotalTime                   string `json:"warmer.total_time"`
	PriWarmerTotalTime                string `json:"pri.warmer.total_time"`
	SuggestCurrent                    int    `json:"suggest.current,string"`
	PriSuggestCurrent                 int    `json:"pri.suggest.current,string"`
	SuggestTime                       string `json:"suggest.time"`
	PriSuggestTime                    string `json:"pri.suggest.time"`
	SuggestTotal                      int    `json:"suggest.total,string"`
	PriSuggestTotal                   int    `json:"pri.suggest.total,string"`
	MemoryTotal                       string `json:"memory.total"`
	PriMemoryTotal                    string `json:"pri.memory.total"`
	LastIndexRequestTimestamp         int64  `json:"last_index_request_timestamp,string"`
	LastIndexRequestTimestampString   string `json:"last_index_request_timestamp_string"`
}

// CatMasterResponse is the response type returned by the cat master API.
// Each row identifies the currently elected cluster manager node.
type CatMasterResponse []CatMasterResponseRow

// CatMasterResponseRow represents a single row in the cat master output,
// containing the master node's ID, host, IP address, and name.
type CatMasterResponseRow struct {
	ID   string `json:"id"`
	Host string `json:"host"`
	IP   string `json:"ip"`
	Node string `json:"node"`
}

// CatShardsResponse is the response type returned by the cat shards API.
// Each row represents a single shard with its allocation, state, and metrics.
type CatShardsResponse []CatShardsResponseRow

// CatShardsResponseRow represents a single row in the cat shards output,
// containing the owning index, shard number, primary/replica designation,
// state, document count, store size, assigned node, and various per-shard
// operational metrics (indexing, search, merges, refresh, flush, segments, etc.).
type CatShardsResponseRow struct {
	Index                          string  `json:"index"`
	UUID                           string  `json:"uuid"`
	Shard                          int     `json:"shard,string"`
	Prirep                         string  `json:"prirep"`
	State                          string  `json:"state"`
	Docs                           int64   `json:"docs,string"`
	Store                          string  `json:"store"`
	IP                             string  `json:"ip"`
	ID                             string  `json:"id"`
	Node                           string  `json:"node"`
	SyncID                         string  `json:"sync_id"`
	UnassignedReason               string  `json:"unassigned.reason"`
	UnassignedAt                   string  `json:"unassigned.at"`
	UnassignedFor                  string  `json:"unassigned.for"`
	UnassignedDetails              string  `json:"unassigned.details"`
	RecoverysourceType             string  `json:"recoverysource.type"`
	CompletionSize                 string  `json:"completion.size"`
	FielddataMemorySize            string  `json:"fielddata.memory_size"`
	FielddataEvictions             int     `json:"fielddata.evictions,string"`
	QueryCacheMemorySize           string  `json:"query_cache.memory_size"`
	QueryCacheEvictions            int     `json:"query_cache.evictions,string"`
	FlushTotal                     int     `json:"flush.total,string"`
	FlushTotalTime                 string  `json:"flush.total_time"`
	GetCurrent                     int     `json:"get.current,string"`
	GetTime                        string  `json:"get.time"`
	GetTotal                       int     `json:"get.total,string"`
	GetExistsTime                  string  `json:"get.exists_time"`
	GetExistsTotal                 int     `json:"get.exists_total,string"`
	GetMissingTime                 string  `json:"get.missing_time"`
	GetMissingTotal                int     `json:"get.missing_total,string"`
	IndexingDeleteCurrent          int     `json:"indexing.delete_current,string"`
	IndexingDeleteTime             string  `json:"indexing.delete_time"`
	IndexingDeleteTotal            int     `json:"indexing.delete_total,string"`
	IndexingIndexCurrent           int     `json:"indexing.index_current,string"`
	IndexingIndexTime              string  `json:"indexing.index_time"`
	IndexingIndexTotal             int     `json:"indexing.index_total,string"`
	IndexingIndexFailed            int     `json:"indexing.index_failed,string"`
	MergesCurrent                  int     `json:"merges.current,string"`
	MergesCurrentDocs              int     `json:"merges.current_docs,string"`
	MergesCurrentSize              string  `json:"merges.current_size"`
	MergesTotal                    int     `json:"merges.total,string"`
	MergesTotalDocs                int     `json:"merges.total_docs,string"`
	MergesTotalSize                string  `json:"merges.total_size"`
	MergesTotalTime                string  `json:"merges.total_time"`
	MergesWarmerTotalInvocations   int     `json:"merges.warmer.total_invocations,string"`
	MergesWarmerTotalTime          string  `json:"merges.warmer.total_time"`
	MergesWarmerOngoingCount       int     `json:"merges.warmer.ongoing_count,string"`
	MergesWarmerTotalBytesReceived string  `json:"merges.warmer.total_bytes_received"`
	MergesWarmerTotalBytesSent     string  `json:"merges.warmer.total_bytes_sent"`
	MergesWarmerTotalReceiveTime   string  `json:"merges.warmer.total_receive_time"`
	MergesWarmerTotalFailureCount  int     `json:"merges.warmer.total_failure_count,string"`
	MergesWarmerTotalSendTime      string  `json:"merges.warmer.total_send_time"`
	RefreshTotal                   int     `json:"refresh.total,string"`
	RefreshExternalTotal           int     `json:"refresh.external_total,string"`
	RefreshTime                    string  `json:"refresh.time"`
	RefreshExternalTime            string  `json:"refresh.external_time"`
	RefreshListeners               int     `json:"refresh.listeners,string"`
	SearchFetchCurrent             int     `json:"search.fetch_current,string"`
	SearchFetchTime                string  `json:"search.fetch_time"`
	SearchFetchTotal               int     `json:"search.fetch_total,string"`
	SearchOpenContexts             int     `json:"search.open_contexts,string"`
	SearchQueryCurrent             int     `json:"search.query_current,string"`
	SearchQueryTime                string  `json:"search.query_time"`
	SearchQueryTotal               int     `json:"search.query_total,string"`
	SearchQueryFailed              int     `json:"search.query_failed,string"`
	SearchScrollCurrent            int     `json:"search.scroll_current,string"`
	SearchScrollTime               string  `json:"search.scroll_time"`
	SearchScrollTotal              int     `json:"search.scroll_total,string"`
	SearchThrottled                bool    `json:"search.throttled,string"`
	SegmentsCount                  int     `json:"segments.count,string"`
	SegmentsMemory                 string  `json:"segments.memory"`
	SegmentsIndexWriterMemory      string  `json:"segments.index_writer_memory"`
	SegmentsVersionMapMemory       string  `json:"segments.version_map_memory"`
	SegmentsFixedBitsetMemory      string  `json:"segments.fixed_bitset_memory"`
	SeqNoMax                       int     `json:"seq_no.max,string"`
	SeqNoLocalCheckpoint           int     `json:"seq_no.local_checkpoint,string"`
	SeqNoGlobalCheckpoint          int     `json:"seq_no.global_checkpoint,string"`
	WarmerCurrent                  int     `json:"warmer.current,string"`
	WarmerTotal                    int     `json:"warmer.total,string"`
	WarmerTotalTime                string  `json:"warmer.total_time"`
	PathData                       string  `json:"path.data"`
	PathState                      string  `json:"path.state"`
	SearchConcurrentQueryCurrent   int     `json:"search.concurrent_query_current,string"`
	SearchConcurrentQueryTime      string  `json:"search.concurrent_query_time"`
	SearchConcurrentQueryTotal     int     `json:"search.concurrent_query_total,string"`
	SearchConcurrentAvgSliceCount  float64 `json:"search.concurrent_avg_slice_count,string"`
	SearchStartreeQueryCurrent     int     `json:"search.startree_query_current,string"`
	SearchStartreeQueryTime        string  `json:"search.startree_query_time"`
	SearchStartreeQueryTotal       int     `json:"search.startree_query_total,string"`
	SearchStartreeQueryFailed      int     `json:"search.startree_query_failed,string"`
	SearchPointInTimeCurrent       int     `json:"search.point_in_time_current,string"`
	SearchPointInTimeTime          string  `json:"search.point_in_time_time"`
	SearchPointInTimeTotal         int     `json:"search.point_in_time_total,string"`
	SearchIdleReactivateCountTotal int     `json:"search.search_idle_reactivate_count_total,string"`
	DocsDeleted                    int     `json:"docs.deleted,string"`
}

// CatSnapshotsResponse is the response type returned by the cat snapshots API.
// Each row represents a single snapshot with its status, timing, and shard counts.
type CatSnapshotsResponse []CatSnapshotsResponseRow

// CatSnapshotsResponseRow represents a single row in the cat snapshots output,
// containing the snapshot ID, repository, status, start/end times, duration,
// index list, and successful/failed/total shard counts.
type CatSnapshotsResponseRow struct {
	ID               string `json:"id"`
	Repository       string `json:"repository"`
	Status           string `json:"status"`
	StartEpoch       string `json:"start_epoch"`
	StartTime        string `json:"start_time"`
	EndEpoch         string `json:"end_epoch"`
	EndTime          string `json:"end_time"`
	Duration         string `json:"duration"`
	Indices          string `json:"indices"`
	SuccessfulShards string `json:"successful_shards"`
	FailedShards     string `json:"failed_shards"`
	TotalShards      string `json:"total_shards"`
	Reason           string `json:"reason"`
}

// CatNodeAttrsResponse is a slice of node attribute entries.
type CatNodeAttrsResponse []CatNodeAttribute

// CatNodeAttribute represents a single attribute entry for a node.
type CatNodeAttribute struct {
	Node  string `json:"node"`
	Host  string `json:"host"`
	IP    string `json:"ip"`
	Attr  string `json:"attr"`
	Value string `json:"value"`
}

// CatNodesResponse is a slice of node info entries.
type CatNodesResponse []CatNodeInfo

// CatNodeInfo represents a single node entry from the cat nodes API.
type CatNodeInfo struct {
	IP              string `json:"ip"`
	HeapPercent     string `json:"heap.percent"`
	RamPercent      string `json:"ram.percent"`
	CPU             string `json:"cpu"`
	Load1m          string `json:"load_1m"`
	Load5m          string `json:"load_5m"`
	Load15m         string `json:"load_15m"`
	NodeRole        string `json:"node.role"`
	Master          string `json:"master"`
	Name            string `json:"name"`
	Jdk             string `json:"jdk"`
	Version         string `json:"version"`
	DiskUsedPercent string `json:"disk.used_percent"`
}

// CatPendingTasksResponse is a slice of pending task entries.
type CatPendingTasksResponse []CatPendingTask

// CatPendingTask represents a single pending task.
type CatPendingTask struct {
	InsertOrder string `json:"insertOrder"`
	TimeInQueue string `json:"timeInQueue"`
	Priority    string `json:"priority"`
	Source      string `json:"source"`
}

// CatPluginsResponse is a slice of plugin entries.
type CatPluginsResponse []CatPluginInfo

// CatPluginInfo represents a single plugin entry from the cat plugins API.
type CatPluginInfo struct {
	Name        string `json:"name"`
	Component   string `json:"component"`
	Version     string `json:"version"`
	Description string `json:"description,omitempty"`
}

// CatRecoveryResponse is a slice of recovery entries.
type CatRecoveryResponse []CatRecoveryInfo

// CatRecoveryInfo represents a single shard recovery.
type CatRecoveryInfo struct {
	Index                string `json:"index"`
	Shard                string `json:"shard"`
	StartTime            string `json:"start_time"`
	Time                 string `json:"time"`
	Type                 string `json:"type"`
	Stage                string `json:"stage"`
	SourceHost           string `json:"source_host"`
	SourceNode           string `json:"source_node"`
	TargetHost           string `json:"target_host"`
	TargetNode           string `json:"target_node"`
	Repository           string `json:"repository"`
	Snapshot             string `json:"snapshot"`
	Files                string `json:"files"`
	FilesRecovered       string `json:"files_recovered"`
	FilesPercent         string `json:"files_percent"`
	Bytes                string `json:"bytes"`
	BytesRecovered       string `json:"bytes_recovered"`
	BytesPercent         string `json:"bytes_percent"`
	TranslogOps          string `json:"translog_ops"`
	TranslogOpsRecovered string `json:"translog_ops_recovered"`
	TranslogOpsPercent   string `json:"translog_ops_percent"`
}

// CatRepositoriesResponse is a slice of repository entries.
type CatRepositoriesResponse []CatRepository

// CatRepository represents a single snapshot repository.
type CatRepository struct {
	ID   string `json:"id"`
	Type string `json:"type"`
}

// CatSegmentsResponse is a slice of segment entries.
type CatSegmentsResponse []CatSegment

// CatSegment represents a single index segment.
type CatSegment struct {
	Index      string `json:"index"`
	Shard      string `json:"shard"`
	Prirep     string `json:"prirep"`
	IP         string `json:"ip"`
	ID         string `json:"id"`
	Segment    string `json:"segment"`
	Version    string `json:"version"`
	Compound   string `json:"compound"`
	Size       string `json:"size"`
	DocsCount  string `json:"docs.count"`
	SizeMemory string `json:"size.memory"`
}

// CatSegmentReplicationResponse is a slice of segment replication entries.
type CatSegmentReplicationResponse []CatSegmentReplication

// CatSegmentReplication represents a segment replication checkpoint.
type CatSegmentReplication struct {
	Shard                string `json:"shard"`
	Checkpoint           string `json:"checkpoint"`
	ReplicatingTimeTaken string `json:"replicating.time_taken"`
	GetChangesTimeTaken  string `json:"get_changes.time_taken"`
	TotalTimeTaken       string `json:"total.time_taken"`
	AcceptedTranslogOps  string `json:"accepted.translog_ops"`
}

// CatTasksResponse is a slice of cat task entries.
type CatTasksResponse []CatTaskInfo

// CatTaskInfo represents a single task from the cat tasks API.
type CatTaskInfo struct {
	ID           string `json:"id"`
	Action       string `json:"action"`
	TaskID       string `json:"task_id"`
	ParentTaskID string `json:"parent_task_id"`
	NodeID       string `json:"node_id"`
	NodeIP       string `json:"node_ip"`
	RunningTime  string `json:"running_time"`
	Type         string `json:"type"`
	Version      string `json:"version,omitempty"`
	Description  string `json:"description,omitempty"`
	XOpaqueID    string `json:"x_opaque_id,omitempty"`
}

// CatTemplatesResponse is a slice of cat template entries.
type CatTemplatesResponse []CatTemplateInfo

// CatTemplateInfo represents a single template from the cat templates API.
type CatTemplateInfo struct {
	Name          string `json:"name"`
	IndexPatterns string `json:"index_patterns"`
	Order         string `json:"order"`
	Version       string `json:"version"`
}

// CatThreadPoolResponse is a slice of cat thread pool entries.
type CatThreadPoolResponse []CatThreadPoolInfo

// CatThreadPoolInfo represents a single thread pool entry from the cat thread_pool API.
type CatThreadPoolInfo struct {
	NodeName  string `json:"node_name"`
	Name      string `json:"name"`
	Active    string `json:"active"`
	PoolSize  string `json:"pool_size"`
	Queue     string `json:"queue"`
	QueueSize string `json:"queue_size"`
	Rejected  string `json:"rejected"`
	Largest   string `json:"largest"`
	Completed string `json:"completed"`
	Type      string `json:"type"`
}

// CatClusterManagerResponse is a slice with a single cluster manager entry.
type CatClusterManagerResponse []CatClusterManager

// CatClusterManager represents the cluster manager entry.
type CatClusterManager struct {
	IP   string `json:"ip"`
	ID   string `json:"id"`
	Host string `json:"host"`
	Node string `json:"node"`
}
