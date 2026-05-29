package api

import "github.com/disaster37/opensearch/v3/types"

// IndicesGetResponse represents the full metadata of an index returned by the get index API,
// including its aliases, mappings, settings, and warmers.
type IndicesGetResponse struct {
	// Aliases maps alias names to their configuration (filters, routing).
	Aliases map[string]any `json:"aliases"`
	// Mappings contains the index mapping definition keyed by type name.
	Mappings map[string]any `json:"mappings"`
	// Settings holds the index settings including number of shards, replicas, and analysis config.
	Settings map[string]any `json:"settings"`
	// Warmers contains index warmer definitions, if any.
	Warmers map[string]any `json:"warmers"`
}

// IndicesRolloverResponse represents the result of a rollover operation.
// It indicates whether the rollover occurred, the old and new index names, and
// which conditions were met.
type IndicesRolloverResponse struct {
	// OldIndex is the name of the index that was rolled over from.
	OldIndex string `json:"old_index"`
	// NewIndex is the name of the newly created index after rollover.
	NewIndex string `json:"new_index"`
	// RolledOver indicates whether the rollover was actually performed.
	RolledOver bool `json:"rolled_over"`
	// DryRun indicates whether this was a dry-run that did not perform the rollover.
	DryRun bool `json:"dry_run"`
	// Acknowledged indicates whether the request was acknowledged by the cluster.
	Acknowledged bool `json:"acknowledged"`
	// ShardsAcknowledged indicates whether the required number of shard copies started before timeout.
	ShardsAcknowledged bool `json:"shards_acknowledged"`
	// Conditions maps each rollover condition to a boolean indicating whether it was met.
	Conditions map[string]bool `json:"conditions"`
}

// IndicesFlushResponse represents the result of a flush operation, containing shard-level status.
type IndicesFlushResponse struct {
	// Shards provides shard-level statistics for the flush operation.
	Shards *types.ShardsInfo `json:"_shards"`
}

// IndicesForcemergeResponse represents the result of a forcemerge operation, containing shard-level status.
type IndicesForcemergeResponse struct {
	// Shards provides shard-level statistics for the forcemerge operation.
	Shards *types.ShardsInfo `json:"_shards"`
}

// RefreshResult wraps a BroadcastResponse to represent the result of a refresh operation.
type RefreshResult struct {
	types.BroadcastResponse
}

// IndicesStatsResponse represents the response from the indices stats API.
// It includes overall cluster shard statistics, aggregate stats across all indices,
// and per-index statistics when requested.
type IndicesStatsResponse struct {
	// Shards provides shard-level statistics for the stats request itself.
	Shards *types.ShardsInfo `json:"_shards"`
	// All contains aggregated statistics across all indices.
	All *IndexStats `json:"_all,omitempty"`
	// Indices maps individual index names to their statistics.
	Indices map[string]*IndexStats `json:"indices,omitempty"`
}

// IndexStats represents statistics for a single index or an aggregated group of indices.
// Primaries contains stats for primary shards only, while Total includes both primary and replicas.
type IndexStats struct {
	// UUID is the unique identifier of the index.
	UUID string `json:"uuid,omitempty"`
	// Primaries holds statistics for primary shards only.
	Primaries *IndexStatsDetails `json:"primaries,omitempty"`
	// Total holds statistics for all shards (primary and replica).
	Total *IndexStatsDetails `json:"total,omitempty"`
	// Shards maps shard identifiers to per-shard statistics.
	Shards map[string][]*IndexStatsDetails `json:"shards,omitempty"`
}

// IndexStatsDetails provides detailed statistics for an index or shard group,
// covering documents, storage, indexing, search, merges, refresh, recovery, and more.
type IndexStatsDetails struct {
	Routing         *IndexStatsRouting         `json:"routing,omitempty"`
	Docs            *IndexStatsDocs            `json:"docs,omitempty"`
	Store           *IndexStatsStore           `json:"store,omitempty"`
	Indexing        *IndexStatsIndexing        `json:"indexing,omitempty"`
	Get             *IndexStatsGet             `json:"get,omitempty"`
	Search          *IndexStatsSearch          `json:"search,omitempty"`
	Merges          *IndexStatsMerges          `json:"merges,omitempty"`
	Refresh         *IndexStatsRefresh         `json:"refresh,omitempty"`
	Recovery        *IndexStatsRecovery        `json:"recovery,omitempty"`
	Flush           *IndexStatsFlush           `json:"flush,omitempty"`
	Warmer          *IndexStatsWarmer          `json:"warmer,omitempty"`
	FilterCache     *IndexStatsFilterCache     `json:"filter_cache,omitempty"`
	IdCache         *IndexStatsIdCache         `json:"id_cache,omitempty"`
	Fielddata       *IndexStatsFielddata       `json:"fielddata,omitempty"`
	Percolate       *IndexStatsPercolate       `json:"percolate,omitempty"`
	Completion      *IndexStatsCompletion      `json:"completion,omitempty"`
	Segments        *IndexStatsSegments        `json:"segments,omitempty"`
	Translog        *IndexStatsTranslog        `json:"translog,omitempty"`
	Suggest         *IndexStatsSuggest         `json:"suggest,omitempty"`
	QueryCache      *IndexStatsQueryCache      `json:"query_cache,omitempty"`
	RequestCache    *IndexStatsRequestCache    `json:"request_cache,omitempty"`
	Commit          *IndexStatsCommit          `json:"commit,omitempty"`
	SeqNo           *IndexStatsSeqNo           `json:"seq_no,omitempty"`
	RetentionLeases *IndexStatsRetentionLeases `json:"retention_leases,omitempty"`
	ShardPath       *IndexStatsShardPath       `json:"shard_path,omitempty"`
	ShardStats      *IndexStatsShardStats      `json:"shard_stats,omitempty"`
}

// IndexStatsRouting describes shard routing information for a specific shard.
type IndexStatsRouting struct {
	// State is the routing state of the shard (e.g. "STARTED", "INITIALIZING").
	State string `json:"state"`
	// Primary indicates whether this is a primary shard.
	Primary bool `json:"primary"`
	// Node is the name of the node this shard is allocated to.
	Node string `json:"node"`
	// RelocatingNode is the name of the node the shard is being relocated to, if any.
	RelocatingNode *string `json:"relocating_node"`
}

// IndexStatsShardPath describes the filesystem paths used by a shard.
type IndexStatsShardPath struct {
	// StatePath is the path to the shard state directory.
	StatePath string `json:"state_path"`
	// DataPath is the path to the shard data directory.
	DataPath string `json:"data_path"`
	// IsCustomDataPath indicates whether a custom data path was configured.
	IsCustomDataPath bool `json:"is_custom_data_path"`
}

// IndexStatsShardStats provides aggregate shard-level counts.
type IndexStatsShardStats struct {
	// TotalCount is the total number of shards.
	TotalCount int64 `json:"total_count,omitempty"`
}

// IndexStatsDocs provides document count statistics for an index.
type IndexStatsDocs struct {
	// Count is the number of documents in the index (excluding nested documents).
	Count int64 `json:"count,omitempty"`
	// Deleted is the number of deleted documents not yet reclaimed by merges.
	Deleted int64 `json:"deleted,omitempty"`
}

// IndexStatsStore provides storage size statistics for an index.
type IndexStatsStore struct {
	// Size is the human-readable size of the index on disk.
	Size string `json:"size,omitempty"`
	// SizeInBytes is the size of the index on disk in bytes.
	SizeInBytes int64 `json:"size_in_bytes,omitempty"`
	// TotalDataSetSize is the human-readable total dataset size including footers and metadata.
	TotalDataSetSize string `json:"total_data_set_size,omitempty"`
	// TotalDataSetSizeInBytes is the total dataset size in bytes.
	TotalDataSetSizeInBytes int64 `json:"total_data_set_size_in_bytes,omitempty"`
	// Reserved is the human-readable reserved storage size.
	Reserved string `json:"reserved,omitempty"`
	// ReservedInBytes is the reserved storage size in bytes.
	ReservedInBytes int64 `json:"reserved_in_bytes,omitempty"`
}

// IndexStatsIndexing provides statistics about indexing operations for an index.
type IndexStatsIndexing struct {
	IndexTotal           int64  `json:"index_total,omitempty"`
	IndexTime            string `json:"index_time,omitempty"`
	IndexTimeInMillis    int64  `json:"index_time_in_millis,omitempty"`
	IndexCurrent         int64  `json:"index_current,omitempty"`
	IndexFailed          int64  `json:"index_failed,omitempty"`
	DeleteTotal          int64  `json:"delete_total,omitempty"`
	DeleteTime           string `json:"delete_time,omitempty"`
	DeleteTimeInMillis   int64  `json:"delete_time_in_millis,omitempty"`
	DeleteCurrent        int64  `json:"delete_current,omitempty"`
	NoopUpdateTotal      int64  `json:"noop_update_total,omitempty"`
	IsThrottled          bool   `json:"is_throttled,omitempty"`
	ThrottleTime         string `json:"throttle_time,omitempty"`
	ThrottleTimeInMillis int64  `json:"throttle_time_in_millis,omitempty"`
}

// IndexStatsGet provides statistics about get operations for an index.
type IndexStatsGet struct {
	Total               int64  `json:"total,omitempty"`
	GetTime             string `json:"getTime,omitempty"`
	TimeInMillis        int64  `json:"time_in_millis,omitempty"`
	ExistsTotal         int64  `json:"exists_total,omitempty"`
	ExistsTime          string `json:"exists_time,omitempty"`
	ExistsTimeInMillis  int64  `json:"exists_time_in_millis,omitempty"`
	MissingTotal        int64  `json:"missing_total,omitempty"`
	MissingTime         string `json:"missing_time,omitempty"`
	MissingTimeInMillis int64  `json:"missing_time_in_millis,omitempty"`
	Current             int64  `json:"current,omitempty"`
}

// IndexStatsSearch provides statistics about search operations for an index,
// including queries, fetches, scrolls, and suggestions.
type IndexStatsSearch struct {
	OpenContexts        int64  `json:"open_contexts,omitempty"`
	QueryTotal          int64  `json:"query_total,omitempty"`
	QueryTime           string `json:"query_time,omitempty"`
	QueryTimeInMillis   int64  `json:"query_time_in_millis,omitempty"`
	QueryCurrent        int64  `json:"query_current,omitempty"`
	FetchTotal          int64  `json:"fetch_total,omitempty"`
	FetchTime           string `json:"fetch_time,omitempty"`
	FetchTimeInMillis   int64  `json:"fetch_time_in_millis,omitempty"`
	FetchCurrent        int64  `json:"fetch_current,omitempty"`
	ScrollTotal         int64  `json:"scroll_total,omitempty"`
	ScrollTime          string `json:"scroll_time,omitempty"`
	ScrollTimeInMillis  int64  `json:"scroll_time_in_millis,omitempty"`
	ScrollCurrent       int64  `json:"scroll_current,omitempty"`
	SuggestTotal        int64  `json:"suggest_total,omitempty"`
	SuggestTime         string `json:"suggest_time,omitempty"`
	SuggestTimeInMillis int64  `json:"suggest_time_in_millis,omitempty"`
	SuggestCurrent      int64  `json:"suggest_current,omitempty"`
}

// IndexStatsMerges provides statistics about segment merge operations for an index.
type IndexStatsMerges struct {
	Current                    int64  `json:"current,omitempty"`
	CurrentDocs                int64  `json:"current_docs,omitempty"`
	CurrentSize                string `json:"current_size,omitempty"`
	CurrentSizeInBytes         int64  `json:"current_size_in_bytes,omitempty"`
	Total                      int64  `json:"total,omitempty"`
	TotalTime                  string `json:"total_time,omitempty"`
	TotalTimeInMillis          int64  `json:"total_time_in_millis,omitempty"`
	TotalDocs                  int64  `json:"total_docs,omitempty"`
	TotalSize                  string `json:"total_size,omitempty"`
	TotalSizeInBytes           int64  `json:"total_size_in_bytes,omitempty"`
	TotalStoppedTime           string `json:"total_stopped_time,omitempty"`
	TotalStoppedTimeInMillis   int64  `json:"total_stopped_time_in_millis,omitempty"`
	TotalThrottledTime         string `json:"total_throttled_time,omitempty"`
	TotalThrottledTimeInMillis int64  `json:"total_throttled_time_in_millis,omitempty"`
	TotalAutoThrottle          string `json:"total_auto_throttle,omitempty"`
	TotalAutoThrottleInBytes   int64  `json:"total_auto_throttle_in_bytes,omitempty"`
}

// IndexStatsRefresh provides statistics about refresh operations for an index.
type IndexStatsRefresh struct {
	Total                     int64  `json:"total,omitempty"`
	TotalTime                 string `json:"total_time,omitempty"`
	TotalTimeInMillis         int64  `json:"total_time_in_millis,omitempty"`
	ExternalTotal             int64  `json:"external_total,omitempty"`
	ExternalTotalTime         string `json:"external_total_time,omitempty"`
	ExternalTotalTimeInMillis int64  `json:"external_total_time_in_millis,omitempty"`
	Listeners                 int64  `json:"listeners,omitempty"`
}

// IndexStatsRecovery provides statistics about shard recovery operations for an index.
type IndexStatsRecovery struct {
	CurrentAsSource      int64  `json:"current_as_source,omitempty"`
	CurrentAsTarget      int64  `json:"current_as_target,omitempty"`
	ThrottleTime         string `json:"throttle_time,omitempty"`
	ThrottleTimeInMillis int64  `json:"throttle_time_in_millis,omitempty"`
}

// IndexStatsFlush provides statistics about flush operations for an index.
type IndexStatsFlush struct {
	Total             int64  `json:"total,omitempty"`
	TotalTime         string `json:"total_time,omitempty"`
	TotalTimeInMillis int64  `json:"total_time_in_millis,omitempty"`
	Periodic          int64  `json:"periodic,omitempty"`
}

// IndexStatsWarmer provides statistics about index warmer operations.
type IndexStatsWarmer struct {
	Current           int64  `json:"current,omitempty"`
	Total             int64  `json:"total,omitempty"`
	TotalTime         string `json:"total_time,omitempty"`
	TotalTimeInMillis int64  `json:"total_time_in_millis,omitempty"`
}

// IndexStatsRequestCache provides statistics about the shard-level request cache.
type IndexStatsRequestCache struct {
	// MemorySize is the human-readable cache size.
	MemorySize string `json:"memory_size,omitempty"`
	// MemorySizeInBytes is the cache size in bytes.
	MemorySizeInBytes int64 `json:"memory_size_in_bytes,omitempty"`
	// Evictions is the number of entries evicted from the cache.
	Evictions int64 `json:"evictions,omitempty"`
	// HitCount is the number of cache hits.
	HitCount int64 `json:"hit_count,omitempty"`
	// MissCount is the number of cache misses.
	MissCount int64 `json:"miss_count,omitempty"`
}

// IndexStatsCommit describes the state of the most recent commit for a shard.
type IndexStatsCommit struct {
	// ID is the opaque commit identifier.
	ID string `json:"id,omitempty"`
	// Generation is the commit generation number.
	Generation int64 `json:"generation,omitempty"`
	// UserData contains arbitrary user-provided key-value pairs stored with the commit.
	UserData map[string]string `json:"user_data,omitempty"`
	// NumDocs is the number of documents in the commit.
	NumDocs int64 `json:"num_docs,omitempty"`
}

// IndexStatsFilterCache provides statistics about the deprecated Lucene filter cache.
type IndexStatsFilterCache struct {
	MemorySize        string `json:"memory_size,omitempty"`
	MemorySizeInBytes int64  `json:"memory_size_in_bytes,omitempty"`
	Evictions         int64  `json:"evictions,omitempty"`
}

// IndexStatsIdCache provides statistics about the deprecated ID cache.
type IndexStatsIdCache struct {
	MemorySize        string `json:"memory_size,omitempty"`
	MemorySizeInBytes int64  `json:"memory_size_in_bytes,omitempty"`
}

// IndexStatsFielddata provides statistics about the fielddata cache,
// which stores field values in memory for sorting and aggregations.
type IndexStatsFielddata struct {
	MemorySize        string `json:"memory_size,omitempty"`
	MemorySizeInBytes int64  `json:"memory_size_in_bytes,omitempty"`
	Evictions         int64  `json:"evictions,omitempty"`
}

// IndexStatsPercolate provides statistics about percolate operations (deprecated).
type IndexStatsPercolate struct {
	Total             int64  `json:"total,omitempty"`
	GetTime           string `json:"get_time,omitempty"`
	TimeInMillis      int64  `json:"time_in_millis,omitempty"`
	Current           int64  `json:"current,omitempty"`
	MemorySize        string `json:"memory_size,omitempty"`
	MemorySizeInBytes int64  `json:"memory_size_in_bytes,omitempty"`
	Queries           int64  `json:"queries,omitempty"`
}

// IndexStatsCompletion provides statistics about the completion suggester data structure size.
type IndexStatsCompletion struct {
	Size        string `json:"size,omitempty"`
	SizeInBytes int64  `json:"size_in_bytes,omitempty"`
}

// IndexStatsSegments provides statistics about the Lucene segments within an index,
// including memory usage breakdown by data structure type.
type IndexStatsSegments struct {
	// Count is the total number of segments.
	Count int64 `json:"count"`
	// Memory is the human-readable total memory used by all segments.
	Memory string `json:"memory"`
	// MemoryInBytes is the total memory used by all segments in bytes.
	MemoryInBytes int64 `json:"memory_in_bytes"`
	// TermsMemory is the human-readable memory used by term dictionaries.
	TermsMemory string `json:"terms_memory"`
	// TermsMemoryInBytes is the memory used by term dictionaries in bytes.
	TermsMemoryInBytes int64 `json:"terms_memory_in_bytes"`
	// StoredFieldsMemory is the human-readable memory used by stored fields.
	StoredFieldsMemory string `json:"stored_fields_memory"`
	// StoredFieldsMemoryInBytes is the memory used by stored fields in bytes.
	StoredFieldsMemoryInBytes int64 `json:"stored_fields_memory_in_bytes"`
	// TermVectorsMemory is the human-readable memory used by term vectors.
	TermVectorsMemory string `json:"term_vectors_memory"`
	// TermVectorsMemoryInBytes is the memory used by term vectors in bytes.
	TermVectorsMemoryInBytes int64 `json:"term_vectors_memory_in_bytes"`
	// NormsMemory is the human-readable memory used by field norms.
	NormsMemory string `json:"norms_memory"`
	// NormsMemoryInBytes is the memory used by field norms in bytes.
	NormsMemoryInBytes int64 `json:"norms_memory_in_bytes"`
	// PointsMemory is the human-readable memory used by point values (BKD trees).
	PointsMemory string `json:"points_memory"`
	// PointsMemoryInBytes is the memory used by point values in bytes.
	PointsMemoryInBytes int64 `json:"points_memory_in_bytes"`
	// DocValuesMemory is the human-readable memory used by doc values.
	DocValuesMemory string `json:"doc_values_memory"`
	// DocValuesMemoryInBytes is the memory used by doc values in bytes.
	DocValuesMemoryInBytes int64 `json:"doc_values_memory_in_bytes"`
	// IndexWriterMemory is the human-readable memory used by the index writer.
	IndexWriterMemory string `json:"index_writer_memory"`
	// IndexWriterMemoryInBytes is the memory used by the index writer in bytes.
	IndexWriterMemoryInBytes int64 `json:"index_writer_memory_in_bytes"`
	// VersionMapMemory is the human-readable memory used by the version map.
	VersionMapMemory string `json:"version_map_memory"`
	// VersionMapMemoryInBytes is the memory used by the version map in bytes.
	VersionMapMemoryInBytes int64 `json:"version_map_memory_in_bytes"`
	// FixedBitSet is the human-readable memory used by fixed bit sets (nested docs).
	FixedBitSet string `json:"fixed_bit_set"`
	// FixedBitSetInBytes is the memory used by fixed bit sets in bytes.
	FixedBitSetInBytes int64 `json:"fixed_bit_set_memory_in_bytes"`
	// MaxUnsafeAutoIDTimestamp is the timestamp of the most recent unsafe auto-generated ID.
	MaxUnsafeAutoIDTimestamp int64 `json:"max_unsafe_auto_id_timestamp"`
	// FileSizes maps file types to their size information.
	FileSizes map[string]*ClusterStatsIndicesSegmentsFile `json:"file_sizes"`
}

// IndexStatsTranslog provides statistics about the transaction log (translog) for an index.
type IndexStatsTranslog struct {
	Operations              int64  `json:"operations,omitempty"`
	Size                    string `json:"size,omitempty"`
	SizeInBytes             int64  `json:"size_in_bytes,omitempty"`
	UncommittedOperations   int64  `json:"uncommitted_operations,omitempty"`
	UncommittedSize         string `json:"uncommitted_size,omitempty"`
	UncommittedSizeInBytes  int64  `json:"uncommitted_size_in_bytes,omitempty"`
	EarliestLastModifiedAge int64  `json:"earliest_last_modified_age,omitempty"`
}

// IndexStatsSuggest provides statistics about suggest operations for an index.
type IndexStatsSuggest struct {
	Total        int64  `json:"total,omitempty"`
	Time         string `json:"time,omitempty"`
	TimeInMillis int64  `json:"time_in_millis,omitempty"`
	Current      int64  `json:"current,omitempty"`
}

// IndexStatsQueryCache provides statistics about the node-level query cache.
type IndexStatsQueryCache struct {
	MemorySize        string `json:"memory_size,omitempty"`
	MemorySizeInBytes int64  `json:"memory_size_in_bytes,omitempty"`
	TotalCount        int64  `json:"total_count,omitempty"`
	HitCount          int64  `json:"hit_count,omitempty"`
	MissCount         int64  `json:"miss_count,omitempty"`
	CacheSize         int64  `json:"cache_size,omitempty"`
	CacheCount        int64  `json:"cache_count,omitempty"`
	Evictions         int64  `json:"evictions,omitempty"`
}

// IndexStatsSeqNo provides sequence number statistics for an index shard,
// which are relevant for optimistic concurrency control and replication.
type IndexStatsSeqNo struct {
	// MaxSeqNo is the highest sequence number assigned to any operation.
	MaxSeqNo int64 `json:"max_seq_no,omitempty"`
	// LocalCheckpoint is the sequence number below which all operations have been processed locally.
	LocalCheckpoint int64 `json:"local_checkpoint,omitempty"`
	// GlobalCheckpoint is the sequence number below which all operations have been processed on all shard copies.
	GlobalCheckpoint int64 `json:"global_checkpoint,omitempty"`
}

// IndexStatsRetentionLeases provides retention lease information for an index shard.
type IndexStatsRetentionLeases struct {
	// PrimaryTerm is the primary term during which the leases were created.
	PrimaryTerm int64 `json:"primary_term,omitempty"`
	// Version is the monotonically increasing version of the lease set.
	Version int64 `json:"version,omitempty"`
	// Leases lists the active retention leases.
	Leases []*IndexStatsRetentionLease `json:"leases,omitempty"`
}

// IndexStatsRetentionLease describes a single retention lease that prevents
// operations at or below a sequence number from being merged away.
type IndexStatsRetentionLease struct {
	// Id is the unique identifier for this lease.
	Id string `json:"id,omitempty"`
	// RetainingSeqNo is the minimum sequence number this lease retains.
	RetainingSeqNo int64 `json:"retaining_seq_no,omitempty"`
	// Timestamp is the creation time of the lease in milliseconds since epoch.
	Timestamp int64 `json:"timestamp,omitempty"`
	// Source describes what created the lease (e.g. a snapshot or CCR process).
	Source string `json:"source,omitempty"`
}

// IndicesSegmentsResponse represents the response from the segments API,
// containing low-level Lucene segment details per index and per shard.
type IndicesSegmentsResponse struct {
	// Shards provides shard-level statistics for the request.
	Shards *types.ShardsInfo `json:"_shards"`
	// Indices maps index names to their segment information.
	Indices map[string]*IndexSegments `json:"indices,omitempty"`
}

// IndexSegments contains per-shard segment information for a single index.
type IndexSegments struct {
	// Shards maps shard identifiers to their segment arrays.
	Shards map[string][]*IndexSegmentsShards `json:"shards,omitempty"`
}

// IndexSegmentsShards represents segment information for a single shard copy.
type IndexSegmentsShards struct {
	// Routing describes shard routing information for this shard copy.
	Routing *IndexSegmentsRouting `json:"routing,omitempty"`
	// NumCommittedSegments is the number of segments that have been fsync'd to disk.
	NumCommittedSegments int64 `json:"num_committed_segments,omitempty"`
	// NumSearchSegments is the number of segments that are visible to searches.
	NumSearchSegments int64 `json:"num_search_segments"`
	// Segments maps segment names to their detailed information.
	Segments map[string]*IndexSegmentsDetails `json:"segments,omitempty"`
}

// IndexSegmentsRouting describes shard routing state for a specific shard copy.
type IndexSegmentsRouting struct {
	// State is the shard state (e.g. "STARTED", "RELOCATING").
	State string `json:"state,omitempty"`
	// Primary indicates whether this is a primary shard.
	Primary bool `json:"primary,omitempty"`
	// Node is the node name this shard is allocated to.
	Node string `json:"node,omitempty"`
	// RelocatingNode is the target node name during shard relocation.
	RelocatingNode string `json:"relocating_node,omitempty"`
}

// IndexSegmentsDetails provides detailed information for a single Lucene segment,
// including document counts, size, memory usage, and sort configuration.
type IndexSegmentsDetails struct {
	// Generation is the segment generation number.
	Generation int64 `json:"generation,omitempty"`
	// NumDocs is the number of live documents in this segment.
	NumDocs int64 `json:"num_docs,omitempty"`
	// DeletedDocs is the number of deleted (but not yet merged) documents.
	DeletedDocs int64 `json:"deleted_docs,omitempty"`
	// Size is the human-readable segment size on disk.
	Size string `json:"size,omitempty"`
	// SizeInBytes is the segment size on disk in bytes.
	SizeInBytes int64 `json:"size_in_bytes,omitempty"`
	// Memory is the human-readable memory used by this segment.
	Memory string `json:"memory,omitempty"`
	// MemoryInBytes is the memory used by this segment in bytes.
	MemoryInBytes int64 `json:"memory_in_bytes,omitempty"`
	// Committed indicates whether the segment has been committed to disk.
	Committed bool `json:"committed,omitempty"`
	// Search indicates whether the segment is visible to searches.
	Search bool `json:"search,omitempty"`
	// Version is the Lucene version that created this segment.
	Version string `json:"version,omitempty"`
	// Compound indicates whether this segment is stored in compound file format.
	Compound bool `json:"compound,omitempty"`
	// MergeId is the identifier of the merge that produced this segment, if any.
	MergeId string `json:"merge_id,omitempty"`
	// Sort describes the index sort configuration for this segment.
	Sort []*IndexSegmentsSort `json:"sort,omitempty"`
	// RAMTree provides a hierarchical breakdown of memory usage within the segment.
	RAMTree []*IndexSegmentsRamTree `json:"ram_tree,omitempty"`
	// Attributes contains custom codec-level attributes stored with the segment.
	Attributes map[string]string `json:"attributes,omitempty"`
}

// IndexSegmentsSort describes the sort specification applied to an index-sorted segment.
type IndexSegmentsSort struct {
	// Field is the field name used for sorting.
	Field string `json:"field,omitempty"`
	// Mode is the sort mode (e.g. "min", "max").
	Mode string `json:"mode,omitempty"`
	// Missing specifies how missing values are treated.
	Missing any `json:"missing,omitempty"`
	// Reverse indicates whether the sort order is reversed.
	Reverse bool `json:"reverse,omitempty"`
}

// IndexSegmentsRamTree represents a node in the hierarchical breakdown of memory
// usage within a Lucene segment.
type IndexSegmentsRamTree struct {
	// Description describes what this node accounts for.
	Description string `json:"description,omitempty"`
	// Size is the human-readable memory size for this node.
	Size string `json:"size,omitempty"`
	// SizeInBytes is the memory size in bytes.
	SizeInBytes int64 `json:"size_in_bytes,omitempty"`
	// Children contains sub-nodes in the memory usage hierarchy.
	Children []*IndexSegmentsRamTree `json:"children,omitempty"`
}

// IndicesAnalyzeResponse represents the result of an analyze API call,
// containing the tokens produced by the analysis chain.
type IndicesAnalyzeResponse struct {
	// Tokens contains the individual tokens produced during analysis.
	Tokens []IndicesAnalyzeToken `json:"tokens"`
	// Detail provides information about the analyzers, tokenizers, and filters that were applied.
	Detail IndicesAnalyzeDetail `json:"detail"`
}

// IndicesAnalyzeToken represents a single token produced by the analyze API.
type IndicesAnalyzeToken struct {
	// Token is the token text.
	Token string `json:"token"`
	// StartOffset is the character offset where the token starts in the original text.
	StartOffset int64 `json:"start_offset"`
	// EndOffset is the character offset where the token ends (exclusive).
	EndOffset int64 `json:"end_offset"`
	// Type is the token type (e.g. "word", "NUM", "<ALPHANUM>").
	Type string `json:"type"`
	// Position is the ordinal position of the token in the token stream.
	Position int64 `json:"position"`
}

// IndicesAnalyzeDetail describes the analysis chain applied during an analyze request,
// including the tokenizer, character filters, and token filters used.
type IndicesAnalyzeDetail struct {
	// CustomAnalyzer indicates whether a custom analyzer was used.
	CustomAnalyzer bool `json:"custom_analyzer"`
	// Analyzer is the name of the analyzer used.
	Analyzer string `json:"analyzer,omitempty"`
	// Tokenizer describes the tokenizer component and its output.
	Tokenizer *IndicesAnalyzeTokenizer `json:"tokenizer,omitempty"`
	// TokenFilters lists the token filters applied and their outputs.
	TokenFilters []IndicesAnalyzeTokenFilter `json:"tokenfilters,omitempty"`
	// CharFilters lists the character filters applied and their outputs.
	CharFilters []IndicesAnalyzeCharFilter `json:"charfilters,omitempty"`
}

// IndicesAnalyzeTokenizer describes the tokenizer used during analysis and the tokens it produced.
type IndicesAnalyzeTokenizer struct {
	// Name is the tokenizer type name (e.g. "standard").
	Name string `json:"name"`
	// Tokens are the tokens produced by this tokenizer.
	Tokens []IndicesAnalyzeToken `json:"tokens"`
}

// IndicesAnalyzeTokenFilter describes a token filter applied during analysis and the tokens it produced.
type IndicesAnalyzeTokenFilter struct {
	// Name is the token filter name (e.g. "lowercase", "stop").
	Name string `json:"name"`
	// Tokens are the tokens produced after this filter was applied.
	Tokens []IndicesAnalyzeToken `json:"tokens"`
}

// IndicesAnalyzeCharFilter describes a character filter applied during analysis.
type IndicesAnalyzeCharFilter struct {
	// Name is the character filter name (e.g. "html_strip", "pattern_replace").
	Name string `json:"name"`
	// FilteredText contains the text output after this character filter was applied.
	FilteredText []string `json:"filtered_text"`
}

// IndexTemplateMetaData represents the metadata of a legacy index template.
type IndexTemplateMetaData struct {
	// IndexPatterns lists the glob patterns that determine which indices this template applies to.
	IndexPatterns []string `json:"index_patterns"`
	// Order controls precedence when multiple templates match; higher values take priority.
	Order int `json:"order"`
	// Settings holds the index settings defined by this template.
	Settings map[string]any `json:"settings"`
	// Mappings holds the index mapping defined by this template.
	Mappings map[string]any `json:"mappings"`
	// Aliases holds the aliases this template creates on new matching indices.
	Aliases map[string]any `json:"aliases"`
}

// DataStreamGetResponse represents the response from getting a data stream.
// It wraps a list of data stream definitions.
type DataStreamGetResponse struct {
	// DataStreams contains the data stream definitions returned.
	DataStreams []DataStream `json:"data_streams"`
}

// DataStream represents an OpenSearch data stream, which is a time-series
// collection of backing indices optimized for append-only data.
type DataStream struct {
	// Name is the data stream name.
	Name string `json:"name"`
	// TimestampField specifies which field is used as the timestamp.
	TimestampField TimestampField `json:"timestamp_field"`
	// Indices lists the backing indices that hold the data stream's documents.
	Indices []DataStreamIndex `json:"indices"`
	// Generation is the current generation number of the data stream.
	Generation int64 `json:"generation"`
	// Status is the health status of the data stream (e.g. "GREEN", "YELLOW", "RED").
	Status string `json:"status"`
	// Template is the name of the composable index template associated with this data stream.
	Template string `json:"template"`
}

// TimestampField identifies the field used as the timestamp in a data stream.
type TimestampField struct {
	// Name is the name of the timestamp field (typically "@timestamp").
	Name string `json:"name"`
}

// DataStreamIndex identifies a backing index that belongs to a data stream.
type DataStreamIndex struct {
	// IndexName is the name of the backing index.
	IndexName string `json:"index_name"`
	// IndexUUID is the unique identifier of the backing index.
	IndexUUID string `json:"index_uuid"`
}

// IndicesClearCacheResponse represents the result of a clear cache operation,
// containing shard-level status.
type IndicesClearCacheResponse struct {
	// Shards provides shard-level statistics for the clear cache operation.
	Shards *types.ShardsInfo `json:"_shards"`
}

// IndicesGetSettingsResponse represents the settings for a single index returned
// by the get settings API, including both user-defined and default settings.
type IndicesGetSettingsResponse struct {
	// Settings contains the user-defined settings for the index.
	Settings map[string]any `json:"settings"`
	// Defaults contains the default settings applied by OpenSearch when not explicitly set.
	Defaults map[string]any `json:"defaults,omitempty"`
}

// IndicesGetTemplateResponse represents the definition of a legacy index template
// returned by the get template API.
type IndicesGetTemplateResponse struct {
	// IndexPatterns lists the glob patterns that determine which indices this template applies to.
	IndexPatterns []string `json:"index_patterns"`
	// Order controls precedence when multiple templates match.
	Order int `json:"order"`
	// Settings holds the index settings defined by this template.
	Settings map[string]any `json:"settings,omitempty"`
	// Mappings holds the index mapping defined by this template.
	Mappings map[string]any `json:"mappings,omitempty"`
	// Aliases holds the aliases this template creates on new matching indices.
	Aliases map[string]any `json:"aliases,omitempty"`
	// Version is an optional version number for the template.
	Version *int64 `json:"version,omitempty"`
}

// IndicesGetIndexTemplateResponse represents the response from the get index template API,
// containing composable index templates.
type IndicesGetIndexTemplateResponse struct {
	// IndexTemplates lists the composable index templates returned.
	IndexTemplates []IndicesIndexTemplateItem `json:"index_templates"`
}

// IndicesIndexTemplateItem represents a single composable index template within
// a get index template response.
type IndicesIndexTemplateItem struct {
	// Name is the template name.
	Name string `json:"name"`
	// IndexTemplate contains the template definition.
	IndexTemplate IndicesIndexTemplateBody `json:"index_template"`
}

// IndicesIndexTemplateBody holds the full definition of a composable index template,
// including index patterns, component composition, and data stream configuration.
type IndicesIndexTemplateBody struct {
	// IndexPatterns lists the glob patterns that determine which indices this template applies to.
	IndexPatterns []string `json:"index_patterns"`
	// Template defines the index settings, mappings, and aliases applied by this template.
	Template IndicesTemplateContent `json:"template,omitempty"`
	// Priority determines precedence when multiple templates match; higher values take priority.
	Priority int `json:"priority,omitempty"`
	// ComposedOf lists the component template names that are composed into this index template.
	ComposedOf []string `json:"composed_of,omitempty"`
	// Meta holds optional user-defined metadata for the template.
	Meta map[string]any `json:"_meta,omitempty"`
	// DataStream configures data stream behavior when this template is used to create a data stream.
	DataStream *DataStreamTemplate `json:"data_stream,omitempty"`
	// AllowAutoCreate controls whether indices matching the template patterns are auto-created on write.
	AllowAutoCreate *bool `json:"allow_auto_create,omitempty"`
}

// IndicesTemplateContent defines the settings, mappings, and aliases that a template
// applies to new indices.
type IndicesTemplateContent struct {
	// Settings holds the index settings to apply.
	Settings map[string]any `json:"settings,omitempty"`
	// Mappings holds the index mapping to apply.
	Mappings map[string]any `json:"mappings,omitempty"`
	// Aliases holds the aliases to create on new indices matching the template.
	Aliases map[string]any `json:"aliases,omitempty"`
}

// DataStreamTemplate configures data stream behavior within a composable index template.
type DataStreamTemplate struct {
	// Hidden indicates whether the data stream's backing indices are hidden from wildcard queries.
	Hidden *bool `json:"hidden,omitempty"`
	// AllowCustomRouting indicates whether custom routing is allowed for the data stream.
	AllowCustomRouting *bool `json:"allow_custom_routing,omitempty"`
}

// IndicesGetComponentTemplateResponse represents the response from the get component template API.
type IndicesGetComponentTemplateResponse struct {
	// ComponentTemplates lists the component templates returned.
	ComponentTemplates []IndicesComponentTemplateItem `json:"component_templates"`
}

// IndicesComponentTemplateItem represents a single component template within
// a get component template response.
type IndicesComponentTemplateItem struct {
	// Name is the component template name.
	Name string `json:"name"`
	// ComponentTemplate contains the component template definition.
	ComponentTemplate IndicesComponentTemplateBody `json:"component_template"`
}

// IndicesComponentTemplateBody holds the definition of a component template,
// which serves as a reusable building block for composable index templates.
type IndicesComponentTemplateBody struct {
	// Template defines the settings, mappings, and aliases provided by this component.
	Template IndicesTemplateContent `json:"template"`
	// Version is an optional version number for the component template.
	Version *int64 `json:"version,omitempty"`
	// Meta holds optional user-defined metadata for the component template.
	Meta map[string]any `json:"_meta,omitempty"`
}

// IndicesDataStreamGetResponse represents the response from the get data stream API.
type IndicesDataStreamGetResponse struct {
	// DataStreams lists the data stream definitions returned.
	DataStreams []DataStream `json:"data_streams"`
}

// IndicesBlockResponse is returned by the add block API.
type IndicesBlockResponse struct {
	Acknowledged       bool `json:"acknowledged"`
	ShardsAcknowledged bool `json:"shards_acknowledged,omitempty"`
}

// IndicesRecoveryResponse maps index names to their recovery state.
type IndicesRecoveryResponse struct {
	Indices map[string]*IndexRecovery `json:"indices,omitempty"`
}

// IndexRecovery contains shard recoveries for a single index.
type IndexRecovery struct {
	Shards []*ShardRecovery `json:"shards,omitempty"`
}

// ShardRecovery describes a single shard's recovery progress.
type ShardRecovery struct {
	Id        int            `json:"id"`
	Type      string         `json:"type"`
	Stage     string         `json:"stage"`
	Primary   bool           `json:"primary"`
	StartTime string         `json:"start_time,omitempty"`
	StopTime  string         `json:"stop_time,omitempty"`
	TotalTime string         `json:"total_time_in_millis,omitempty"`
	Source    map[string]any `json:"source,omitempty"`
	Target    map[string]any `json:"target,omitempty"`
	Index     map[string]any `json:"index,omitempty"`
}

// IndicesShardStoresResponse represents shard store information.
type IndicesShardStoresResponse struct {
	Indices map[string]*IndexShardStores `json:"indices,omitempty"`
}

// IndexShardStores contains shard stores info for an index.
type IndexShardStores struct {
	Shards map[string]*ShardStoreWrapper `json:"shards,omitempty"`
}

// ShardStoreWrapper wraps the stores info for a single shard.
type ShardStoreWrapper struct {
	Stores []map[string]any `json:"stores,omitempty"`
}

// IndicesResolveIndexResponse is the response from the resolve index API.
type IndicesResolveIndexResponse struct {
	Indices     []*ResolvedIndex      `json:"indices,omitempty"`
	Aliases     []*ResolvedAlias      `json:"aliases,omitempty"`
	DataStreams []*ResolvedDataStream `json:"data_streams,omitempty"`
}

// ResolvedIndex is a resolved index entry.
type ResolvedIndex struct {
	Name        string   `json:"name"`
	Indices     []string `json:"indices,omitempty"`
	Aliases     []string `json:"aliases,omitempty"`
	DataStreams []string `json:"data_streams,omitempty"`
	Attributes  []string `json:"attributes,omitempty"`
}

// ResolvedAlias is a resolved alias entry.
type ResolvedAlias struct {
	Name        string   `json:"name"`
	Indices     []string `json:"indices,omitempty"`
	DataStreams []string `json:"data_streams,omitempty"`
}

// ResolvedDataStream is a resolved data stream entry.
type ResolvedDataStream struct {
	Name           string   `json:"name"`
	BackingIndices []string `json:"backing_indices,omitempty"`
}

// IndicesSimulateTemplateResponse is the response from simulate template/index APIs.
type IndicesSimulateTemplateResponse struct {
	Template    map[string]any   `json:"template,omitempty"`
	Mappings    map[string]any   `json:"mappings,omitempty"`
	Overlapping []map[string]any `json:"overlapping,omitempty"`
}

// IndicesDataStreamsStatsResponse represents data stream statistics.
type IndicesDataStreamsStatsResponse struct {
	Shards          *types.ShardsInfo  `json:"_shards,omitempty"`
	DataStreamCount int                `json:"data_stream_count"`
	BackingIndices  int                `json:"backing_indices"`
	TotalStoreSize  int64              `json:"total_store_size_bytes"`
	DataStreams     []*DataStreamStats `json:"data_streams,omitempty"`
}

// DataStreamStats contains statistics for a single data stream.
type DataStreamStats struct {
	DataStream       string `json:"data_stream"`
	BackingIndices   int    `json:"backing_indices"`
	StoreSize        string `json:"store_size,omitempty"`
	MaximumTimestamp int64  `json:"maximum_timestamp,omitempty"`
}
