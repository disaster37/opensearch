package api

import "time"

// NodesInfoResponse represents the result of a nodes info request.
// It contains the cluster name and a map of node IDs to their info details.
type NodesInfoResponse struct {
	ClusterName string                    `json:"cluster_name"`
	Nodes       map[string]*NodesInfoNode `json:"nodes"`
}

// NodesInfoNode represents the static information for a single cluster node,
// including its name, version, roles, OS, JVM, thread pools, transport,
// HTTP, plugins, modules, and ingest processor configuration.
type NodesInfoNode struct {
	Name                       string                   `json:"name"`
	TransportAddress           string                   `json:"transport_address"`
	Host                       string                   `json:"host"`
	IP                         string                   `json:"ip"`
	Version                    string                   `json:"version"`
	BuildHash                  string                   `json:"build_hash"`
	TotalIndexingBuffer        string                   `json:"total_indexing_buffer"`
	TotalIndexingBufferInBytes int64                    `json:"total_indexing_buffer_in_bytes"`
	Roles                      []string                 `json:"roles"`
	Attributes                 map[string]string        `json:"attributes"`
	Settings                   map[string]any           `json:"settings"`
	OS                         *NodesInfoNodeOS         `json:"os"`
	Process                    *NodesInfoNodeProcess    `json:"process"`
	JVM                        *NodesInfoNodeJVM        `json:"jvm"`
	ThreadPool                 *NodesInfoNodeThreadPool `json:"thread_pool"`
	Transport                  *NodesInfoNodeTransport  `json:"transport"`
	HTTP                       *NodesInfoNodeHTTP       `json:"http"`
	Plugins                    []*NodesInfoNodePlugin   `json:"plugins"`
	Modules                    []*NodesInfoNodeModule   `json:"modules"`
	Ingest                     *NodesInfoNodeIngest     `json:"ingest"`
}

// HasRole returns true if the node has the specified role assigned.
func (n *NodesInfoNode) HasRole(role string) bool {
	for _, r := range n.Roles {
		if r == role {
			return true
		}
	}
	return false
}

// IsMaster returns true if the node has the "cluster_manager" role.
func (n *NodesInfoNode) IsMaster() bool {
	return n.HasRole("cluster_manager")
}

// IsData returns true if the node has the "data" role.
func (n *NodesInfoNode) IsData() bool {
	return n.HasRole("data")
}

// IsIngest returns true if the node has the "ingest" role.
func (n *NodesInfoNode) IsIngest() bool {
	return n.HasRole("ingest")
}

// NodesInfoNodeOS represents the operating system information for a node,
// including OS name, architecture, version, and processor counts.
type NodesInfoNodeOS struct {
	RefreshInterval         string `json:"refresh_interval"`
	RefreshIntervalInMillis int    `json:"refresh_interval_in_millis"`
	Name                    string `json:"name"`
	Arch                    string `json:"arch"`
	Version                 string `json:"version"`
	AvailableProcessors     int    `json:"available_processors"`
	AllocatedProcessors     int    `json:"allocated_processors"`
}

// NodesInfoNodeProcess represents process-level information for a node,
// including the process ID, refresh interval, and mlockall status.
type NodesInfoNodeProcess struct {
	RefreshInterval         string `json:"refresh_interval"`
	RefreshIntervalInMillis int64  `json:"refresh_interval_in_millis"`
	ID                      int    `json:"id"`
	Mlockall                bool   `json:"mlockall"`
}

// NodesInfoNodeJVM represents JVM information for a node, including
// the JVM version, VM details, heap memory configuration, GC collectors,
// memory pools, and startup arguments.
type NodesInfoNodeJVM struct {
	PID               int       `json:"pid"`
	Version           string    `json:"version"`
	VMName            string    `json:"vm_name"`
	VMVersion         string    `json:"vm_version"`
	VMVendor          string    `json:"vm_vendor"`
	StartTime         time.Time `json:"start_time"`
	StartTimeInMillis int64     `json:"start_time_in_millis"`

	Mem struct {
		HeapInit           string `json:"heap_init"`
		HeapInitInBytes    int    `json:"heap_init_in_bytes"`
		HeapMax            string `json:"heap_max"`
		HeapMaxInBytes     int    `json:"heap_max_in_bytes"`
		NonHeapInit        string `json:"non_heap_init"`
		NonHeapInitInBytes int    `json:"non_heap_init_in_bytes"`
		NonHeapMax         string `json:"non_heap_max"`
		NonHeapMaxInBytes  int    `json:"non_heap_max_in_bytes"`
		DirectMax          string `json:"direct_max"`
		DirectMaxInBytes   int    `json:"direct_max_in_bytes"`
	} `json:"mem"`

	GCCollectors []string `json:"gc_collectors"`
	MemoryPools  []string `json:"memory_pools"`

	UsingCompressedOrdinaryObjectPointers any `json:"using_compressed_ordinary_object_pointers"`

	InputArguments []string `json:"input_arguments"`
}

// NodesInfoNodeThreadPool holds the thread pool configuration for all
// named thread pools on a node, such as search, index, bulk, and flush.
type NodesInfoNodeThreadPool struct {
	ForceMerge        *NodesInfoNodeThreadPoolSection `json:"force_merge"`
	FetchShardStarted *NodesInfoNodeThreadPoolSection `json:"fetch_shard_started"`
	Listener          *NodesInfoNodeThreadPoolSection `json:"listener"`
	Index             *NodesInfoNodeThreadPoolSection `json:"index"`
	Refresh           *NodesInfoNodeThreadPoolSection `json:"refresh"`
	Generic           *NodesInfoNodeThreadPoolSection `json:"generic"`
	Warmer            *NodesInfoNodeThreadPoolSection `json:"warmer"`
	Search            *NodesInfoNodeThreadPoolSection `json:"search"`
	Flush             *NodesInfoNodeThreadPoolSection `json:"flush"`
	FetchShardStore   *NodesInfoNodeThreadPoolSection `json:"fetch_shard_store"`
	Management        *NodesInfoNodeThreadPoolSection `json:"management"`
	Get               *NodesInfoNodeThreadPoolSection `json:"get"`
	Bulk              *NodesInfoNodeThreadPoolSection `json:"bulk"`
	Snapshot          *NodesInfoNodeThreadPoolSection `json:"snapshot"`

	Percolate *NodesInfoNodeThreadPoolSection `json:"percolate"`
	Bench     *NodesInfoNodeThreadPoolSection `json:"bench"`
	Suggest   *NodesInfoNodeThreadPoolSection `json:"suggest"`
	Optimize  *NodesInfoNodeThreadPoolSection `json:"optimize"`
	Merge     *NodesInfoNodeThreadPoolSection `json:"merge"`
}

// NodesInfoNodeThreadPoolSection describes the configuration of a single
// thread pool, including its type (fixed/scaling), min/max thread counts,
// keep-alive duration, and queue size.
type NodesInfoNodeThreadPoolSection struct {
	Type      string `json:"type"`
	Min       int    `json:"min"`
	Max       int    `json:"max"`
	KeepAlive string `json:"keep_alive"`
	QueueSize any    `json:"queue_size"`
}

// NodesInfoNodeTransport represents the transport layer configuration for
// a node, including bound/publish addresses and transport profiles.
type NodesInfoNodeTransport struct {
	BoundAddress   []string                                  `json:"bound_address"`
	PublishAddress string                                    `json:"publish_address"`
	Profiles       map[string]*NodesInfoNodeTransportProfile `json:"profiles"`
}

// NodesInfoNodeTransportProfile describes a named transport profile with
// its own bound and publish addresses.
type NodesInfoNodeTransportProfile struct {
	BoundAddress   []string `json:"bound_address"`
	PublishAddress string   `json:"publish_address"`
}

// NodesInfoNodeHTTP represents the HTTP layer configuration for a node,
// including bound/publish addresses and the max content length setting.
type NodesInfoNodeHTTP struct {
	BoundAddress            []string `json:"bound_address"`
	PublishAddress          string   `json:"publish_address"`
	MaxContentLength        string   `json:"max_content_length"`
	MaxContentLengthInBytes int64    `json:"max_content_length_in_bytes"`
}

// NodesInfoNodePlugin describes a plugin installed on a node, including
// its name, version, description, and compatibility requirements.
type NodesInfoNodePlugin struct {
	Name                string   `json:"name"`
	Version             string   `json:"version"`
	OpensearchVersion   string   `json:"opensearchsearch_version"`
	JavaVersion         string   `json:"java_version"`
	Description         string   `json:"description"`
	Classname           string   `json:"classname"`
	ExtendedPlugins     []string `json:"extended_plugins"`
	HasNativeController bool     `json:"has_native_controller"`
	RequiresKeystore    bool     `json:"requires_keystore"`
}

// NodesInfoNodeModule describes a built-in module loaded on a node,
// including its name, version, and compatibility requirements.
type NodesInfoNodeModule struct {
	Name                string   `json:"name"`
	Version             string   `json:"version"`
	OpensearchVersion   string   `json:"opensearchsearch_version"`
	JavaVersion         string   `json:"java_version"`
	Description         string   `json:"description"`
	Classname           string   `json:"classname"`
	ExtendedPlugins     []string `json:"extended_plugins"`
	HasNativeController bool     `json:"has_native_controller"`
	RequiresKeystore    bool     `json:"requires_keystore"`
}

// NodesInfoNodeIngest represents the ingest pipeline configuration for
// a node, listing the available ingest processor types.
type NodesInfoNodeIngest struct {
	Processors []*NodesInfoNodeIngestProcessorInfo `json:"processors"`
}

// NodesInfoNodeIngestProcessorInfo identifies a processor type available
// in the node's ingest pipeline system.
type NodesInfoNodeIngestProcessorInfo struct {
	Type string `json:"type"`
}

// NodesStatsResponse represents the result of a nodes stats request.
// It contains the cluster name and a map of node IDs to their runtime statistics.
type NodesStatsResponse struct {
	ClusterName string                     `json:"cluster_name"`
	Nodes       map[string]*NodesStatsNode `json:"nodes"`
}

// NodesStatsNode represents the runtime statistics for a single cluster node,
// covering indices, OS, process, JVM, thread pools, filesystem, transport,
// HTTP, circuit breakers, scripts, discovery, and ingest.
type NodesStatsNode struct {
	Timestamp        int64                                `json:"timestamp"`
	Name             string                               `json:"name"`
	TransportAddress string                               `json:"transport_address"`
	Host             string                               `json:"host"`
	IP               string                               `json:"ip"`
	Roles            []string                             `json:"roles"`
	Attributes       map[string]any                       `json:"attributes"`
	Indices          *NodesStatsIndex                     `json:"indices"`
	OS               *NodesStatsNodeOS                    `json:"os"`
	Process          *NodesStatsNodeProcess               `json:"process"`
	JVM              *NodesStatsNodeJVM                   `json:"jvm"`
	ThreadPool       map[string]*NodesStatsNodeThreadPool `json:"thread_pool"`
	FS               *NodesStatsNodeFS                    `json:"fs"`
	Transport        *NodesStatsNodeTransport             `json:"transport"`
	HTTP             *NodesStatsNodeHTTP                  `json:"http"`
	Breaker          map[string]*NodesStatsBreaker        `json:"breakers"`
	ScriptStats      *NodesStatsScriptStats               `json:"script"`
	Discovery        *NodesStatsDiscovery                 `json:"discovery"`
	Ingest           *NodesStatsIngest                    `json:"ingest"`
}

// NodesStatsIndex represents index-level statistics for a node, including
// document counts, store size, indexing/search/merge/refresh/flush stats,
// query cache, fielddata, segments, translog, and recovery metrics.
type NodesStatsIndex struct {
	Docs         *NodesStatsDocsStats         `json:"docs"`
	Shards       *NodesStatsShardCountStats   `json:"shards_stats"`
	Store        *NodesStatsStoreStats        `json:"store"`
	Indexing     *NodesStatsIndexingStats     `json:"indexing"`
	Get          *NodesStatsGetStats          `json:"get"`
	Search       *NodesStatsSearchStats       `json:"search"`
	Merges       *NodesStatsMergeStats        `json:"merges"`
	Refresh      *NodesStatsRefreshStats      `json:"refresh"`
	Flush        *NodesStatsFlushStats        `json:"flush"`
	Warmer       *NodesStatsWarmerStats       `json:"warmer"`
	QueryCache   *NodesStatsQueryCacheStats   `json:"query_cache"`
	Fielddata    *NodesStatsFielddataStats    `json:"fielddata"`
	Completion   *NodesStatsCompletionStats   `json:"completion"`
	Segments     *NodesStatsSegmentsStats     `json:"segments"`
	Translog     *NodesStatsTranslogStats     `json:"translog"`
	RequestCache *NodesStatsRequestCacheStats `json:"request_cache"`
	Recovery     NodesStatsRecoveryStats      `json:"recovery"`

	IndicesLevel map[string]*NodesStatsIndex `json:"indices"`
	ShardsLevel  map[string]*NodesStatsIndex `json:"shards"`
}

// NodesStatsDocsStats holds document count statistics including total
// count and the number of deleted documents.
type NodesStatsDocsStats struct {
	Count   int64 `json:"count"`
	Deleted int64 `json:"deleted"`
}

// NodesStatsShardCountStats holds the total shard count assigned to a node.
type NodesStatsShardCountStats struct {
	TotalCount int64 `json:"total_count"`
}

// NodesStatsStoreStats holds index store size information in both
// human-readable and byte representations.
type NodesStatsStoreStats struct {
	Size        string `json:"size"`
	SizeInBytes int64  `json:"size_in_bytes"`
}

// NodesStatsIndexingStats holds indexing and delete operation statistics
// including total counts, time spent, current operations, and throttle info.
type NodesStatsIndexingStats struct {
	IndexTotal            int64  `json:"index_total"`
	IndexTime             string `json:"index_time"`
	IndexTimeInMillis     int64  `json:"index_time_in_millis"`
	IndexCurrent          int64  `json:"index_current"`
	IndexFailed           int64  `json:"index_failed"`
	DeleteTotal           int64  `json:"delete_total"`
	DeleteTime            string `json:"delete_time"`
	DeleteTimeInMillis    int64  `json:"delete_time_in_millis"`
	DeleteCurrent         int64  `json:"delete_current"`
	NoopUpdateTotal       int64  `json:"noop_update_total"`
	IsThrottled           bool   `json:"is_throttled"`
	ThrottledTime         string `json:"throttle_time"`
	ThrottledTimeInMillis int64  `json:"throttle_time_in_millis"`

	Types map[string]*NodesStatsIndexingStats `json:"types"`
}

// NodesStatsGetStats holds get operation statistics including total, exists,
// missing, and current operation counts with timing information.
type NodesStatsGetStats struct {
	Total               int64  `json:"total"`
	Time                string `json:"get_time"`
	TimeInMillis        int64  `json:"time_in_millis"`
	Exists              int64  `json:"exists"`
	ExistsTime          string `json:"exists_time"`
	ExistsTimeInMillis  int64  `json:"exists_in_millis"`
	Missing             int64  `json:"missing"`
	MissingTime         string `json:"missing_time"`
	MissingTimeInMillis int64  `json:"missing_in_millis"`
	Current             int64  `json:"current"`
}

// NodesStatsSearchStats holds search operation statistics including query,
// fetch, and scroll metrics with timing and current counts.
type NodesStatsSearchStats struct {
	OpenContexts       int64  `json:"open_contexts"`
	QueryTotal         int64  `json:"query_total"`
	QueryTime          string `json:"query_time"`
	QueryTimeInMillis  int64  `json:"query_time_in_millis"`
	QueryCurrent       int64  `json:"query_current"`
	FetchTotal         int64  `json:"fetch_total"`
	FetchTime          string `json:"fetch_time"`
	FetchTimeInMillis  int64  `json:"fetch_time_in_millis"`
	FetchCurrent       int64  `json:"fetch_current"`
	ScrollTotal        int64  `json:"scroll_total"`
	ScrollTime         string `json:"scroll_time"`
	ScrollTimeInMillis int64  `json:"scroll_time_in_millis"`
	ScrollCurrent      int64  `json:"scroll_current"`

	Groups map[string]*NodesStatsSearchStats `json:"groups"`
}

// NodesStatsMergeStats holds segment merge statistics including current
// and total merge counts, sizes, durations, and throttle metrics.
type NodesStatsMergeStats struct {
	Current                    int64  `json:"current"`
	CurrentDocs                int64  `json:"current_docs"`
	CurrentSize                string `json:"current_size"`
	CurrentSizeInBytes         int64  `json:"current_size_in_bytes"`
	Total                      int64  `json:"total"`
	TotalTime                  string `json:"total_time"`
	TotalTimeInMillis          int64  `json:"total_time_in_millis"`
	TotalDocs                  int64  `json:"total_docs"`
	TotalSize                  string `json:"total_size"`
	TotalSizeInBytes           int64  `json:"total_size_in_bytes"`
	TotalStoppedTime           string `json:"total_stopped_time"`
	TotalStoppedTimeInMillis   int64  `json:"total_stopped_time_in_millis"`
	TotalThrottledTime         string `json:"total_throttled_time"`
	TotalThrottledTimeInMillis int64  `json:"total_throttled_time_in_millis"`
	TotalThrottleBytes         string `json:"total_auto_throttle"`
	TotalThrottleBytesInBytes  int64  `json:"total_auto_throttle_in_bytes"`
}

// NodesStatsRefreshStats holds refresh operation statistics including
// total count and cumulative time.
type NodesStatsRefreshStats struct {
	Total             int64  `json:"total"`
	TotalTime         string `json:"total_time"`
	TotalTimeInMillis int64  `json:"total_time_in_millis"`
}

// NodesStatsFlushStats holds flush operation statistics including
// total count and cumulative time.
type NodesStatsFlushStats struct {
	Total             int64  `json:"total"`
	TotalTime         string `json:"total_time"`
	TotalTimeInMillis int64  `json:"total_time_in_millis"`
}

// NodesStatsWarmerStats holds index warmer operation statistics including
// current and total counts and cumulative time.
type NodesStatsWarmerStats struct {
	Current           int64  `json:"current"`
	Total             int64  `json:"total"`
	TotalTime         string `json:"total_time"`
	TotalTimeInMillis int64  `json:"total_time_in_millis"`
}

// NodesStatsQueryCacheStats holds query cache statistics including
// memory usage, hit/miss counts, cache size, and evictions.
type NodesStatsQueryCacheStats struct {
	MemorySize        string `json:"memory_size"`
	MemorySizeInBytes int64  `json:"memory_size_in_bytes"`
	TotalCount        int64  `json:"total_count"`
	HitCount          int64  `json:"hit_count"`
	MissCount         int64  `json:"miss_count"`
	CacheSize         int64  `json:"cache_size"`
	CacheCount        int64  `json:"cache_count"`
	Evictions         int64  `json:"evictions"`
}

// NodesStatsFielddataStats holds fielddata cache statistics including
// memory usage, evictions, and optional per-field breakdowns.
type NodesStatsFielddataStats struct {
	MemorySize        string `json:"memory_size"`
	MemorySizeInBytes int64  `json:"memory_size_in_bytes"`
	Evictions         int64  `json:"evictions"`
	Fields            map[string]struct {
		MemorySize        string `json:"memory_size"`
		MemorySizeInBytes int64  `json:"memory_size_in_bytes"`
	} `json:"fields"`
}

// NodesStatsCompletionStats holds completion suggester statistics including
// size and optional per-field breakdowns.
type NodesStatsCompletionStats struct {
	Size        string `json:"size"`
	SizeInBytes int64  `json:"size_in_bytes"`
	Fields      map[string]struct {
		Size        string `json:"size"`
		SizeInBytes int64  `json:"size_in_bytes"`
	} `json:"fields"`
}

// NodesStatsSegmentsStats holds detailed memory usage statistics for
// Lucene segments, broken down by component (terms, stored fields,
// term vectors, norms, doc values, index writer, version map, etc.).
type NodesStatsSegmentsStats struct {
	Count                       int64  `json:"count"`
	Memory                      string `json:"memory"`
	MemoryInBytes               int64  `json:"memory_in_bytes"`
	TermsMemory                 string `json:"terms_memory"`
	TermsMemoryInBytes          int64  `json:"terms_memory_in_bytes"`
	StoredFieldsMemory          string `json:"stored_fields_memory"`
	StoredFieldsMemoryInBytes   int64  `json:"stored_fields_memory_in_bytes"`
	TermVectorsMemory           string `json:"term_vectors_memory"`
	TermVectorsMemoryInBytes    int64  `json:"term_vectors_memory_in_bytes"`
	NormsMemory                 string `json:"norms_memory"`
	NormsMemoryInBytes          int64  `json:"norms_memory_in_bytes"`
	DocValuesMemory             string `json:"doc_values_memory"`
	DocValuesMemoryInBytes      int64  `json:"doc_values_memory_in_bytes"`
	IndexWriterMemory           string `json:"index_writer_memory"`
	IndexWriterMemoryInBytes    int64  `json:"index_writer_memory_in_bytes"`
	IndexWriterMaxMemory        string `json:"index_writer_max_memory"`
	IndexWriterMaxMemoryInBytes int64  `json:"index_writer_max_memory_in_bytes"`
	VersionMapMemory            string `json:"version_map_memory"`
	VersionMapMemoryInBytes     int64  `json:"version_map_memory_in_bytes"`
	FixedBitSetMemory           string `json:"fixed_bit_set"`
	FixedBitSetMemoryInBytes    int64  `json:"fixed_bit_set_memory_in_bytes"`
}

// NodesStatsTranslogStats holds transaction log statistics including
// operation count and size.
type NodesStatsTranslogStats struct {
	Operations  int64  `json:"operations"`
	Size        string `json:"size"`
	SizeInBytes int64  `json:"size_in_bytes"`
}

// NodesStatsRequestCacheStats holds request cache statistics including
// memory usage, hit/miss counts, and evictions.
type NodesStatsRequestCacheStats struct {
	MemorySize        string `json:"memory_size"`
	MemorySizeInBytes int64  `json:"memory_size_in_bytes"`
	Evictions         int64  `json:"evictions"`
	HitCount          int64  `json:"hit_count"`
	MissCount         int64  `json:"miss_count"`
}

// NodesStatsRecoveryStats holds recovery statistics tracking how many
// recoveries this node is participating in as source and target.
type NodesStatsRecoveryStats struct {
	CurrentAsSource int `json:"current_as_source"`
	CurrentAsTarget int `json:"current_as_target"`
}

// NodesStatsNodeOS represents OS-level runtime statistics for a node,
// including CPU usage, memory, and swap information.
type NodesStatsNodeOS struct {
	Timestamp int64                 `json:"timestamp"`
	CPU       *NodesStatsNodeOSCPU  `json:"cpu"`
	Mem       *NodesStatsNodeOSMem  `json:"mem"`
	Swap      *NodesStatsNodeOSSwap `json:"swap"`
}

// NodesStatsNodeOSCPU represents CPU usage statistics for a node,
// including the usage percentage and load averages.
type NodesStatsNodeOSCPU struct {
	Percent     int                `json:"percent"`
	LoadAverage map[string]float64 `json:"load_average"`
}

// NodesStatsNodeOSMem represents memory usage statistics for a node
// in both human-readable and byte representations.
type NodesStatsNodeOSMem struct {
	Total        string `json:"total"`
	TotalInBytes int64  `json:"total_in_bytes"`
	Free         string `json:"free"`
	FreeInBytes  int64  `json:"free_in_bytes"`
	Used         string `json:"used"`
	UsedInBytes  int64  `json:"used_in_bytes"`
	FreePercent  int    `json:"free_percent"`
	UsedPercent  int    `json:"used_percent"`
}

// NodesStatsNodeOSSwap represents swap space usage statistics for a node.
type NodesStatsNodeOSSwap struct {
	Total        string `json:"total"`
	TotalInBytes int64  `json:"total_in_bytes"`
	Free         string `json:"free"`
	FreeInBytes  int64  `json:"free_in_bytes"`
	Used         string `json:"used"`
	UsedInBytes  int64  `json:"used_in_bytes"`
}

// NodesStatsNodeProcess represents process-level runtime statistics for
// a node, including open file descriptors, CPU usage, and virtual memory.
type NodesStatsNodeProcess struct {
	Timestamp           int64 `json:"timestamp"`
	OpenFileDescriptors int64 `json:"open_file_descriptors"`
	MaxFileDescriptors  int64 `json:"max_file_descriptors"`
	CPU                 struct {
		Percent       int    `json:"percent"`
		Total         string `json:"total"`
		TotalInMillis int64  `json:"total_in_millis"`
	} `json:"cpu"`
	Mem struct {
		TotalVirtual        string `json:"total_virtual"`
		TotalVirtualInBytes int64  `json:"total_virtual_in_bytes"`
	} `json:"mem"`
}

// NodesStatsNodeJVM represents JVM runtime statistics for a node,
// including uptime, heap/non-heap memory, threads, GC, buffer pools,
// and class loading metrics.
type NodesStatsNodeJVM struct {
	Timestamp      int64                                   `json:"timestamp"`
	Uptime         string                                  `json:"uptime"`
	UptimeInMillis int64                                   `json:"uptime_in_millis"`
	Mem            *NodesStatsNodeJVMMem                   `json:"mem"`
	Threads        *NodesStatsNodeJVMThreads               `json:"threads"`
	GC             *NodesStatsNodeJVMGC                    `json:"gc"`
	BufferPools    map[string]*NodesStatsNodeJVMBufferPool `json:"buffer_pools"`
	Classes        *NodesStatsNodeJVMClasses               `json:"classes"`
}

// NodesStatsNodeJVMMem represents JVM memory usage statistics including
// heap and non-heap memory with committed and max values, and per-pool breakdowns.
type NodesStatsNodeJVMMem struct {
	HeapUsed                string `json:"heap_used"`
	HeapUsedInBytes         int64  `json:"heap_used_in_bytes"`
	HeapUsedPercent         int    `json:"heap_used_percent"`
	HeapCommitted           string `json:"heap_committed"`
	HeapCommittedInBytes    int64  `json:"heap_committed_in_bytes"`
	HeapMax                 string `json:"heap_max"`
	HeapMaxInBytes          int64  `json:"heap_max_in_bytes"`
	NonHeapUsed             string `json:"non_heap_used"`
	NonHeapUsedInBytes      int64  `json:"non_heap_used_in_bytes"`
	NonHeapCommitted        string `json:"non_heap_committed"`
	NonHeapCommittedInBytes int64  `json:"non_heap_committed_in_bytes"`
	Pools                   map[string]struct {
		Used            string `json:"used"`
		UsedInBytes     int64  `json:"used_in_bytes"`
		Max             string `json:"max"`
		MaxInBytes      int64  `json:"max_in_bytes"`
		PeakUsed        string `json:"peak_used"`
		PeakUsedInBytes int64  `json:"peak_used_in_bytes"`
		PeakMax         string `json:"peak_max"`
		PeakMaxInBytes  int64  `json:"peak_max_in_bytes"`
	} `json:"pools"`
}

// NodesStatsNodeJVMThreads represents JVM thread count statistics
// including current and peak thread counts.
type NodesStatsNodeJVMThreads struct {
	Count     int64 `json:"count"`
	PeakCount int64 `json:"peak_count"`
}

// NodesStatsNodeJVMGC holds garbage collection statistics, organized by
// collector name (e.g. "young", "old").
type NodesStatsNodeJVMGC struct {
	Collectors map[string]*NodesStatsNodeJVMGCCollector `json:"collectors"`
}

// NodesStatsNodeJVMGCCollector represents GC statistics for a single
// collector, including collection count and time spent collecting.
type NodesStatsNodeJVMGCCollector struct {
	CollectionCount        int64  `json:"collection_count"`
	CollectionTime         string `json:"collection_time"`
	CollectionTimeInMillis int64  `json:"collection_time_in_millis"`
}

// NodesStatsNodeJVMBufferPool represents statistics for a single JVM
// buffer pool, including buffer count and total capacity.
type NodesStatsNodeJVMBufferPool struct {
	Count                int64  `json:"count"`
	TotalCapacity        string `json:"total_capacity"`
	TotalCapacityInBytes int64  `json:"total_capacity_in_bytes"`
}

// NodesStatsNodeJVMClasses represents class loading statistics for the
// JVM, including current, total loaded, and total unloaded class counts.
type NodesStatsNodeJVMClasses struct {
	CurrentLoadedCount int64 `json:"current_loaded_count"`
	TotalLoadedCount   int64 `json:"total_loaded_count"`
	TotalUnloadedCount int64 `json:"total_unloaded_count"`
}

// NodesStatsNodeThreadPool represents runtime statistics for a single
// thread pool, including thread count, queue size, active threads,
// rejected tasks, and completed count.
type NodesStatsNodeThreadPool struct {
	Threads   int   `json:"threads"`
	Queue     int   `json:"queue"`
	Active    int   `json:"active"`
	Rejected  int64 `json:"rejected"`
	Largest   int   `json:"largest"`
	Completed int64 `json:"completed"`
}

// NodesStatsNodeFS represents filesystem statistics for a node,
// including a total summary and per-data-path breakdowns plus I/O stats.
type NodesStatsNodeFS struct {
	Timestamp int64                    `json:"timestamp"`
	Total     *NodesStatsNodeFSEntry   `json:"total"`
	Data      []*NodesStatsNodeFSEntry `json:"data"`
	IOStats   *NodesStatsNodeFSIOStats `json:"io_stats"`
}

// NodesStatsNodeFSEntry represents a single filesystem mount point with
// its path, type, total/free/available space in both human-readable and byte form.
type NodesStatsNodeFSEntry struct {
	Path             string `json:"path"`
	Mount            string `json:"mount"`
	Type             string `json:"type"`
	Total            string `json:"total"`
	TotalInBytes     int64  `json:"total_in_bytes"`
	Free             string `json:"free"`
	FreeInBytes      int64  `json:"free_in_bytes"`
	Available        string `json:"available"`
	AvailableInBytes int64  `json:"available_in_bytes"`
	Spins            string `json:"spins"`
}

// NodesStatsNodeFSIOStats holds I/O statistics for filesystem devices,
// including per-device and aggregate totals.
type NodesStatsNodeFSIOStats struct {
	Devices []*NodesStatsNodeFSIOStatsEntry `json:"devices"`
	Total   *NodesStatsNodeFSIOStatsEntry   `json:"total"`
}

// NodesStatsNodeFSIOStatsEntry represents I/O statistics for a single
// block device, including read/write operations and kilobyte counts.
type NodesStatsNodeFSIOStatsEntry struct {
	DeviceName      string `json:"device_name"`
	Operations      int64  `json:"operations"`
	ReadOperations  int64  `json:"read_operations"`
	WriteOperations int64  `json:"write_operations"`
	ReadKilobytes   int64  `json:"read_kilobytes"`
	WriteKilobytes  int64  `json:"write_kilobytes"`
}

// NodesStatsNodeTransport represents transport layer statistics for a node,
// including open connection count and bytes transmitted/received.
type NodesStatsNodeTransport struct {
	ServerOpen    int    `json:"server_open"`
	RxCount       int64  `json:"rx_count"`
	RxSize        string `json:"rx_size"`
	RxSizeInBytes int64  `json:"rx_size_in_bytes"`
	TxCount       int64  `json:"tx_count"`
	TxSize        string `json:"tx_size"`
	TxSizeInBytes int64  `json:"tx_size_in_bytes"`
}

// NodesStatsNodeHTTP represents HTTP statistics for a node, including
// currently open and total opened connections.
type NodesStatsNodeHTTP struct {
	CurrentOpen int `json:"current_open"`
	TotalOpened int `json:"total_opened"`
}

// NodesStatsBreaker represents circuit breaker statistics including limit,
// estimated size, overhead multiplier, and tripped count.
type NodesStatsBreaker struct {
	LimitSize            string  `json:"limit_size"`
	LimitSizeInBytes     int64   `json:"limit_size_in_bytes"`
	EstimatedSize        string  `json:"estimated_size"`
	EstimatedSizeInBytes int64   `json:"estimated_size_in_bytes"`
	Overhead             float64 `json:"overhead"`
	Tripped              int64   `json:"tripped"`
}

// NodesStatsScriptStats holds script compilation and cache eviction statistics
// for a node.
type NodesStatsScriptStats struct {
	Compilations   int64 `json:"compilations"`
	CacheEvictions int64 `json:"cache_evictions"`
}

// NodesStatsDiscovery represents discovery-related statistics for a node,
// including the cluster state queue status.
type NodesStatsDiscovery struct {
	ClusterStateQueue *NodesStatsDiscoveryStats `json:"cluster_state_queue"`
}

// NodesStatsDiscoveryStats holds cluster state queue statistics including
// total, pending, and committed counts.
type NodesStatsDiscoveryStats struct {
	Total     int64 `json:"total"`
	Pending   int64 `json:"pending"`
	Committed int64 `json:"committed"`
}

// NodesStatsIngest represents ingest statistics for a node, including
// total ingest metrics and per-pipeline breakdowns.
type NodesStatsIngest struct {
	Total     *NodesStatsIngestStats `json:"total"`
	Pipelines any                    `json:"pipelines"`
}

// NodesStatsIngestStats holds aggregate ingest processing statistics
// including document count, time spent, current operations, and failures.
type NodesStatsIngestStats struct {
	Count        int64  `json:"count"`
	Time         string `json:"time"`
	TimeInMillis int64  `json:"time_in_millis"`
	Current      int64  `json:"current"`
	Failed       int64  `json:"failed"`
}

// NodesReloadSecureSettingsResponse represents the result of reloading secure settings across cluster nodes.
type NodesReloadSecureSettingsResponse struct {
	ClusterName string                          `json:"cluster_name"`
	Nodes       map[string]*NodesReloadResponse `json:"nodes"`
}

// NodesReloadResponse represents the reload result for a single node.
type NodesReloadResponse struct {
	Name string `json:"name"`
}

// NodesUsageResponse represents node usage statistics.
type NodesUsageResponse struct {
	ClusterName string                     `json:"cluster_name"`
	Nodes       map[string]*NodesUsageNode `json:"nodes"`
}

// NodesUsageNode represents usage statistics for a single node.
type NodesUsageNode struct {
	Timestamp   int64            `json:"timestamp"`
	Since       int64            `json:"since"`
	RestActions map[string]int64 `json:"rest_actions,omitempty"`
}
