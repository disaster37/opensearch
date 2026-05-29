package api

import "github.com/disaster37/opensearch/v3/types"

// ClusterHealthResponse represents the result of a cluster health request.
// It contains the overall cluster status (green, yellow, red), node counts,
// shard statistics, and optionally per-index health breakdowns.
// Key fields include Status, NumberOfNodes, ActiveShards, and UnassignedShards.
type ClusterHealthResponse struct {
	ClusterName                    string                         `json:"cluster_name"`
	Status                         string                         `json:"status"`
	TimedOut                       bool                           `json:"timed_out"`
	NumberOfNodes                  int                            `json:"number_of_nodes"`
	NumberOfDataNodes              int                            `json:"number_of_data_nodes"`
	ActivePrimaryShards            int                            `json:"active_primary_shards"`
	ActiveShards                   int                            `json:"active_shards"`
	RelocatingShards               int                            `json:"relocating_shards"`
	InitializingShards             int                            `json:"initializing_shards"`
	UnassignedShards               int                            `json:"unassigned_shards"`
	DelayedUnassignedShards        int                            `json:"delayed_unassigned_shards"`
	NumberOfPendingTasks           int                            `json:"number_of_pending_tasks"`
	NumberOfInFlightFetch          int                            `json:"number_of_in_flight_fetch"`
	TaskMaxWaitTimeInQueue         string                         `json:"task_max_waiting_in_queue"`
	TaskMaxWaitTimeInQueueInMillis int                            `json:"task_max_waiting_in_queue_millis"`
	ActiveShardsPercent            string                         `json:"active_shards_percent"`
	ActiveShardsPercentAsNumber    float64                        `json:"active_shards_percent_as_number"`
	Indices                        map[string]*ClusterIndexHealth `json:"indices"`
}

// ClusterIndexHealth represents the health status of a single index within
// the cluster health response. It includes shard-level statistics such as
// active, relocating, initializing, and unassigned shard counts.
type ClusterIndexHealth struct {
	Status              string                         `json:"status"`
	NumberOfShards      int                            `json:"number_of_shards"`
	NumberOfReplicas    int                            `json:"number_of_replicas"`
	ActivePrimaryShards int                            `json:"active_primary_shards"`
	ActiveShards        int                            `json:"active_shards"`
	RelocatingShards    int                            `json:"relocating_shards"`
	InitializingShards  int                            `json:"initializing_shards"`
	UnassignedShards    int                            `json:"unassigned_shards"`
	Shards              map[string]*ClusterShardHealth `json:"shards"`
}

// ClusterShardHealth represents the health of a single shard group within
// an index, including whether the primary is active and replica shard counts.
type ClusterShardHealth struct {
	Status             string `json:"status"`
	PrimaryActive      bool   `json:"primary_active"`
	ActiveShards       int    `json:"active_shards"`
	RelocatingShards   int    `json:"relocating_shards"`
	InitializingShards int    `json:"initializing_shards"`
	UnassignedShards   int    `json:"unassigned_shards"`
}

// ClusterStateResponse represents the full or filtered state of an OpenSearch
// cluster. Key fields include the cluster name/UUID, master node identifier,
// block information, discovery nodes, metadata (templates, indices), and
// the shard routing table.
type ClusterStateResponse struct {
	ClusterName       string                    `json:"cluster_name"`
	ClusterUUID       string                    `json:"cluster_uuid"`
	Version           int64                     `json:"version"`
	StateUUID         string                    `json:"state_uuid"`
	MasterNode        string                    `json:"master_node"`
	Blocks            map[string]*clusterBlocks `json:"blocks"`
	Nodes             map[string]*discoveryNode `json:"nodes"`
	Metadata          *clusterStateMetadata     `json:"metadata"`
	RoutingTable      *clusterStateRoutingTable `json:"routing_table"`
	RoutingNodes      *clusterStateRoutingNode  `json:"routing_nodes"`
	Snapshots         map[string]any            `json:"snapshots"`
	SnapshotDeletions map[string]any            `json:"snapshot_deletions"`
	Customs           map[string]any            `json:"customs"`
}

// clusterBlocks holds global-level and index-level cluster blocks that
// restrict operations on the cluster or specific indices.
type clusterBlocks struct {
	Global  map[string]*clusterBlock `json:"global"`
	Indices map[string]*clusterBlock `json:"indices"`
}

// clusterBlock describes a single cluster block including its description,
// whether it is retryable, and the levels at which it applies.
type clusterBlock struct {
	Description             string   `json:"description"`
	Retryable               bool     `json:"retryable"`
	DisableStatePersistence bool     `json:"disable_state_persistence"`
	Levels                  []string `json:"levels"`
}

// clusterStateMetadata contains the full metadata section of the cluster
// state, including index definitions, templates, routing info, and customs.
type clusterStateMetadata struct {
	ClusterUUID          string                            `json:"cluster_uuid"`
	ClusterUUIDCommitted bool                              `json:"cluster_uuid_committed"`
	ClusterCoordination  *clusterCoordinationMetaData      `json:"cluster_coordination"`
	Templates            map[string]*indexTemplateMetaData `json:"templates"`
	Indices              map[string]*indexMetaData         `json:"indices"`
	RoutingTable         struct {
		Indices map[string]*indexRoutingTable `json:"indices"`
	} `json:"routing_table"`
	RoutingNodes struct {
		Unassigned []*shardRouting `json:"unassigned"`
		Nodes      []*shardRouting `json:"nodes"`
	} `json:"routing_nodes"`
	DataStream        map[string]any `json:"data_stream,omitempty"`
	Customs           map[string]any `json:"customs"`
	Ingest            map[string]any `json:"ingest"`
	StoredScripts     map[string]any `json:"stored_scripts"`
	IndexGraveyard    map[string]any `json:"index-graveyard"`
	IndexLifecycle    map[string]any `json:"index_lifecycle"`
	Repositories      map[string]any `json:"repositories"`
	IndexTemplate     map[string]any `json:"index_template"`
	PersistentTasks   map[string]any `json:"persistent_tasks"`
	ComponentTemplate map[string]any `json:"component_template"`
}

// clusterCoordinationMetaData holds cluster coordination state such as
// the current term and voting configuration.
type clusterCoordinationMetaData struct {
	Term                   int64 `json:"term"`
	LastCommittedConfig    any   `json:"last_committed_config,omitempty"`
	LastAcceptedConfig     any   `json:"last_accepted_config,omitempty"`
	VotingConfigExclusions []any `json:"voting_config_exclusions,omitempty"`
}

// discoveryNode represents a node as seen by the cluster state, including
// its name, transport address, assigned roles, and custom attributes.
type discoveryNode struct {
	Name             string         `json:"name"`
	EphemeralID      string         `json:"ephemeral_id"`
	TransportAddress string         `json:"transport_address"`
	Attributes       map[string]any `json:"attributes"`
	Roles            []string       `json:"roles,omitempty"`
}

// clusterStateRoutingTable holds the shard routing table for each index
// in the cluster state.
type clusterStateRoutingTable struct {
	Indices map[string]any `json:"indices"`
}

// clusterStateRoutingNode maps nodes to their assigned and unassigned
// shard routings as tracked by the cluster state.
type clusterStateRoutingNode struct {
	Unassigned []*shardRouting            `json:"unassigned"`
	Nodes      map[string][]*shardRouting `json:"nodes"`
}

// indexTemplateMetaData represents a legacy index template stored in
// cluster state, including its index patterns, settings, mappings, and aliases.
type indexTemplateMetaData struct {
	IndexPatterns []string       `json:"index_patterns"`
	Order         int            `json:"order"`
	Version       int            `json:"version"`
	Settings      map[string]any `json:"settings"`
	Mappings      map[string]any `json:"mappings"`
	Aliases       map[string]any `json:"aliases"`
}

// indexMetaData represents the stored metadata for a single index,
// including its settings, mappings, aliases, and version information.
type indexMetaData struct {
	State             string         `json:"state"`
	Settings          map[string]any `json:"settings"`
	Mappings          map[string]any `json:"mappings"`
	Aliases           []string       `json:"aliases"`
	PrimaryTerms      map[string]any `json:"primary_terms"`
	InSyncAllocations map[string]any `json:"in_sync_allocations"`
	Version           int            `json:"version"`
	MappingVersion    int            `json:"mapping_version"`
	SettingsVersion   int            `json:"settings_version"`
	AliasesVersion    int            `json:"aliases_version"`
	RoutingNumShards  int            `json:"routing_num_shards"`
	RolloverInfo      any            `json:"rollover_info,omitempty"`
	System            any            `json:"system,omitempty"`
	TimestampRange    any            `json:"timestamp_range,omitempty"`
	ILM               map[string]any `json:"ilm,omitempty"`
}

// indexRoutingTable holds shard routing information for a single index.
type indexRoutingTable struct {
	Shards map[string]*shardRouting `json:"shards"`
}

// shardRouting describes the routing state of a single shard replica,
// including its assigned node, relocation target, and unassignment reason.
type shardRouting struct {
	State          string          `json:"state"`
	Primary        bool            `json:"primary"`
	Node           string          `json:"node"`
	RelocatingNode string          `json:"relocating_node"`
	Shard          int             `json:"shard"`
	Index          string          `json:"index"`
	Version        int64           `json:"version"`
	RestoreSource  *RestoreSource  `json:"restore_source"`
	AllocationId   *allocationId   `json:"allocation_id"`
	UnassignedInfo *unassignedInfo `json:"unassigned_info"`
}

// RestoreSource identifies the snapshot repository, snapshot name, and
// source index from which a shard was restored.
type RestoreSource struct {
	Repository string `json:"repository"`
	Snapshot   string `json:"snapshot"`
	Version    string `json:"version"`
	Index      string `json:"index"`
}

// allocationId tracks unique identifiers for a shard allocation, including
// a relocation ID when the shard is being moved.
type allocationId struct {
	Id           string `json:"id"`
	RelocationId string `json:"relocation_id"`
}

// unassignedInfo explains why a shard is unassigned, including the reason
// code, timestamp, and any additional details.
type unassignedInfo struct {
	Reason  string `json:"reason"`
	At      string `json:"at"`
	Details string `json:"details"`
}

// ClusterStatsResponse represents the result of a cluster stats request.
// It aggregates index-level and node-level statistics across the entire
// cluster, including shard counts, document counts, storage, JVM, and OS stats.
type ClusterStatsResponse struct {
	NodesStats  *ClusterStatsNodesResponse `json:"_nodes,omitempty"`
	Timestamp   int64                      `json:"timestamp"`
	ClusterName string                     `json:"cluster_name"`
	ClusterUUID string                     `json:"cluster_uuid"`
	Status      string                     `json:"status,omitempty"`
	Indices     *ClusterStatsIndices       `json:"indices"`
	Nodes       *ClusterStatsNodes         `json:"nodes"`
}

// ClusterStatsNodesResponse contains counts of total, successful, and
// failed nodes when collecting cluster-wide statistics.
type ClusterStatsNodesResponse struct {
	Total      int                          `json:"total"`
	Successful int                          `json:"successful"`
	Failed     int                          `json:"failed"`
	Failures   []*types.FailedNodeException `json:"failures,omitempty"`
}

// ClusterStatsIndices aggregates index-level statistics across all indices
// in the cluster, including document counts, store size, fielddata usage,
// query cache stats, segment information, and analysis/mapping stats.
type ClusterStatsIndices struct {
	Count      int                            `json:"count"`
	Shards     *ClusterStatsIndicesShards     `json:"shards"`
	Docs       *ClusterStatsIndicesDocs       `json:"docs"`
	Store      *ClusterStatsIndicesStore      `json:"store"`
	FieldData  *ClusterStatsIndicesFieldData  `json:"fielddata"`
	QueryCache *ClusterStatsIndicesQueryCache `json:"query_cache"`
	Completion *ClusterStatsIndicesCompletion `json:"completion"`
	Segments   *IndexStatsSegments            `json:"segments"`
	Analysis   *ClusterStatsAnalysisStats     `json:"analysis"`
	Mappings   *ClusterStatsMappingStats      `json:"mappings"`
	Versions   []*ClusterStatsVersionStats    `json:"versions"`
}

// ClusterStatsAnalysisStats summarizes the types of character filters,
// tokenizers, filters, and analyzers used across all indices in the cluster.
type ClusterStatsAnalysisStats struct {
	CharFilterTypes    []IndexFeatureStats `json:"char_filter_types,omitempty"`
	TokenizerTypes     []IndexFeatureStats `json:"tokenizer_types,omitempty"`
	FilterTypes        []IndexFeatureStats `json:"filter_types,omitempty"`
	AnalyzerTypes      []IndexFeatureStats `json:"analyzer_types,omitempty"`
	BuiltInCharFilters []IndexFeatureStats `json:"built_in_char_filters,omitempty"`
	BuiltInTokenizers  []IndexFeatureStats `json:"built_in_tokenizers,omitempty"`
	BuiltInFilters     []IndexFeatureStats `json:"built_in_filters,omitempty"`
	BuiltInAnalyzers   []IndexFeatureStats `json:"built_in_analyzers,omitempty"`
}

// ClusterStatsMappingStats contains statistics about field types and
// runtime field types used across all index mappings in the cluster.
type ClusterStatsMappingStats struct {
	FieldTypes        []IndexFeatureStats `json:"field_types"`
	RuntimeFieldTypes []RuntimeFieldStats `json:"runtime_field_types"`
}

// IndexFeatureStats tracks usage counts for a named index feature,
// including how many indices use it and associated script counts.
type IndexFeatureStats struct {
	Name        string `json:"name"`
	Count       int    `json:"count"`
	IndexCount  int    `json:"index_count"`
	ScriptCount int    `json:"script_count"`
}

// RuntimeFieldStats provides detailed usage statistics for runtime fields,
// including character, line, and source size metrics as well as the
// scripting languages used.
type RuntimeFieldStats struct {
	Name            string   `json:"name"`
	Count           int      `json:"count"`
	IndexCount      int      `json:"index_count"`
	ScriptlessCount int      `json:"scriptless_count"`
	ShadowedCount   int      `json:"shadowed_count"`
	Lang            []string `json:"lang"`
	LinesMax        int64    `json:"lines_max"`
	LinesTotal      int64    `json:"lines_total"`
	CharsMax        int64    `json:"chars_max"`
	CharsTotal      int64    `json:"chars_total"`
	SourceMax       int64    `json:"source_max"`
	SourceTotal     int64    `json:"source_total"`
	DocMax          int64    `json:"doc_max"`
	DocTotal        int64    `json:"doc_total"`
}

// FieldScriptStats holds size metrics (lines, characters, source, doc)
// for script-heavy fields in the cluster.
type FieldScriptStats struct {
	LinesMax    int64 `json:"lines_max"`
	LinesTotal  int64 `json:"lines_total"`
	CharsMax    int64 `json:"chars_max"`
	CharsTotal  int64 `json:"chars_total"`
	SourceMax   int64 `json:"source_max"`
	SourceTotal int64 `json:"source_total"`
	DocMax      int64 `json:"doc_max"`
	DocTotal    int64 `json:"doc_total"`
}

// ClusterStatsVersionStats tracks how many indices and primary shards
// are running a specific OpenSearch version along with total primary size.
type ClusterStatsVersionStats struct {
	Version           string `json:"version"`
	IndexCount        int    `json:"index_count"`
	PrimaryShardCount int    `json:"primary_shard_count"`
	TotalPrimarySize  string `json:"total_primary_size,omitempty"`
	TotalPrimaryBytes int64  `json:"total_primary_bytes,omitempty"`
}

// ClusterStatsIndicesShards contains aggregate shard statistics for all
// indices in the cluster, including total shard count, primary count,
// replication factor, and per-index min/max/avg breakdowns.
type ClusterStatsIndicesShards struct {
	Total       int                             `json:"total"`
	Primaries   int                             `json:"primaries"`
	Replication float64                         `json:"replication"`
	Index       *ClusterStatsIndicesShardsIndex `json:"index"`
}

// ClusterStatsIndicesShardsIndex provides per-index min/max/avg statistics
// for shard counts, primary counts, and replication factors.
type ClusterStatsIndicesShardsIndex struct {
	Shards      *ClusterStatsIndicesShardsIndexIntMinMax     `json:"shards"`
	Primaries   *ClusterStatsIndicesShardsIndexIntMinMax     `json:"primaries"`
	Replication *ClusterStatsIndicesShardsIndexFloat64MinMax `json:"replication"`
}

// ClusterStatsIndicesShardsIndexIntMinMax holds minimum, maximum, and
// average integer statistics for a per-index shard metric.
type ClusterStatsIndicesShardsIndexIntMinMax struct {
	Min int     `json:"min"`
	Max int     `json:"max"`
	Avg float64 `json:"avg"`
}

// ClusterStatsIndicesShardsIndexFloat64MinMax holds minimum, maximum, and
// average float64 statistics for a per-index replication metric.
type ClusterStatsIndicesShardsIndexFloat64MinMax struct {
	Min float64 `json:"min"`
	Max float64 `json:"max"`
	Avg float64 `json:"avg"`
}

// ClusterStatsIndicesDocs holds aggregate document counts across all
// indices in the cluster, including deleted documents.
type ClusterStatsIndicesDocs struct {
	Count   int `json:"count"`
	Deleted int `json:"deleted"`
}

// ClusterStatsIndicesStore reports the total storage size used by all
// indices in the cluster in both human-readable and byte representations.
type ClusterStatsIndicesStore struct {
	Size                    string `json:"size"`
	SizeInBytes             int64  `json:"size_in_bytes"`
	TotalDataSetSize        string `json:"total_data_set_size,omitempty"`
	TotalDataSetSizeInBytes int64  `json:"total_data_set_size_in_bytes,omitempty"`
	Reserved                string `json:"reserved,omitempty"`
	ReservedInBytes         int64  `json:"reserved_in_bytes,omitempty"`
}

// ClusterStatsIndicesFieldData reports fielddata cache memory usage and
// eviction counts, optionally broken down by individual field name.
type ClusterStatsIndicesFieldData struct {
	MemorySize        string `json:"memory_size"`
	MemorySizeInBytes int64  `json:"memory_size_in_bytes"`
	Evictions         int64  `json:"evictions"`
	Fields            map[string]struct {
		MemorySize        string `json:"memory_size"`
		MemorySizeInBytes int64  `json:"memory_size_in_bytes"`
	} `json:"fields,omitempty"`
}

// ClusterStatsIndicesQueryCache reports query cache memory usage, hit/miss
// counts, cache size, and eviction statistics across the cluster.
type ClusterStatsIndicesQueryCache struct {
	MemorySize        string `json:"memory_size"`
	MemorySizeInBytes int64  `json:"memory_size_in_bytes"`
	TotalCount        int64  `json:"total_count"`
	HitCount          int64  `json:"hit_count"`
	MissCount         int64  `json:"miss_count"`
	CacheSize         int64  `json:"cache_size"`
	CacheCount        int64  `json:"cache_count"`
	Evictions         int64  `json:"evictions"`
}

// ClusterStatsIndicesCompletion reports completion suggester size in
// both human-readable and byte representations, optionally per field.
type ClusterStatsIndicesCompletion struct {
	Size        string `json:"size"`
	SizeInBytes int64  `json:"size_in_bytes"`
	Fields      map[string]struct {
		Size        string `json:"size"`
		SizeInBytes int64  `json:"size_in_bytes"`
	} `json:"fields,omitempty"`
}

// ClusterStatsIndicesSegmentsFile represents an individual segment file's
// size and optional description within the segments statistics.
type ClusterStatsIndicesSegmentsFile struct {
	Size        string `json:"size"`
	SizeInBytes int64  `json:"size_in_bytes"`
	Description string `json:"description,omitempty"`
}

// ClusterStatsNodes aggregates node-level statistics across the cluster,
// including node role counts, OS, JVM, filesystem, plugin, network,
// discovery, ingest, and packaging information.
type ClusterStatsNodes struct {
	Count    *ClusterStatsNodesCount        `json:"count"`
	Versions []string                       `json:"versions"`
	OS       *ClusterStatsNodesOsStats      `json:"os"`
	Process  *ClusterStatsNodesProcessStats `json:"process"`
	JVM      *ClusterStatsNodesJvmStats     `json:"jvm"`
	FS       *ClusterStatsNodesFsStats      `json:"fs"`
	Plugins  []*ClusterStatsNodesPlugin     `json:"plugins"`

	NetworkTypes   *ClusterStatsNodesNetworkTypes   `json:"network_types"`
	DiscoveryTypes *ClusterStatsNodesDiscoveryTypes `json:"discovery_types"`
	PackagingTypes *ClusterStatsNodesPackagingTypes `json:"packaging_types"`

	Ingest *ClusterStatsNodesIngest `json:"ingest"`
}

// ClusterStatsNodesCount breaks down the total node count by assigned roles
// such as data, master, ingest, ML, and coordinating-only nodes.
type ClusterStatsNodesCount struct {
	Total               int `json:"total"`
	Data                int `json:"data"`
	DataCold            int `json:"data_cold"`
	DataContent         int `json:"data_content"`
	DataFrozen          int `json:"data_frozen"`
	DataHot             int `json:"data_hot"`
	DataWarm            int `json:"data_warm"`
	CoordinatingOnly    int `json:"coordinating_only"`
	Master              int `json:"master"`
	Ingest              int `json:"ingest"`
	ML                  int `json:"ml"`
	RemoteClusterClient int `json:"remote_cluster_client"`
	Transform           int `json:"transform"`
	VotingOnly          int `json:"voting_only"`
}

// ClusterStatsNodesOsStats reports aggregate operating system statistics
// across cluster nodes, including processor counts, OS distribution,
// architecture, and memory utilization.
type ClusterStatsNodesOsStats struct {
	AvailableProcessors int `json:"available_processors"`
	AllocatedProcessors int `json:"allocated_processors"`
	Names               []struct {
		Name  string `json:"name"`
		Value int    `json:"count"`
	} `json:"names"`
	PrettyNames []struct {
		PrettyName string `json:"pretty_name"`
		Value      int    `json:"count"`
	} `json:"pretty_names"`
	Mem           *ClusterStatsNodesOsStatsMem `json:"mem"`
	Architectures []struct {
		Arch  string `json:"arch"`
		Count int    `json:"count"`
	} `json:"architectures"`
}

// ClusterStatsNodesOsStatsMem represents aggregate memory statistics across
// cluster nodes in both human-readable and byte representations,
// including total, free, used, and percentage values.
type ClusterStatsNodesOsStatsMem struct {
	Total        string `json:"total"`
	TotalInBytes int64  `json:"total_in_bytes"`
	Free         string `json:"free"`
	FreeInBytes  int64  `json:"free_in_bytes"`
	Used         string `json:"used"`
	UsedInBytes  int64  `json:"used_in_bytes"`
	FreePercent  int    `json:"free_percent"`
	UsedPercent  int    `json:"used_percent"`
}

// ClusterStatsNodesOsStatsCPU describes the CPU specifications of cluster
// nodes in aggregate, including vendor, model, core count, and cache size.
type ClusterStatsNodesOsStatsCPU struct {
	Vendor           string `json:"vendor"`
	Model            string `json:"model"`
	MHz              int    `json:"mhz"`
	TotalCores       int    `json:"total_cores"`
	TotalSockets     int    `json:"total_sockets"`
	CoresPerSocket   int    `json:"cores_per_socket"`
	CacheSize        string `json:"cache_size"`
	CacheSizeInBytes int64  `json:"cache_size_in_bytes"`
	Count            int    `json:"count"`
}

// ClusterStatsNodesProcessStats reports aggregate process-level statistics
// across cluster nodes, including CPU usage and open file descriptor counts.
type ClusterStatsNodesProcessStats struct {
	CPU                 *ClusterStatsNodesProcessStatsCPU                 `json:"cpu"`
	OpenFileDescriptors *ClusterStatsNodesProcessStatsOpenFileDescriptors `json:"open_file_descriptors"`
}

// ClusterStatsNodesProcessStatsCPU reports the average CPU percentage
// used by OpenSearch processes across the cluster.
type ClusterStatsNodesProcessStatsCPU struct {
	Percent float64 `json:"percent"`
}

// ClusterStatsNodesProcessStatsOpenFileDescriptors reports the min, max,
// and average open file descriptor counts across cluster nodes.
type ClusterStatsNodesProcessStatsOpenFileDescriptors struct {
	Min int64 `json:"min"`
	Max int64 `json:"max"`
	Avg int64 `json:"avg"`
}

// ClusterStatsNodesJvmStats reports aggregate JVM statistics across the
// cluster, including uptime, JVM versions, heap memory usage, and thread counts.
type ClusterStatsNodesJvmStats struct {
	MaxUptime         string                              `json:"max_uptime"`
	MaxUptimeInMillis int64                               `json:"max_uptime_in_millis"`
	Versions          []*ClusterStatsNodesJvmStatsVersion `json:"versions"`
	Mem               *ClusterStatsNodesJvmStatsMem       `json:"mem"`
	Threads           int64                               `json:"threads"`
}

// ClusterStatsNodesJvmStatsVersion describes a specific JVM version running
// on one or more nodes, including vendor, VM details, and node count.
type ClusterStatsNodesJvmStatsVersion struct {
	Version         string `json:"version"`
	VMName          string `json:"vm_name"`
	VMVersion       string `json:"vm_version"`
	VMVendor        string `json:"vm_vendor"`
	BundledJDK      bool   `json:"bundled_jdk"`
	UsingBundledJDK bool   `json:"using_bundled_jdk"`
	Count           int    `json:"count"`
}

// ClusterStatsNodesJvmStatsMem reports aggregate JVM heap memory statistics
// in both human-readable and byte representations.
type ClusterStatsNodesJvmStatsMem struct {
	HeapUsed        string `json:"heap_used"`
	HeapUsedInBytes int64  `json:"heap_used_in_bytes"`
	HeapMax         string `json:"heap_max"`
	HeapMaxInBytes  int64  `json:"heap_max_in_bytes"`
}

// ClusterStatsNodesFsStats reports aggregate filesystem statistics across
// cluster nodes, including total, free, and available disk space, as well
// as I/O read/write metrics.
type ClusterStatsNodesFsStats struct {
	Path                 string `json:"path"`
	Mount                string `json:"mount"`
	Dev                  string `json:"dev"`
	Total                string `json:"total"`
	TotalInBytes         int64  `json:"total_in_bytes"`
	Free                 string `json:"free"`
	FreeInBytes          int64  `json:"free_in_bytes"`
	Available            string `json:"available"`
	AvailableInBytes     int64  `json:"available_in_bytes"`
	DiskReads            int64  `json:"disk_reads"`
	DiskWrites           int64  `json:"disk_writes"`
	DiskIOOp             int64  `json:"disk_io_op"`
	DiskReadSize         string `json:"disk_read_size"`
	DiskReadSizeInBytes  int64  `json:"disk_read_size_in_bytes"`
	DiskWriteSize        string `json:"disk_write_size"`
	DiskWriteSizeInBytes int64  `json:"disk_write_size_in_bytes"`
	DiskIOSize           string `json:"disk_io_size"`
	DiskIOSizeInBytes    int64  `json:"disk_io_size_in_bytes"`
	DiskQueue            string `json:"disk_queue"`
	DiskServiceTime      string `json:"disk_service_time"`
}

// ClusterStatsNodesPlugin describes a plugin installed on cluster nodes,
// including its name, version, and whether it is a JVM or site plugin.
type ClusterStatsNodesPlugin struct {
	Name        string `json:"name"`
	Version     string `json:"version"`
	Description string `json:"description"`
	URL         string `json:"url"`
	JVM         bool   `json:"jvm"`
	Site        bool   `json:"site"`
}

// ClusterStatsNodesNetworkTypes reports the network transport and HTTP
// types used across cluster nodes.
type ClusterStatsNodesNetworkTypes struct {
	TransportTypes map[string]any `json:"transport_types"`
	HTTPTypes      map[string]any `json:"http_types"`
}

// ClusterStatsNodesDiscoveryTypes represents the discovery mechanism types
// in use across the cluster. It is an alias for any JSON-serializable value.
type ClusterStatsNodesDiscoveryTypes any

// ClusterStatsNodesPackagingTypes is a list of packaging type entries
// describing how OpenSearch is distributed across cluster nodes.
type ClusterStatsNodesPackagingTypes []*ClusterStatsNodesPackagingType

// ClusterStatsNodesPackagingType describes a single packaging distribution
// (flavor and type) and how many nodes use it.
type ClusterStatsNodesPackagingType struct {
	Flavor string `json:"flavor"`
	Type   string `json:"type"`
	Count  int    `json:"count"`
}

// ClusterStatsNodesIngest reports ingest pipeline statistics at the
// cluster level, including the number of pipelines and per-processor stats.
type ClusterStatsNodesIngest struct {
	NumberOfPipelines int            `json:"number_of_pipelines"`
	ProcessorStats    map[string]any `json:"processor_stats"`
}

// ClusterRerouteResponse represents the result of a cluster reroute request.
// It contains the updated cluster state and, when explain is enabled,
// a list of explanations for each reroute decision.
type ClusterRerouteResponse struct {
	State        *ClusterStateResponse `json:"state"`
	Explanations []RerouteExplanation  `json:"explanations,omitempty"`
}

// RerouteExplanation describes a single reroute command and the allocation
// decisions made for it, including the command name and its parameters.
type RerouteExplanation struct {
	Command    string            `json:"command"`
	Parameters map[string]any    `json:"parameters"`
	Decisions  []RerouteDecision `json:"decisions"`
}

// RerouteDecision represents an individual allocation decision made by
// the cluster allocator during a reroute operation.
// The concrete type depends on the allocator output and is unstructured.
type RerouteDecision any

// ClusterAllocationExplainResponse represents shard allocation explanation
// from the cluster allocation explain API.
type ClusterAllocationExplainResponse struct {
	Index          string         `json:"index"`
	Shard          int            `json:"shard"`
	Primary        bool           `json:"primary"`
	CurrentState   string         `json:"current_state"`
	UnassignedInfo map[string]any `json:"unassigned_info,omitempty"`
	Explanation    string         `json:"explanation,omitempty"`
}

// ClusterPendingTasksResponse represents pending cluster tasks.
type ClusterPendingTasksResponse struct {
	Tasks []map[string]any `json:"tasks"`
}

// ClusterDecommissionAwarenessResponse represents decommission awareness state.
type ClusterDecommissionAwarenessResponse struct {
	Status             string `json:"status,omitempty"`
	DecommissionStatus string `json:"decommission_status,omitempty"`
}

// ClusterWeightedRoutingResponse represents weighted routing awareness state.
type ClusterWeightedRoutingResponse struct {
	Weights map[string]int `json:"weights,omitempty"`
}
