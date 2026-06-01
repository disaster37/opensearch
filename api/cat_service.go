package api

import (
	"context"
	"fmt"
	"strings"

	json "github.com/goccy/go-json"

	"github.com/go-resty/resty/v2"
	"github.com/sirupsen/logrus"
)

// CatService provides access to the OpenSearch Cat API group, which returns
// human- and machine-readable data in plain text format (requested as JSON
// by this client). Cat APIs are commonly used for quick cluster inspection.
//
// Example usage:
//
//	indices, err := svc.Indices(ctx, nil)
//	for _, idx := range indices {
//	    fmt.Println(idx.Index, idx.Health, idx.DocsCount)
//	}
type CatService interface {
	// Indices returns cat-style information about indices including health,
	// status, shard counts, document counts, and storage sizes.
	// Pass nil for indices to list all indices.
	Indices(ctx context.Context, indices []string) (CatIndicesResponse, error)

	// Shards returns cat-style information about shard allocation across nodes,
	// including state, document count, and store size per shard.
	// Pass nil for indices to list shards from all indices.
	Shards(ctx context.Context, indices []string) (CatShardsResponse, error)

	// Aliases returns cat-style information about index aliases.
	// Pass nil for names to list all aliases.
	Aliases(ctx context.Context, names []string) (CatAliasesResponse, error)

	// Health returns a compact cluster health overview from the cat API.
	Health(ctx context.Context) (CatHealthResponse, error)

	// Count returns the total document count for the cluster or specific indices.
	// Pass nil for indices to count across all indices.
	Count(ctx context.Context, indices []string) (CatCountResponse, error)

	// Allocation returns shard allocation information per node, including
	// disk usage statistics. Pass nil for nodeIds to list all nodes.
	Allocation(ctx context.Context, nodeIds []string) (CatAllocationResponse, error)

	// Fielddata returns fielddata memory usage per node and per field.
	// Pass nil for fields to list all fields.
	Fielddata(ctx context.Context, fields []string) (CatFielddataResponse, error)

	// Snapshots returns cat-style information about snapshots in a repository.
	// An empty repository string lists snapshots from all repositories.
	Snapshots(ctx context.Context, repository string) (CatSnapshotsResponse, error)

	// Master returns information about the currently elected cluster manager node.
	Master(ctx context.Context) (CatMasterResponse, error)

	// Help returns the list of available cat API endpoints as plain text.
	Help(ctx context.Context) (string, error)

	// NodeAttrs returns cat-style information about custom node attributes
	// configured on each node in the cluster.
	NodeAttrs(ctx context.Context) (CatNodeAttrsResponse, error)

	// CatNodes returns cat-style information about all nodes in the cluster,
	// including heap, CPU, load, role, and version details.
	CatNodes(ctx context.Context) (CatNodesResponse, error)

	// CatPendingTasks returns the list of cluster-level pending tasks,
	// including priority, time in queue, and source.
	CatPendingTasks(ctx context.Context) (CatPendingTasksResponse, error)

	// Plugins returns information about plugins installed on each node.
	Plugins(ctx context.Context) (CatPluginsResponse, error)

	// CatRecovery returns shard recovery information for the cluster.
	// Pass nil for indices to list recoveries across all indices.
	CatRecovery(ctx context.Context, indices []string) (CatRecoveryResponse, error)

	// Repositories returns information about registered snapshot repositories.
	Repositories(ctx context.Context) (CatRepositoriesResponse, error)

	// Segments returns low-level Lucene segment information per shard.
	// Pass nil for indices to list segments across all indices.
	Segments(ctx context.Context, indices []string) (CatSegmentsResponse, error)

	// SegmentReplication returns segment replication checkpoint information.
	// Pass nil for indices to list across all indices.
	SegmentReplication(ctx context.Context, indices []string) (CatSegmentReplicationResponse, error)

	// CatTasks returns the list of currently running tasks on the cluster
	// from the cat tasks API.
	CatTasks(ctx context.Context) (CatTasksResponse, error)

	// Templates returns information about index templates.
	// Pass an empty string to list all templates.
	Templates(ctx context.Context, name string) (CatTemplatesResponse, error)

	// ThreadPool returns thread pool statistics per node.
	// Pass nil for patterns to list all thread pools.
	ThreadPool(ctx context.Context, patterns []string) (CatThreadPoolResponse, error)

	// ClusterManager returns information about the currently elected cluster
	// manager node via the /_cat/cluster_manager endpoint, which is the
	// non-deprecated alias for /_cat/master.
	ClusterManager(ctx context.Context) (CatClusterManagerResponse, error)
}

// DefaultCatService is the default implementation of CatService,
// backed by an HTTP client and a structured logger.
type DefaultCatService struct {
	client *resty.Client
	logger *logrus.Entry
}

// NewCatService creates a new CatService using the provided HTTP client
// and logger. The logger is scoped with a "service=cat" field.
func NewCatService(client *resty.Client, logger *logrus.Entry) CatService {
	return &DefaultCatService{client: client, logger: logger.WithField("service", "cat")}
}

// Indices retrieves compact index information from the /_cat/indices endpoint.
// If indices is non-empty, only the specified indices are returned.
// Returns a CatIndicesResponse containing rows with health, status, doc counts,
// store sizes, and other per-index metrics.
func (s *DefaultCatService) Indices(ctx context.Context, indices []string) (CatIndicesResponse, error) {
	path := "/_cat/indices"
	if len(indices) > 0 {
		path = fmt.Sprintf("/_cat/indices/%s", strings.Join(indices, ","))
	}

	resp, err := s.client.R().SetContext(ctx).
		SetHeader("Accept", "application/json").
		SetQueryParam("format", "json").
		Get(path)
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result CatIndicesResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return result, nil
}

// Shards retrieves compact shard information from the /_cat/shards endpoint.
// If indices is non-empty, only shards of the specified indices are returned.
// Returns a CatShardsResponse containing rows with index, shard number,
// state, node, and per-shard metrics.
func (s *DefaultCatService) Shards(ctx context.Context, indices []string) (CatShardsResponse, error) {
	path := "/_cat/shards"
	if len(indices) > 0 {
		path = fmt.Sprintf("/_cat/shards/%s", strings.Join(indices, ","))
	}

	resp, err := s.client.R().SetContext(ctx).
		SetHeader("Accept", "application/json").
		SetQueryParam("format", "json").
		Get(path)
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result CatShardsResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return result, nil
}

// Aliases retrieves alias information from the /_cat/aliases endpoint.
// If names is non-empty, only the specified alias names are returned.
// Returns a CatAliasesResponse containing rows with alias, index, filter,
// and routing information.
func (s *DefaultCatService) Aliases(ctx context.Context, names []string) (CatAliasesResponse, error) {
	path := "/_cat/aliases"
	if len(names) > 0 {
		path = fmt.Sprintf("/_cat/aliases/%s", strings.Join(names, ","))
	}

	resp, err := s.client.R().SetContext(ctx).
		SetHeader("Accept", "application/json").
		SetQueryParam("format", "json").
		Get(path)
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result CatAliasesResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return result, nil
}

// Health retrieves a compact cluster health overview from the /_cat/health endpoint.
// Returns a CatHealthResponse containing a row with cluster name, status,
// node and shard counts, and active shard percentage.
func (s *DefaultCatService) Health(ctx context.Context) (CatHealthResponse, error) {
	resp, err := s.client.R().SetContext(ctx).
		SetHeader("Accept", "application/json").
		SetQueryParam("format", "json").
		Get("/_cat/health")
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result CatHealthResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return result, nil
}

// Count retrieves document counts from the /_cat/count endpoint.
// If indices is non-empty, only the specified indices are counted.
// Returns a CatCountResponse containing a row with epoch, timestamp, and count.
func (s *DefaultCatService) Count(ctx context.Context, indices []string) (CatCountResponse, error) {
	path := "/_cat/count"
	if len(indices) > 0 {
		path = fmt.Sprintf("/_cat/count/%s", strings.Join(indices, ","))
	}

	resp, err := s.client.R().SetContext(ctx).
		SetHeader("Accept", "application/json").
		SetQueryParam("format", "json").
		Get(path)
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result CatCountResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return result, nil
}

// Allocation retrieves shard allocation and disk usage information per node
// from the /_cat/allocation endpoint.
// If nodeIds is non-empty, only the specified nodes are returned.
// Returns a CatAllocationResponse containing rows with shard count, disk usage,
// and node details.
func (s *DefaultCatService) Allocation(ctx context.Context, nodeIds []string) (CatAllocationResponse, error) {
	path := "/_cat/allocation"
	if len(nodeIds) > 0 {
		path = fmt.Sprintf("/_cat/allocation/%s", strings.Join(nodeIds, ","))
	}

	resp, err := s.client.R().SetContext(ctx).
		SetHeader("Accept", "application/json").
		SetQueryParam("format", "json").
		Get(path)
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result CatAllocationResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return result, nil
}

// Fielddata retrieves fielddata memory usage per node and per field from
// the /_cat/fielddata endpoint.
// If fields is non-empty, only the specified fields are returned.
// Returns a CatFielddataResponse containing rows with node and field memory info.
func (s *DefaultCatService) Fielddata(ctx context.Context, fields []string) (CatFielddataResponse, error) {
	path := "/_cat/fielddata"
	if len(fields) > 0 {
		path = fmt.Sprintf("/_cat/fielddata/%s", strings.Join(fields, ","))
	}

	resp, err := s.client.R().SetContext(ctx).
		SetHeader("Accept", "application/json").
		SetQueryParam("format", "json").
		Get(path)
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result CatFielddataResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return result, nil
}

// Snapshots retrieves snapshot information from the /_cat/snapshots endpoint.
// If repository is non-empty, only snapshots from that repository are returned.
// Returns a CatSnapshotsResponse containing rows with snapshot ID, status,
// timing, and shard counts.
func (s *DefaultCatService) Snapshots(ctx context.Context, repository string) (CatSnapshotsResponse, error) {
	path := "/_cat/snapshots"
	if repository != "" {
		path = fmt.Sprintf("/_cat/snapshots/%s", repository)
	}

	resp, err := s.client.R().SetContext(ctx).
		SetHeader("Accept", "application/json").
		SetQueryParam("format", "json").
		Get(path)
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result CatSnapshotsResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return result, nil
}

// Master retrieves information about the currently elected cluster manager
// from the /_cat/master endpoint.
// Returns a CatMasterResponse containing a row with the master node's ID,
// host, IP, and name.
func (s *DefaultCatService) Master(ctx context.Context) (CatMasterResponse, error) {
	resp, err := s.client.R().SetContext(ctx).
		SetHeader("Accept", "application/json").
		SetQueryParam("format", "json").
		Get("/_cat/master")
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result CatMasterResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return result, nil
}

// Help retrieves the list of available cat API endpoints from the /_cat endpoint.
// The response is plain text, not JSON.
func (s *DefaultCatService) Help(ctx context.Context) (string, error) {
	resp, err := s.client.R().SetContext(ctx).Get("/_cat")
	if err != nil {
		return "", wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return "", logAndReturnError(s.logger, resp)
	}
	return string(resp.Body()), nil
}

// NodeAttrs retrieves custom node attribute information from the /_cat/nodeattrs endpoint.
// Returns a CatNodeAttrsResponse containing rows with node, host, IP, attribute name, and value.
func (s *DefaultCatService) NodeAttrs(ctx context.Context) (CatNodeAttrsResponse, error) {
	resp, err := s.client.R().SetContext(ctx).
		SetHeader("Accept", "application/json").
		SetQueryParam("format", "json").
		Get("/_cat/nodeattrs")
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result CatNodeAttrsResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return result, nil
}

// CatNodes retrieves node information from the /_cat/nodes endpoint.
// Returns a CatNodesResponse containing rows with IP, heap, CPU, load, role, and version data.
func (s *DefaultCatService) CatNodes(ctx context.Context) (CatNodesResponse, error) {
	resp, err := s.client.R().SetContext(ctx).
		SetHeader("Accept", "application/json").
		SetQueryParam("format", "json").
		Get("/_cat/nodes")
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result CatNodesResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return result, nil
}

// CatPendingTasks retrieves cluster-level pending tasks from the /_cat/pending_tasks endpoint.
// Returns a CatPendingTasksResponse containing rows with insert order, time in queue,
// priority, and source.
func (s *DefaultCatService) CatPendingTasks(ctx context.Context) (CatPendingTasksResponse, error) {
	resp, err := s.client.R().SetContext(ctx).
		SetHeader("Accept", "application/json").
		SetQueryParam("format", "json").
		Get("/_cat/pending_tasks")
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result CatPendingTasksResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return result, nil
}

// Plugins retrieves plugin information from the /_cat/plugins endpoint.
// Returns a CatPluginsResponse containing rows with plugin name, component, and version.
func (s *DefaultCatService) Plugins(ctx context.Context) (CatPluginsResponse, error) {
	resp, err := s.client.R().SetContext(ctx).
		SetHeader("Accept", "application/json").
		SetQueryParam("format", "json").
		Get("/_cat/plugins")
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result CatPluginsResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return result, nil
}

// CatRecovery retrieves shard recovery information from the /_cat/recovery endpoint.
// If indices is non-empty, only recoveries for the specified indices are returned.
// Returns a CatRecoveryResponse containing rows with recovery stage, timing, and progress.
func (s *DefaultCatService) CatRecovery(ctx context.Context, indices []string) (CatRecoveryResponse, error) {
	path := "/_cat/recovery"
	if len(indices) > 0 {
		path = fmt.Sprintf("/_cat/recovery/%s", strings.Join(indices, ","))
	}

	resp, err := s.client.R().SetContext(ctx).
		SetHeader("Accept", "application/json").
		SetQueryParam("format", "json").
		Get(path)
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result CatRecoveryResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return result, nil
}

// Repositories retrieves registered snapshot repository information from the
// /_cat/repositories endpoint.
// Returns a CatRepositoriesResponse containing rows with repository ID and type.
func (s *DefaultCatService) Repositories(ctx context.Context) (CatRepositoriesResponse, error) {
	resp, err := s.client.R().SetContext(ctx).
		SetHeader("Accept", "application/json").
		SetQueryParam("format", "json").
		Get("/_cat/repositories")
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result CatRepositoriesResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return result, nil
}

// Segments retrieves low-level Lucene segment information from the /_cat/segments endpoint.
// If indices is non-empty, only segments for the specified indices are returned.
// Returns a CatSegmentsResponse containing rows with index, shard, segment ID, version, and size.
func (s *DefaultCatService) Segments(ctx context.Context, indices []string) (CatSegmentsResponse, error) {
	path := "/_cat/segments"
	if len(indices) > 0 {
		path = fmt.Sprintf("/_cat/segments/%s", strings.Join(indices, ","))
	}

	resp, err := s.client.R().SetContext(ctx).
		SetHeader("Accept", "application/json").
		SetQueryParam("format", "json").
		Get(path)
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result CatSegmentsResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return result, nil
}

// SegmentReplication retrieves segment replication checkpoint information from the
// /_cat/segment_replication endpoint.
// If indices is non-empty, only segment replication data for the specified indices is returned.
// Returns a CatSegmentReplicationResponse containing rows with shard, checkpoint, and timing data.
func (s *DefaultCatService) SegmentReplication(ctx context.Context, indices []string) (CatSegmentReplicationResponse, error) {
	path := "/_cat/segment_replication"
	if len(indices) > 0 {
		path = fmt.Sprintf("/_cat/segment_replication/%s", strings.Join(indices, ","))
	}

	resp, err := s.client.R().SetContext(ctx).
		SetHeader("Accept", "application/json").
		SetQueryParam("format", "json").
		Get(path)
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result CatSegmentReplicationResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return result, nil
}

// CatTasks retrieves currently running task information from the /_cat/tasks endpoint.
// Returns a CatTasksResponse containing rows with task ID, action, node, running time, and type.
func (s *DefaultCatService) CatTasks(ctx context.Context) (CatTasksResponse, error) {
	resp, err := s.client.R().SetContext(ctx).
		SetHeader("Accept", "application/json").
		SetQueryParam("format", "json").
		Get("/_cat/tasks")
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result CatTasksResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return result, nil
}

// Templates retrieves index template information from the /_cat/templates endpoint.
// If name is non-empty, only the specified template is returned.
// Returns a CatTemplatesResponse containing rows with name, index patterns, order, and version.
func (s *DefaultCatService) Templates(ctx context.Context, name string) (CatTemplatesResponse, error) {
	path := "/_cat/templates"
	if name != "" {
		path = fmt.Sprintf("/_cat/templates/%s", name)
	}

	resp, err := s.client.R().SetContext(ctx).
		SetHeader("Accept", "application/json").
		SetQueryParam("format", "json").
		Get(path)
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result CatTemplatesResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return result, nil
}

// ThreadPool retrieves thread pool statistics from the /_cat/thread_pool endpoint.
// If patterns is non-empty, only thread pools matching the given patterns are returned.
// Returns a CatThreadPoolResponse containing rows with node name, pool name, active count,
// queue, rejected, and completed counters.
func (s *DefaultCatService) ThreadPool(ctx context.Context, patterns []string) (CatThreadPoolResponse, error) {
	path := "/_cat/thread_pool"
	if len(patterns) > 0 {
		path = fmt.Sprintf("/_cat/thread_pool/%s", strings.Join(patterns, ","))
	}

	resp, err := s.client.R().SetContext(ctx).
		SetHeader("Accept", "application/json").
		SetQueryParam("format", "json").
		Get(path)
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result CatThreadPoolResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return result, nil
}

// ClusterManager retrieves information about the currently elected cluster manager
// from the /_cat/cluster_manager endpoint, which is the non-deprecated alias
// for /_cat/master.
// Returns a CatClusterManagerResponse containing a row with the manager node's
// IP, ID, host, and name.
func (s *DefaultCatService) ClusterManager(ctx context.Context) (CatClusterManagerResponse, error) {
	resp, err := s.client.R().SetContext(ctx).
		SetHeader("Accept", "application/json").
		SetQueryParam("format", "json").
		Get("/_cat/cluster_manager")
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result CatClusterManagerResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return result, nil
}
