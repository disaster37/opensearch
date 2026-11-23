package api

import (
	"context"
	"fmt"
	"strings"

	json "github.com/goccy/go-json"

	"github.com/go-resty/resty/v2"
	"github.com/sirupsen/logrus"
)

// NodesService provides access to the OpenSearch nodes APIs: node information
// (GET /_nodes), node statistics (GET /_nodes/stats), hot threads, secure
// settings reload, and node feature usage. Statistics may be scoped by node ID
// and metric, including [NodesStatsMetricResourceUsageStats] for cgroup-aware
// CPU, memory, and I/O utilization.
type NodesService interface {
	// Info returns static information about cluster nodes from GET /_nodes.
	Info(ctx context.Context, req *NodesInfoRequest) (*NodesInfoResponse, error)
	// Stats returns runtime statistics from GET /_nodes/stats. Pass a nil
	// request to collect all metrics for all nodes.
	Stats(ctx context.Context, req *NodesStatsRequest) (*NodesStatsResponse, error)
	// HotThreads returns the raw hot threads report from GET /_nodes/hot_threads.
	HotThreads(ctx context.Context, nodeIds []string) (string, error)
	// ReloadSecureSettings reloads secure settings on all nodes.
	ReloadSecureSettings(ctx context.Context, body any) (*NodesReloadSecureSettingsResponse, error)
	// Usage returns feature usage statistics from GET /_nodes/usage.
	Usage(ctx context.Context, req *NodesUsageRequest) (*NodesUsageResponse, error)
}

// DefaultNodesService is the default [NodesService] implementation.
type DefaultNodesService struct {
	client *resty.Client
	logger *logrus.Entry
}

// NewNodesService returns a [NodesService] backed by client, logging under the
// "nodes" service field.
func NewNodesService(client *resty.Client, logger *logrus.Entry) NodesService {
	return &DefaultNodesService{client: client, logger: logger.WithField("service", "nodes")}
}

// Info returns static information about cluster nodes from GET /_nodes.
// A nil request collects all nodes and all metrics.
func (s *DefaultNodesService) Info(ctx context.Context, req *NodesInfoRequest) (*NodesInfoResponse, error) {
	if req == nil {
		req = &NodesInfoRequest{}
	}

	nodeId := "_all"
	if len(req.NodeIds) > 0 {
		nodeId = strings.Join(req.NodeIds, ",")
	}
	metric := "_all"
	if len(req.Metrics) > 0 {
		metric = strings.Join(req.Metrics, ",")
	}
	path := fmt.Sprintf("/_nodes/%s/%s", nodeId, metric)

	resp, err := s.client.R().SetContext(ctx).Get(path)
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result NodesInfoResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

// Stats returns runtime statistics from GET /_nodes/stats. A nil request
// collects all metrics for all nodes. Set Metrics to
// [NodesStatsMetricResourceUsageStats] to collect cgroup-aware CPU, memory, and
// I/O utilization, then read percentages with [NodesStatsNode.ResourceCPUUtilization].
func (s *DefaultNodesService) Stats(ctx context.Context, req *NodesStatsRequest) (*NodesStatsResponse, error) {
	if req == nil {
		req = &NodesStatsRequest{}
	}

	var path string

	if len(req.NodeIds) > 0 && len(req.Metrics) > 0 {
		path = fmt.Sprintf("/_nodes/%s/stats/%s", strings.Join(req.NodeIds, ","), strings.Join(req.Metrics, ","))
	} else if len(req.NodeIds) > 0 {
		path = fmt.Sprintf("/_nodes/%s/stats", strings.Join(req.NodeIds, ","))
	} else if len(req.Metrics) > 0 {
		path = fmt.Sprintf("/_nodes/stats/%s", strings.Join(req.Metrics, ","))
	} else {
		path = "/_nodes/stats"
	}

	r := s.client.R().SetContext(ctx)
	if req.Detailed {
		r.SetQueryParam("detailed", "true")
	}

	resp, err := r.Get(path)
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result NodesStatsResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

// HotThreads returns the raw hot threads report from GET /_nodes/hot_threads
// for the given node IDs, or all nodes when nodeIds is empty.
func (s *DefaultNodesService) HotThreads(ctx context.Context, nodeIds []string) (string, error) {
	var path string
	if len(nodeIds) > 0 {
		path = fmt.Sprintf("/_nodes/%s/hot_threads", strings.Join(nodeIds, ","))
	} else {
		path = "/_nodes/hot_threads"
	}

	resp, err := s.client.R().SetContext(ctx).Get(path)
	if err != nil {
		return "", wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return "", logAndReturnError(s.logger, resp)
	}

	return string(resp.Body()), nil
}

// ReloadSecureSettings reloads secure settings on all nodes via
// POST /_nodes/reload_secure_settings. A nil body sends an empty request.
func (s *DefaultNodesService) ReloadSecureSettings(ctx context.Context, body any) (*NodesReloadSecureSettingsResponse, error) {
	req := s.client.R().SetContext(ctx)
	if body != nil {
		req.SetBody(body)
	}

	resp, err := req.Post("/_nodes/reload_secure_settings")
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result NodesReloadSecureSettingsResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

// Usage returns feature usage statistics from GET /_nodes/usage. A nil request
// collects usage for all nodes.
func (s *DefaultNodesService) Usage(ctx context.Context, req *NodesUsageRequest) (*NodesUsageResponse, error) {
	if req == nil {
		req = &NodesUsageRequest{}
	}

	var path string

	if len(req.NodeIds) > 0 && len(req.Metrics) > 0 {
		path = fmt.Sprintf("/_nodes/%s/usage/%s", strings.Join(req.NodeIds, ","), strings.Join(req.Metrics, ","))
	} else if len(req.NodeIds) > 0 {
		path = fmt.Sprintf("/_nodes/%s/usage", strings.Join(req.NodeIds, ","))
	} else {
		path = "/_nodes/usage"
	}

	resp, err := s.client.R().SetContext(ctx).Get(path)
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result NodesUsageResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}
