package api

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	json "github.com/goccy/go-json"

	"github.com/disaster37/opensearch/v4/types"
	"github.com/go-resty/resty/v2"
	"github.com/sirupsen/logrus"
)

type ClusterService interface {
	Health(ctx context.Context, indices []string) (*ClusterHealthResponse, error)
	State(ctx context.Context, req *ClusterStateRequest) (*ClusterStateResponse, error)
	Stats(ctx context.Context, nodeIds []string) (*ClusterStatsResponse, error)
	Reroute(ctx context.Context, body any) (*ClusterRerouteResponse, error)
	GetSettings(ctx context.Context) (map[string]any, error)
	PutSettings(ctx context.Context, body any) (map[string]any, error)
	AllocationExplain(ctx context.Context, body any) (*ClusterAllocationExplainResponse, error)
	PendingTasks(ctx context.Context) (*ClusterPendingTasksResponse, error)
	RemoteInfo(ctx context.Context) (map[string]any, error)
	ExistsComponentTemplate(ctx context.Context, name string) (bool, error)
	PutDecommissionAwareness(ctx context.Context, attributeName string, attributeValue string) (*types.AcknowledgedResponse, error)
	GetDecommissionAwareness(ctx context.Context, attributeName string) (*ClusterDecommissionAwarenessResponse, error)
	DeleteDecommissionAwareness(ctx context.Context) (*types.AcknowledgedResponse, error)
	PutWeightedRouting(ctx context.Context, attribute string, body any) (*ClusterWeightedRoutingResponse, error)
	GetWeightedRouting(ctx context.Context, attribute string) (*ClusterWeightedRoutingResponse, error)
	DeleteWeightedRouting(ctx context.Context) (*types.AcknowledgedResponse, error)
	PostVotingConfigExclusions(ctx context.Context, params map[string]string) (*types.AcknowledgedResponse, error)
	DeleteVotingConfigExclusions(ctx context.Context, waitForRemoval bool) (*types.AcknowledgedResponse, error)
}

type DefaultClusterService struct {
	client *resty.Client
	logger *logrus.Entry
}

func NewClusterService(client *resty.Client, logger *logrus.Entry) ClusterService {
	return &DefaultClusterService{client: client, logger: logger.WithField("service", "cluster")}
}

func (s *DefaultClusterService) Health(ctx context.Context, indices []string) (*ClusterHealthResponse, error) {
	path := "/_cluster/health"
	if len(indices) > 0 {
		path = fmt.Sprintf("/_cluster/health/%s", strings.Join(indices, ","))
	}

	resp, err := s.client.R().SetContext(ctx).Get(path)
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result ClusterHealthResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultClusterService) State(ctx context.Context, req *ClusterStateRequest) (*ClusterStateResponse, error) {
	m := "_all"
	if len(req.Metrics) > 0 {
		m = strings.Join(req.Metrics, ",")
	}
	idx := "_all"
	if len(req.Indices) > 0 {
		idx = strings.Join(req.Indices, ",")
	}
	path := fmt.Sprintf("/_cluster/state/%s/%s", m, idx)

	resp, err := s.client.R().SetContext(ctx).Get(path)
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result ClusterStateResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultClusterService) Stats(ctx context.Context, nodeIds []string) (*ClusterStatsResponse, error) {
	path := "/_cluster/stats"
	if len(nodeIds) > 0 {
		path = fmt.Sprintf("/_cluster/stats/nodes/%s", strings.Join(nodeIds, ","))
	}

	resp, err := s.client.R().SetContext(ctx).Get(path)
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result ClusterStatsResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultClusterService) Reroute(ctx context.Context, body any) (*ClusterRerouteResponse, error) {
	if body == nil {
		return nil, fmt.Errorf("body is required")
	}

	resp, err := s.client.R().SetContext(ctx).SetBody(body).Post("/_cluster/reroute")
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result ClusterRerouteResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultClusterService) GetSettings(ctx context.Context) (map[string]any, error) {
	resp, err := s.client.R().SetContext(ctx).Get("/_cluster/settings")
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	result := map[string]any{}
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return result, nil
}

func (s *DefaultClusterService) PutSettings(ctx context.Context, body any) (map[string]any, error) {
	if body == nil {
		return nil, fmt.Errorf("body is required")
	}

	resp, err := s.client.R().SetContext(ctx).SetBody(body).Put("/_cluster/settings")
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	result := map[string]any{}
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return result, nil
}

func (s *DefaultClusterService) AllocationExplain(ctx context.Context, body any) (*ClusterAllocationExplainResponse, error) {
	req := s.client.R().SetContext(ctx)
	if body != nil {
		req.SetBody(body)
	}

	var resp *resty.Response
	var err error
	if body != nil {
		resp, err = req.Post("/_cluster/allocation/explain")
	} else {
		resp, err = req.Get("/_cluster/allocation/explain")
	}
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result ClusterAllocationExplainResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultClusterService) PendingTasks(ctx context.Context) (*ClusterPendingTasksResponse, error) {
	resp, err := s.client.R().SetContext(ctx).Get("/_cluster/pending_tasks")
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result ClusterPendingTasksResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultClusterService) RemoteInfo(ctx context.Context) (map[string]any, error) {
	resp, err := s.client.R().SetContext(ctx).Get("/_remote/info")
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	result := map[string]any{}
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return result, nil
}

func (s *DefaultClusterService) ExistsComponentTemplate(ctx context.Context, name string) (bool, error) {
	if name == "" {
		return false, fmt.Errorf("name is required")
	}

	resp, err := s.client.R().
		SetContext(ctx).
		Head(fmt.Sprintf("/_component_template/%s", name))
	if err != nil {
		return false, wrapNetworkError(s.logger, err)
	}

	switch resp.StatusCode() {
	case http.StatusOK:
		return true, nil
	case http.StatusNotFound:
		return false, nil
	default:
		return false, fmt.Errorf("unexpected HTTP status %d", resp.StatusCode())
	}
}

func (s *DefaultClusterService) PutDecommissionAwareness(ctx context.Context, attributeName string, attributeValue string) (*types.AcknowledgedResponse, error) {
	if attributeName == "" {
		return nil, fmt.Errorf("attributeName is required")
	}
	if attributeValue == "" {
		return nil, fmt.Errorf("attributeValue is required")
	}

	resp, err := s.client.R().
		SetContext(ctx).
		Put(fmt.Sprintf("/_cluster/decommission/awareness/%s/%s", attributeName, attributeValue))
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result types.AcknowledgedResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultClusterService) GetDecommissionAwareness(ctx context.Context, attributeName string) (*ClusterDecommissionAwarenessResponse, error) {
	if attributeName == "" {
		return nil, fmt.Errorf("attributeName is required")
	}

	resp, err := s.client.R().
		SetContext(ctx).
		Get(fmt.Sprintf("/_cluster/decommission/awareness/%s/_status", attributeName))
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result ClusterDecommissionAwarenessResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultClusterService) DeleteDecommissionAwareness(ctx context.Context) (*types.AcknowledgedResponse, error) {
	resp, err := s.client.R().SetContext(ctx).Delete("/_cluster/decommission/awareness/")
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result types.AcknowledgedResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultClusterService) PutWeightedRouting(ctx context.Context, attribute string, body any) (*ClusterWeightedRoutingResponse, error) {
	if attribute == "" {
		return nil, fmt.Errorf("attribute is required")
	}

	r := s.client.R().SetContext(ctx)
	if body != nil {
		r = r.SetBody(body)
	}

	resp, err := r.Put(fmt.Sprintf("/_cluster/routing/awareness/%s/weights", attribute))
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result ClusterWeightedRoutingResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultClusterService) GetWeightedRouting(ctx context.Context, attribute string) (*ClusterWeightedRoutingResponse, error) {
	if attribute == "" {
		return nil, fmt.Errorf("attribute is required")
	}

	resp, err := s.client.R().
		SetContext(ctx).
		Get(fmt.Sprintf("/_cluster/routing/awareness/%s/weights", attribute))
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result ClusterWeightedRoutingResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultClusterService) DeleteWeightedRouting(ctx context.Context) (*types.AcknowledgedResponse, error) {
	resp, err := s.client.R().SetContext(ctx).Delete("/_cluster/routing/awareness/weights")
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result types.AcknowledgedResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultClusterService) PostVotingConfigExclusions(ctx context.Context, params map[string]string) (*types.AcknowledgedResponse, error) {
	req := s.client.R().SetContext(ctx)
	for k, v := range params {
		req.SetQueryParam(k, v)
	}

	resp, err := req.Post("/_cluster/voting_config_exclusions")
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result types.AcknowledgedResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultClusterService) DeleteVotingConfigExclusions(ctx context.Context, waitForRemoval bool) (*types.AcknowledgedResponse, error) {
	resp, err := s.client.R().
		SetContext(ctx).
		SetQueryParam("wait_for_removal", fmt.Sprintf("%t", waitForRemoval)).
		Delete("/_cluster/voting_config_exclusions")
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result types.AcknowledgedResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}
