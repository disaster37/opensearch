package api

import (
	"context"
	"fmt"
	"strings"

	json "github.com/goccy/go-json"

	"github.com/go-resty/resty/v2"
	"github.com/sirupsen/logrus"
)

type NodesService interface {
	Info(ctx context.Context, req *NodesInfoRequest) (*NodesInfoResponse, error)
	Stats(ctx context.Context, req *NodesStatsRequest) (*NodesStatsResponse, error)
	HotThreads(ctx context.Context, nodeIds []string) (string, error)
	ReloadSecureSettings(ctx context.Context, body any) (*NodesReloadSecureSettingsResponse, error)
	Usage(ctx context.Context, req *NodesUsageRequest) (*NodesUsageResponse, error)
}

type DefaultNodesService struct {
	client *resty.Client
	logger *logrus.Entry
}

func NewNodesService(client *resty.Client, logger *logrus.Entry) NodesService {
	return &DefaultNodesService{client: client, logger: logger.WithField("service", "nodes")}
}

func (s *DefaultNodesService) Info(ctx context.Context, req *NodesInfoRequest) (*NodesInfoResponse, error) {
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

func (s *DefaultNodesService) Stats(ctx context.Context, req *NodesStatsRequest) (*NodesStatsResponse, error) {
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

	resp, err := s.client.R().SetContext(ctx).Get(path)
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

func (s *DefaultNodesService) Usage(ctx context.Context, req *NodesUsageRequest) (*NodesUsageResponse, error) {
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
