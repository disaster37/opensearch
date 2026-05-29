package api

import (
	"context"
	"fmt"
	json "github.com/goccy/go-json"

	"github.com/go-resty/resty/v2"
	"github.com/sirupsen/logrus"
)

// CcrService interacts with the OpenSearch Cross-Cluster Replication (CCR) plugin.
type CcrService interface {
	AutoFollowStatus(ctx context.Context) (*CcrAutoFollowStatusResponse, error)
	PostAutoFollow(ctx context.Context, body *CcrAutoFollowRule) (*CcrPostAutoFollowResponse, error)
	DeleteAutoFollow(ctx context.Context, req *CcrDeleteAutoFollowOptions) (*CcrDeleteAutoFollowResponse, error)
	StartRule(ctx context.Context, req *CcrStartRuleRequest) (*CcrStartRuleResponse, error)
	StopRule(ctx context.Context, name string) (*CcrStopRuleResponse, error)
	PauseRule(ctx context.Context, name string) (*CcrPauseRuleResponse, error)
	ResumeRule(ctx context.Context, name string) (*CcrResumeRuleResponse, error)
	StatusRule(ctx context.Context, name string) (*CcrStatusRuleResponse, error)
	FollowerStats(ctx context.Context) (*CcrFollowerStatsResponse, error)
	LeaderStats(ctx context.Context) (*CcrLeaderStatsResponse, error)
	UpdateRule(ctx context.Context, name string, body any) (*CcrUpdateRuleResponse, error)
}

type DefaultCcrService struct {
	client *resty.Client
	logger *logrus.Entry
}

func NewCcrService(client *resty.Client, logger *logrus.Entry) CcrService {
	return &DefaultCcrService{client: client, logger: logger.WithField("service", "ccr")}
}

func (s *DefaultCcrService) AutoFollowStatus(ctx context.Context) (*CcrAutoFollowStatusResponse, error) {
	resp, err := s.client.R().SetContext(ctx).Get("/_plugins/_replication/autofollow_stats")
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}
	var result CcrAutoFollowStatusResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultCcrService) PostAutoFollow(ctx context.Context, body *CcrAutoFollowRule) (*CcrPostAutoFollowResponse, error) {
	if body == nil {
		return nil, fmt.Errorf("body is required")
	}
	resp, err := s.client.R().SetContext(ctx).SetBody(body).Post("/_plugins/_replication/_autofollow")
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}
	var result CcrPostAutoFollowResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultCcrService) DeleteAutoFollow(ctx context.Context, req *CcrDeleteAutoFollowOptions) (*CcrDeleteAutoFollowResponse, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}
	body := CcrDeleteAutoFollowRequest{LeaderAlias: req.LeaderAlias, Name: req.Name}
	resp, err := s.client.R().SetContext(ctx).SetBody(&body).Delete("/_plugins/_replication/_autofollow")
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}
	var result CcrDeleteAutoFollowResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultCcrService) StartRule(ctx context.Context, req *CcrStartRuleRequest) (*CcrStartRuleResponse, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}
	resp, err := s.client.R().SetContext(ctx).SetBody(req.Body).Put(fmt.Sprintf("/_plugins/_replication/%s/_start", req.Name))
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}
	var result CcrStartRuleResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultCcrService) StopRule(ctx context.Context, name string) (*CcrStopRuleResponse, error) {
	if name == "" {
		return nil, fmt.Errorf("name is required")
	}
	resp, err := s.client.R().SetContext(ctx).Post(fmt.Sprintf("/_plugins/_replication/%s/_stop", name))
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}
	var result CcrStopRuleResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultCcrService) PauseRule(ctx context.Context, name string) (*CcrPauseRuleResponse, error) {
	if name == "" {
		return nil, fmt.Errorf("name is required")
	}
	resp, err := s.client.R().SetContext(ctx).Post(fmt.Sprintf("/_plugins/_replication/%s/_pause", name))
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}
	var result CcrPauseRuleResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultCcrService) ResumeRule(ctx context.Context, name string) (*CcrResumeRuleResponse, error) {
	if name == "" {
		return nil, fmt.Errorf("name is required")
	}
	resp, err := s.client.R().SetContext(ctx).Post(fmt.Sprintf("/_plugins/_replication/%s/_resume", name))
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}
	var result CcrResumeRuleResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultCcrService) StatusRule(ctx context.Context, name string) (*CcrStatusRuleResponse, error) {
	if name == "" {
		return nil, fmt.Errorf("name is required")
	}
	resp, err := s.client.R().SetContext(ctx).Get(fmt.Sprintf("/_plugins/_replication/%s/_status", name))
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}
	var result CcrStatusRuleResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultCcrService) FollowerStats(ctx context.Context) (*CcrFollowerStatsResponse, error) {
	resp, err := s.client.R().SetContext(ctx).Get("/_plugins/_replication/follower_stats")
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}
	var result CcrFollowerStatsResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultCcrService) LeaderStats(ctx context.Context) (*CcrLeaderStatsResponse, error) {
	resp, err := s.client.R().SetContext(ctx).Get("/_plugins/_replication/leader_stats")
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}
	var result CcrLeaderStatsResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

// UpdateRule updates the configuration of a cross-cluster replication rule.
func (s *DefaultCcrService) UpdateRule(ctx context.Context, name string, body any) (*CcrUpdateRuleResponse, error) {
	if name == "" {
		return nil, fmt.Errorf("name is required")
	}
	if body == nil {
		return nil, fmt.Errorf("body is required")
	}
	resp, err := s.client.R().SetContext(ctx).SetBody(body).Put(fmt.Sprintf("/_plugins/_replication/%s/_update", name))
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}
	var result CcrUpdateRuleResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}
