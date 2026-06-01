package api

import (
	"context"
	"fmt"
	json "github.com/goccy/go-json"

	"github.com/disaster37/opensearch/v4/types"
	"github.com/go-resty/resty/v2"
	"github.com/sirupsen/logrus"
)

// RollupService interacts with the OpenSearch ISM Rollup plugin.
type RollupService interface {
	GetRollup(ctx context.Context, rollupId string) (*RollupGetResponse, error)
	PutRollup(ctx context.Context, rollupId string, body any) (*RollupGetResponse, error)
	DeleteRollup(ctx context.Context, rollupId string) (*types.AcknowledgedResponse, error)
	StartRollup(ctx context.Context, rollupId string) (*types.AcknowledgedResponse, error)
	StopRollup(ctx context.Context, rollupId string) (*types.AcknowledgedResponse, error)
	ExplainRollup(ctx context.Context, rollupId string) (map[string]*RollupExplainResponse, error)
}

// DefaultRollupService is the default RollupService implementation.
type DefaultRollupService struct {
	client *resty.Client
	logger *logrus.Entry
}

// NewRollupService returns a RollupService bound to the given resty client.
func NewRollupService(client *resty.Client, logger *logrus.Entry) RollupService {
	return &DefaultRollupService{
		client: client,
		logger: logger.WithField("service", "rollup"),
	}
}

// GetRollup retrieves a rollup job by its ID.
func (s *DefaultRollupService) GetRollup(ctx context.Context, rollupId string) (*RollupGetResponse, error) {
	if rollupId == "" {
		return nil, fmt.Errorf("rollup id is required")
	}

	resp, err := s.client.R().
		SetContext(ctx).
		Get(fmt.Sprintf("/_plugins/_rollup/jobs/%s", rollupId))
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result RollupGetResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

// PutRollup creates or updates a rollup job.
func (s *DefaultRollupService) PutRollup(ctx context.Context, rollupId string, body any) (*RollupGetResponse, error) {
	if rollupId == "" {
		return nil, fmt.Errorf("rollup id is required")
	}
	if body == nil {
		return nil, fmt.Errorf("body is required")
	}

	resp, err := s.client.R().
		SetContext(ctx).
		SetBody(body).
		Put(fmt.Sprintf("/_plugins/_rollup/jobs/%s", rollupId))
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result RollupGetResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

// DeleteRollup deletes a rollup job.
func (s *DefaultRollupService) DeleteRollup(ctx context.Context, rollupId string) (*types.AcknowledgedResponse, error) {
	if rollupId == "" {
		return nil, fmt.Errorf("rollup id is required")
	}

	resp, err := s.client.R().
		SetContext(ctx).
		Delete(fmt.Sprintf("/_plugins/_rollup/jobs/%s", rollupId))
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

// StartRollup starts a rollup job.
func (s *DefaultRollupService) StartRollup(ctx context.Context, rollupId string) (*types.AcknowledgedResponse, error) {
	if rollupId == "" {
		return nil, fmt.Errorf("rollup id is required")
	}

	resp, err := s.client.R().
		SetContext(ctx).
		Post(fmt.Sprintf("/_plugins/_rollup/jobs/%s/_start", rollupId))
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

// StopRollup stops a rollup job.
func (s *DefaultRollupService) StopRollup(ctx context.Context, rollupId string) (*types.AcknowledgedResponse, error) {
	if rollupId == "" {
		return nil, fmt.Errorf("rollup id is required")
	}

	resp, err := s.client.R().
		SetContext(ctx).
		Post(fmt.Sprintf("/_plugins/_rollup/jobs/%s/_stop", rollupId))
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

// ExplainRollup retrieves runtime explain information for a rollup job.
func (s *DefaultRollupService) ExplainRollup(ctx context.Context, rollupId string) (map[string]*RollupExplainResponse, error) {
	if rollupId == "" {
		return nil, fmt.Errorf("rollup id is required")
	}

	resp, err := s.client.R().
		SetContext(ctx).
		Get(fmt.Sprintf("/_plugins/_rollup/jobs/%s/_explain", rollupId))
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result map[string]*RollupExplainResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return result, nil
}
