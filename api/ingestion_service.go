package api

import (
	"context"
	"fmt"

	json "github.com/goccy/go-json"

	"github.com/go-resty/resty/v2"
	"github.com/sirupsen/logrus"
)

// IngestionService interacts with the pull-based ingestion control endpoints
// (pause/resume/state), promoted to a public, non-experimental API in
// OpenSearch 3.6.0 (PR #20704). The endpoints were introduced in 3.1.
type IngestionService interface {
	// Pause pauses the pull-based ingestion poller for the given index
	// (POST /{index}/ingestion/_pause).
	Pause(ctx context.Context, index string, params ...*IngestionStateParams) (*IngestionStateResponse, error)
	// Resume resumes the pull-based ingestion poller for the given index,
	// optionally resetting consumer positions (POST /{index}/ingestion/_resume).
	Resume(ctx context.Context, req *IngestionResumeRequest) (*IngestionStateResponse, error)
	// GetState returns the per-shard pull-based ingestion state for the
	// given index (GET /{index}/ingestion/_state).
	GetState(ctx context.Context, req *IngestionGetStateRequest) (*GetIngestionStateResponse, error)
}

// DefaultIngestionService is the default implementation of IngestionService.
type DefaultIngestionService struct {
	client *resty.Client
	logger *logrus.Entry
}

// NewIngestionService constructs a new IngestionService.
func NewIngestionService(client *resty.Client, logger *logrus.Entry) IngestionService {
	return &DefaultIngestionService{
		client: client,
		logger: logger.WithField("service", "ingestion"),
	}
}

func (s *DefaultIngestionService) Pause(ctx context.Context, index string, params ...*IngestionStateParams) (*IngestionStateResponse, error) {
	if index == "" {
		return nil, fmt.Errorf("index is required")
	}

	r := s.client.R().SetContext(ctx)
	if len(params) > 0 && params[0] != nil {
		if m := params[0].ToMap(); m != nil {
			r.SetQueryParams(m)
		}
	}

	resp, err := r.Post(fmt.Sprintf("/%s/ingestion/_pause", index))
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result IngestionStateResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultIngestionService) Resume(ctx context.Context, req *IngestionResumeRequest) (*IngestionStateResponse, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}

	r := s.client.R().SetContext(ctx)
	if len(req.ResetSettings) > 0 {
		r.SetBody(map[string]any{"reset_settings": req.ResetSettings})
	}
	if req.Params != nil {
		if m := req.Params.ToMap(); m != nil {
			r.SetQueryParams(m)
		}
	}

	resp, err := r.Post(fmt.Sprintf("/%s/ingestion/_resume", req.Index))
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result IngestionStateResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultIngestionService) GetState(ctx context.Context, req *IngestionGetStateRequest) (*GetIngestionStateResponse, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}

	r := s.client.R().SetContext(ctx)
	if req.Params != nil {
		if m := req.Params.ToMap(); m != nil {
			r.SetQueryParams(m)
		}
	}

	resp, err := r.Get(fmt.Sprintf("/%s/ingestion/_state", req.Index))
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result GetIngestionStateResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}
