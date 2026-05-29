package api

import (
	"context"
	"fmt"
	json "github.com/goccy/go-json"
	"strings"

	"github.com/go-resty/resty/v2"
	"github.com/sirupsen/logrus"
)

type IngestService interface {
	PutPipeline(ctx context.Context, req *IngestPutPipelineRequest) (*IngestPutPipelineResponse, error)
	GetPipeline(ctx context.Context, ids []string) (IngestGetPipelineResponse, error)
	DeletePipeline(ctx context.Context, id string) (*IngestDeletePipelineResponse, error)
	SimulatePipeline(ctx context.Context, req *IngestSimulatePipelineRequest) (*IngestSimulatePipelineResponse, error)
	ProcessorGrok(ctx context.Context) (map[string][]string, error)
}

type DefaultIngestService struct {
	client *resty.Client
	logger *logrus.Entry
}

func NewIngestService(client *resty.Client, logger *logrus.Entry) IngestService {
	return &DefaultIngestService{client: client, logger: logger.WithField("service", "ingest")}
}

func (s *DefaultIngestService) PutPipeline(ctx context.Context, req *IngestPutPipelineRequest) (*IngestPutPipelineResponse, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}

	resp, err := s.client.R().SetContext(ctx).SetBody(req.Body).Put(fmt.Sprintf("/_ingest/pipeline/%s", req.Id))
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result IngestPutPipelineResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultIngestService) GetPipeline(ctx context.Context, ids []string) (IngestGetPipelineResponse, error) {
	path := "/_ingest/pipeline"
	if len(ids) > 0 {
		path = fmt.Sprintf("/_ingest/pipeline/%s", strings.Join(ids, ","))
	}

	resp, err := s.client.R().SetContext(ctx).Get(path)
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result IngestGetPipelineResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return result, nil
}

func (s *DefaultIngestService) DeletePipeline(ctx context.Context, id string) (*IngestDeletePipelineResponse, error) {
	if id == "" {
		return nil, fmt.Errorf("pipeline id is required")
	}

	resp, err := s.client.R().SetContext(ctx).Delete(fmt.Sprintf("/_ingest/pipeline/%s", id))
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result IngestDeletePipelineResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultIngestService) SimulatePipeline(ctx context.Context, req *IngestSimulatePipelineRequest) (*IngestSimulatePipelineResponse, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}

	path := "/_ingest/pipeline/_simulate"
	if req.Id != "" {
		path = fmt.Sprintf("/_ingest/pipeline/%s/_simulate", req.Id)
	}

	resp, err := s.client.R().SetContext(ctx).SetBody(req.Body).Post(path)
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result IngestSimulatePipelineResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultIngestService) ProcessorGrok(ctx context.Context) (map[string][]string, error) {
	resp, err := s.client.R().SetContext(ctx).Get("/_ingest/processor/grok")
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var raw IngestProcessorGrokResponse
	if err := json.Unmarshal(resp.Body(), &raw); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}

	result := make(map[string][]string, len(raw.Patterns))
	for k, v := range raw.Patterns {
		result[k] = []string{v}
	}
	return result, nil
}
