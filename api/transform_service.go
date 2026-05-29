package api

import (
	"context"
	"fmt"
	json "github.com/goccy/go-json"

	"github.com/go-resty/resty/v2"
	"github.com/sirupsen/logrus"
)

type TransformService interface {
	GetJob(ctx context.Context, jobName string) (*TransformGetJobResponse, error)
	PutJob(ctx context.Context, req *TransformPutJobRequest) (*TransformGetJobResponse, error)
	DeleteJob(ctx context.Context, jobName string) (*TransformDeleteJobResponse, error)
	SearchJob(ctx context.Context, body any) (*TransformSearchJobResponse, error)
	ExplainJob(ctx context.Context, jobName string) (map[string]TransformExplainJob, error)
	PreviewJobResults(ctx context.Context, body any) (*TransformPreviewJobResponse, error)
	StartJob(ctx context.Context, jobName string) (*TransformStartJobResponse, error)
	StopJob(ctx context.Context, jobName string) (*TransformStopJobResponse, error)
}

type DefaultTransformService struct {
	client *resty.Client
	logger *logrus.Entry
}

func NewTransformService(client *resty.Client, logger *logrus.Entry) TransformService {
	return &DefaultTransformService{client: client, logger: logger.WithField("service", "transform")}
}

func (s *DefaultTransformService) GetJob(ctx context.Context, jobName string) (*TransformGetJobResponse, error) {
	if jobName == "" {
		return nil, fmt.Errorf("job name is required")
	}
	resp, err := s.client.R().SetContext(ctx).Get(fmt.Sprintf("/_plugins/_transform/%s", jobName))
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}
	var result TransformGetJobResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultTransformService) PutJob(ctx context.Context, req *TransformPutJobRequest) (*TransformGetJobResponse, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}
	r := s.client.R().SetContext(ctx).SetBody(&TransformPutJob{Transform: *req.Body})
	if req.Version != nil && req.Version.SeqNo != nil && req.Version.PrimaryTerm != nil {
		r.SetQueryParam("if_seq_no", fmt.Sprint(*req.Version.SeqNo))
		r.SetQueryParam("if_primary_term", fmt.Sprint(*req.Version.PrimaryTerm))
	}
	resp, err := r.Put(fmt.Sprintf("/_plugins/_transform/%s", req.JobName))
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}
	var result TransformGetJobResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultTransformService) DeleteJob(ctx context.Context, jobName string) (*TransformDeleteJobResponse, error) {
	if jobName == "" {
		return nil, fmt.Errorf("job name is required")
	}
	resp, err := s.client.R().SetContext(ctx).Delete(fmt.Sprintf("/_plugins/_transform/%s", jobName))
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}
	var result TransformDeleteJobResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultTransformService) SearchJob(ctx context.Context, body any) (*TransformSearchJobResponse, error) {
	resp, err := s.client.R().SetContext(ctx).SetBody(body).Get("/_plugins/_transform")
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}
	var result TransformSearchJobResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultTransformService) ExplainJob(ctx context.Context, jobName string) (map[string]TransformExplainJob, error) {
	if jobName == "" {
		return nil, fmt.Errorf("job name is required")
	}
	resp, err := s.client.R().SetContext(ctx).Get(fmt.Sprintf("/_plugins/_transform/%s/_explain", jobName))
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}
	var result map[string]TransformExplainJob
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return result, nil
}

func (s *DefaultTransformService) PreviewJobResults(ctx context.Context, body any) (*TransformPreviewJobResponse, error) {
	resp, err := s.client.R().SetContext(ctx).SetBody(body).Post("/_plugins/_transform/_preview")
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}
	var result TransformPreviewJobResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultTransformService) StartJob(ctx context.Context, jobName string) (*TransformStartJobResponse, error) {
	if jobName == "" {
		return nil, fmt.Errorf("job name is required")
	}
	resp, err := s.client.R().SetContext(ctx).Post(fmt.Sprintf("/_plugins/_transform/%s/_start", jobName))
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}
	var result TransformStartJobResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultTransformService) StopJob(ctx context.Context, jobName string) (*TransformStopJobResponse, error) {
	if jobName == "" {
		return nil, fmt.Errorf("job name is required")
	}
	resp, err := s.client.R().SetContext(ctx).Post(fmt.Sprintf("/_plugins/_transform/%s/_stop", jobName))
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}
	var result TransformStopJobResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}
