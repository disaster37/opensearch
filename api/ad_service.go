package api

import (
	"context"
	"fmt"
	json "github.com/goccy/go-json"

	"github.com/go-resty/resty/v2"
	"github.com/sirupsen/logrus"
)

// AdService defines the interface for interacting with the OpenSearch Anomaly Detection plugin.
type AdService interface {
	IndexDetector(ctx context.Context, req *AdIndexDetectorRequest) (*AdIndexDetectorResponse, error)
	GetDetector(ctx context.Context, detectorId string) (*AdIndexDetectorResponse, error)
	DeleteDetector(ctx context.Context, detectorId string) (*AdDeleteDetectorResponse, error)
	ExecuteDetector(ctx context.Context, detectorId string, body any) (*AdExecuteDetectorResponse, error)
	PreviewDetector(ctx context.Context, detectorId string, body any) (*AdPreviewDetectorResponse, error)
	SearchDetectors(ctx context.Context, body any) (*AdSearchDetectorsResponse, error)
	SearchResults(ctx context.Context, body any) (*AdSearchResultsResponse, error)
	SearchTopResults(ctx context.Context, detectorId string, body any) (*AdSearchResultsResponse, error)
	AdStats(ctx context.Context, stat string) (*AdStatsResponse, error)
	ValidateDetector(ctx context.Context, body any) (*AdValidateResponse, error)
}

// DefaultAdService implements the AdService interface using a REST client.
type DefaultAdService struct {
	client *resty.Client
	logger *logrus.Entry
}

// NewAdService creates a new AdService with the given REST client and logger.
func NewAdService(client *resty.Client, logger *logrus.Entry) AdService {
	return &DefaultAdService{
		client: client,
		logger: logger.WithField("service", "anomaly_detection"),
	}
}

// IndexDetector creates or updates an anomaly detector. When DetectorId is empty,
// a new detector is created via POST; when provided, the existing detector is updated via PUT.
func (s *DefaultAdService) IndexDetector(ctx context.Context, req *AdIndexDetectorRequest) (*AdIndexDetectorResponse, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}

	r := s.client.R().SetContext(ctx).SetBody(req.Body)

	var resp *resty.Response
	var err error
	if req.DetectorId != "" {
		resp, err = r.Put(fmt.Sprintf("/_plugins/_anomaly_detection/detectors/%s", req.DetectorId))
	} else {
		resp, err = r.Post("/_plugins/_anomaly_detection/detectors")
	}
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result AdIndexDetectorResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

// GetDetector retrieves an anomaly detector by its ID.
func (s *DefaultAdService) GetDetector(ctx context.Context, detectorId string) (*AdIndexDetectorResponse, error) {
	if detectorId == "" {
		return nil, fmt.Errorf("detector id is required")
	}

	resp, err := s.client.R().SetContext(ctx).Get(fmt.Sprintf("/_plugins/_anomaly_detection/detectors/%s", detectorId))
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result AdIndexDetectorResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

// DeleteDetector deletes an anomaly detector by its ID.
func (s *DefaultAdService) DeleteDetector(ctx context.Context, detectorId string) (*AdDeleteDetectorResponse, error) {
	if detectorId == "" {
		return nil, fmt.Errorf("detector id is required")
	}

	resp, err := s.client.R().SetContext(ctx).Delete(fmt.Sprintf("/_plugins/_anomaly_detection/detectors/%s", detectorId))
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result AdDeleteDetectorResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

// ExecuteDetector executes an anomaly detector on-demand, optionally with overrides.
func (s *DefaultAdService) ExecuteDetector(ctx context.Context, detectorId string, body any) (*AdExecuteDetectorResponse, error) {
	if detectorId == "" {
		return nil, fmt.Errorf("detector id is required")
	}

	r := s.client.R().SetContext(ctx)
	if body != nil {
		r.SetBody(body)
	}

	resp, err := r.Post(fmt.Sprintf("/_plugins/_anomaly_detection/detectors/%s/_run", detectorId))
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result AdExecuteDetectorResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

// PreviewDetector previews an anomaly detector. When detectorId is empty, a new
// detector definition is previewed via POST to /_preview; when provided, the
// existing detector is previewed via POST to /{detectorId}/_preview. The body is required.
func (s *DefaultAdService) PreviewDetector(ctx context.Context, detectorId string, body any) (*AdPreviewDetectorResponse, error) {
	if body == nil {
		return nil, fmt.Errorf("body is required")
	}

	var path string
	if detectorId != "" {
		path = fmt.Sprintf("/_plugins/_anomaly_detection/detectors/%s/_preview", detectorId)
	} else {
		path = "/_plugins/_anomaly_detection/detectors/_preview"
	}

	resp, err := s.client.R().SetContext(ctx).SetBody(body).Post(path)
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result AdPreviewDetectorResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

// SearchDetectors searches for anomaly detectors using the given query body.
func (s *DefaultAdService) SearchDetectors(ctx context.Context, body any) (*AdSearchDetectorsResponse, error) {
	if body == nil {
		return nil, fmt.Errorf("body is required")
	}

	resp, err := s.client.R().SetContext(ctx).SetBody(body).Post("/_plugins/_anomaly_detection/detectors/_search")
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result AdSearchDetectorsResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

// SearchResults searches for anomaly detection results using the given query body.
func (s *DefaultAdService) SearchResults(ctx context.Context, body any) (*AdSearchResultsResponse, error) {
	if body == nil {
		return nil, fmt.Errorf("body is required")
	}

	resp, err := s.client.R().SetContext(ctx).SetBody(body).Post("/_plugins/_anomaly_detection/detectors/results/_search")
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result AdSearchResultsResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

// SearchTopResults retrieves the top anomaly results for a detector.
func (s *DefaultAdService) SearchTopResults(ctx context.Context, detectorId string, body any) (*AdSearchResultsResponse, error) {
	if detectorId == "" {
		return nil, fmt.Errorf("detector id is required")
	}
	if body == nil {
		return nil, fmt.Errorf("body is required")
	}

	resp, err := s.client.R().SetContext(ctx).SetBody(body).Post(
		fmt.Sprintf("/_plugins/_anomaly_detection/detectors/%s/results/_topAnomalies", detectorId),
	)
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result AdSearchResultsResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

// AdStats retrieves anomaly detection statistics, optionally for a specific stat name.
func (s *DefaultAdService) AdStats(ctx context.Context, stat string) (*AdStatsResponse, error) {
	path := "/_plugins/_anomaly_detection/stats"
	if stat != "" {
		path = fmt.Sprintf("/_plugins/_anomaly_detection/stats/%s", stat)
	}

	resp, err := s.client.R().SetContext(ctx).Get(path)
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result AdStatsResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

// ValidateDetector validates an anomaly detector configuration without persisting it.
func (s *DefaultAdService) ValidateDetector(ctx context.Context, body any) (*AdValidateResponse, error) {
	if body == nil {
		return nil, fmt.Errorf("body is required")
	}

	resp, err := s.client.R().SetContext(ctx).SetBody(body).Post("/_plugins/_anomaly_detection/detectors/_validate")
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result AdValidateResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}
