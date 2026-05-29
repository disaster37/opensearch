package api

import (
	"context"
	"fmt"
	json "github.com/goccy/go-json"

	"github.com/disaster37/opensearch/v3/types"
	"github.com/go-resty/resty/v2"
	"github.com/sirupsen/logrus"
)

// KnnService provides access to the k-NN plugin API. It supports stats,
// warmup, cache management, and model operations. See
// https://opensearch.org/docs/latest/search-plugins/knn/.
type KnnService interface {
	// KnnStats retrieves k-NN plugin statistics. When stat is empty, all
	// stats are returned; otherwise only the named stat is returned.
	KnnStats(ctx context.Context, stat string) (*KnnStatsResponse, error)

	// KnnWarmup warms up k-NN indices by loading their graphs into memory.
	KnnWarmup(ctx context.Context, index string) (*KnnWarmupResponse, error)

	// ClearCache clears the k-NN cache for the specified index.
	ClearCache(ctx context.Context, index string) (*types.AcknowledgedResponse, error)

	// TrainModel trains a k-NN model using the provided request body.
	TrainModel(ctx context.Context, body any) (*KnnTrainModelResponse, error)

	// GetModel retrieves a k-NN model by its identifier.
	GetModel(ctx context.Context, modelId string) (*KnnGetModelResponse, error)

	// SearchModels searches for k-NN models using the provided query body.
	SearchModels(ctx context.Context, body any) (*KnnSearchModelsResponse, error)

	// DeleteModel deletes a k-NN model by its identifier.
	DeleteModel(ctx context.Context, modelId string) (*types.AcknowledgedResponse, error)
}

// DefaultKnnService is the default implementation of KnnService.
type DefaultKnnService struct {
	client *resty.Client
	logger *logrus.Entry
}

// NewKnnService creates a new KnnService instance.
func NewKnnService(client *resty.Client, logger *logrus.Entry) KnnService {
	return &DefaultKnnService{client: client, logger: logger.WithField("service", "knn")}
}

func (s *DefaultKnnService) KnnStats(ctx context.Context, stat string) (*KnnStatsResponse, error) {
	path := "/_plugins/_knn/stats"
	if stat != "" {
		path = fmt.Sprintf("/_plugins/_knn/stats/%s", stat)
	}

	resp, err := s.client.R().SetContext(ctx).Get(path)
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result KnnStatsResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultKnnService) KnnWarmup(ctx context.Context, index string) (*KnnWarmupResponse, error) {
	if index == "" {
		return nil, fmt.Errorf("index is required")
	}

	resp, err := s.client.R().SetContext(ctx).Get(fmt.Sprintf("/_plugins/_knn/warmup/%s", index))
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result KnnWarmupResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultKnnService) ClearCache(ctx context.Context, index string) (*types.AcknowledgedResponse, error) {
	if index == "" {
		return nil, fmt.Errorf("index is required")
	}

	resp, err := s.client.R().SetContext(ctx).Post(fmt.Sprintf("/_plugins/_knn/clear_cache/%s", index))
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

func (s *DefaultKnnService) TrainModel(ctx context.Context, body any) (*KnnTrainModelResponse, error) {
	if body == nil {
		return nil, fmt.Errorf("body is required")
	}

	resp, err := s.client.R().SetContext(ctx).SetBody(body).Post("/_plugins/_knn/models/_train")
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result KnnTrainModelResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultKnnService) GetModel(ctx context.Context, modelId string) (*KnnGetModelResponse, error) {
	if modelId == "" {
		return nil, fmt.Errorf("model id is required")
	}

	resp, err := s.client.R().SetContext(ctx).Get(fmt.Sprintf("/_plugins/_knn/models/%s", modelId))
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result KnnGetModelResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultKnnService) SearchModels(ctx context.Context, body any) (*KnnSearchModelsResponse, error) {
	if body == nil {
		return nil, fmt.Errorf("body is required")
	}

	resp, err := s.client.R().SetContext(ctx).SetBody(body).Post("/_plugins/_knn/models/_search")
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result KnnSearchModelsResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultKnnService) DeleteModel(ctx context.Context, modelId string) (*types.AcknowledgedResponse, error) {
	if modelId == "" {
		return nil, fmt.Errorf("model id is required")
	}

	resp, err := s.client.R().SetContext(ctx).Delete(fmt.Sprintf("/_plugins/_knn/models/%s", modelId))
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
