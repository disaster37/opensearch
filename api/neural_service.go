package api

import (
	"context"
	"fmt"
	json "github.com/goccy/go-json"

	"github.com/disaster37/opensearch/v3/types"
	"github.com/go-resty/resty/v2"
	"github.com/sirupsen/logrus"
)

// NeuralService provides access to the neural search plugin API. It supports
// stats, warmup, and cache management for neural search indices. See
// https://opensearch.org/docs/latest/search-plugins/neural-search/.
type NeuralService interface {
	// NeuralStats retrieves neural plugin statistics. When stat is empty, all
	// stats are returned; otherwise only the named stat is returned.
	NeuralStats(ctx context.Context, stat string) (*NeuralStatsResponse, error)

	// NeuralWarmup warms up neural search indices by loading models into memory.
	NeuralWarmup(ctx context.Context, index string) (*NeuralWarmupResponse, error)

	// NeuralClearCache clears the neural search cache for the specified index.
	NeuralClearCache(ctx context.Context, index string) (*types.AcknowledgedResponse, error)
}

// DefaultNeuralService is the default implementation of NeuralService.
type DefaultNeuralService struct {
	client *resty.Client
	logger *logrus.Entry
}

// NewNeuralService creates a new NeuralService instance.
func NewNeuralService(client *resty.Client, logger *logrus.Entry) NeuralService {
	return &DefaultNeuralService{client: client, logger: logger.WithField("service", "neural")}
}

func (s *DefaultNeuralService) NeuralStats(ctx context.Context, stat string) (*NeuralStatsResponse, error) {
	path := "/_plugins/_neural/stats"
	if stat != "" {
		path = fmt.Sprintf("/_plugins/_neural/stats/%s", stat)
	}

	resp, err := s.client.R().SetContext(ctx).Get(path)
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result NeuralStatsResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultNeuralService) NeuralWarmup(ctx context.Context, index string) (*NeuralWarmupResponse, error) {
	if index == "" {
		return nil, fmt.Errorf("index is required")
	}

	resp, err := s.client.R().SetContext(ctx).Post(fmt.Sprintf("/_plugins/_neural/warmup/%s", index))
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result NeuralWarmupResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultNeuralService) NeuralClearCache(ctx context.Context, index string) (*types.AcknowledgedResponse, error) {
	if index == "" {
		return nil, fmt.Errorf("index is required")
	}

	resp, err := s.client.R().SetContext(ctx).Post(fmt.Sprintf("/_plugins/_neural/clear_cache/%s", index))
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
