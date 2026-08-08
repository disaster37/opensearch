package api

import (
	"context"
	"fmt"

	json "github.com/goccy/go-json"

	"github.com/disaster37/opensearch/v4/types"
	"github.com/go-resty/resty/v2"
	"github.com/sirupsen/logrus"
)

// TieringService interacts with the OpenSearch tiering APIs (OpenSearch 3.7.0+,
// gated server-side by the writable_warm_index feature flag).
type TieringService interface {
	// GetStatus returns the tiering status of a single index
	// (GET /{index}/_tier?detailed=).
	GetStatus(ctx context.Context, index string, detailed bool) (*TieringStatusResponse, error)
	// ListStatus lists the tiering status of all indices in cat-style JSON
	// (GET /_tier/all?target=_hot|_warm&format=json).
	ListStatus(ctx context.Context, target TierTarget) ([]TieringListEntry, error)
	// HotToWarm moves the given index from the hot tier to the warm tier
	// (POST /{index}/_tier/warm).
	HotToWarm(ctx context.Context, index string, params ...*TierOperationParams) (*types.AcknowledgedResponse, error)
	// WarmToHot moves the given index from the warm tier to the hot tier
	// (POST /{index}/_tier/hot).
	WarmToHot(ctx context.Context, index string, params ...*TierOperationParams) (*types.AcknowledgedResponse, error)
	// Cancel cancels an ongoing tiering operation for the given index
	// (POST /_tier/_cancel/{index}).
	Cancel(ctx context.Context, index string, params ...*TierOperationParams) (*types.AcknowledgedResponse, error)
}

// DefaultTieringService is the default implementation of TieringService.
type DefaultTieringService struct {
	client *resty.Client
	logger *logrus.Entry
}

// NewTieringService constructs a new TieringService.
func NewTieringService(client *resty.Client, logger *logrus.Entry) TieringService {
	return &DefaultTieringService{
		client: client,
		logger: logger.WithField("service", "tiering"),
	}
}

func (s *DefaultTieringService) GetStatus(ctx context.Context, index string, detailed bool) (*TieringStatusResponse, error) {
	if index == "" {
		return nil, fmt.Errorf("index is required")
	}

	r := s.client.R().SetContext(ctx)
	if detailed {
		r.SetQueryParam("detailed", "true")
	}

	resp, err := r.Get(fmt.Sprintf("/%s/_tier", index))
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result TieringStatusResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultTieringService) ListStatus(ctx context.Context, target TierTarget) ([]TieringListEntry, error) {
	if target != "" && target != TierTargetHot && target != TierTargetWarm {
		return nil, fmt.Errorf("invalid target %q: must be %q, %q or empty", target, TierTargetHot, TierTargetWarm)
	}

	r := s.client.R().SetContext(ctx).SetQueryParam("format", "json")
	if target != "" {
		r.SetQueryParam("target", string(target))
	}

	resp, err := r.Get("/_tier/all")
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result []TieringListEntry
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return result, nil
}

func (s *DefaultTieringService) HotToWarm(ctx context.Context, index string, params ...*TierOperationParams) (*types.AcknowledgedResponse, error) {
	return s.tierPost(ctx, fmt.Sprintf("/%s/_tier/warm", index), index, params)
}

func (s *DefaultTieringService) WarmToHot(ctx context.Context, index string, params ...*TierOperationParams) (*types.AcknowledgedResponse, error) {
	return s.tierPost(ctx, fmt.Sprintf("/%s/_tier/hot", index), index, params)
}

func (s *DefaultTieringService) Cancel(ctx context.Context, index string, params ...*TierOperationParams) (*types.AcknowledgedResponse, error) {
	return s.tierPost(ctx, fmt.Sprintf("/_tier/_cancel/%s", index), index, params)
}

func (s *DefaultTieringService) tierPost(ctx context.Context, path, index string, params []*TierOperationParams) (*types.AcknowledgedResponse, error) {
	if index == "" {
		return nil, fmt.Errorf("index is required")
	}

	r := s.client.R().SetContext(ctx)
	if len(params) > 0 && params[0] != nil {
		if m := params[0].ToMap(); m != nil {
			r.SetQueryParams(m)
		}
	}

	resp, err := r.Post(path)
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
