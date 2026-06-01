package api

import (
	"context"
	"fmt"

	json "github.com/goccy/go-json"

	"github.com/disaster37/opensearch/v4/types"
	"github.com/go-resty/resty/v2"
	"github.com/sirupsen/logrus"
)

// AsyncSearchService provides access to the asynchronous search plugin API.
// It supports submitting, retrieving, deleting, and monitoring asynchronous
// searches. See https://opensearch.org/docs/latest/search-plugins/async/.
type AsyncSearchService interface {
	// Submit submits an asynchronous search. The body parameter contains the
	// search request body, and params holds optional query parameters such as
	// wait_for_completion_timeout, keep_on_completion, and keep_alive.
	Submit(ctx context.Context, body any, params map[string]string) (*AsyncSearchSubmitResponse, error)

	// AsyncGet retrieves the result of a previously submitted asynchronous
	// search by its identifier.
	AsyncGet(ctx context.Context, id string) (*AsyncSearchGetResponse, error)

	// AsyncDelete deletes a stored asynchronous search by its identifier.
	AsyncDelete(ctx context.Context, id string) (*types.AcknowledgedResponse, error)

	// AsyncStats retrieves cluster-level statistics about asynchronous
	// search execution.
	AsyncStats(ctx context.Context) (*AsyncSearchStatsResponse, error)
}

// DefaultAsyncSearchService is the default implementation of AsyncSearchService.
type DefaultAsyncSearchService struct {
	client *resty.Client
	logger *logrus.Entry
}

// NewAsyncSearchService creates a new AsyncSearchService instance.
func NewAsyncSearchService(client *resty.Client, logger *logrus.Entry) AsyncSearchService {
	return &DefaultAsyncSearchService{client: client, logger: logger.WithField("service", "async_search")}
}

func (s *DefaultAsyncSearchService) Submit(ctx context.Context, body any, params map[string]string) (*AsyncSearchSubmitResponse, error) {
	if body == nil {
		return nil, fmt.Errorf("body is required")
	}

	r := s.client.R().SetContext(ctx).SetBody(body)
	for k, v := range params {
		r.SetQueryParam(k, v)
	}

	resp, err := r.Post("/_plugins/_asynchronous_search")
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result AsyncSearchSubmitResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultAsyncSearchService) AsyncGet(ctx context.Context, id string) (*AsyncSearchGetResponse, error) {
	if id == "" {
		return nil, fmt.Errorf("id is required")
	}

	resp, err := s.client.R().SetContext(ctx).Get(fmt.Sprintf("/_plugins/_asynchronous_search/%s", id))
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result AsyncSearchGetResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultAsyncSearchService) AsyncDelete(ctx context.Context, id string) (*types.AcknowledgedResponse, error) {
	if id == "" {
		return nil, fmt.Errorf("id is required")
	}

	resp, err := s.client.R().SetContext(ctx).Delete(fmt.Sprintf("/_plugins/_asynchronous_search/%s", id))
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

func (s *DefaultAsyncSearchService) AsyncStats(ctx context.Context) (*AsyncSearchStatsResponse, error) {
	resp, err := s.client.R().SetContext(ctx).Get("/_plugins/_asynchronous_search/stats")
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result AsyncSearchStatsResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}
