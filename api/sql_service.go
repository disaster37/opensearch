package api

import (
	"context"
	"fmt"

	json "github.com/goccy/go-json"

	"github.com/go-resty/resty/v2"
	"github.com/sirupsen/logrus"
)

// SqlService defines the interface for interacting with the OpenSearch SQL/PPL plugin.
type SqlService interface {
	PPLQuery(ctx context.Context, body any) (*SQLResponse, error)
	PPLExplain(ctx context.Context, body any) (*SQLExplainResponse, error)
	SQLQuery(ctx context.Context, body any, format string) (*SQLResponse, error)
	SQLExplain(ctx context.Context, body any) (*SQLExplainResponse, error)
	SQLCloseCursor(ctx context.Context, body any) (*SQLCloseResponse, error)
}

// DefaultSqlService implements the SqlService interface using a REST client.
type DefaultSqlService struct {
	client *resty.Client
	logger *logrus.Entry
}

// NewSqlService creates a new SqlService with the given REST client and logger.
func NewSqlService(client *resty.Client, logger *logrus.Entry) SqlService {
	return &DefaultSqlService{
		client: client,
		logger: logger.WithField("service", "sql"),
	}
}

// PPLQuery executes a PPL query against the OpenSearch PPL plugin.
func (s *DefaultSqlService) PPLQuery(ctx context.Context, body any) (*SQLResponse, error) {
	if body == nil {
		return nil, fmt.Errorf("body is required")
	}

	resp, err := s.client.R().SetContext(ctx).SetBody(body).Post("/_plugins/_ppl")
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result SQLResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

// PPLExplain returns the execution plan for a PPL query.
func (s *DefaultSqlService) PPLExplain(ctx context.Context, body any) (*SQLExplainResponse, error) {
	if body == nil {
		return nil, fmt.Errorf("body is required")
	}

	resp, err := s.client.R().SetContext(ctx).SetBody(body).Post("/_plugins/_ppl/_explain")
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result SQLExplainResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

// SQLQuery executes an SQL query against the OpenSearch SQL plugin with an optional format.
func (s *DefaultSqlService) SQLQuery(ctx context.Context, body any, format string) (*SQLResponse, error) {
	if body == nil {
		return nil, fmt.Errorf("body is required")
	}

	r := s.client.R().SetContext(ctx).SetBody(body)
	if format != "" {
		r.SetQueryParam("format", format)
	}

	resp, err := r.Post("/_plugins/_sql")
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result SQLResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

// SQLExplain returns the execution plan for an SQL query.
func (s *DefaultSqlService) SQLExplain(ctx context.Context, body any) (*SQLExplainResponse, error) {
	if body == nil {
		return nil, fmt.Errorf("body is required")
	}

	resp, err := s.client.R().SetContext(ctx).SetBody(body).Post("/_plugins/_sql/_explain")
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result SQLExplainResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

// SQLCloseCursor closes an open SQL cursor to free server-side resources.
func (s *DefaultSqlService) SQLCloseCursor(ctx context.Context, body any) (*SQLCloseResponse, error) {
	if body == nil {
		return nil, fmt.Errorf("body is required")
	}

	resp, err := s.client.R().SetContext(ctx).SetBody(body).Post("/_plugins/_sql/close")
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result SQLCloseResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}
