package api

import (
	"context"
	"net/http"

	json "github.com/goccy/go-json"

	"github.com/go-resty/resty/v2"
	"github.com/sirupsen/logrus"
)

// InfoService provides access to cluster-level info and ping endpoints.
type InfoService interface {
	Info(ctx context.Context) (*InfoResponse, error)
	Ping(ctx context.Context) (bool, error)
}

// DefaultInfoService is the default InfoService implementation.
type DefaultInfoService struct {
	client *resty.Client
	logger *logrus.Entry
}

// NewInfoService creates a new InfoService.
func NewInfoService(client *resty.Client, logger *logrus.Entry) InfoService {
	return &DefaultInfoService{client: client, logger: logger.WithField("service", "info")}
}

func (s *DefaultInfoService) Info(ctx context.Context) (*InfoResponse, error) {
	resp, err := s.client.R().SetContext(ctx).Get("/")
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result InfoResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultInfoService) Ping(ctx context.Context) (bool, error) {
	resp, err := s.client.R().SetContext(ctx).Head("/")
	if err != nil {
		return false, wrapNetworkError(s.logger, err)
	}

	switch resp.StatusCode() {
	case http.StatusOK:
		return true, nil
	default:
		return false, nil
	}
}
