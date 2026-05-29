package api

import (
	"context"
	"fmt"
	json "github.com/goccy/go-json"

	"github.com/go-resty/resty/v2"
	"github.com/sirupsen/logrus"
)

type ScriptService interface {
	Get(ctx context.Context, id string) (*ScriptGetResponse, error)
	Put(ctx context.Context, req *ScriptPutRequest) (*ScriptPutResponse, error)
	Delete(ctx context.Context, id string) (*ScriptDeleteResponse, error)
	PainlessExecute(ctx context.Context, body any) (*PainlessExecuteResponse, error)
	GetContext(ctx context.Context) (*ScriptContextResponse, error)
	GetLanguages(ctx context.Context) (*ScriptLanguagesResponse, error)
}

type DefaultScriptService struct {
	client *resty.Client
	logger *logrus.Entry
}

func NewScriptService(client *resty.Client, logger *logrus.Entry) ScriptService {
	return &DefaultScriptService{client: client, logger: logger.WithField("service", "script")}
}

func (s *DefaultScriptService) Get(ctx context.Context, id string) (*ScriptGetResponse, error) {
	if id == "" {
		return nil, fmt.Errorf("script id is required")
	}

	path := fmt.Sprintf("/_scripts/%s", id)

	resp, err := s.client.R().SetContext(ctx).Get(path)
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result ScriptGetResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultScriptService) Put(ctx context.Context, req *ScriptPutRequest) (*ScriptPutResponse, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}

	path := fmt.Sprintf("/_scripts/%s", req.Id)

	resp, err := s.client.R().SetContext(ctx).SetBody(req.Body).Put(path)
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result ScriptPutResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultScriptService) Delete(ctx context.Context, id string) (*ScriptDeleteResponse, error) {
	if id == "" {
		return nil, fmt.Errorf("script id is required")
	}

	path := fmt.Sprintf("/_scripts/%s", id)

	resp, err := s.client.R().SetContext(ctx).Delete(path)
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result ScriptDeleteResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultScriptService) PainlessExecute(ctx context.Context, body any) (*PainlessExecuteResponse, error) {
	r := s.client.R().SetContext(ctx)
	if body != nil {
		r = r.SetBody(body)
	}

	resp, err := r.Post("/_scripts/painless/_execute")
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result PainlessExecuteResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultScriptService) GetContext(ctx context.Context) (*ScriptContextResponse, error) {
	resp, err := s.client.R().SetContext(ctx).Get("/_script_context")
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result ScriptContextResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultScriptService) GetLanguages(ctx context.Context) (*ScriptLanguagesResponse, error) {
	resp, err := s.client.R().SetContext(ctx).Get("/_script_language")
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result ScriptLanguagesResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}
