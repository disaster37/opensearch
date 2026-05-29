package api

import (
	"context"
	"fmt"
	json "github.com/goccy/go-json"
	"strings"

	"github.com/go-resty/resty/v2"
	"github.com/sirupsen/logrus"
)

type DocumentService interface {
	Index(ctx context.Context, req *IndexRequest) (*IndexResponse, error)
	Create(ctx context.Context, req *CreateRequest) (*IndexResponse, error)
	Get(ctx context.Context, req *GetRequest) (*GetResult, error)
	GetSource(ctx context.Context, index string, id string) (json.RawMessage, error)
	MultiGet(ctx context.Context, items []*MultiGetItem) (*MgetResponse, error)
	Delete(ctx context.Context, req *DeleteRequest) (*DeleteResponse, error)
	DeleteByQuery(ctx context.Context, indices []string, body any) (*BulkIndexByScrollResponse, error)
	DeleteByQueryRethrottle(ctx context.Context, req *RethrottleRequest) (*TasksListResponse, error)
	Update(ctx context.Context, req *UpdateRequest) (*UpdateResponse, error)
	UpdateByQuery(ctx context.Context, indices []string, body any) (*BulkIndexByScrollResponse, error)
	UpdateByQueryRethrottle(ctx context.Context, req *RethrottleRequest) (*TasksListResponse, error)
	Bulk(ctx context.Context, index string, body string) (*BulkResponse, error)
	Exists(ctx context.Context, index string, id string) (bool, error)
	ExistsSource(ctx context.Context, index string, id string) (bool, error)
	Explain(ctx context.Context, index string, id string, body any) (*ExplainResponse, error)
	TermVectors(ctx context.Context, index string, id string, body any) (*TermvectorsResponse, error)
	MultiTermVectors(ctx context.Context, index string, body any) (*MultiTermvectorResponse, error)
	Reindex(ctx context.Context, body any) (*BulkIndexByScrollResponse, error)
	ReindexRethrottle(ctx context.Context, req *RethrottleRequest) (*TasksListResponse, error)
}

type DefaultDocumentService struct {
	client *resty.Client
	logger *logrus.Entry
}

func NewDocumentService(client *resty.Client, logger *logrus.Entry) DocumentService {
	return &DefaultDocumentService{
		client: client,
		logger: logger.WithField("service", "document"),
	}
}

func (s *DefaultDocumentService) Index(ctx context.Context, req *IndexRequest) (*IndexResponse, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}

	var path string
	var method string
	if req.Id != "" {
		path = fmt.Sprintf("/%s/_doc/%s", req.Index, req.Id)
		method = "PUT"
	} else {
		path = fmt.Sprintf("/%s/_doc", req.Index)
		method = "POST"
	}

	r := s.client.R().SetContext(ctx).SetBody(req.Body)
	for k, v := range req.Params {
		r.SetQueryParam(k, v)
	}

	resp, err := r.Execute(method, path)
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result IndexResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultDocumentService) Get(ctx context.Context, req *GetRequest) (*GetResult, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}

	path := fmt.Sprintf("/%s/_doc/%s", req.Index, req.Id)
	resp, err := s.client.R().SetContext(ctx).SetQueryParams(req.Params).Get(path)
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result GetResult
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultDocumentService) MultiGet(ctx context.Context, items []*MultiGetItem) (*MgetResponse, error) {
	if len(items) == 0 {
		return nil, fmt.Errorf("items is required")
	}

	docs := make([]map[string]string, 0, len(items))
	for _, item := range items {
		docs = append(docs, map[string]string{
			"_index": item.Index,
			"_id":    item.Id,
		})
	}

	body := map[string]any{
		"docs": docs,
	}

	resp, err := s.client.R().SetContext(ctx).SetBody(body).Get("/_mget")
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result MgetResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultDocumentService) Delete(ctx context.Context, req *DeleteRequest) (*DeleteResponse, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}

	path := fmt.Sprintf("/%s/_doc/%s", req.Index, req.Id)
	resp, err := s.client.R().SetContext(ctx).SetQueryParams(req.Params).Delete(path)
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() && resp.StatusCode() != 404 {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result DeleteResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	if resp.StatusCode() == 404 {
		return &result, fmt.Errorf("document not found")
	}
	return &result, nil
}

func (s *DefaultDocumentService) DeleteByQuery(ctx context.Context, indices []string, body any) (*BulkIndexByScrollResponse, error) {
	if len(indices) == 0 {
		return nil, fmt.Errorf("indices is required")
	}

	path := fmt.Sprintf("/%s/_delete_by_query", strings.Join(indices, ","))
	resp, err := s.client.R().SetContext(ctx).SetBody(body).Post(path)
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result BulkIndexByScrollResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultDocumentService) Update(ctx context.Context, req *UpdateRequest) (*UpdateResponse, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}

	path := fmt.Sprintf("/%s/_update/%s", req.Index, req.Id)
	resp, err := s.client.R().SetContext(ctx).SetBody(req.Body).SetQueryParams(req.Params).Post(path)
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result UpdateResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultDocumentService) UpdateByQuery(ctx context.Context, indices []string, body any) (*BulkIndexByScrollResponse, error) {
	if len(indices) == 0 {
		return nil, fmt.Errorf("indices is required")
	}

	path := fmt.Sprintf("/%s/_update_by_query", strings.Join(indices, ","))
	resp, err := s.client.R().SetContext(ctx).SetBody(body).Post(path)
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result BulkIndexByScrollResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultDocumentService) Bulk(ctx context.Context, index string, body string) (*BulkResponse, error) {
	var path string
	if index != "" {
		path = fmt.Sprintf("/%s/_bulk", index)
	} else {
		path = "/_bulk"
	}

	resp, err := s.client.R().
		SetContext(ctx).
		SetBody(body).
		SetHeader("Content-Type", "application/x-ndjson").
		Post(path)
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result BulkResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultDocumentService) Exists(ctx context.Context, index string, id string) (bool, error) {
	if index == "" {
		return false, fmt.Errorf("index is required")
	}
	if id == "" {
		return false, fmt.Errorf("id is required")
	}

	path := fmt.Sprintf("/%s/_doc/%s", index, id)
	resp, err := s.client.R().
		SetContext(ctx).
		SetDoNotParseResponse(true).
		Head(path)
	if err != nil {
		return false, wrapNetworkError(s.logger, err)
	}
	defer resp.RawResponse.Body.Close()

	switch resp.StatusCode() {
	case 200:
		return true, nil
	case 404:
		return false, nil
	default:
		return false, fmt.Errorf("opensearch: unexpected status code %d", resp.StatusCode())
	}
}

func (s *DefaultDocumentService) Explain(ctx context.Context, index string, id string, body any) (*ExplainResponse, error) {
	if index == "" {
		return nil, fmt.Errorf("index is required")
	}
	if id == "" {
		return nil, fmt.Errorf("id is required")
	}

	path := fmt.Sprintf("/%s/_explain/%s", index, id)
	resp, err := s.client.R().SetContext(ctx).SetBody(body).Get(path)
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result ExplainResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultDocumentService) TermVectors(ctx context.Context, index string, id string, body any) (*TermvectorsResponse, error) {
	if index == "" {
		return nil, fmt.Errorf("index is required")
	}
	if id == "" {
		return nil, fmt.Errorf("id is required")
	}

	path := fmt.Sprintf("/%s/_termvectors/%s", index, id)
	req := s.client.R().SetContext(ctx)
	if body != nil {
		req = req.SetBody(body)
	}

	resp, err := req.Get(path)
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result TermvectorsResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultDocumentService) MultiTermVectors(ctx context.Context, index string, body any) (*MultiTermvectorResponse, error) {
	var path string
	if index != "" {
		path = fmt.Sprintf("/%s/_mtermvectors", index)
	} else {
		path = "/_mtermvectors"
	}

	req := s.client.R().SetContext(ctx)
	if body != nil {
		req = req.SetBody(body)
	}

	resp, err := req.Get(path)
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result MultiTermvectorResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultDocumentService) Reindex(ctx context.Context, body any) (*BulkIndexByScrollResponse, error) {
	resp, err := s.client.R().SetContext(ctx).SetBody(body).Post("/_reindex")
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result BulkIndexByScrollResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultDocumentService) Create(ctx context.Context, req *CreateRequest) (*IndexResponse, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}

	path := fmt.Sprintf("/%s/_create/%s", req.Index, req.Id)

	r := s.client.R().SetContext(ctx).SetBody(req.Body)
	for k, v := range req.Params {
		r.SetQueryParam(k, v)
	}

	resp, err := r.Put(path)
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result IndexResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultDocumentService) GetSource(ctx context.Context, index string, id string) (json.RawMessage, error) {
	if index == "" {
		return nil, fmt.Errorf("index is required")
	}
	if id == "" {
		return nil, fmt.Errorf("id is required")
	}

	path := fmt.Sprintf("/%s/_source/%s", index, id)

	resp, err := s.client.R().SetContext(ctx).Get(path)
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	return json.RawMessage(resp.Body()), nil
}

func (s *DefaultDocumentService) ExistsSource(ctx context.Context, index string, id string) (bool, error) {
	if index == "" {
		return false, fmt.Errorf("index is required")
	}
	if id == "" {
		return false, fmt.Errorf("id is required")
	}

	path := fmt.Sprintf("/%s/_source/%s", index, id)

	resp, err := s.client.R().
		SetContext(ctx).
		SetDoNotParseResponse(true).
		Head(path)
	if err != nil {
		return false, wrapNetworkError(s.logger, err)
	}
	defer resp.RawResponse.Body.Close()

	switch resp.StatusCode() {
	case 200:
		return true, nil
	case 404:
		return false, nil
	default:
		return false, fmt.Errorf("opensearch: unexpected status code %d", resp.StatusCode())
	}
}

func (s *DefaultDocumentService) ReindexRethrottle(ctx context.Context, req *RethrottleRequest) (*TasksListResponse, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}

	path := fmt.Sprintf("/_reindex/%s/_rethrottle", req.TaskId)

	resp, err := s.client.R().
		SetContext(ctx).
		SetQueryParam("requests_per_second", fmt.Sprintf("%f", req.RequestsPerSecond)).
		Post(path)
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result TasksListResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultDocumentService) DeleteByQueryRethrottle(ctx context.Context, req *RethrottleRequest) (*TasksListResponse, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}

	path := fmt.Sprintf("/_delete_by_query/%s/_rethrottle", req.TaskId)

	resp, err := s.client.R().
		SetContext(ctx).
		SetQueryParam("requests_per_second", fmt.Sprintf("%f", req.RequestsPerSecond)).
		Post(path)
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result TasksListResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultDocumentService) UpdateByQueryRethrottle(ctx context.Context, req *RethrottleRequest) (*TasksListResponse, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}

	path := fmt.Sprintf("/_update_by_query/%s/_rethrottle", req.TaskId)

	resp, err := s.client.R().
		SetContext(ctx).
		SetQueryParam("requests_per_second", fmt.Sprintf("%f", req.RequestsPerSecond)).
		Post(path)
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result TasksListResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}
