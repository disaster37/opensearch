package api

import (
	"context"
	"fmt"
	json "github.com/goccy/go-json"
	"strings"

	"github.com/disaster37/opensearch/v3/querydsl"
	"github.com/go-resty/resty/v2"
	"github.com/sirupsen/logrus"
)

type SearchService interface {
	Search(ctx context.Context, req *SearchRequest) (*querydsl.SearchResult, error)
	MultiSearch(ctx context.Context, body any) (*querydsl.MultiSearchResult, error)
	SearchTemplate(ctx context.Context, req *SearchTemplateRequest) (*querydsl.SearchResult, error)
	MultiSearchTemplate(ctx context.Context, req *MultiSearchTemplateRequest) (*querydsl.MultiSearchResult, error)
	RenderSearchTemplate(ctx context.Context, req *RenderSearchTemplateRequest) (*querydsl.RenderSearchTemplateResponse, error)
	RankEval(ctx context.Context, req *RankEvalRequest) (*querydsl.RankEvalResponse, error)
	Count(ctx context.Context, indices []string, body any) (int64, error)
	Scroll(ctx context.Context, req *ScrollRequest) (*querydsl.SearchResult, error)
	ClearScroll(ctx context.Context, scrollIds []string) (*querydsl.ClearScrollResponse, error)
	Validate(ctx context.Context, indices []string, body any) (*querydsl.ValidateResponse, error)
	SearchShards(ctx context.Context, indices []string) (*querydsl.SearchShardsResponse, error)
	FieldCaps(ctx context.Context, req *FieldCapsRequest) (*querydsl.FieldCapsResponse, error)
	CreatePIT(ctx context.Context, req *CreatePITRequest) (*querydsl.CreatePITResponse, error)
	DeletePIT(ctx context.Context, req *DeletePITRequest) (*querydsl.DeletePITResponse, error)
	GetAllPITs(ctx context.Context) (*querydsl.ListPITResponse, error)
	DeleteAllPITs(ctx context.Context) (*querydsl.DeletePITResponse, error)
}

type DefaultSearchService struct {
	client *resty.Client
	logger *logrus.Entry
}

func NewSearchService(client *resty.Client, logger *logrus.Entry) SearchService {
	return &DefaultSearchService{
		client: client,
		logger: logger.WithField("service", "search"),
	}
}

func (s *DefaultSearchService) Search(ctx context.Context, req *SearchRequest) (*querydsl.SearchResult, error) {
	var path string
	if len(req.Indices) > 0 {
		path = fmt.Sprintf("/%s/_search", strings.Join(req.Indices, ","))
	} else {
		path = "/_search"
	}

	r := s.client.R().SetContext(ctx).SetBody(req.Body)
	for k, v := range req.Params {
		r.SetQueryParam(k, v)
	}

	resp, err := r.Post(path)
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result querydsl.SearchResult
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultSearchService) MultiSearch(ctx context.Context, body any) (*querydsl.MultiSearchResult, error) {
	resp, err := s.client.R().
		SetContext(ctx).
		SetBody(body).
		SetHeader("Content-Type", "application/x-ndjson").
		Get("/_msearch")
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result querydsl.MultiSearchResult
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultSearchService) Count(ctx context.Context, indices []string, body any) (int64, error) {
	var path string
	if len(indices) > 0 {
		path = fmt.Sprintf("/%s/_count", strings.Join(indices, ","))
	} else {
		path = "/_count"
	}

	req := s.client.R().SetContext(ctx)
	if body != nil {
		req = req.SetBody(body)
	}

	resp, err := req.Post(path)
	if err != nil {
		return 0, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return 0, logAndReturnError(s.logger, resp)
	}

	var result querydsl.CountResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return 0, wrapUnmarshalError(s.logger, resp, err)
	}
	return result.Count, nil
}

func (s *DefaultSearchService) Scroll(ctx context.Context, req *ScrollRequest) (*querydsl.SearchResult, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}

	keepAlive := req.KeepAlive
	if keepAlive == "" {
		keepAlive = "5m"
	}

	body := map[string]string{
		"scroll":    keepAlive,
		"scroll_id": req.ScrollId,
	}

	resp, err := s.client.R().SetContext(ctx).SetBody(body).Post("/_search/scroll")
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result querydsl.SearchResult
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultSearchService) ClearScroll(ctx context.Context, scrollIds []string) (*querydsl.ClearScrollResponse, error) {
	if len(scrollIds) == 0 {
		return nil, fmt.Errorf("scroll_ids is required")
	}

	body := map[string][]string{
		"scroll_id": scrollIds,
	}

	resp, err := s.client.R().SetContext(ctx).SetBody(body).Delete("/_search/scroll")
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result querydsl.ClearScrollResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultSearchService) Validate(ctx context.Context, indices []string, body any) (*querydsl.ValidateResponse, error) {
	var path string
	if len(indices) > 0 {
		path = fmt.Sprintf("/%s/_validate/query", strings.Join(indices, ","))
	} else {
		path = "/_validate/query"
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

	var result querydsl.ValidateResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultSearchService) SearchShards(ctx context.Context, indices []string) (*querydsl.SearchShardsResponse, error) {
	if len(indices) == 0 {
		return nil, fmt.Errorf("indices is required")
	}

	path := fmt.Sprintf("/%s/_search_shards", strings.Join(indices, ","))
	resp, err := s.client.R().SetContext(ctx).Get(path)
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result querydsl.SearchShardsResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultSearchService) FieldCaps(ctx context.Context, req *FieldCapsRequest) (*querydsl.FieldCapsResponse, error) {
	var path string
	if len(req.Indices) > 0 {
		path = fmt.Sprintf("/%s/_field_caps", strings.Join(req.Indices, ","))
	} else {
		path = "/_field_caps"
	}

	r := s.client.R().SetContext(ctx)
	if len(req.Fields) > 0 {
		r = r.SetQueryParam("fields", strings.Join(req.Fields, ","))
	}

	resp, err := r.Get(path)
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		if resp.StatusCode() == 404 {
			return &querydsl.FieldCapsResponse{}, nil
		}
		return nil, logAndReturnError(s.logger, resp)
	}

	var result querydsl.FieldCapsResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultSearchService) SearchTemplate(ctx context.Context, req *SearchTemplateRequest) (*querydsl.SearchResult, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}

	var path string
	if len(req.Indices) > 0 {
		path = fmt.Sprintf("/%s/_search/template", strings.Join(req.Indices, ","))
	} else {
		path = "/_search/template"
	}

	r := s.client.R().SetContext(ctx).SetBody(req.Body)
	for k, v := range req.Params {
		r.SetQueryParam(k, v)
	}

	resp, err := r.Post(path)
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result querydsl.SearchResult
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultSearchService) MultiSearchTemplate(ctx context.Context, req *MultiSearchTemplateRequest) (*querydsl.MultiSearchResult, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}

	var path string
	if len(req.Indices) > 0 {
		path = fmt.Sprintf("/%s/_msearch/template", strings.Join(req.Indices, ","))
	} else {
		path = "/_msearch/template"
	}

	r := s.client.R().
		SetContext(ctx).
		SetBody(req.Body).
		SetHeader("Content-Type", "application/x-ndjson")
	for k, v := range req.Params {
		r.SetQueryParam(k, v)
	}

	resp, err := r.Post(path)
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result querydsl.MultiSearchResult
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultSearchService) RenderSearchTemplate(ctx context.Context, req *RenderSearchTemplateRequest) (*querydsl.RenderSearchTemplateResponse, error) {
	var path string
	if req.Id != "" {
		path = fmt.Sprintf("/_render/template/%s", req.Id)
	} else {
		path = "/_render/template"
	}

	r := s.client.R().SetContext(ctx)
	if req.Body != nil {
		r = r.SetBody(req.Body)
	}

	resp, err := r.Post(path)
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result querydsl.RenderSearchTemplateResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultSearchService) RankEval(ctx context.Context, req *RankEvalRequest) (*querydsl.RankEvalResponse, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}

	var path string
	if len(req.Indices) > 0 {
		path = fmt.Sprintf("/%s/_rank_eval", strings.Join(req.Indices, ","))
	} else {
		path = "/_rank_eval"
	}

	r := s.client.R().SetContext(ctx).SetBody(req.Body)
	for k, v := range req.Params {
		r.SetQueryParam(k, v)
	}

	resp, err := r.Post(path)
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result querydsl.RankEvalResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultSearchService) CreatePIT(ctx context.Context, req *CreatePITRequest) (*querydsl.CreatePITResponse, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}

	path := fmt.Sprintf("/%s/_search/point_in_time", strings.Join(req.Indices, ","))

	r := s.client.R().SetContext(ctx).SetQueryParam("keep_alive", req.KeepAlive)
	for k, v := range req.Params {
		r.SetQueryParam(k, v)
	}

	resp, err := r.Post(path)
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result querydsl.CreatePITResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultSearchService) DeletePIT(ctx context.Context, req *DeletePITRequest) (*querydsl.DeletePITResponse, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}

	body := map[string][]string{
		"pit_id": req.PitIds,
	}

	resp, err := s.client.R().SetContext(ctx).SetBody(body).Delete("/_search/point_in_time")
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result querydsl.DeletePITResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultSearchService) GetAllPITs(ctx context.Context) (*querydsl.ListPITResponse, error) {
	resp, err := s.client.R().SetContext(ctx).Get("/_search/point_in_time/_all")
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result querydsl.ListPITResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultSearchService) DeleteAllPITs(ctx context.Context) (*querydsl.DeletePITResponse, error) {
	resp, err := s.client.R().SetContext(ctx).Delete("/_search/point_in_time/_all")
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result querydsl.DeletePITResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}
