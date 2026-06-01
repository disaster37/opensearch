package api

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	json "github.com/goccy/go-json"

	"github.com/disaster37/opensearch/v4/types"
	"github.com/go-resty/resty/v2"
	"github.com/sirupsen/logrus"
)

type IndicesService interface {
	Create(ctx context.Context, index string, body any) (*types.AcknowledgedResponse, error)
	Delete(ctx context.Context, indices []string) (*types.AcknowledgedResponse, error)
	Get(ctx context.Context, indices []string) (map[string]*IndicesGetResponse, error)
	Exists(ctx context.Context, indices []string) (bool, error)
	Open(ctx context.Context, index string) (*types.AcknowledgedResponse, error)
	Close(ctx context.Context, index string) (*types.AcknowledgedResponse, error)
	Rollover(ctx context.Context, alias string, body any) (*IndicesRolloverResponse, error)
	Shrink(ctx context.Context, req *ShrinkRequest) (*types.AcknowledgedResponse, error)
	Flush(ctx context.Context, indices []string) (*IndicesFlushResponse, error)
	Refresh(ctx context.Context, indices []string) (*RefreshResult, error)
	Forcemerge(ctx context.Context, indices []string) (*IndicesForcemergeResponse, error)
	// Deprecated: Freeze is not supported in OpenSearch (Elasticsearch-only).
	Freeze(ctx context.Context, index string) (*types.AcknowledgedResponse, error)
	// Deprecated: Unfreeze is not supported in OpenSearch (Elasticsearch-only).
	Unfreeze(ctx context.Context, index string) (*types.AcknowledgedResponse, error)
	ClearCache(ctx context.Context, indices []string) (*IndicesClearCacheResponse, error)
	Stats(ctx context.Context, req *IndicesStatsRequest) (*IndicesStatsResponse, error)
	Segments(ctx context.Context, indices []string) (*IndicesSegmentsResponse, error)
	Analyze(ctx context.Context, index string, body any) (*IndicesAnalyzeResponse, error)
	PutAlias(ctx context.Context, req *PutAliasRequest) (*types.AcknowledgedResponse, error)
	GetAliases(ctx context.Context, indices []string) (map[string]*IndicesGetResponse, error)
	GetSettings(ctx context.Context, indices []string) (map[string]*IndicesGetSettingsResponse, error)
	PutSettings(ctx context.Context, indices []string, body any) (*types.AcknowledgedResponse, error)
	GetMapping(ctx context.Context, indices []string) (map[string]any, error)
	PutMapping(ctx context.Context, req *PutMappingRequest) (*types.AcknowledgedResponse, error)
	GetFieldMapping(ctx context.Context, req *GetFieldMappingRequest) (map[string]any, error)
	GetTemplate(ctx context.Context, names []string) (map[string]*IndicesGetTemplateResponse, error)
	PutTemplate(ctx context.Context, req *PutTemplateRequest) (*types.AcknowledgedResponse, error)
	ExistsTemplate(ctx context.Context, name string) (bool, error)
	DeleteTemplate(ctx context.Context, name string) (*types.AcknowledgedResponse, error)
	PutIndexTemplate(ctx context.Context, req *PutIndexTemplateRequest) (*types.AcknowledgedResponse, error)
	GetIndexTemplate(ctx context.Context, names []string) (*IndicesGetIndexTemplateResponse, error)
	DeleteIndexTemplate(ctx context.Context, name string) (*types.AcknowledgedResponse, error)
	PutComponentTemplate(ctx context.Context, req *PutComponentTemplateRequest) (*types.AcknowledgedResponse, error)
	GetComponentTemplate(ctx context.Context, names []string) (*IndicesGetComponentTemplateResponse, error)
	DeleteComponentTemplate(ctx context.Context, name string) (*types.AcknowledgedResponse, error)
	CreateDataStream(ctx context.Context, name string) (*types.AcknowledgedResponse, error)
	GetDataStream(ctx context.Context, names []string) (*IndicesDataStreamGetResponse, error)
	DeleteDataStream(ctx context.Context, names []string) (*types.AcknowledgedResponse, error)
	AddBlock(ctx context.Context, req *AddBlockRequest) (*IndicesBlockResponse, error)
	Clone(ctx context.Context, req *CloneRequest) (*types.AcknowledgedResponse, error)
	Split(ctx context.Context, req *SplitRequest) (*types.AcknowledgedResponse, error)
	DeleteAlias(ctx context.Context, req *DeleteAliasRequest) (*types.AcknowledgedResponse, error)
	ExistsAlias(ctx context.Context, indices []string, name string) (bool, error)
	ExistsIndexTemplate(ctx context.Context, name string) (bool, error)
	Recovery(ctx context.Context, indices []string) (*IndicesRecoveryResponse, error)
	ShardStores(ctx context.Context, indices []string) (*IndicesShardStoresResponse, error)
	UpdateAliases(ctx context.Context, body any) (*types.AcknowledgedResponse, error)
	ResolveIndex(ctx context.Context, name string) (*IndicesResolveIndexResponse, error)
	SimulateIndexTemplate(ctx context.Context, req *SimulateIndexTemplateRequest) (*IndicesSimulateTemplateResponse, error)
	SimulateTemplate(ctx context.Context, req *SimulateTemplateRequest) (*IndicesSimulateTemplateResponse, error)
	DataStreamsStats(ctx context.Context, names []string) (*IndicesDataStreamsStatsResponse, error)
}

type DefaultIndicesService struct {
	client *resty.Client
	logger *logrus.Entry
}

func NewIndicesService(client *resty.Client, logger *logrus.Entry) IndicesService {
	return &DefaultIndicesService{
		client: client,
		logger: logger.WithField("service", "indices"),
	}
}

func (s *DefaultIndicesService) Create(ctx context.Context, index string, body any) (*types.AcknowledgedResponse, error) {
	if index == "" {
		return nil, fmt.Errorf("index is required")
	}

	req := s.client.R().SetContext(ctx)
	if body != nil {
		req.SetBody(body)
	}

	resp, err := req.Put(fmt.Sprintf("/%s", index))
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

func (s *DefaultIndicesService) Delete(ctx context.Context, indices []string) (*types.AcknowledgedResponse, error) {
	if len(indices) == 0 {
		return nil, fmt.Errorf("indices is required")
	}

	resp, err := s.client.R().
		SetContext(ctx).
		Delete(fmt.Sprintf("/%s", strings.Join(indices, ",")))
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

func (s *DefaultIndicesService) Get(ctx context.Context, indices []string) (map[string]*IndicesGetResponse, error) {
	if len(indices) == 0 {
		return nil, fmt.Errorf("indices is required")
	}

	resp, err := s.client.R().
		SetContext(ctx).
		Get(fmt.Sprintf("/%s", strings.Join(indices, ",")))
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result map[string]*IndicesGetResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return result, nil
}

func (s *DefaultIndicesService) Exists(ctx context.Context, indices []string) (bool, error) {
	if len(indices) == 0 {
		return false, fmt.Errorf("indices is required")
	}

	req := s.client.R().SetContext(ctx)
	resp, err := req.Head(fmt.Sprintf("/%s", strings.Join(indices, ",")))
	if err != nil {
		return false, wrapNetworkError(s.logger, err)
	}

	switch resp.StatusCode() {
	case http.StatusOK:
		return true, nil
	case http.StatusNotFound:
		return false, nil
	default:
		return false, fmt.Errorf("unexpected HTTP status %d", resp.StatusCode())
	}
}

func (s *DefaultIndicesService) Open(ctx context.Context, index string) (*types.AcknowledgedResponse, error) {
	if index == "" {
		return nil, fmt.Errorf("index is required")
	}

	resp, err := s.client.R().
		SetContext(ctx).
		Post(fmt.Sprintf("/%s/_open", index))
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

func (s *DefaultIndicesService) Close(ctx context.Context, index string) (*types.AcknowledgedResponse, error) {
	if index == "" {
		return nil, fmt.Errorf("index is required")
	}

	resp, err := s.client.R().
		SetContext(ctx).
		Post(fmt.Sprintf("/%s/_close", index))
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

func (s *DefaultIndicesService) Rollover(ctx context.Context, alias string, body any) (*IndicesRolloverResponse, error) {
	if alias == "" {
		return nil, fmt.Errorf("alias is required")
	}

	req := s.client.R().SetContext(ctx)
	if body != nil {
		req.SetBody(body)
	}

	resp, err := req.Post(fmt.Sprintf("/%s/_rollover", alias))
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result IndicesRolloverResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultIndicesService) Shrink(ctx context.Context, req *ShrinkRequest) (*types.AcknowledgedResponse, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}

	r := s.client.R().SetContext(ctx)
	if req.Body != nil {
		r.SetBody(req.Body)
	}

	resp, err := r.Post(fmt.Sprintf("/%s/_shrink/%s", req.Source, req.Target))
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

func (s *DefaultIndicesService) Flush(ctx context.Context, indices []string) (*IndicesFlushResponse, error) {
	var path string
	if len(indices) > 0 {
		path = fmt.Sprintf("/%s/_flush", strings.Join(indices, ","))
	} else {
		path = "/_flush"
	}

	resp, err := s.client.R().
		SetContext(ctx).
		Post(path)
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result IndicesFlushResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultIndicesService) Refresh(ctx context.Context, indices []string) (*RefreshResult, error) {
	var path string
	if len(indices) > 0 {
		path = fmt.Sprintf("/%s/_refresh", strings.Join(indices, ","))
	} else {
		path = "/_refresh"
	}

	resp, err := s.client.R().
		SetContext(ctx).
		Post(path)
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result RefreshResult
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultIndicesService) Forcemerge(ctx context.Context, indices []string) (*IndicesForcemergeResponse, error) {
	var path string
	if len(indices) > 0 {
		path = fmt.Sprintf("/%s/_forcemerge", strings.Join(indices, ","))
	} else {
		path = "/_forcemerge"
	}

	resp, err := s.client.R().
		SetContext(ctx).
		Post(path)
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result IndicesForcemergeResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

// Deprecated: Freeze is not supported in OpenSearch (Elasticsearch-only).
func (s *DefaultIndicesService) Freeze(ctx context.Context, index string) (*types.AcknowledgedResponse, error) {
	if index == "" {
		return nil, fmt.Errorf("index is required")
	}

	resp, err := s.client.R().
		SetContext(ctx).
		Post(fmt.Sprintf("/%s/_freeze", index))
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

// Deprecated: Unfreeze is not supported in OpenSearch (Elasticsearch-only).
func (s *DefaultIndicesService) Unfreeze(ctx context.Context, index string) (*types.AcknowledgedResponse, error) {
	if index == "" {
		return nil, fmt.Errorf("index is required")
	}

	resp, err := s.client.R().
		SetContext(ctx).
		Post(fmt.Sprintf("/%s/_unfreeze", index))
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

func (s *DefaultIndicesService) ClearCache(ctx context.Context, indices []string) (*IndicesClearCacheResponse, error) {
	var path string
	if len(indices) > 0 {
		path = fmt.Sprintf("/%s/_cache/clear", strings.Join(indices, ","))
	} else {
		path = "/_cache/clear"
	}

	resp, err := s.client.R().
		SetContext(ctx).
		Post(path)
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result IndicesClearCacheResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultIndicesService) Stats(ctx context.Context, req *IndicesStatsRequest) (*IndicesStatsResponse, error) {
	var path string
	switch {
	case len(req.Indices) > 0 && len(req.Metrics) > 0:
		path = fmt.Sprintf("/%s/_stats/%s", strings.Join(req.Indices, ","), strings.Join(req.Metrics, ","))
	case len(req.Indices) > 0:
		path = fmt.Sprintf("/%s/_stats", strings.Join(req.Indices, ","))
	case len(req.Metrics) > 0:
		path = fmt.Sprintf("/_stats/%s", strings.Join(req.Metrics, ","))
	default:
		path = "/_stats"
	}

	resp, err := s.client.R().
		SetContext(ctx).
		Get(path)
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result IndicesStatsResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultIndicesService) Segments(ctx context.Context, indices []string) (*IndicesSegmentsResponse, error) {
	var path string
	if len(indices) > 0 {
		path = fmt.Sprintf("/%s/_segments", strings.Join(indices, ","))
	} else {
		path = "/_segments"
	}

	resp, err := s.client.R().
		SetContext(ctx).
		Get(path)
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result IndicesSegmentsResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultIndicesService) Analyze(ctx context.Context, index string, body any) (*IndicesAnalyzeResponse, error) {
	var path string
	if index != "" {
		path = fmt.Sprintf("/%s/_analyze", index)
	} else {
		path = "/_analyze"
	}

	req := s.client.R().SetContext(ctx)
	if body != nil {
		req.SetBody(body)
	}

	resp, err := req.Post(path)
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result IndicesAnalyzeResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultIndicesService) PutAlias(ctx context.Context, req *PutAliasRequest) (*types.AcknowledgedResponse, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}

	r := s.client.R().SetContext(ctx)
	if req.Body != nil {
		r.SetBody(req.Body)
	}

	resp, err := r.Put(fmt.Sprintf("/%s/_alias/%s", req.Index, req.Alias))
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

func (s *DefaultIndicesService) GetAliases(ctx context.Context, indices []string) (map[string]*IndicesGetResponse, error) {
	var path string
	if len(indices) > 0 {
		path = fmt.Sprintf("/%s/_alias", strings.Join(indices, ","))
	} else {
		path = "/_alias"
	}

	resp, err := s.client.R().
		SetContext(ctx).
		Get(path)
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result map[string]*IndicesGetResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return result, nil
}

func (s *DefaultIndicesService) GetSettings(ctx context.Context, indices []string) (map[string]*IndicesGetSettingsResponse, error) {
	var path string
	if len(indices) > 0 {
		path = fmt.Sprintf("/%s/_settings", strings.Join(indices, ","))
	} else {
		path = "/_settings"
	}

	resp, err := s.client.R().
		SetContext(ctx).
		Get(path)
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result map[string]*IndicesGetSettingsResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return result, nil
}

func (s *DefaultIndicesService) PutSettings(ctx context.Context, indices []string, body any) (*types.AcknowledgedResponse, error) {
	var path string
	if len(indices) > 0 {
		path = fmt.Sprintf("/%s/_settings", strings.Join(indices, ","))
	} else {
		path = "/_settings"
	}

	req := s.client.R().SetContext(ctx)
	if body != nil {
		req.SetBody(body)
	}

	resp, err := req.Put(path)
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

func (s *DefaultIndicesService) GetMapping(ctx context.Context, indices []string) (map[string]any, error) {
	var path string
	if len(indices) > 0 {
		path = fmt.Sprintf("/%s/_mapping", strings.Join(indices, ","))
	} else {
		path = "/_mapping"
	}

	resp, err := s.client.R().
		SetContext(ctx).
		Get(path)
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result map[string]any
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return result, nil
}

func (s *DefaultIndicesService) PutMapping(ctx context.Context, req *PutMappingRequest) (*types.AcknowledgedResponse, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}

	resp, err := s.client.R().
		SetContext(ctx).
		SetBody(req.Body).
		Put(fmt.Sprintf("/%s/_mapping", strings.Join(req.Indices, ",")))
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

func (s *DefaultIndicesService) GetFieldMapping(ctx context.Context, req *GetFieldMappingRequest) (map[string]any, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}

	var path string
	if len(req.Indices) > 0 {
		path = fmt.Sprintf("/%s/_mapping/field/%s", strings.Join(req.Indices, ","), strings.Join(req.Fields, ","))
	} else {
		path = fmt.Sprintf("/_mapping/field/%s", strings.Join(req.Fields, ","))
	}

	resp, err := s.client.R().
		SetContext(ctx).
		Get(path)
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result map[string]any
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return result, nil
}

func (s *DefaultIndicesService) GetTemplate(ctx context.Context, names []string) (map[string]*IndicesGetTemplateResponse, error) {
	var path string
	if len(names) > 0 {
		path = fmt.Sprintf("/_template/%s", strings.Join(names, ","))
	} else {
		path = "/_template"
	}

	resp, err := s.client.R().
		SetContext(ctx).
		Get(path)
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result map[string]*IndicesGetTemplateResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return result, nil
}

func (s *DefaultIndicesService) PutTemplate(ctx context.Context, req *PutTemplateRequest) (*types.AcknowledgedResponse, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}

	resp, err := s.client.R().
		SetContext(ctx).
		SetBody(req.Body).
		Put(fmt.Sprintf("/_template/%s", req.Name))
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

func (s *DefaultIndicesService) ExistsTemplate(ctx context.Context, name string) (bool, error) {
	if name == "" {
		return false, fmt.Errorf("name is required")
	}

	resp, err := s.client.R().
		SetContext(ctx).
		Head(fmt.Sprintf("/_template/%s", name))
	if err != nil {
		return false, wrapNetworkError(s.logger, err)
	}

	switch resp.StatusCode() {
	case http.StatusOK:
		return true, nil
	case http.StatusNotFound:
		return false, nil
	default:
		return false, fmt.Errorf("unexpected HTTP status %d", resp.StatusCode())
	}
}

func (s *DefaultIndicesService) DeleteTemplate(ctx context.Context, name string) (*types.AcknowledgedResponse, error) {
	if name == "" {
		return nil, fmt.Errorf("name is required")
	}

	resp, err := s.client.R().
		SetContext(ctx).
		Delete(fmt.Sprintf("/_template/%s", name))
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

func (s *DefaultIndicesService) PutIndexTemplate(ctx context.Context, req *PutIndexTemplateRequest) (*types.AcknowledgedResponse, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}

	resp, err := s.client.R().
		SetContext(ctx).
		SetBody(req.Body).
		Put(fmt.Sprintf("/_index_template/%s", req.Name))
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

func (s *DefaultIndicesService) GetIndexTemplate(ctx context.Context, names []string) (*IndicesGetIndexTemplateResponse, error) {
	var path string
	if len(names) > 0 {
		path = fmt.Sprintf("/_index_template/%s", strings.Join(names, ","))
	} else {
		path = "/_index_template"
	}

	resp, err := s.client.R().
		SetContext(ctx).
		Get(path)
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result IndicesGetIndexTemplateResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultIndicesService) DeleteIndexTemplate(ctx context.Context, name string) (*types.AcknowledgedResponse, error) {
	if name == "" {
		return nil, fmt.Errorf("name is required")
	}

	resp, err := s.client.R().
		SetContext(ctx).
		Delete(fmt.Sprintf("/_index_template/%s", name))
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

func (s *DefaultIndicesService) PutComponentTemplate(ctx context.Context, req *PutComponentTemplateRequest) (*types.AcknowledgedResponse, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}

	resp, err := s.client.R().
		SetContext(ctx).
		SetBody(req.Body).
		Put(fmt.Sprintf("/_component_template/%s", req.Name))
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

func (s *DefaultIndicesService) GetComponentTemplate(ctx context.Context, names []string) (*IndicesGetComponentTemplateResponse, error) {
	var path string
	if len(names) > 0 {
		path = fmt.Sprintf("/_component_template/%s", strings.Join(names, ","))
	} else {
		path = "/_component_template"
	}

	resp, err := s.client.R().
		SetContext(ctx).
		Get(path)
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result IndicesGetComponentTemplateResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultIndicesService) DeleteComponentTemplate(ctx context.Context, name string) (*types.AcknowledgedResponse, error) {
	if name == "" {
		return nil, fmt.Errorf("name is required")
	}

	resp, err := s.client.R().
		SetContext(ctx).
		Delete(fmt.Sprintf("/_component_template/%s", name))
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

func (s *DefaultIndicesService) CreateDataStream(ctx context.Context, name string) (*types.AcknowledgedResponse, error) {
	if name == "" {
		return nil, fmt.Errorf("name is required")
	}

	resp, err := s.client.R().
		SetContext(ctx).
		Put(fmt.Sprintf("/_data_stream/%s", name))
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

func (s *DefaultIndicesService) GetDataStream(ctx context.Context, names []string) (*IndicesDataStreamGetResponse, error) {
	var path string
	if len(names) > 0 {
		path = fmt.Sprintf("/_data_stream/%s", strings.Join(names, ","))
	} else {
		path = "/_data_stream"
	}

	resp, err := s.client.R().
		SetContext(ctx).
		Get(path)
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result IndicesDataStreamGetResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultIndicesService) DeleteDataStream(ctx context.Context, names []string) (*types.AcknowledgedResponse, error) {
	if len(names) == 0 {
		return nil, fmt.Errorf("names is required")
	}

	resp, err := s.client.R().
		SetContext(ctx).
		Delete(fmt.Sprintf("/_data_stream/%s", strings.Join(names, ",")))
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

func (s *DefaultIndicesService) AddBlock(ctx context.Context, req *AddBlockRequest) (*IndicesBlockResponse, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}

	resp, err := s.client.R().
		SetContext(ctx).
		Put(fmt.Sprintf("/%s/_block/%s", strings.Join(req.Indices, ","), req.Block))
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result IndicesBlockResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultIndicesService) Clone(ctx context.Context, req *CloneRequest) (*types.AcknowledgedResponse, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}

	r := s.client.R().SetContext(ctx)
	if req.Body != nil {
		r.SetBody(req.Body)
	}

	resp, err := r.Post(fmt.Sprintf("/%s/_clone/%s", req.Source, req.Target))
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

func (s *DefaultIndicesService) Split(ctx context.Context, req *SplitRequest) (*types.AcknowledgedResponse, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}

	r := s.client.R().SetContext(ctx)
	if req.Body != nil {
		r.SetBody(req.Body)
	}

	resp, err := r.Post(fmt.Sprintf("/%s/_split/%s", req.Source, req.Target))
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

func (s *DefaultIndicesService) DeleteAlias(ctx context.Context, req *DeleteAliasRequest) (*types.AcknowledgedResponse, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}

	resp, err := s.client.R().
		SetContext(ctx).
		Delete(fmt.Sprintf("/%s/_alias/%s", strings.Join(req.Indices, ","), strings.Join(req.Names, ",")))
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

func (s *DefaultIndicesService) ExistsAlias(ctx context.Context, indices []string, name string) (bool, error) {
	if name == "" {
		return false, fmt.Errorf("name is required")
	}

	var path string
	if len(indices) > 0 {
		path = fmt.Sprintf("/%s/_alias/%s", strings.Join(indices, ","), name)
	} else {
		path = fmt.Sprintf("/_alias/%s", name)
	}

	resp, err := s.client.R().
		SetContext(ctx).
		Head(path)
	if err != nil {
		return false, wrapNetworkError(s.logger, err)
	}

	switch resp.StatusCode() {
	case http.StatusOK:
		return true, nil
	case http.StatusNotFound:
		return false, nil
	default:
		return false, fmt.Errorf("unexpected HTTP status %d", resp.StatusCode())
	}
}

func (s *DefaultIndicesService) ExistsIndexTemplate(ctx context.Context, name string) (bool, error) {
	if name == "" {
		return false, fmt.Errorf("name is required")
	}

	resp, err := s.client.R().
		SetContext(ctx).
		Head(fmt.Sprintf("/_index_template/%s", name))
	if err != nil {
		return false, wrapNetworkError(s.logger, err)
	}

	switch resp.StatusCode() {
	case http.StatusOK:
		return true, nil
	case http.StatusNotFound:
		return false, nil
	default:
		return false, fmt.Errorf("unexpected HTTP status %d", resp.StatusCode())
	}
}

func (s *DefaultIndicesService) Recovery(ctx context.Context, indices []string) (*IndicesRecoveryResponse, error) {
	var path string
	if len(indices) > 0 {
		path = fmt.Sprintf("/%s/_recovery", strings.Join(indices, ","))
	} else {
		path = "/_recovery"
	}

	resp, err := s.client.R().
		SetContext(ctx).
		Get(path)
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result IndicesRecoveryResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultIndicesService) ShardStores(ctx context.Context, indices []string) (*IndicesShardStoresResponse, error) {
	var path string
	if len(indices) > 0 {
		path = fmt.Sprintf("/%s/_shard_stores", strings.Join(indices, ","))
	} else {
		path = "/_shard_stores"
	}

	resp, err := s.client.R().
		SetContext(ctx).
		Get(path)
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result IndicesShardStoresResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultIndicesService) UpdateAliases(ctx context.Context, body any) (*types.AcknowledgedResponse, error) {
	if body == nil {
		return nil, fmt.Errorf("body is required")
	}

	resp, err := s.client.R().
		SetContext(ctx).
		SetBody(body).
		Post("/_aliases")
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

func (s *DefaultIndicesService) ResolveIndex(ctx context.Context, name string) (*IndicesResolveIndexResponse, error) {
	if name == "" {
		return nil, fmt.Errorf("name is required")
	}

	resp, err := s.client.R().
		SetContext(ctx).
		Get(fmt.Sprintf("/_resolve/index/%s", name))
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result IndicesResolveIndexResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultIndicesService) SimulateIndexTemplate(ctx context.Context, req *SimulateIndexTemplateRequest) (*IndicesSimulateTemplateResponse, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}

	r := s.client.R().SetContext(ctx)
	if req.Body != nil {
		r.SetBody(req.Body)
	}

	resp, err := r.Post(fmt.Sprintf("/_index_template/_simulate_index/%s", req.Name))
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result IndicesSimulateTemplateResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultIndicesService) SimulateTemplate(ctx context.Context, req *SimulateTemplateRequest) (*IndicesSimulateTemplateResponse, error) {
	var path string
	if req.Name != "" {
		path = fmt.Sprintf("/_index_template/_simulate/%s", req.Name)
	} else {
		path = "/_index_template/_simulate"
	}

	r := s.client.R().SetContext(ctx)
	if req.Body != nil {
		r.SetBody(req.Body)
	}

	resp, err := r.Post(path)
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result IndicesSimulateTemplateResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultIndicesService) DataStreamsStats(ctx context.Context, names []string) (*IndicesDataStreamsStatsResponse, error) {
	var path string
	if len(names) > 0 {
		path = fmt.Sprintf("/_data_stream/%s/_stats", strings.Join(names, ","))
	} else {
		path = "/_data_stream/_stats"
	}

	resp, err := s.client.R().
		SetContext(ctx).
		Get(path)
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result IndicesDataStreamsStatsResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}
