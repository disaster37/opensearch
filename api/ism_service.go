package api

import (
	"context"
	"fmt"

	json "github.com/goccy/go-json"

	"github.com/go-resty/resty/v2"
	"github.com/sirupsen/logrus"
)

// IsmService interacts with the OpenSearch Index State Management (ISM) plugin.
type IsmService interface {
	GetPolicy(ctx context.Context, policyName string) (*IsmGetPolicyResponse, error)
	PutPolicy(ctx context.Context, req *IsmPutPolicyRequest) (*IsmGetPolicyResponse, error)
	DeletePolicy(ctx context.Context, policyName string) (*IsmDeletePolicyResponse, error)
	ExplainPolicy(ctx context.Context, indexName string) (*IsmExplainPolicyResponse, error)
	AddPolicy(ctx context.Context, index string, body any) (*IsmActionResponse, error)
	RemovePolicy(ctx context.Context, index string) (*IsmActionResponse, error)
	ChangePolicy(ctx context.Context, index string, body any) (*IsmActionResponse, error)
	RetryFailedIndex(ctx context.Context, index string, body any) (*IsmActionResponse, error)
	ListPolicies(ctx context.Context) (*IsmListPoliciesResponse, error)
	// RefreshSearchAnalyzers reloads updateable search analyzers (e.g. synonym
	// or hunspell dictionaries) for the given index via
	// POST /_plugins/_refresh_search_analyzers/{index} (ISM-plugin endpoint;
	// hunspell hot-reload supported since OpenSearch 3.7.0).
	RefreshSearchAnalyzers(ctx context.Context, index string) (*RefreshSearchAnalyzersResponse, error)
}

type DefaultIsmService struct {
	client *resty.Client
	logger *logrus.Entry
}

func NewIsmService(client *resty.Client, logger *logrus.Entry) IsmService {
	return &DefaultIsmService{
		client: client,
		logger: logger.WithField("service", "ism"),
	}
}

func (s *DefaultIsmService) GetPolicy(ctx context.Context, policyName string) (*IsmGetPolicyResponse, error) {
	if policyName == "" {
		return nil, fmt.Errorf("policy name is required")
	}

	resp, err := s.client.R().
		SetContext(ctx).
		Get(fmt.Sprintf("/_plugins/_ism/policies/%s", policyName))
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result IsmGetPolicyResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultIsmService) PutPolicy(ctx context.Context, req *IsmPutPolicyRequest) (*IsmGetPolicyResponse, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}

	r := s.client.R().SetContext(ctx).SetBody(&IsmPutPolicy{Policy: *req.Body})

	if req.Version != nil && req.Version.SeqNo != nil && req.Version.PrimaryTerm != nil {
		r.SetQueryParam("if_seq_no", fmt.Sprint(*req.Version.SeqNo))
		r.SetQueryParam("if_primary_term", fmt.Sprint(*req.Version.PrimaryTerm))
	}

	resp, err := r.Put(fmt.Sprintf("/_plugins/_ism/policies/%s", req.PolicyName))
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result IsmGetPolicyResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultIsmService) DeletePolicy(ctx context.Context, policyName string) (*IsmDeletePolicyResponse, error) {
	if policyName == "" {
		return nil, fmt.Errorf("policy name is required")
	}

	resp, err := s.client.R().
		SetContext(ctx).
		Delete(fmt.Sprintf("/_plugins/_ism/policies/%s", policyName))
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result IsmDeletePolicyResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultIsmService) ExplainPolicy(ctx context.Context, indexName string) (*IsmExplainPolicyResponse, error) {
	resp, err := s.client.R().
		SetContext(ctx).
		Get(fmt.Sprintf("/_plugins/_ism/explain/%s", indexName))
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result IsmExplainPolicyResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

// AddPolicy attaches an ISM policy to one or more indices.
func (s *DefaultIsmService) AddPolicy(ctx context.Context, index string, body any) (*IsmActionResponse, error) {
	if index == "" {
		return nil, fmt.Errorf("index is required")
	}
	if body == nil {
		return nil, fmt.Errorf("body is required")
	}

	resp, err := s.client.R().
		SetContext(ctx).
		SetBody(body).
		Post(fmt.Sprintf("/_plugins/_ism/add/%s", index))
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result IsmActionResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

// RemovePolicy detaches any ISM policy from one or more indices.
func (s *DefaultIsmService) RemovePolicy(ctx context.Context, index string) (*IsmActionResponse, error) {
	if index == "" {
		return nil, fmt.Errorf("index is required")
	}

	resp, err := s.client.R().
		SetContext(ctx).
		Post(fmt.Sprintf("/_plugins/_ism/remove/%s", index))
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result IsmActionResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

// ChangePolicy updates the ISM policy attached to one or more indices, optionally
// transitioning them to a new state.
func (s *DefaultIsmService) ChangePolicy(ctx context.Context, index string, body any) (*IsmActionResponse, error) {
	if index == "" {
		return nil, fmt.Errorf("index is required")
	}
	if body == nil {
		return nil, fmt.Errorf("body is required")
	}

	resp, err := s.client.R().
		SetContext(ctx).
		SetBody(body).
		Post(fmt.Sprintf("/_plugins/_ism/change_policy/%s", index))
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result IsmActionResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

// RetryFailedIndex retries a failed ISM policy action on one or more indices,
// optionally specifying a target state.
func (s *DefaultIsmService) RetryFailedIndex(ctx context.Context, index string, body any) (*IsmActionResponse, error) {
	if index == "" {
		return nil, fmt.Errorf("index is required")
	}

	r := s.client.R().SetContext(ctx)
	if body != nil {
		r = r.SetBody(body)
	}

	resp, err := r.Post(fmt.Sprintf("/_plugins/_ism/retry/%s", index))
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result IsmActionResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

// ListPolicies returns all ISM policies defined in the cluster.
func (s *DefaultIsmService) ListPolicies(ctx context.Context) (*IsmListPoliciesResponse, error) {
	resp, err := s.client.R().
		SetContext(ctx).
		Get("/_plugins/_ism/policies")
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result IsmListPoliciesResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

// RefreshSearchAnalyzers reloads updateable search analyzers (e.g. synonym
// or hunspell dictionaries) for the given index via
// POST /_plugins/_refresh_search_analyzers/{index} (ISM-plugin endpoint;
// hunspell hot-reload supported since OpenSearch 3.7.0).
func (s *DefaultIsmService) RefreshSearchAnalyzers(ctx context.Context, index string) (*RefreshSearchAnalyzersResponse, error) {
	if index == "" {
		return nil, fmt.Errorf("index is required")
	}

	resp, err := s.client.R().
		SetContext(ctx).
		Post(fmt.Sprintf("/_plugins/_refresh_search_analyzers/%s", index))
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result RefreshSearchAnalyzersResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}
