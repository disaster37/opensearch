package api

import (
	"context"
	"fmt"
	"strings"

	json "github.com/goccy/go-json"

	"github.com/go-resty/resty/v2"
	"github.com/sirupsen/logrus"
)

// SmService interacts with the OpenSearch Snapshot Management (SM) plugin.
type SmService interface {
	GetPolicy(ctx context.Context, policyName string) (*SmGetPolicyResponse, error)
	PutPolicy(ctx context.Context, req *SmPutPolicyRequest) (*SmGetPolicyResponse, error)
	PostPolicy(ctx context.Context, policyName string, body *SmPutPolicy) (*SmGetPolicyResponse, error)
	DeletePolicy(ctx context.Context, policyName string) (*SmDeletePolicyResponse, error)
	ExplainPolicy(ctx context.Context, policyNames []string) (*SmExplainPolicyResponse, error)
	StartPolicy(ctx context.Context, policyName string) (*SmStartStopResponse, error)
	StopPolicy(ctx context.Context, policyName string) (*SmStartStopResponse, error)
	ListPolicies(ctx context.Context) (*SmListPoliciesResponse, error)
}

type DefaultSmService struct {
	client *resty.Client
	logger *logrus.Entry
}

func NewSmService(client *resty.Client, logger *logrus.Entry) SmService {
	return &DefaultSmService{
		client: client,
		logger: logger.WithField("service", "sm"),
	}
}

func (s *DefaultSmService) GetPolicy(ctx context.Context, policyName string) (*SmGetPolicyResponse, error) {
	if policyName == "" {
		return nil, fmt.Errorf("policy name is required")
	}

	resp, err := s.client.R().
		SetContext(ctx).
		Get(fmt.Sprintf("/_plugins/_sm/policies/%s", policyName))
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result SmGetPolicyResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultSmService) PutPolicy(ctx context.Context, req *SmPutPolicyRequest) (*SmGetPolicyResponse, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}

	r := s.client.R().SetContext(ctx).SetBody(req.Body)

	if req.Version != nil && req.Version.SeqNo != nil && req.Version.PrimaryTerm != nil {
		r.SetQueryParam("if_seq_no", fmt.Sprint(*req.Version.SeqNo))
		r.SetQueryParam("if_primary_term", fmt.Sprint(*req.Version.PrimaryTerm))
	}

	resp, err := r.Put(fmt.Sprintf("/_plugins/_sm/policies/%s", req.PolicyName))
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result SmGetPolicyResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultSmService) PostPolicy(ctx context.Context, policyName string, body *SmPutPolicy) (*SmGetPolicyResponse, error) {
	if policyName == "" {
		return nil, fmt.Errorf("policy name is required")
	}
	if body == nil {
		return nil, fmt.Errorf("body is required")
	}

	resp, err := s.client.R().
		SetContext(ctx).
		SetBody(body).
		Post(fmt.Sprintf("/_plugins/_sm/policies/%s", policyName))
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result SmGetPolicyResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultSmService) DeletePolicy(ctx context.Context, policyName string) (*SmDeletePolicyResponse, error) {
	if policyName == "" {
		return nil, fmt.Errorf("policy name is required")
	}

	resp, err := s.client.R().
		SetContext(ctx).
		Delete(fmt.Sprintf("/_plugins/_sm/policies/%s", policyName))
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result SmDeletePolicyResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultSmService) ExplainPolicy(ctx context.Context, policyNames []string) (*SmExplainPolicyResponse, error) {
	path := "/_plugins/_sm/policies/" + strings.Join(policyNames, ",") + "/_explain"

	resp, err := s.client.R().
		SetContext(ctx).
		Get(path)
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result SmExplainPolicyResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

// StartPolicy starts an SM policy, enabling its scheduled snapshot creation.
func (s *DefaultSmService) StartPolicy(ctx context.Context, policyName string) (*SmStartStopResponse, error) {
	if policyName == "" {
		return nil, fmt.Errorf("policy name is required")
	}

	resp, err := s.client.R().
		SetContext(ctx).
		Post(fmt.Sprintf("/_plugins/_sm/policies/%s/_start", policyName))
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result SmStartStopResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

// StopPolicy stops an SM policy, pausing its scheduled snapshot creation.
func (s *DefaultSmService) StopPolicy(ctx context.Context, policyName string) (*SmStartStopResponse, error) {
	if policyName == "" {
		return nil, fmt.Errorf("policy name is required")
	}

	resp, err := s.client.R().
		SetContext(ctx).
		Post(fmt.Sprintf("/_plugins/_sm/policies/%s/_stop", policyName))
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result SmStartStopResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

// ListPolicies returns all SM policies defined in the cluster.
func (s *DefaultSmService) ListPolicies(ctx context.Context) (*SmListPoliciesResponse, error) {
	resp, err := s.client.R().
		SetContext(ctx).
		Get("/_plugins/_sm/policies")
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result SmListPoliciesResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}
