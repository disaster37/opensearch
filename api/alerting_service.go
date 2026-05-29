package api

import (
	"context"
	"fmt"
	json "github.com/goccy/go-json"

	"github.com/go-resty/resty/v2"
	"github.com/sirupsen/logrus"
)

// AlertingService defines the interface for interacting with the OpenSearch Alerting plugin.
type AlertingService interface {
	GetMonitor(ctx context.Context, monitorId string) (*AlertingGetMonitorResponse, error)
	PutMonitor(ctx context.Context, req *AlertingPutMonitorRequest) (*AlertingGetMonitorResponse, error)
	PostMonitor(ctx context.Context, body any) (*AlertingGetMonitorResponse, error)
	DeleteMonitor(ctx context.Context, monitorId string) (*AlertingDeleteMonitorResponse, error)
	SearchMonitor(ctx context.Context, body any) ([]AlertingSearchMonitorHit, error)
	ExecuteMonitor(ctx context.Context, monitorId string, body any) (*AlertingExecuteMonitorResponse, error)
	AcknowledgeAlert(ctx context.Context, monitorId string, body any) (*AlertingAcknowledgeAlertResponse, error)
	GetAlerts(ctx context.Context, params map[string]string) (*AlertingGetAlertsResponse, error)
	GetFindings(ctx context.Context, params map[string]string) (*AlertingGetFindingsResponse, error)
	GetDestinations(ctx context.Context, destinationId string) (*AlertingGetDestinationsResponse, error)
	IndexWorkflow(ctx context.Context, req *AlertingIndexWorkflowRequest) (*AlertingGetWorkflowResponse, error)
	GetWorkflow(ctx context.Context, workflowId string) (*AlertingGetWorkflowResponse, error)
	DeleteWorkflow(ctx context.Context, workflowId string) (*AlertingDeleteWorkflowResponse, error)
	ExecuteWorkflow(ctx context.Context, workflowId string, body any) (*AlertingExecuteWorkflowResponse, error)
	GetWorkflowAlerts(ctx context.Context, params map[string]string) (*AlertingGetAlertsResponse, error)
	AcknowledgeChainedAlerts(ctx context.Context, workflowId string, body any) (*AlertingAcknowledgeAlertResponse, error)
}

// DefaultAlertingService implements the AlertingService interface using a REST client.
type DefaultAlertingService struct {
	client *resty.Client
	logger *logrus.Entry
}

// NewAlertingService creates a new AlertingService with the given REST client and logger.
func NewAlertingService(client *resty.Client, logger *logrus.Entry) AlertingService {
	return &DefaultAlertingService{
		client: client,
		logger: logger.WithField("service", "alerting"),
	}
}

// GetMonitor retrieves a monitor by its ID.
func (s *DefaultAlertingService) GetMonitor(ctx context.Context, monitorId string) (*AlertingGetMonitorResponse, error) {
	if monitorId == "" {
		return nil, fmt.Errorf("monitor id is required")
	}

	resp, err := s.client.R().SetContext(ctx).Get(fmt.Sprintf("/_plugins/_alerting/monitors/%s", monitorId))
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result AlertingGetMonitorResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

// PutMonitor updates an existing monitor.
func (s *DefaultAlertingService) PutMonitor(ctx context.Context, req *AlertingPutMonitorRequest) (*AlertingGetMonitorResponse, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}

	r := s.client.R().SetContext(ctx).SetBody(req.Body)
	if req.Version != nil && req.Version.SeqNo != nil && req.Version.PrimaryTerm != nil {
		r.SetQueryParam("if_seq_no", fmt.Sprint(*req.Version.SeqNo))
		r.SetQueryParam("if_primary_term", fmt.Sprint(*req.Version.PrimaryTerm))
	}

	resp, err := r.Put(fmt.Sprintf("/_plugins/_alerting/monitors/%s", req.MonitorId))
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result AlertingGetMonitorResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

// PostMonitor creates a new monitor.
func (s *DefaultAlertingService) PostMonitor(ctx context.Context, body any) (*AlertingGetMonitorResponse, error) {
	if body == nil {
		return nil, fmt.Errorf("body is required")
	}

	resp, err := s.client.R().SetContext(ctx).SetBody(body).Post("/_plugins/_alerting/monitors")
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result AlertingGetMonitorResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

// DeleteMonitor deletes a monitor by its ID.
func (s *DefaultAlertingService) DeleteMonitor(ctx context.Context, monitorId string) (*AlertingDeleteMonitorResponse, error) {
	if monitorId == "" {
		return nil, fmt.Errorf("monitor id is required")
	}

	resp, err := s.client.R().SetContext(ctx).Delete(fmt.Sprintf("/_plugins/_alerting/monitors/%s", monitorId))
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result AlertingDeleteMonitorResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

// SearchMonitor searches for monitors using the given query body.
func (s *DefaultAlertingService) SearchMonitor(ctx context.Context, body any) ([]AlertingSearchMonitorHit, error) {
	resp, err := s.client.R().SetContext(ctx).SetBody(body).Get("/_plugins/_alerting/monitors/_search")
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result AlertingSearchMonitorResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return result.Hits.Hits, nil
}

// ExecuteMonitor executes a monitor on-demand, optionally with trigger overrides.
func (s *DefaultAlertingService) ExecuteMonitor(ctx context.Context, monitorId string, body any) (*AlertingExecuteMonitorResponse, error) {
	if monitorId == "" {
		return nil, fmt.Errorf("monitor id is required")
	}

	r := s.client.R().SetContext(ctx)
	if body != nil {
		r.SetBody(body)
	}

	resp, err := r.Post(fmt.Sprintf("/_plugins/_alerting/monitors/%s/_execute", monitorId))
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result AlertingExecuteMonitorResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

// AcknowledgeAlert acknowledges one or more alerts for a monitor.
func (s *DefaultAlertingService) AcknowledgeAlert(ctx context.Context, monitorId string, body any) (*AlertingAcknowledgeAlertResponse, error) {
	if monitorId == "" {
		return nil, fmt.Errorf("monitor id is required")
	}
	if body == nil {
		return nil, fmt.Errorf("body is required")
	}

	resp, err := s.client.R().SetContext(ctx).SetBody(body).Post(fmt.Sprintf("/_plugins/_alerting/monitors/%s/_acknowledge/alerts", monitorId))
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result AlertingAcknowledgeAlertResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

// GetAlerts retrieves alerts with optional query parameters.
func (s *DefaultAlertingService) GetAlerts(ctx context.Context, params map[string]string) (*AlertingGetAlertsResponse, error) {
	resp, err := s.client.R().SetContext(ctx).SetQueryParams(params).Get("/_plugins/_alerting/monitors/alerts")
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result AlertingGetAlertsResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

// GetFindings retrieves alerting findings with optional query parameters.
func (s *DefaultAlertingService) GetFindings(ctx context.Context, params map[string]string) (*AlertingGetFindingsResponse, error) {
	resp, err := s.client.R().SetContext(ctx).SetQueryParams(params).Get("/_plugins/_alerting/monitors/findings/_search")
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result AlertingGetFindingsResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

// GetDestinations retrieves destinations, optionally filtered by destination ID.
func (s *DefaultAlertingService) GetDestinations(ctx context.Context, destinationId string) (*AlertingGetDestinationsResponse, error) {
	url := "/_plugins/_alerting/destinations"
	if destinationId != "" {
		url = fmt.Sprintf("/_plugins/_alerting/destinations/%s", destinationId)
	}

	resp, err := s.client.R().SetContext(ctx).Get(url)
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result AlertingGetDestinationsResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

// IndexWorkflow creates or updates a workflow.
func (s *DefaultAlertingService) IndexWorkflow(ctx context.Context, req *AlertingIndexWorkflowRequest) (*AlertingGetWorkflowResponse, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}

	r := s.client.R().SetContext(ctx).SetBody(req.Body)
	if req.Version != nil && req.Version.SeqNo != nil && req.Version.PrimaryTerm != nil {
		r.SetQueryParam("if_seq_no", fmt.Sprint(*req.Version.SeqNo))
		r.SetQueryParam("if_primary_term", fmt.Sprint(*req.Version.PrimaryTerm))
	}

	var resp *resty.Response
	var err error
	if req.WorkflowId != "" {
		resp, err = r.Put(fmt.Sprintf("/_plugins/_alerting/workflows/%s", req.WorkflowId))
	} else {
		resp, err = r.Post("/_plugins/_alerting/workflows")
	}
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result AlertingGetWorkflowResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

// GetWorkflow retrieves a workflow by its ID.
func (s *DefaultAlertingService) GetWorkflow(ctx context.Context, workflowId string) (*AlertingGetWorkflowResponse, error) {
	if workflowId == "" {
		return nil, fmt.Errorf("workflow id is required")
	}

	resp, err := s.client.R().SetContext(ctx).Get(fmt.Sprintf("/_plugins/_alerting/workflows/%s", workflowId))
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result AlertingGetWorkflowResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

// DeleteWorkflow deletes a workflow by its ID.
func (s *DefaultAlertingService) DeleteWorkflow(ctx context.Context, workflowId string) (*AlertingDeleteWorkflowResponse, error) {
	if workflowId == "" {
		return nil, fmt.Errorf("workflow id is required")
	}

	resp, err := s.client.R().SetContext(ctx).Delete(fmt.Sprintf("/_plugins/_alerting/workflows/%s", workflowId))
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result AlertingDeleteWorkflowResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

// ExecuteWorkflow executes a workflow on-demand, optionally with overrides.
func (s *DefaultAlertingService) ExecuteWorkflow(ctx context.Context, workflowId string, body any) (*AlertingExecuteWorkflowResponse, error) {
	if workflowId == "" {
		return nil, fmt.Errorf("workflow id is required")
	}

	r := s.client.R().SetContext(ctx)
	if body != nil {
		r.SetBody(body)
	}

	resp, err := r.Post(fmt.Sprintf("/_plugins/_alerting/workflows/%s/_execute", workflowId))
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result AlertingExecuteWorkflowResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

// GetWorkflowAlerts retrieves workflow alerts with optional query parameters.
func (s *DefaultAlertingService) GetWorkflowAlerts(ctx context.Context, params map[string]string) (*AlertingGetAlertsResponse, error) {
	resp, err := s.client.R().SetContext(ctx).SetQueryParams(params).Get("/_plugins/_alerting/workflows/alerts")
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result AlertingGetAlertsResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

// AcknowledgeChainedAlerts acknowledges chained alerts for a workflow.
func (s *DefaultAlertingService) AcknowledgeChainedAlerts(ctx context.Context, workflowId string, body any) (*AlertingAcknowledgeAlertResponse, error) {
	if workflowId == "" {
		return nil, fmt.Errorf("workflow id is required")
	}
	if body == nil {
		return nil, fmt.Errorf("body is required")
	}

	resp, err := s.client.R().SetContext(ctx).SetBody(body).Post(fmt.Sprintf("/_plugins/_alerting/workflows/%s/_acknowledge/alerts", workflowId))
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result AlertingAcknowledgeAlertResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}
