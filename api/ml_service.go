package api

import (
	"context"
	"fmt"

	json "github.com/goccy/go-json"

	"github.com/disaster37/opensearch/v4/types"
	"github.com/go-resty/resty/v2"
	"github.com/sirupsen/logrus"
)

// MlService defines the interface for interacting with the OpenSearch ML Commons plugin.
type MlService interface {
	RegisterModel(ctx context.Context, body any) (*MlRegisterModelResponse, error)
	DeployModel(ctx context.Context, modelId string) (*MlDeployModelResponse, error)
	UndeployModel(ctx context.Context, modelId string) (MlUndeployModelResponse, error)
	GetModel(ctx context.Context, modelId string) (MlGetModelResponse, error)
	SearchModels(ctx context.Context, body any) (MlSearchModelsResponse, error)
	UpdateModel(ctx context.Context, modelId string, body any) (MlUpdateModelResponse, error)
	DeleteModel(ctx context.Context, modelId string) (*types.AcknowledgedResponse, error)
	Predict(ctx context.Context, modelId string, body any) (*MlPredictResponse, error)
	Train(ctx context.Context, algorithm string, body any) (*MlTrainResponse, error)
	TrainAndPredict(ctx context.Context, algorithm string, body any) (*MlPredictResponse, error)
	Execute(ctx context.Context, algorithm string, body any) (MlExecuteResponse, error)
	MlStats(ctx context.Context, stat string) (MlStatsResponse, error)
	GetTask(ctx context.Context, taskId string) (MlGetTaskResponse, error)
	DeleteTask(ctx context.Context, taskId string) (*types.AcknowledgedResponse, error)
	CreateConnector(ctx context.Context, body any) (*MlCreateConnectorResponse, error)
	GetConnector(ctx context.Context, connectorId string) (MlGetConnectorResponse, error)
	DeleteConnector(ctx context.Context, connectorId string) (*types.AcknowledgedResponse, error)
	SearchConnectors(ctx context.Context, body any) (MlSearchConnectorsResponse, error)
	RegisterAgent(ctx context.Context, body any) (*MlRegisterAgentResponse, error)
	GetAgent(ctx context.Context, agentId string) (MlGetAgentResponse, error)
	DeleteAgent(ctx context.Context, agentId string) (*types.AcknowledgedResponse, error)
	SearchAgents(ctx context.Context, body any) (MlSearchAgentsResponse, error)
	DeleteModelGroup(ctx context.Context, modelGroupId string) (*types.AcknowledgedResponse, error)
	RegisterModelGroup(ctx context.Context, body any) (*MlRegisterModelGroupResponse, error)
	Profile(ctx context.Context, path string) (MlProfileResponse, error)
	ListTools(ctx context.Context) (MlListToolsResponse, error)
	ExecuteTool(ctx context.Context, toolName string, body any) (MlExecuteToolResponse, error)
}

// DefaultMlService implements the MlService interface using a REST client.
type DefaultMlService struct {
	client *resty.Client
	logger *logrus.Entry
}

// NewMlService creates a new MlService with the given REST client and logger.
func NewMlService(client *resty.Client, logger *logrus.Entry) MlService {
	return &DefaultMlService{
		client: client,
		logger: logger.WithField("service", "ml"),
	}
}

// RegisterModel registers a new model in the ML Commons plugin.
func (s *DefaultMlService) RegisterModel(ctx context.Context, body any) (*MlRegisterModelResponse, error) {
	resp, err := s.client.R().SetContext(ctx).SetBody(body).Post("/_plugins/_ml/models/_register")
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}
	var result MlRegisterModelResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

// DeployModel deploys a registered model to memory for inference.
func (s *DefaultMlService) DeployModel(ctx context.Context, modelId string) (*MlDeployModelResponse, error) {
	if modelId == "" {
		return nil, fmt.Errorf("model id is required")
	}
	resp, err := s.client.R().SetContext(ctx).Post(fmt.Sprintf("/_plugins/_ml/models/%s/_deploy", modelId))
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}
	var result MlDeployModelResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

// UndeployModel undeploys a deployed model from memory.
func (s *DefaultMlService) UndeployModel(ctx context.Context, modelId string) (MlUndeployModelResponse, error) {
	if modelId == "" {
		return nil, fmt.Errorf("model id is required")
	}
	resp, err := s.client.R().SetContext(ctx).Post(fmt.Sprintf("/_plugins/_ml/models/%s/_undeploy", modelId))
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}
	var result MlUndeployModelResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return result, nil
}

// GetModel retrieves the details of a model by its ID.
func (s *DefaultMlService) GetModel(ctx context.Context, modelId string) (MlGetModelResponse, error) {
	if modelId == "" {
		return nil, fmt.Errorf("model id is required")
	}
	resp, err := s.client.R().SetContext(ctx).Get(fmt.Sprintf("/_plugins/_ml/models/%s", modelId))
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}
	var result MlGetModelResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return result, nil
}

// SearchModels searches for models using the given query body.
func (s *DefaultMlService) SearchModels(ctx context.Context, body any) (MlSearchModelsResponse, error) {
	resp, err := s.client.R().SetContext(ctx).SetBody(body).Post("/_plugins/_ml/models/_search")
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}
	var result MlSearchModelsResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return result, nil
}

// UpdateModel updates a model by its ID with the given body.
func (s *DefaultMlService) UpdateModel(ctx context.Context, modelId string, body any) (MlUpdateModelResponse, error) {
	if modelId == "" {
		return nil, fmt.Errorf("model id is required")
	}
	resp, err := s.client.R().SetContext(ctx).SetBody(body).Put(fmt.Sprintf("/_plugins/_ml/models/%s", modelId))
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}
	var result MlUpdateModelResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return result, nil
}

// DeleteModel deletes a model by its ID.
func (s *DefaultMlService) DeleteModel(ctx context.Context, modelId string) (*types.AcknowledgedResponse, error) {
	if modelId == "" {
		return nil, fmt.Errorf("model id is required")
	}
	resp, err := s.client.R().SetContext(ctx).Delete(fmt.Sprintf("/_plugins/_ml/models/%s", modelId))
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

// Predict runs inference on a deployed model using the given input body.
func (s *DefaultMlService) Predict(ctx context.Context, modelId string, body any) (*MlPredictResponse, error) {
	if modelId == "" {
		return nil, fmt.Errorf("model id is required")
	}
	resp, err := s.client.R().SetContext(ctx).SetBody(body).Post(fmt.Sprintf("/_plugins/_ml/models/%s/_predict", modelId))
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}
	var result MlPredictResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

// Train trains a model using the specified algorithm and training data.
func (s *DefaultMlService) Train(ctx context.Context, algorithm string, body any) (*MlTrainResponse, error) {
	if algorithm == "" {
		return nil, fmt.Errorf("algorithm is required")
	}
	resp, err := s.client.R().SetContext(ctx).SetBody(body).Post(fmt.Sprintf("/_plugins/_ml/_train/%s", algorithm))
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}
	var result MlTrainResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

// TrainAndPredict trains a model and runs prediction in a single step using the specified algorithm.
func (s *DefaultMlService) TrainAndPredict(ctx context.Context, algorithm string, body any) (*MlPredictResponse, error) {
	if algorithm == "" {
		return nil, fmt.Errorf("algorithm is required")
	}
	resp, err := s.client.R().SetContext(ctx).SetBody(body).Post(fmt.Sprintf("/_plugins/_ml/_train_predict/%s", algorithm))
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}
	var result MlPredictResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

// Execute runs an ML algorithm with the given input body.
func (s *DefaultMlService) Execute(ctx context.Context, algorithm string, body any) (MlExecuteResponse, error) {
	if algorithm == "" {
		return nil, fmt.Errorf("algorithm is required")
	}
	resp, err := s.client.R().SetContext(ctx).SetBody(body).Post(fmt.Sprintf("/_plugins/_ml/_execute/%s", algorithm))
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}
	var result MlExecuteResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return result, nil
}

// MlStats retrieves ML plugin statistics, optionally filtered by stat name.
func (s *DefaultMlService) MlStats(ctx context.Context, stat string) (MlStatsResponse, error) {
	path := "/_plugins/_ml/stats"
	if stat != "" {
		path = fmt.Sprintf("/_plugins/_ml/stats/%s", stat)
	}
	resp, err := s.client.R().SetContext(ctx).Get(path)
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}
	var result MlStatsResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return result, nil
}

// GetTask retrieves the details of an ML task by its ID.
func (s *DefaultMlService) GetTask(ctx context.Context, taskId string) (MlGetTaskResponse, error) {
	if taskId == "" {
		return nil, fmt.Errorf("task id is required")
	}
	resp, err := s.client.R().SetContext(ctx).Get(fmt.Sprintf("/_plugins/_ml/tasks/%s", taskId))
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}
	var result MlGetTaskResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return result, nil
}

// DeleteTask deletes an ML task by its ID.
func (s *DefaultMlService) DeleteTask(ctx context.Context, taskId string) (*types.AcknowledgedResponse, error) {
	if taskId == "" {
		return nil, fmt.Errorf("task id is required")
	}
	resp, err := s.client.R().SetContext(ctx).Delete(fmt.Sprintf("/_plugins/_ml/tasks/%s", taskId))
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

// CreateConnector creates a new connector for external model integration.
func (s *DefaultMlService) CreateConnector(ctx context.Context, body any) (*MlCreateConnectorResponse, error) {
	resp, err := s.client.R().SetContext(ctx).SetBody(body).Post("/_plugins/_ml/connectors/_create")
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}
	var result MlCreateConnectorResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

// GetConnector retrieves the details of a connector by its ID.
func (s *DefaultMlService) GetConnector(ctx context.Context, connectorId string) (MlGetConnectorResponse, error) {
	if connectorId == "" {
		return nil, fmt.Errorf("connector id is required")
	}
	resp, err := s.client.R().SetContext(ctx).Get(fmt.Sprintf("/_plugins/_ml/connectors/%s", connectorId))
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}
	var result MlGetConnectorResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return result, nil
}

// DeleteConnector deletes a connector by its ID.
func (s *DefaultMlService) DeleteConnector(ctx context.Context, connectorId string) (*types.AcknowledgedResponse, error) {
	if connectorId == "" {
		return nil, fmt.Errorf("connector id is required")
	}
	resp, err := s.client.R().SetContext(ctx).Delete(fmt.Sprintf("/_plugins/_ml/connectors/%s", connectorId))
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

// SearchConnectors searches for connectors using the given query body.
func (s *DefaultMlService) SearchConnectors(ctx context.Context, body any) (MlSearchConnectorsResponse, error) {
	resp, err := s.client.R().SetContext(ctx).SetBody(body).Post("/_plugins/_ml/connectors/_search")
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}
	var result MlSearchConnectorsResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return result, nil
}

// RegisterAgent registers a new agent in the ML Commons plugin.
func (s *DefaultMlService) RegisterAgent(ctx context.Context, body any) (*MlRegisterAgentResponse, error) {
	resp, err := s.client.R().SetContext(ctx).SetBody(body).Post("/_plugins/_ml/agents/_register")
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}
	var result MlRegisterAgentResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

// GetAgent retrieves the details of an agent by its ID.
func (s *DefaultMlService) GetAgent(ctx context.Context, agentId string) (MlGetAgentResponse, error) {
	if agentId == "" {
		return nil, fmt.Errorf("agent id is required")
	}
	resp, err := s.client.R().SetContext(ctx).Get(fmt.Sprintf("/_plugins/_ml/agents/%s", agentId))
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}
	var result MlGetAgentResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return result, nil
}

// DeleteAgent deletes an agent by its ID.
func (s *DefaultMlService) DeleteAgent(ctx context.Context, agentId string) (*types.AcknowledgedResponse, error) {
	if agentId == "" {
		return nil, fmt.Errorf("agent id is required")
	}
	resp, err := s.client.R().SetContext(ctx).Delete(fmt.Sprintf("/_plugins/_ml/agents/%s", agentId))
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

// SearchAgents searches for agents using the given query body.
func (s *DefaultMlService) SearchAgents(ctx context.Context, body any) (MlSearchAgentsResponse, error) {
	resp, err := s.client.R().SetContext(ctx).SetBody(body).Post("/_plugins/_ml/agents/_search")
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}
	var result MlSearchAgentsResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return result, nil
}

// DeleteModelGroup deletes a model group by its ID.
func (s *DefaultMlService) DeleteModelGroup(ctx context.Context, modelGroupId string) (*types.AcknowledgedResponse, error) {
	if modelGroupId == "" {
		return nil, fmt.Errorf("model group id is required")
	}
	resp, err := s.client.R().SetContext(ctx).Delete(fmt.Sprintf("/_plugins/_ml/model_groups/%s", modelGroupId))
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

// RegisterModelGroup registers a new model group in the ML Commons plugin.
func (s *DefaultMlService) RegisterModelGroup(ctx context.Context, body any) (*MlRegisterModelGroupResponse, error) {
	resp, err := s.client.R().SetContext(ctx).SetBody(body).Post("/_plugins/_ml/model_groups/_register")
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}
	var result MlRegisterModelGroupResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

// Profile retrieves ML profile information, optionally for a specific model or task.
// Pass an empty string for the base profile endpoint, or a path such as
// "models/{modelId}" or "tasks/{taskId}" for targeted profile information.
func (s *DefaultMlService) Profile(ctx context.Context, path string) (MlProfileResponse, error) {
	urlPath := "/_plugins/_ml/profile"
	if path != "" {
		urlPath = fmt.Sprintf("/_plugins/_ml/profile/%s", path)
	}
	resp, err := s.client.R().SetContext(ctx).Get(urlPath)
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}
	var result MlProfileResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return result, nil
}

// ListTools lists all available tools in the ML Commons plugin.
func (s *DefaultMlService) ListTools(ctx context.Context) (MlListToolsResponse, error) {
	resp, err := s.client.R().SetContext(ctx).Get("/_plugins/_ml/tools")
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}
	var result MlListToolsResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return result, nil
}

// ExecuteTool executes a specific tool by name with the given input body.
func (s *DefaultMlService) ExecuteTool(ctx context.Context, toolName string, body any) (MlExecuteToolResponse, error) {
	if toolName == "" {
		return nil, fmt.Errorf("tool name is required")
	}
	resp, err := s.client.R().SetContext(ctx).SetBody(body).Post(fmt.Sprintf("/_plugins/_ml/tools/_execute/%s", toolName))
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}
	var result MlExecuteToolResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return result, nil
}
