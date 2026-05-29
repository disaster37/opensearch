package api

// MlRegisterModelResponse represents the response from registering a model.
type MlRegisterModelResponse struct {
	TaskId   string `json:"task_id,omitempty"`
	TaskType string `json:"task_type,omitempty"`
	Status   string `json:"status,omitempty"`
}

// MlDeployModelResponse represents the response from deploying a model.
type MlDeployModelResponse struct {
	TaskId string `json:"task_id,omitempty"`
	Status string `json:"status,omitempty"`
}

// MlUndeployModelResponse represents the response from undeploying a model.
type MlUndeployModelResponse map[string]any

// MlGetModelResponse represents a model detail response.
type MlGetModelResponse map[string]any

// MlSearchModelsResponse represents the response from searching models.
type MlSearchModelsResponse map[string]any

// MlUpdateModelResponse represents the response from updating a model.
type MlUpdateModelResponse map[string]any

// MlPredictResponse represents the response from predicting with a model.
type MlPredictResponse struct {
	InferenceResults []map[string]any `json:"inference_results,omitempty"`
}

// MlTrainResponse represents the response from training a model.
type MlTrainResponse struct {
	TaskId string `json:"task_id,omitempty"`
}

// MlExecuteResponse represents the response from executing an algorithm.
type MlExecuteResponse map[string]any

// MlStatsResponse represents ML plugin stats.
type MlStatsResponse map[string]any

// MlGetTaskResponse represents a task detail response.
type MlGetTaskResponse map[string]any

// MlCreateConnectorResponse represents the response from creating a connector.
type MlCreateConnectorResponse struct {
	ConnectorId string `json:"connector_id,omitempty"`
}

// MlGetConnectorResponse represents a connector detail response.
type MlGetConnectorResponse map[string]any

// MlSearchConnectorsResponse represents the response from searching connectors.
type MlSearchConnectorsResponse map[string]any

// MlRegisterAgentResponse represents the response from registering an agent.
type MlRegisterAgentResponse struct {
	AgentId string `json:"agent_id,omitempty"`
}

// MlGetAgentResponse represents an agent detail response.
type MlGetAgentResponse map[string]any

// MlSearchAgentsResponse represents the response from searching agents.
type MlSearchAgentsResponse map[string]any

// MlRegisterModelGroupResponse represents the response from registering a model group.
type MlRegisterModelGroupResponse struct {
	ModelGroupId string `json:"model_group_id,omitempty"`
	Status       string `json:"status,omitempty"`
}

// MlProfileResponse represents the response from getting ML profile info.
type MlProfileResponse map[string]any

// MlListToolsResponse represents the response from listing available tools.
type MlListToolsResponse map[string]any

// MlExecuteToolResponse represents the response from executing a tool.
type MlExecuteToolResponse map[string]any
