package api

// AlertingMonitor represents the definition of an alert monitor in the OpenSearch Alerting plugin,
// including the monitor type, name, schedule, inputs, and trigger conditions.
type AlertingMonitor struct {
	Type        string           `json:"type"`
	Name        string           `json:"name"`
	MonitorType string           `json:"monitor_type"`
	Enabled     *bool            `json:"enabled,omitempty"`
	Schedule    map[string]any   `json:"schedule"`
	Inputs      []map[string]any `json:"inputs"`
	Triggers    []map[string]any `json:"triggers"`
}

// AlertingGetMonitor represents a monitor as returned from the Alerting GET API,
// extending AlertingMonitor with enabled time and last updated timestamp.
type AlertingGetMonitor struct {
	AlertingMonitor `json:",inline"`
	EnabledTime     *int64 `json:"enabled_time,omitempty"`
	LastUpdatedTime *int64 `json:"last_updated_time,omitempty"`
}

// AlertingGetMonitorResponse represents the full response from the Alerting GET monitor API,
// including the document ID, version, sequence number, primary term, and the monitor definition.
type AlertingGetMonitorResponse struct {
	Id             string             `json:"_id"`
	Version        int64              `json:"_version"`
	SequenceNumber int64              `json:"_seq_no"`
	PrimaryTerm    int64              `json:"_primary_term"`
	Monitor        AlertingGetMonitor `json:"monitor"`
}

// AlertingDeleteMonitorResponse represents the response from the Alerting delete monitor API,
// confirming the deletion with the document ID, version, and result status.
type AlertingDeleteMonitorResponse struct {
	Index          *string        `json:"_index,omitempty"`
	ID             *string        `json:"_id,omitempty"`
	Version        *int64         `json:"_version,omitempty"`
	Result         *string        `json:"result,omitempty"`
	ForcedRefresh  *bool          `json:"forced_refresh,omitempty"`
	Shards         map[string]any `json:"_shards,omitempty"`
	SequenceNumber *int64         `json:"_seq_no,omitempty"`
	PrimaryTerm    *int64         `json:"_primary_term,omitempty"`
}

// AlertingSearchMonitorResponse wraps the hit results from the Alerting search monitors API.
type AlertingSearchMonitorResponse struct {
	Hits AlertingSearchMonitorHits `json:"hits"`
}

// AlertingSearchMonitorHits contains the list of monitor search results.
type AlertingSearchMonitorHits struct {
	Hits []AlertingSearchMonitorHit `json:"hits"`
}

// AlertingSearchMonitorHit represents a single monitor document returned from
// the Alerting search monitors API, including the document ID and the monitor source.
type AlertingSearchMonitorHit struct {
	Id     string             `json:"_id"`
	Source AlertingGetMonitor `json:"_source"`
}

// AlertingExecuteMonitorResponse represents the response from executing a monitor on-demand.
type AlertingExecuteMonitorResponse struct {
	MonitorName    string         `json:"monitor_name,omitempty"`
	PeriodStart    string         `json:"period_start,omitempty"`
	PeriodEnd      string         `json:"period_end,omitempty"`
	Error          *string        `json:"error,omitempty"`
	InputResults   map[string]any `json:"input_results,omitempty"`
	TriggerResults map[string]any `json:"trigger_results,omitempty"`
}

// AlertingAcknowledgeAlertResponse represents the result of acknowledging alerts.
type AlertingAcknowledgeAlertResponse struct {
	MissingAlertIds      []string                    `json:"missing_alert_ids,omitempty"`
	FailedAlertIds       []string                    `json:"failed_alert_ids,omitempty"`
	AcknowledgedAlertIds []AlertingAcknowledgedAlert `json:"acknowledged_alerts,omitempty"`
}

// AlertingAcknowledgedAlert represents a single acknowledged alert.
type AlertingAcknowledgedAlert struct {
	Id      string         `json:"_id"`
	Version int64          `json:"_version"`
	Source  map[string]any `json:"_source,omitempty"`
}

// AlertingGetAlertsResponse represents the response from the get alerts API.
type AlertingGetAlertsResponse struct {
	TotalAlerts int             `json:"totalAlerts"`
	Alerts      []AlertingAlert `json:"alerts"`
}

// AlertingAlert represents a single alert.
type AlertingAlert struct {
	Id               string           `json:"id"`
	MonitorId        string           `json:"monitor_id"`
	MonitorVersion   int64            `json:"monitor_version,omitempty"`
	MonitorName      string           `json:"monitor_name"`
	State            string           `json:"state"`
	Severity         string           `json:"severity,omitempty"`
	TriggerName      string           `json:"trigger_name,omitempty"`
	StartTime        string           `json:"start_time,omitempty"`
	EndTime          string           `json:"end_time,omitempty"`
	AcknowledgedTime string           `json:"acknowledged_time,omitempty"`
	ErrorMessage     string           `json:"error_message,omitempty"`
	AlertHistory     []map[string]any `json:"alert_history,omitempty"`
}

// AlertingGetFindingsResponse represents the response from the get findings API.
type AlertingGetFindingsResponse struct {
	TotalFindings int               `json:"total_findings"`
	Findings      []AlertingFinding `json:"findings"`
}

// AlertingFinding represents a single alerting finding.
type AlertingFinding struct {
	Id            string           `json:"id"`
	RelatedDocIds []string         `json:"related_doc_ids,omitempty"`
	Index         string           `json:"index"`
	MonitorId     string           `json:"monitor_id"`
	MonitorName   string           `json:"monitor_name"`
	QueryIds      []string         `json:"query_ids,omitempty"`
	Timestamp     string           `json:"timestamp,omitempty"`
	DocumentList  []map[string]any `json:"document_list,omitempty"`
}

// AlertingGetDestinationsResponse represents destinations from the API.
type AlertingGetDestinationsResponse struct {
	TotalDestinations int                   `json:"totalDestinations"`
	Destinations      []AlertingDestination `json:"destinations"`
}

// AlertingDestination represents a single notification destination.
type AlertingDestination struct {
	Id              string `json:"id"`
	Name            string `json:"name"`
	Type            string `json:"type"`
	SchemaVersion   int64  `json:"schema_version,omitempty"`
	SeqNo           int64  `json:"seq_no,omitempty"`
	PrimaryTerm     int64  `json:"primary_term,omitempty"`
	LastUpdatedTime string `json:"last_update_time,omitempty"`
}

// AlertingWorkflow represents a workflow definition.
type AlertingWorkflow struct {
	Name         string           `json:"name"`
	WorkflowType string           `json:"workflow_type,omitempty"`
	Enabled      *bool            `json:"enabled,omitempty"`
	Schedule     map[string]any   `json:"schedule,omitempty"`
	Inputs       []map[string]any `json:"inputs,omitempty"`
	Triggers     []map[string]any `json:"triggers,omitempty"`
	Monitors     []map[string]any `json:"monitors,omitempty"`
}

// AlertingGetWorkflowResponse represents the full response from the get workflow API.
type AlertingGetWorkflowResponse struct {
	Id             string           `json:"_id"`
	Version        int64            `json:"_version"`
	SequenceNumber int64            `json:"_seq_no"`
	PrimaryTerm    int64            `json:"_primary_term"`
	Workflow       AlertingWorkflow `json:"workflow"`
}

// AlertingDeleteWorkflowResponse represents the response from deleting a workflow.
type AlertingDeleteWorkflowResponse struct {
	Index          *string        `json:"_index,omitempty"`
	ID             *string        `json:"_id,omitempty"`
	Version        *int64         `json:"_version,omitempty"`
	Result         *string        `json:"result,omitempty"`
	ForcedRefresh  *bool          `json:"forced_refresh,omitempty"`
	Shards         map[string]any `json:"_shards,omitempty"`
	SequenceNumber *int64         `json:"_seq_no,omitempty"`
	PrimaryTerm    *int64         `json:"_primary_term,omitempty"`
}

// AlertingExecuteWorkflowResponse represents the response from executing a workflow.
type AlertingExecuteWorkflowResponse struct {
	ExecutionId    string           `json:"execution_id,omitempty"`
	Error          *string          `json:"error,omitempty"`
	MonitorResults []map[string]any `json:"monitor_results,omitempty"`
}
