package api

// IsmPolicyBase represents the base definition of an Index State Management (ISM) policy,
// including its states, transitions, default state, error notifications, and ISM templates
// that auto-apply the policy to matching indices.
type IsmPolicyBase struct {
	ID                *string               `json:"policy_id,omitempty"`
	Description       *string               `json:"description,omitempty"`
	ErrorNotification *IsmErrorNotification `json:"error_notification,omitempty"`
	DefaultState      *string               `json:"default_state,omitempty"`
	States            []IsmPolicyState      `json:"states,omitempty"`
	IsmTemplate       []IsmPolicyTemplate   `json:"ism_template,omitempty"`
}

// IsmErrorNotification defines the error notification configuration for an ISM policy,
// specifying where to send notifications when policy actions fail.
type IsmErrorNotification struct {
	Destination     *IsmErrorNotificationDestination     `json:"destination,omitempty"`
	Channel         *IsmErrorNotificationChannel         `json:"channel,omitempty"`
	MessageTemplate *IsmErrorNotificationMessageTemplate `json:"message_template,omitempty"`
}

// IsmPolicyState represents a state within an ISM policy, defining the actions
// to perform when an index is in this state and the conditions for transitioning to other states.
type IsmPolicyState struct {
	Name        string                     `json:"name"`
	Actions     []map[string]any           `json:"actions,omitempty"`
	Transitions []IsmPolicyStateTransition `json:"transitions,omitempty"`
}

// IsmErrorNotificationDestination defines the destination for ISM error notifications,
// supporting Chime, Slack, or custom webhook endpoints.
type IsmErrorNotificationDestination struct {
	Type          string                                        `json:"type"`
	Chime         *IsmErrorNotificationDestinationChime         `json:"chime,omitempty"`
	Slack         *IsmErrorNotificationDestinationSlack         `json:"slack,omitempty"`
	CustomWebhook *IsmErrorNotificationDestinationCustomWebhook `json:"custom_webhook,omitempty"`
}

// IsmErrorNotificationDestinationChime defines the Chime webhook URL for ISM error notifications.
type IsmErrorNotificationDestinationChime struct {
	Url string `json:"url"`
}

// IsmErrorNotificationDestinationCustomWebhook defines a custom webhook destination
// for ISM error notifications, with configurable URL, authentication, headers, and query parameters.
type IsmErrorNotificationDestinationCustomWebhook struct {
	Url          *string           `json:"url,omitempty"`
	Scheme       *string           `json:"scheme,omitempty"`
	Host         *string           `json:"host,omitempty"`
	Port         *int64            `json:"port,omitempty"`
	Path         *string           `json:"path,omitempty"`
	QueryParams  map[string]string `json:"query_params,omitempty"`
	HeaderParams map[string]string `json:"header_params,omitempty"`
	Username     *string           `json:"username,omitempty"`
	Password     *string           `json:"password,omitempty"`
}

// IsmErrorNotificationDestinationSlack defines the Slack webhook URL for ISM error notifications.
type IsmErrorNotificationDestinationSlack struct {
	Url string `json:"url"`
}

// IsmErrorNotificationChannel defines a notification channel by ID for ISM error notifications.
type IsmErrorNotificationChannel struct {
	ID string `json:"id"`
}

// IsmErrorNotificationMessageTemplate defines the message template for ISM error notifications,
// supporting inline scripts and stored script references.
type IsmErrorNotificationMessageTemplate struct {
	ScriptType string            `json:"type"`
	Lang       string            `json:"lang"`
	IdOrCode   string            `json:"idOrCode"`
	Options    map[string]string `json:"options,omitempty"`
	Params     map[string]string `json:"params,omitempty"`
}

// IsmPolicyStateTransition defines a transition rule between ISM policy states,
// specifying the target state name and optional conditions that trigger the transition.
type IsmPolicyStateTransition struct {
	StateName  string         `json:"state_name"`
	Conditions map[string]any `json:"conditions,omitempty"`
}

// IsmPolicyTemplate defines an ISM template that auto-applies an ISM policy to indices
// matching the specified index patterns, with a priority for resolving template conflicts.
type IsmPolicyTemplate struct {
	IndexPatterns   []string `json:"index_patterns,omitempty"`
	Priority        *int64   `json:"priority,omitempty"`
	LastUpdatedTime *int64   `json:"last_updated_time,omitempty"`
}

// IsmPutPolicy wraps an ISM policy for the PUT API request body.
type IsmPutPolicy struct {
	Policy IsmPolicyBase `json:"policy"`
}

// IsmGetPolicy represents the policy data returned within an ISM GET response,
// extending IsmPolicyBase with schema version and last updated timestamp.
type IsmGetPolicy struct {
	IsmPolicyBase   `json:",inline"`
	SchemaVersion   *int64 `json:"schema_version,omitempty"`
	LastUpdatedTime *int64 `json:"last_updated_time,omitempty"`
}

// IsmGetPolicyResponse represents the full response from the ISM GET policy API,
// including the document ID, version, sequence number, primary term, and the policy definition.
type IsmGetPolicyResponse struct {
	Id             string       `json:"_id"`
	Version        int64        `json:"_version"`
	SequenceNumber int64        `json:"_seq_no"`
	PrimaryTerm    int64        `json:"_primary_term"`
	Policy         IsmGetPolicy `json:"policy"`
}

// IsmDeletePolicyResponse represents the response from the ISM delete policy API,
// confirming the deletion with the document ID, version, and result status.
type IsmDeletePolicyResponse struct {
	Index          *string        `json:"_index,omitempty"`
	ID             *string        `json:"_id,omitempty"`
	Version        *int64         `json:"_version,omitempty"`
	Result         *string        `json:"result,omitempty"`
	ForcedRefresh  *bool          `json:"forced_refresh,omitempty"`
	Shards         map[string]any `json:"_shards,omitempty"`
	SequenceNumber *int64         `json:"_seq_no,omitempty"`
	PrimaryTerm    *int64         `json:"_primary_term,omitempty"`
}

// IsmExplainPolicyResponse represents the response from the ISM explain API,
// containing a map of index names to their ISM policy status and the total count of managed indices.
type IsmExplainPolicyResponse struct {
	Indexes             map[string]IsmExplainPolicy `json:",inline"`
	TotalManagedIndices int64                       `json:"total_managed_indices"`
}

// IsmExplainPolicy represents the ISM policy status for a single index,
// including the current state, action, step, retry info, and policy metadata.
type IsmExplainPolicy struct {
	PolicyId             string                    `json:"policy_id,omitempty"`
	PolicySequenceNumber int64                     `json:"policy_seq_no,omitempty"`
	PolicyPrimaryTerm    int64                     `json:"policy_primary_term,omitempty"`
	Index                string                    `json:"index,omitempty"`
	IndexId              string                    `json:"index_uuid,omitempty"`
	IndexCreationDate    int64                     `json:"index_creation_date,omitempty"`
	Enabled              bool                      `json:"enabled,omitempty"`
	Policy               *IsmGetPolicy             `json:"policy,omitempty"`
	State                IsmExplainPolicyState     `json:"state"`
	Action               IsmExplainPolicyAction    `json:"action"`
	Step                 IsmExplainPolicyStep      `json:"step"`
	RetryInfo            IsmExplainPolicyRetryInfo `json:"retry_info"`
	Info                 IsmExplainPolicyInfo      `json:"info"`
}

// IsmExplainPolicyState represents the current state information of an ISM policy on a managed index.
type IsmExplainPolicyState struct {
	Name      string `json:"name"`
	StartTime int64  `json:"start_time"`
}

// IsmExplainPolicyAction represents the currently executing action information
// within an ISM policy on a managed index.
type IsmExplainPolicyAction struct {
	Name            string `json:"name"`
	StartTime       int64  `json:"start_time"`
	Index           int64  `json:"index"`
	Failed          bool   `json:"failed"`
	ConsumedRetries int64  `json:"consumed_retries"`
	LastRetryTime   int64  `json:"last_retry_time"`
}

// IsmExplainPolicyStep represents the current step status within an ISM policy action.
type IsmExplainPolicyStep struct {
	Name       string `json:"name"`
	StartTime  int64  `json:"start_time"`
	StepStatus string `json:"step_status"`
}

// IsmExplainPolicyRetryInfo contains retry information for a failed ISM policy action.
type IsmExplainPolicyRetryInfo struct {
	Failed          bool  `json:"failed"`
	ConsumedRetries int64 `json:"consumed_retries"`
}

// IsmExplainPolicyInfo contains informational messages and error causes
// about the current ISM policy execution on a managed index.
type IsmExplainPolicyInfo struct {
	Message string `json:"message"`
	Cause   string `json:"cause"`
}

// IsmActionResponse represents the response from ISM add/remove/change/retry actions.
type IsmActionResponse struct {
	UpdatedIndices int64            `json:"updated_indices"`
	FailedIndices  []IsmFailedIndex `json:"failed_indices,omitempty"`
	Failures       *bool            `json:"failures,omitempty"`
}

// IsmFailedIndex represents a single index that failed an ISM action.
type IsmFailedIndex struct {
	IndexName string `json:"index_name"`
	Reason    string `json:"reason"`
}

// IsmListPoliciesResponse represents the response from the list ISM policies API.
type IsmListPoliciesResponse struct {
	Policies      []IsmPolicySummary `json:"policies"`
	TotalPolicies int64              `json:"total_policies"`
}

// IsmPolicySummary represents a single ISM policy entry as returned by the list policies API.
type IsmPolicySummary struct {
	IsmPolicyBase   `json:",inline"`
	SchemaVersion   *int64 `json:"schema_version,omitempty"`
	LastUpdatedTime *int64 `json:"last_updated_time,omitempty"`
}

// RefreshSearchAnalyzersResponse is the response from
// POST /_plugins/_refresh_search_analyzers/{index} (ISM-plugin endpoint;
// hunspell hot-reload supported since OpenSearch 3.7.0).
type RefreshSearchAnalyzersResponse struct {
	SuccessfulRefreshDetails []RefreshSearchAnalyzersDetail `json:"successful_refresh_details"`
}

// RefreshSearchAnalyzersDetail describes the analyzers refreshed for a
// single index.
type RefreshSearchAnalyzersDetail struct {
	Index              string   `json:"index"`
	RefreshedAnalyzers []string `json:"refreshed_analyzers"`
}
