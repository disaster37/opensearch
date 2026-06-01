package api

// SmPutPolicy represents the request body for creating or updating a Snapshot Management policy.
// It includes the snapshot configuration, creation and deletion schedules, and notification settings.
type SmPutPolicy struct {
	Description    *string                `json:"description,omitempty"`
	Enabled        *bool                  `json:"enabled,omitempty"`
	SnapshotConfig SmPolicySnapshotConfig `json:"snapshot_config"`
	Creation       SmPolicyCreation       `json:"creation"`
	Deletion       *SmPolicyDeletion      `json:"deletion,omitempty"`
	Notification   *SmPolicyNotification  `json:"notification,omitempty"`
}

// SmPolicySnapshotConfig defines the snapshot-specific configuration for an SM policy,
// including repository, indices, date format, and snapshot options.
type SmPolicySnapshotConfig struct {
	DateFormat         *string        `json:"date_format,omitempty"`
	Timezone           *string        `json:"timezone,omitempty"`
	Indices            *string        `json:"indices,omitempty"`
	Repository         string         `json:"repository,omitempty"`
	IgnoreUnavailable  *bool          `json:"ignore_unavailable,omitempty"`
	IncludeGlobalState *bool          `json:"include_global_state,omitempty"`
	Partial            *bool          `json:"partial,omitempty"`
	Metadata           map[string]any `json:"metadata,omitempty"`
}

// SmPolicyCreation defines the creation schedule for snapshots in an SM policy.
type SmPolicyCreation struct {
	Schedule  map[string]any `json:"schedule"`
	TimeLimit *string        `json:"time_limit,omitempty"`
}

// SmPolicyDeletion defines the deletion (retention) schedule for snapshots in an SM policy.
type SmPolicyDeletion struct {
	Schedule  map[string]any           `json:"schedule,omitempty"`
	Condition *SmPolicyDeleteCondition `json:"condition,omitempty"`
	TimeLimit *string                  `json:"time_limit,omitempty"`
}

// SmPolicyDeleteCondition defines the retention conditions for snapshot deletion,
// including maximum count, maximum age, and minimum count constraints.
type SmPolicyDeleteCondition struct {
	MaxCount *int64  `json:"max_count,omitempty"`
	MaxAge   *string `json:"max_age,omitempty"`
	MinCount *int64  `json:"min_count,omitempty"`
}

// SmPolicyNotification defines the notification configuration for SM policy events.
type SmPolicyNotification struct {
	Channel    SmPolicyNotificationChannel    `json:"channel"`
	Conditions *SmPolicyNotificationCondition `json:"conditions,omitempty"`
}

// SmPolicyNotificationChannel identifies a notification channel by its ID.
type SmPolicyNotificationChannel struct {
	ID string `json:"id"`
}

// SmPolicyNotificationCondition specifies which SM policy events trigger notifications,
// such as snapshot creation, deletion, failure, or time limit exceeded.
type SmPolicyNotificationCondition struct {
	Creation          *bool `json:"creation,omitempty"`
	Deletion          *bool `json:"deletion,omitempty"`
	Failure           *bool `json:"failure,omitempty"`
	TimeLimitExceeded *bool `json:"time_limit_exceeded,omitempty"`
}

// SmPolicy represents a Snapshot Management policy as returned from the SM API,
// extending SmPutPolicy with metadata like name, schema version, and timestamps.
type SmPolicy struct {
	SmPutPolicy    `json:",inline"`
	Name           *string        `json:"policy_id,omitempty"`
	SchemaVersion  *int64         `json:"schema_version,omitempty"`
	LastUpdateTime *int64         `json:"last_updated_time,omitempty"`
	EnabledTime    *int64         `json:"enabled_time,omitempty"`
	Schedule       map[string]any `json:"schedule,omitempty"`
}

// SmGetPolicyResponse represents the full response from the SM GET policy API,
// including the document ID, version, sequence number, primary term, and the policy definition.
type SmGetPolicyResponse struct {
	Id             string   `json:"_id"`
	Version        int64    `json:"_version"`
	SequenceNumber int64    `json:"_seq_no"`
	PrimaryTerm    int64    `json:"_primary_term"`
	Policy         SmPolicy `json:"sm_policy"`
}

// SmDeletePolicyResponse represents the response from the SM delete policy API,
// confirming the deletion with the document ID, version, and result status.
type SmDeletePolicyResponse struct {
	Index          string         `json:"_index"`
	ID             string         `json:"_id"`
	Version        int64          `json:"_version"`
	Result         string         `json:"result"`
	ForcedRefresh  bool           `json:"forced_refresh"`
	Shard          map[string]any `json:"_shards"`
	SequenceNumber int64          `json:"_seq_no"`
	PrimaryTerm    int64          `json:"_primary_term"`
}

// SmExplainPolicyResponse represents the response from the SM explain API,
// containing execution status for one or more snapshot management policies.
type SmExplainPolicyResponse struct {
	Policies []SmExplainPolicy `json:"policies"`
}

// SmExplainPolicy represents the execution status of a single SM policy,
// including creation and deletion state, sequences numbers, and enabled status.
type SmExplainPolicy struct {
	Name           string                `json:"policy_id,omitempty"`
	SequenceNumber int64                 `json:"policy_seq_no,omitempty"`
	PrimaryTerm    int64                 `json:"policy_primary_term,omitempty"`
	Enabled        bool                  `json:"enabled,omitempty"`
	Creation       *SmExplainPolicyState `json:"creation,omitempty"`
	Deletion       *SmExplainPolicyState `json:"deletion,omitempty"`
}

// SmExplainPolicyState represents the current execution state of an SM policy's
// creation or deletion schedule, including trigger details and latest execution info.
type SmExplainPolicyState struct {
	CurrentState    string                         `json:"current_state,omitempty"`
	Trigger         SmExplainPolicyTrigger         `json:"trigger,omitempty"`
	LatestExecution SmExplainPolicyLatestExecution `json:"latest_execution,omitempty"`
	Retry           SmExplainPolicyRetry           `json:"retry,omitempty"`
}

// SmExplainPolicyTrigger represents the trigger timing for an SM policy schedule event.
type SmExplainPolicyTrigger struct {
	Time int64 `json:"time,omitempty"`
}

// SmExplainPolicyLatestExecution contains details about the most recent execution
// of an SM policy snapshot creation or deletion.
type SmExplainPolicyLatestExecution struct {
	Status    string              `json:"status,omitempty"`
	StartTime int64               `json:"start_time,omitempty"`
	EndTime   int64               `json:"end_time,omitempty"`
	Info      SmExplainPolicyInfo `json:"info,omitempty"`
}

// SmExplainPolicyRetry contains retry count information for a failed SM policy execution.
type SmExplainPolicyRetry struct {
	Count int64 `json:"count,omitempty"`
}

// SmExplainPolicyInfo contains informational messages and error causes about an SM policy execution.
type SmExplainPolicyInfo struct {
	Message string `json:"message,omitempty"`
	Cause   string `json:"cause,omitempty"`
}

// SmStartStopResponse represents the response from starting or stopping an SM policy.
type SmStartStopResponse struct {
	Acknowledged bool   `json:"acknowledged,omitempty"`
	Status       string `json:"status,omitempty"`
}

// SmListPoliciesResponse represents the response from listing SM policies.
type SmListPoliciesResponse struct {
	Policies      []SmPolicyBase `json:"policies"`
	TotalPolicies int64          `json:"total_policies"`
}

// SmPolicyBase represents a single SM policy entry as returned by the list policies API.
type SmPolicyBase struct {
	SmPolicy       `json:",inline"`
	SequenceNumber *int64 `json:"_seq_no,omitempty"`
	PrimaryTerm    *int64 `json:"_primary_term,omitempty"`
}
