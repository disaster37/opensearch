package api

import (
	"net/http"

	"github.com/disaster37/opensearch/v4/types"
)

// TasksListResponse represents the result of listing all running tasks.
// It contains per-node task listings, any task or node-level failures,
// and optional HTTP response headers.
type TasksListResponse struct {
	Header       http.Header                  `json:"-"`
	TaskFailures []*TaskOperationFailure      `json:"task_failures"`
	NodeFailures []*types.FailedNodeException `json:"node_failures"`
	Nodes        map[string]*DiscoveryNode    `json:"nodes"`
}

// TaskOperationFailure represents a failure that occurred during a task
// operation, including the task ID, node ID, status, and error reason.
type TaskOperationFailure struct {
	TaskId int64               `json:"task_id"`
	NodeId string              `json:"node_id"`
	Status string              `json:"status"`
	Reason *types.OpenSearchErrorDetails `json:"reason"`
}

// DiscoveryNode represents a cluster node as reported by the Tasks API,
// including its identification details, assigned roles, custom attributes,
// and the currently running tasks on that node.
type DiscoveryNode struct {
	Name             string               `json:"name"`
	TransportAddress string               `json:"transport_address"`
	Host             string               `json:"host"`
	IP               string               `json:"ip"`
	Roles            []string             `json:"roles"`
	Attributes       map[string]any       `json:"attributes"`
	Tasks            map[string]*TaskInfo `json:"tasks"`
}

// TaskInfo represents detailed information about a single running task,
// including its type, action, start time, running duration, cancellability,
// parent task reference, headers, status, and description.
type TaskInfo struct {
	Node               string            `json:"node"`
	Id                 int64             `json:"id"`
	Type               string            `json:"type"`
	Action             string            `json:"action"`
	Status             any               `json:"status"`
	Description        any               `json:"description"`
	StartTime          string            `json:"start_time"`
	StartTimeInMillis  int64             `json:"start_time_in_millis"`
	RunningTime        string            `json:"running_time"`
	RunningTimeInNanos int64             `json:"running_time_in_nanos"`
	Cancellable        bool              `json:"cancellable"`
	Cancelled          bool              `json:"cancelled"`
	ParentTaskId       string            `json:"parent_task_id"`
	Headers            map[string]string `json:"headers"`
}

// StartTaskResult represents the result of starting an async task,
// containing the task identifier that can be used to poll for completion.
type StartTaskResult struct {
	Header http.Header `json:"-"`
	TaskId string      `json:"task"`
}

// TasksGetTaskResponse represents the result of retrieving a specific task.
// The Completed field indicates whether the task has finished; if so, the
// Task field contains its final state, and Error contains any failure details.
type TasksGetTaskResponse struct {
	Header    http.Header         `json:"-"`
	Completed bool                `json:"completed"`
	Task      *TaskInfo           `json:"task,omitempty"`
	Error     *types.OpenSearchErrorDetails `json:"error,omitempty"`
}

// TasksCancelResponse represents the result of cancelling a running task.
// It contains per-node task listings showing which nodes held the task,
// along with any task or node-level failures encountered during cancellation.
type TasksCancelResponse struct {
	Header       http.Header                  `json:"-"`
	TaskFailures []*TaskOperationFailure      `json:"task_failures"`
	NodeFailures []*types.FailedNodeException `json:"node_failures"`
	Nodes        map[string]*DiscoveryNode    `json:"nodes"`
}
