package api

import (
	"context"
	"fmt"

	json "github.com/goccy/go-json"

	"github.com/go-resty/resty/v2"
	"github.com/sirupsen/logrus"
)

// TasksService provides access to the OpenSearch Tasks API group.
// It exposes operations for listing, retrieving, and cancelling
// long-running tasks across cluster nodes.
//
// Example usage:
//
//	list, err := svc.List(ctx)
//	for _, node := range list.Nodes {
//	    for _, task := range node.Tasks {
//	        fmt.Println(task.Action, taskRunningTime)
//	    }
//	}
type TasksService interface {
	// List returns all currently running tasks across cluster nodes.
	List(ctx context.Context) (*TasksListResponse, error)

	// Get retrieves a specific task by its ID.
	// The taskId must be in the format "nodeId:taskId".
	Get(ctx context.Context, taskId string) (*TasksGetTaskResponse, error)

	// Cancel cancels a running task identified by its ID.
	// The taskId must be in the format "nodeId:taskId".
	Cancel(ctx context.Context, taskId string) (*TasksCancelResponse, error)
}

// DefaultTasksService is the default implementation of TasksService,
// backed by an HTTP client and a structured logger.
type DefaultTasksService struct {
	client *resty.Client
	logger *logrus.Entry
}

// NewTasksService creates a new TasksService using the provided HTTP client
// and logger. The logger is scoped with a "service=tasks" field.
func NewTasksService(client *resty.Client, logger *logrus.Entry) TasksService {
	return &DefaultTasksService{client: client, logger: logger.WithField("service", "tasks")}
}

// List retrieves all currently running tasks from GET /_tasks.
// Returns a TasksListResponse containing per-node task listings and
// any node or task-level failures encountered during collection.
func (s *DefaultTasksService) List(ctx context.Context) (*TasksListResponse, error) {
	resp, err := s.client.R().SetContext(ctx).Get("/_tasks")
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result TasksListResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

// Get retrieves a specific task by its ID from GET /_tasks/{taskId}.
// The taskId must be in the "nodeId:taskId" format.
// Returns a TasksGetTaskResponse indicating whether the task has completed
// and, if so, the task details or any error that occurred.
func (s *DefaultTasksService) Get(ctx context.Context, taskId string) (*TasksGetTaskResponse, error) {
	if taskId == "" {
		return nil, fmt.Errorf("task id is required")
	}

	path := fmt.Sprintf("/_tasks/%s", taskId)

	resp, err := s.client.R().SetContext(ctx).Get(path)
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result TasksGetTaskResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

// Cancel cancels a running task via POST /_tasks/{taskId}/_cancel.
// The taskId must be in the "nodeId:taskId" format.
// Returns a TasksCancelResponse with the nodes that held the task and
// any failures encountered during cancellation.
func (s *DefaultTasksService) Cancel(ctx context.Context, taskId string) (*TasksCancelResponse, error) {
	if taskId == "" {
		return nil, fmt.Errorf("task id is required")
	}

	path := fmt.Sprintf("/_tasks/%s/_cancel", taskId)

	resp, err := s.client.R().SetContext(ctx).Post(path)
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result TasksCancelResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}
