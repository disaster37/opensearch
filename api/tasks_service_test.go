package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUnitTasksService_List(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "GET", r.Method)
		assert.Equal(t, "/_tasks", r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"task_failures":[],"node_failures":[],"nodes":{"n1":{"name":"node1","transport_address":"127.0.0.1:9300","host":"127.0.0.1","ip":"127.0.0.1","roles":["data"],"attributes":{},"tasks":{"n1:100":{"node":"n1","id":100,"type":"transport","action":"indices:data/write/bulk","status":{},"description":"bulk","start_time":"2023-01-01T00:00:00.000Z","start_time_in_millis":1700000000000,"running_time":"100ms","running_time_in_nanos":100000000,"cancellable":true,"cancelled":false,"parent_task_id":"","headers":{}}}}}}`))
	}))
	defer ts.Close()

	svc := NewTasksService(restyClient(ts), testLogger())
	ctx := context.Background()

	resp, err := svc.List(ctx)
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Contains(t, resp.Nodes, "n1")
	assert.Len(t, resp.Nodes["n1"].Tasks, 1)
}

func TestUnitTasksService_Get(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "GET", r.Method)
		assert.Contains(t, r.URL.Path, "/_tasks/n1:100")
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"completed":true,"task":{"node":"n1","id":100,"type":"transport","action":"indices:data/write/bulk","status":{},"description":"bulk","start_time":"2023-01-01T00:00:00.000Z","start_time_in_millis":1700000000000,"running_time":"100ms","running_time_in_nanos":100000000,"cancellable":true,"cancelled":false,"parent_task_id":"","headers":{}}}`))
	}))
	defer ts.Close()

	svc := NewTasksService(restyClient(ts), testLogger())
	ctx := context.Background()

	resp, err := svc.Get(ctx, "n1:100")
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.True(t, resp.Completed)
	assert.Equal(t, "indices:data/write/bulk", resp.Task.Action)
}

func TestUnitTasksService_Cancel(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "POST", r.Method)
		assert.Contains(t, r.URL.Path, "/_tasks/n1:100/_cancel")
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"task_failures":[],"node_failures":[],"nodes":{"n1":{"name":"node1","transport_address":"127.0.0.1:9300","host":"127.0.0.1","ip":"127.0.0.1","roles":["data"],"attributes":{},"tasks":{}}}}`))
	}))
	defer ts.Close()

	svc := NewTasksService(restyClient(ts), testLogger())
	ctx := context.Background()

	resp, err := svc.Cancel(ctx, "n1:100")
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Contains(t, resp.Nodes, "n1")
	assert.Empty(t, resp.TaskFailures)
}
