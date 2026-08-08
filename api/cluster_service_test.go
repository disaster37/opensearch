package api

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUnitClusterServiceHealth(t *testing.T) {
	respJSON := `{"cluster_name":"test","status":"green","timed_out":false,"number_of_nodes":3,"number_of_data_nodes":3,"active_primary_shards":10,"active_shards":20,"relocating_shards":0,"initializing_shards":0,"unassigned_shards":0,"delayed_unassigned_shards":0,"number_of_pending_tasks":0,"number_of_in_flight_fetch":0,"task_max_waiting_in_queue_millis":0,"active_shards_percent_as_number":100.0}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/_cluster/health") {
			w.WriteHeader(200)
			_, _ = fmt.Fprint(w, respJSON)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewClusterService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success without indices", func(t *testing.T) {
		resp, err := svc.Health(ctx, nil)
		require.NoError(t, err)
		assert.Equal(t, "green", resp.Status)
		assert.Equal(t, 3, resp.NumberOfNodes)
	})

	t.Run("success with indices", func(t *testing.T) {
		resp, err := svc.Health(ctx, []string{"idx1"})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("success with multiple indices", func(t *testing.T) {
		resp, err := svc.Health(ctx, []string{"idx1", "idx2"})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})
}

func TestUnitClusterServiceState(t *testing.T) {
	respJSON := `{"cluster_name":"test","cluster_uuid":"abc","version":1,"state_uuid":"x","master_node":"node1"}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/_cluster/state") {
			w.WriteHeader(200)
			_, _ = fmt.Fprint(w, respJSON)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewClusterService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success with metrics and indices", func(t *testing.T) {
		resp, err := svc.State(ctx, &ClusterStateRequest{Metrics: []string{"metadata"}, Indices: []string{"idx1"}})
		require.NoError(t, err)
		assert.Equal(t, "test", resp.ClusterName)
	})

	t.Run("success without metrics", func(t *testing.T) {
		resp, err := svc.State(ctx, &ClusterStateRequest{})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("success with only metrics", func(t *testing.T) {
		resp, err := svc.State(ctx, &ClusterStateRequest{Metrics: []string{"metadata"}})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("success with only indices", func(t *testing.T) {
		resp, err := svc.State(ctx, &ClusterStateRequest{Indices: []string{"idx1"}})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})
}

func TestUnitClusterServiceStats(t *testing.T) {
	respJSON := `{"cluster_name":"test","cluster_uuid":"abc","timestamp":1234567890,"status":"green"}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/_cluster/stats") {
			w.WriteHeader(200)
			_, _ = fmt.Fprint(w, respJSON)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewClusterService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success without node ids", func(t *testing.T) {
		resp, err := svc.Stats(ctx, nil)
		require.NoError(t, err)
		assert.Equal(t, "test", resp.ClusterName)
	})

	t.Run("success with node ids", func(t *testing.T) {
		resp, err := svc.Stats(ctx, []string{"node1"})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("success with multiple node ids", func(t *testing.T) {
		resp, err := svc.Stats(ctx, []string{"node1", "node2"})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})
}

func TestUnitClusterServiceReroute(t *testing.T) {
	respJSON := `{"state":{"cluster_name":"test","cluster_uuid":"abc"}}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/_cluster/reroute" {
			w.WriteHeader(200)
			_, _ = fmt.Fprint(w, respJSON)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewClusterService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		body := map[string]any{"commands": []any{}}
		resp, err := svc.Reroute(ctx, body)
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("validation error nil body", func(t *testing.T) {
		resp, err := svc.Reroute(ctx, nil)
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Contains(t, err.Error(), "body is required")
	})
}

func TestUnitClusterServiceGetSettings(t *testing.T) {
	respJSON := `{"persistent":{},"transient":{}}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == "/_cluster/settings" {
			w.WriteHeader(200)
			_, _ = fmt.Fprint(w, respJSON)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewClusterService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		resp, err := svc.GetSettings(ctx)
		require.NoError(t, err)
		require.NotNil(t, resp)
	})
}

func TestUnitClusterServicePutSettings(t *testing.T) {
	respJSON := `{"acknowledged":true,"persistent":{},"transient":{}}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut && r.URL.Path == "/_cluster/settings" {
			w.WriteHeader(200)
			_, _ = fmt.Fprint(w, respJSON)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewClusterService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		body := map[string]any{"persistent": map[string]any{"cluster.routing.allocation.enable": "all"}}
		resp, err := svc.PutSettings(ctx, body)
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("validation error nil body", func(t *testing.T) {
		resp, err := svc.PutSettings(ctx, nil)
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Contains(t, err.Error(), "body is required")
	})
}

func TestUnitClusterServiceErrorPaths(t *testing.T) {
	ctx := context.Background()

	t.Run("Health server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewClusterService(restyClient(srv), testLogger())
		_, err := s.Health(ctx, nil)
		require.Error(t, err)
	})

	t.Run("Health unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewClusterService(restyClient(srv), testLogger())
		_, err := s.Health(ctx, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("State server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewClusterService(restyClient(srv), testLogger())
		_, err := s.State(ctx, &ClusterStateRequest{})
		require.Error(t, err)
	})

	t.Run("State unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewClusterService(restyClient(srv), testLogger())
		_, err := s.State(ctx, &ClusterStateRequest{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("Stats server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewClusterService(restyClient(srv), testLogger())
		_, err := s.Stats(ctx, nil)
		require.Error(t, err)
	})

	t.Run("Stats unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewClusterService(restyClient(srv), testLogger())
		_, err := s.Stats(ctx, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("Reroute server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewClusterService(restyClient(srv), testLogger())
		_, err := s.Reroute(ctx, map[string]any{})
		require.Error(t, err)
	})

	t.Run("Reroute unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewClusterService(restyClient(srv), testLogger())
		_, err := s.Reroute(ctx, map[string]any{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("GetSettings server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewClusterService(restyClient(srv), testLogger())
		_, err := s.GetSettings(ctx)
		require.Error(t, err)
	})

	t.Run("GetSettings unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewClusterService(restyClient(srv), testLogger())
		_, err := s.GetSettings(ctx)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("PutSettings server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewClusterService(restyClient(srv), testLogger())
		_, err := s.PutSettings(ctx, map[string]any{})
		require.Error(t, err)
	})

	t.Run("PutSettings unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewClusterService(restyClient(srv), testLogger())
		_, err := s.PutSettings(ctx, map[string]any{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("AllocationExplain server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewClusterService(restyClient(srv), testLogger())
		_, err := s.AllocationExplain(ctx, nil)
		require.Error(t, err)
	})

	t.Run("AllocationExplain unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewClusterService(restyClient(srv), testLogger())
		_, err := s.AllocationExplain(ctx, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("AllocationExplain with body server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewClusterService(restyClient(srv), testLogger())
		_, err := s.AllocationExplain(ctx, map[string]any{})
		require.Error(t, err)
	})

	t.Run("AllocationExplain with body unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewClusterService(restyClient(srv), testLogger())
		_, err := s.AllocationExplain(ctx, map[string]any{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("PendingTasks server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewClusterService(restyClient(srv), testLogger())
		_, err := s.PendingTasks(ctx)
		require.Error(t, err)
	})

	t.Run("PendingTasks unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewClusterService(restyClient(srv), testLogger())
		_, err := s.PendingTasks(ctx)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("RemoteInfo server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewClusterService(restyClient(srv), testLogger())
		_, err := s.RemoteInfo(ctx)
		require.Error(t, err)
	})

	t.Run("RemoteInfo unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewClusterService(restyClient(srv), testLogger())
		_, err := s.RemoteInfo(ctx)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("PutDecommissionAwareness server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewClusterService(restyClient(srv), testLogger())
		_, err := s.PutDecommissionAwareness(ctx, "zone", "a")
		require.Error(t, err)
	})

	t.Run("PutDecommissionAwareness unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewClusterService(restyClient(srv), testLogger())
		_, err := s.PutDecommissionAwareness(ctx, "zone", "a")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("GetDecommissionAwareness server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewClusterService(restyClient(srv), testLogger())
		_, err := s.GetDecommissionAwareness(ctx, "zone")
		require.Error(t, err)
	})

	t.Run("GetDecommissionAwareness unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewClusterService(restyClient(srv), testLogger())
		_, err := s.GetDecommissionAwareness(ctx, "zone")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("DeleteDecommissionAwareness server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewClusterService(restyClient(srv), testLogger())
		_, err := s.DeleteDecommissionAwareness(ctx)
		require.Error(t, err)
	})

	t.Run("DeleteDecommissionAwareness unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewClusterService(restyClient(srv), testLogger())
		_, err := s.DeleteDecommissionAwareness(ctx)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("PutWeightedRouting server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewClusterService(restyClient(srv), testLogger())
		_, err := s.PutWeightedRouting(ctx, "zone", nil)
		require.Error(t, err)
	})

	t.Run("PutWeightedRouting unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewClusterService(restyClient(srv), testLogger())
		_, err := s.PutWeightedRouting(ctx, "zone", nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("GetWeightedRouting server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewClusterService(restyClient(srv), testLogger())
		_, err := s.GetWeightedRouting(ctx, "zone")
		require.Error(t, err)
	})

	t.Run("GetWeightedRouting unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewClusterService(restyClient(srv), testLogger())
		_, err := s.GetWeightedRouting(ctx, "zone")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("DeleteWeightedRouting server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewClusterService(restyClient(srv), testLogger())
		_, err := s.DeleteWeightedRouting(ctx)
		require.Error(t, err)
	})

	t.Run("DeleteWeightedRouting unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewClusterService(restyClient(srv), testLogger())
		_, err := s.DeleteWeightedRouting(ctx)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("PostVotingConfigExclusions server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewClusterService(restyClient(srv), testLogger())
		_, err := s.PostVotingConfigExclusions(ctx, nil)
		require.Error(t, err)
	})

	t.Run("PostVotingConfigExclusions unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewClusterService(restyClient(srv), testLogger())
		_, err := s.PostVotingConfigExclusions(ctx, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("DeleteVotingConfigExclusions server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewClusterService(restyClient(srv), testLogger())
		_, err := s.DeleteVotingConfigExclusions(ctx, true)
		require.Error(t, err)
	})

	t.Run("DeleteVotingConfigExclusions unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewClusterService(restyClient(srv), testLogger())
		_, err := s.DeleteVotingConfigExclusions(ctx, true)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})
}

func TestUnitClusterServiceNetworkErrors(t *testing.T) {
	ctx := context.Background()
	s := NewClusterService(deadClient(), testLogger())

	t.Run("Health network error", func(t *testing.T) {
		_, err := s.Health(ctx, nil)
		require.Error(t, err)
	})

	t.Run("State network error", func(t *testing.T) {
		_, err := s.State(ctx, &ClusterStateRequest{})
		require.Error(t, err)
	})

	t.Run("Stats network error", func(t *testing.T) {
		_, err := s.Stats(ctx, nil)
		require.Error(t, err)
	})

	t.Run("Reroute network error", func(t *testing.T) {
		_, err := s.Reroute(ctx, map[string]any{})
		require.Error(t, err)
	})

	t.Run("GetSettings network error", func(t *testing.T) {
		_, err := s.GetSettings(ctx)
		require.Error(t, err)
	})

	t.Run("PutSettings network error", func(t *testing.T) {
		_, err := s.PutSettings(ctx, map[string]any{})
		require.Error(t, err)
	})

	t.Run("AllocationExplain network error", func(t *testing.T) {
		_, err := s.AllocationExplain(ctx, nil)
		require.Error(t, err)
	})

	t.Run("PendingTasks network error", func(t *testing.T) {
		_, err := s.PendingTasks(ctx)
		require.Error(t, err)
	})

	t.Run("RemoteInfo network error", func(t *testing.T) {
		_, err := s.RemoteInfo(ctx)
		require.Error(t, err)
	})

	t.Run("ExistsComponentTemplate network error", func(t *testing.T) {
		_, err := s.ExistsComponentTemplate(ctx, "test")
		require.Error(t, err)
	})

	t.Run("PutDecommissionAwareness network error", func(t *testing.T) {
		_, err := s.PutDecommissionAwareness(ctx, "zone", "a")
		require.Error(t, err)
	})

	t.Run("GetDecommissionAwareness network error", func(t *testing.T) {
		_, err := s.GetDecommissionAwareness(ctx, "zone")
		require.Error(t, err)
	})

	t.Run("DeleteDecommissionAwareness network error", func(t *testing.T) {
		_, err := s.DeleteDecommissionAwareness(ctx)
		require.Error(t, err)
	})

	t.Run("PutWeightedRouting network error", func(t *testing.T) {
		_, err := s.PutWeightedRouting(ctx, "zone", nil)
		require.Error(t, err)
	})

	t.Run("GetWeightedRouting network error", func(t *testing.T) {
		_, err := s.GetWeightedRouting(ctx, "zone")
		require.Error(t, err)
	})

	t.Run("DeleteWeightedRouting network error", func(t *testing.T) {
		_, err := s.DeleteWeightedRouting(ctx)
		require.Error(t, err)
	})

	t.Run("PostVotingConfigExclusions network error", func(t *testing.T) {
		_, err := s.PostVotingConfigExclusions(ctx, nil)
		require.Error(t, err)
	})

	t.Run("DeleteVotingConfigExclusions network error", func(t *testing.T) {
		_, err := s.DeleteVotingConfigExclusions(ctx, true)
		require.Error(t, err)
	})
}

func TestUnitClusterServiceAllocationExplain(t *testing.T) {
	respJSON := `{"index":"test","shard":0,"primary":true,"current_state":"started"}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/_cluster/allocation/explain") {
			w.WriteHeader(200)
			_, _ = fmt.Fprint(w, respJSON)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewClusterService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success without body", func(t *testing.T) {
		resp, err := svc.AllocationExplain(ctx, nil)
		require.NoError(t, err)
		assert.Equal(t, "test", resp.Index)
		assert.Equal(t, 0, resp.Shard)
	})

	t.Run("success with body", func(t *testing.T) {
		body := map[string]any{"index": "test", "shard": 0}
		resp, err := svc.AllocationExplain(ctx, body)
		require.NoError(t, err)
		assert.Equal(t, "test", resp.Index)
	})
}

func TestUnitClusterServicePendingTasks(t *testing.T) {
	respJSON := `{"tasks":[{"source":"create-index","time_in_queue_millis":100}]}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == "/_cluster/pending_tasks" {
			w.WriteHeader(200)
			_, _ = fmt.Fprint(w, respJSON)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewClusterService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		resp, err := svc.PendingTasks(ctx)
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Len(t, resp.Tasks, 1)
	})
}

func TestUnitClusterServiceRemoteInfo(t *testing.T) {
	respJSON := `{"cluster1":{"seeds":[{"publish_address":"127.0.0.1:9300"}]}}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == "/_remote/info" {
			w.WriteHeader(200)
			_, _ = fmt.Fprint(w, respJSON)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewClusterService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		resp, err := svc.RemoteInfo(ctx)
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Contains(t, resp, "cluster1")
	})
}

func TestUnitClusterServiceExistsComponentTemplate(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead && strings.HasPrefix(r.URL.Path, "/_component_template/") {
			if strings.Contains(r.URL.Path, "exists") {
				w.WriteHeader(200)
			} else {
				w.WriteHeader(404)
			}
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewClusterService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("template exists", func(t *testing.T) {
		exists, err := svc.ExistsComponentTemplate(ctx, "exists")
		require.NoError(t, err)
		assert.True(t, exists)
	})

	t.Run("template not found", func(t *testing.T) {
		exists, err := svc.ExistsComponentTemplate(ctx, "notfound")
		require.NoError(t, err)
		assert.False(t, exists)
	})

	t.Run("server error", func(t *testing.T) {
		errSrv := errServer(500)
		defer errSrv.Close()
		errSvc := NewClusterService(restyClient(errSrv), testLogger())
		exists, err := errSvc.ExistsComponentTemplate(ctx, "test")
		require.Error(t, err)
		assert.False(t, exists)
		assert.Contains(t, err.Error(), "unexpected HTTP status")
	})

	t.Run("validation error empty name", func(t *testing.T) {
		exists, err := svc.ExistsComponentTemplate(ctx, "")
		require.Error(t, err)
		assert.False(t, exists)
		assert.Contains(t, err.Error(), "name is required")
	})
}

func TestUnitClusterServiceDecommissionAwareness(t *testing.T) {
	ackJSON := `{"acknowledged":true}`
	decomJSON := `{"status":"active","decommission_status":"running"}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/_cluster/decommission/awareness") {
			if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "_status") {
				w.WriteHeader(200)
				_, _ = fmt.Fprint(w, decomJSON)
				return
			}
			w.WriteHeader(200)
			_, _ = fmt.Fprint(w, ackJSON)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewClusterService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("put decommission", func(t *testing.T) {
		resp, err := svc.PutDecommissionAwareness(ctx, "zone", "a")
		require.NoError(t, err)
		assert.True(t, resp.Acknowledged)
	})

	t.Run("get decommission", func(t *testing.T) {
		resp, err := svc.GetDecommissionAwareness(ctx, "zone")
		require.NoError(t, err)
		assert.Equal(t, "active", resp.Status)
		assert.Equal(t, "running", resp.DecommissionStatus)
	})

	t.Run("delete decommission", func(t *testing.T) {
		resp, err := svc.DeleteDecommissionAwareness(ctx)
		require.NoError(t, err)
		assert.True(t, resp.Acknowledged)
	})

	t.Run("put validation error empty attribute name", func(t *testing.T) {
		resp, err := svc.PutDecommissionAwareness(ctx, "", "a")
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Contains(t, err.Error(), "attributeName is required")
	})

	t.Run("put validation error empty attribute value", func(t *testing.T) {
		resp, err := svc.PutDecommissionAwareness(ctx, "zone", "")
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Contains(t, err.Error(), "attributeValue is required")
	})

	t.Run("get validation error empty attribute name", func(t *testing.T) {
		resp, err := svc.GetDecommissionAwareness(ctx, "")
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Contains(t, err.Error(), "attributeName is required")
	})
}

func TestUnitClusterServiceWeightedRouting(t *testing.T) {
	ackJSON := `{"acknowledged":true}`
	weightsJSON := `{"weights":{"zone_a":1,"zone_b":2}}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/_cluster/routing/awareness") {
			if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/weights") {
				w.WriteHeader(200)
				_, _ = fmt.Fprint(w, weightsJSON)
				return
			}
			w.WriteHeader(200)
			_, _ = fmt.Fprint(w, ackJSON)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewClusterService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("put weighted routing", func(t *testing.T) {
		resp, err := svc.PutWeightedRouting(ctx, "zone", map[string]any{"zone_a": 1, "zone_b": 2})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("get weighted routing", func(t *testing.T) {
		resp, err := svc.GetWeightedRouting(ctx, "zone")
		require.NoError(t, err)
		assert.Equal(t, 1, resp.Weights["zone_a"])
		assert.Equal(t, 2, resp.Weights["zone_b"])
	})

	t.Run("delete weighted routing", func(t *testing.T) {
		resp, err := svc.DeleteWeightedRouting(ctx)
		require.NoError(t, err)
		assert.True(t, resp.Acknowledged)
	})

	t.Run("put validation error empty attribute", func(t *testing.T) {
		resp, err := svc.PutWeightedRouting(ctx, "", nil)
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Contains(t, err.Error(), "attribute is required")
	})

	t.Run("get validation error empty attribute", func(t *testing.T) {
		resp, err := svc.GetWeightedRouting(ctx, "")
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Contains(t, err.Error(), "attribute is required")
	})
}

func TestUnitClusterServiceVotingConfigExclusions(t *testing.T) {
	ackJSON := `{"acknowledged":true}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/_cluster/voting_config_exclusions" {
			w.WriteHeader(200)
			_, _ = fmt.Fprint(w, ackJSON)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewClusterService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("post voting config exclusions", func(t *testing.T) {
		params := map[string]string{"node_names": "node1", "timeout": "30s"}
		resp, err := svc.PostVotingConfigExclusions(ctx, params)
		require.NoError(t, err)
		assert.True(t, resp.Acknowledged)
	})

	t.Run("post voting config exclusions without params", func(t *testing.T) {
		resp, err := svc.PostVotingConfigExclusions(ctx, nil)
		require.NoError(t, err)
		assert.True(t, resp.Acknowledged)
	})

	t.Run("delete voting config exclusions true", func(t *testing.T) {
		resp, err := svc.DeleteVotingConfigExclusions(ctx, true)
		require.NoError(t, err)
		assert.True(t, resp.Acknowledged)
	})

	t.Run("delete voting config exclusions false", func(t *testing.T) {
		resp, err := svc.DeleteVotingConfigExclusions(ctx, false)
		require.NoError(t, err)
		assert.True(t, resp.Acknowledged)
	})
}

func TestUnitClusterServicePruneBlockCache(t *testing.T) {
	respJSON := `{"acknowledged":true,"summary":{"total_nodes_targeted":2,"successful_nodes":1,"failed_nodes":1},"nodes":{"n1":{"name":"warm-1","cleared":true}},"failures":[{"node_id":"n2","reason":"not a warm node"}]}`

	var capturedPath, capturedMethod string
	var capturedNodes, capturedTimeout string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedPath = r.URL.Path
		capturedMethod = r.Method
		capturedNodes = r.URL.Query().Get("nodes")
		capturedTimeout = r.URL.Query().Get("timeout")
		w.WriteHeader(200)
		_, _ = fmt.Fprint(w, respJSON)
	}))
	defer srv.Close()

	svc := NewClusterService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success with params", func(t *testing.T) {
		resp, err := svc.PruneBlockCache(ctx, &PruneBlockCacheParams{
			Nodes:   []string{"n1", "n2"},
			Timeout: "30s",
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.True(t, resp.Acknowledged)
		require.NotNil(t, resp.Summary)
		assert.Equal(t, 2, resp.Summary.TotalNodesTargeted)
		assert.Equal(t, 1, resp.Summary.SuccessfulNodes)
		assert.Equal(t, 1, resp.Summary.FailedNodes)
		require.Contains(t, resp.Nodes, "n1")
		assert.True(t, resp.Nodes["n1"].Cleared)
		require.Len(t, resp.Failures, 1)
		assert.Equal(t, "n2", resp.Failures[0].NodeId)
		assert.Equal(t, "not a warm node", resp.Failures[0].Reason)
		assert.Equal(t, "/_blockcache/prune", capturedPath)
		assert.Equal(t, http.MethodPost, capturedMethod)
		assert.Equal(t, "n1,n2", capturedNodes)
		assert.Equal(t, "30s", capturedTimeout)
	})

	t.Run("success without params", func(t *testing.T) {
		resp, err := svc.PruneBlockCache(ctx)
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.True(t, resp.Acknowledged)
	})
}

func TestUnitClusterServicePruneBlockCache_ErrorPaths(t *testing.T) {
	ctx := context.Background()

	t.Run("server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewClusterService(restyClient(srv), testLogger())
		_, err := s.PruneBlockCache(ctx)
		require.Error(t, err)
	})

	t.Run("unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewClusterService(restyClient(srv), testLogger())
		_, err := s.PruneBlockCache(ctx)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("network error", func(t *testing.T) {
		s := NewClusterService(deadClient(), testLogger())
		_, err := s.PruneBlockCache(ctx)
		require.Error(t, err)
	})
}
