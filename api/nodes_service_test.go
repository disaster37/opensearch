package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUnitNodesService_Info(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/_nodes/_all/_all", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"cluster_name":"test-cluster","nodes":{}}`))
	})
	mux.HandleFunc("/_nodes/node1/os", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"cluster_name":"test-cluster","nodes":{"n1":{"name":"node1","roles":["cluster_manager"]}}}`))
	})

	ts := httptest.NewServer(mux)
	defer ts.Close()

	svc := NewNodesService(restyClient(ts), testLogger())
	ctx := context.Background()

	t.Run("all nodes all metrics", func(t *testing.T) {
		resp, err := svc.Info(ctx, &NodesInfoRequest{})
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, "test-cluster", resp.ClusterName)
	})

	t.Run("specific node with metric", func(t *testing.T) {
		resp, err := svc.Info(ctx, &NodesInfoRequest{NodeIds: []string{"node1"}, Metrics: []string{"os"}})
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, "test-cluster", resp.ClusterName)
		assert.Contains(t, resp.Nodes, "n1")
		assert.Equal(t, "node1", resp.Nodes["n1"].Name)
	})
}

func TestUnitNodesService_Stats(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/_nodes/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"cluster_name":"test-cluster","nodes":{}}`))
	})
	mux.HandleFunc("/_nodes/node1/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"cluster_name":"test-cluster","nodes":{"n1":{"name":"node1","roles":["data"]}}}`))
	})
	mux.HandleFunc("/_nodes/stats/os", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"cluster_name":"test-cluster","nodes":{}}`))
	})
	mux.HandleFunc("/_nodes/stats/file_cache", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		// Echo the detailed query param so the test can assert it.
		detailed := r.URL.Query().Get("detailed")
		body := `{"cluster_name":"test-cluster","nodes":{"n1":{"name":"node1","file_cache":{"store_size":"1mb"},"native_memory":{"total_estimated_bytes":1024,"analytics_backend":{"allocated_bytes":512,"resident_bytes":256}}}}}`
		if detailed != "" {
			body = `{"cluster_name":"test-cluster","nodes":{"n1":{"name":"node1","file_cache":{"store_size":"1mb","block_cache":{"evictions":0},"detailed":true},"native_memory":{"total_estimated_bytes":1024,"analytics_backend":{"allocated_bytes":512,"resident_bytes":256}}}}}`
		}
		_, _ = w.Write([]byte(body))
	})

	ts := httptest.NewServer(mux)
	defer ts.Close()

	svc := NewNodesService(restyClient(ts), testLogger())
	ctx := context.Background()

	t.Run("all nodes", func(t *testing.T) {
		resp, err := svc.Stats(ctx, &NodesStatsRequest{})
		require.NoError(t, err)
		assert.Equal(t, "test-cluster", resp.ClusterName)
	})

	t.Run("specific node", func(t *testing.T) {
		resp, err := svc.Stats(ctx, &NodesStatsRequest{NodeIds: []string{"node1"}})
		require.NoError(t, err)
		assert.Contains(t, resp.Nodes, "n1")
	})

	t.Run("specific metric", func(t *testing.T) {
		resp, err := svc.Stats(ctx, &NodesStatsRequest{Metrics: []string{"os"}})
		require.NoError(t, err)
		assert.NotNil(t, resp)
	})

	t.Run("detailed=true sends query param", func(t *testing.T) {
		resp, err := svc.Stats(ctx, &NodesStatsRequest{Metrics: []string{"file_cache"}, Detailed: true})
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Contains(t, resp.Nodes, "n1")
		fc := resp.Nodes["n1"].FileCache
		require.NotNil(t, fc)
		assert.Equal(t, true, fc["detailed"])
		require.NotNil(t, resp.Nodes["n1"].NativeMemory)
		assert.Equal(t, int64(1024), resp.Nodes["n1"].NativeMemory.TotalEstimatedBytes)
		require.NotNil(t, resp.Nodes["n1"].NativeMemory.AnalyticsBackend)
		assert.Equal(t, int64(512), resp.Nodes["n1"].NativeMemory.AnalyticsBackend.AllocatedBytes)
	})

	t.Run("detailed=false omits query param", func(t *testing.T) {
		resp, err := svc.Stats(ctx, &NodesStatsRequest{Metrics: []string{"file_cache"}, Detailed: false})
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Contains(t, resp.Nodes, "n1")
		fc := resp.Nodes["n1"].FileCache
		require.NotNil(t, fc)
		_, hasDetailed := fc["detailed"]
		assert.False(t, hasDetailed)
	})
}

func TestUnitNodesService_HotThreads(t *testing.T) {
	threadDump := "::: {node-1}[abcdef][127.0.0.1][9300]\n  Hot threads at 2026-01-01T00:00:00.000Z"

	mux := http.NewServeMux()
	mux.HandleFunc("/_nodes/hot_threads", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(threadDump))
	})
	mux.HandleFunc("/_nodes/node1/hot_threads", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(threadDump))
	})

	ts := httptest.NewServer(mux)
	defer ts.Close()

	svc := NewNodesService(restyClient(ts), testLogger())
	ctx := context.Background()

	t.Run("all nodes", func(t *testing.T) {
		resp, err := svc.HotThreads(ctx, nil)
		require.NoError(t, err)
		assert.Contains(t, resp, "Hot threads")
	})

	t.Run("specific node", func(t *testing.T) {
		resp, err := svc.HotThreads(ctx, []string{"node1"})
		require.NoError(t, err)
		assert.Contains(t, resp, "Hot threads")
	})
}

func TestUnitNodesService_ReloadSecureSettings(t *testing.T) {
	respJSON := `{"cluster_name":"test","nodes":{"n1":{"name":"node1"}}}`

	mux := http.NewServeMux()
	mux.HandleFunc("/_nodes/reload_secure_settings", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(respJSON))
	})

	ts := httptest.NewServer(mux)
	defer ts.Close()

	svc := NewNodesService(restyClient(ts), testLogger())
	ctx := context.Background()

	t.Run("success without body", func(t *testing.T) {
		resp, err := svc.ReloadSecureSettings(ctx, nil)
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, "test", resp.ClusterName)
		assert.Contains(t, resp.Nodes, "n1")
		assert.Equal(t, "node1", resp.Nodes["n1"].Name)
	})

	t.Run("success with body", func(t *testing.T) {
		body := map[string]any{"secure_settings_password": "test"}
		resp, err := svc.ReloadSecureSettings(ctx, body)
		require.NoError(t, err)
		require.NotNil(t, resp)
	})
}

func TestUnitNodesService_Usage(t *testing.T) {
	respJSON := `{"cluster_name":"test","nodes":{"n1":{"timestamp":1234567890,"since":1234567880,"rest_actions":{"_count":100}}}}`

	mux := http.NewServeMux()
	mux.HandleFunc("/_nodes/usage", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(respJSON))
	})
	mux.HandleFunc("/_nodes/node1/usage", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(respJSON))
	})
	mux.HandleFunc("/_nodes/node1/usage/rest_actions", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(respJSON))
	})

	ts := httptest.NewServer(mux)
	defer ts.Close()

	svc := NewNodesService(restyClient(ts), testLogger())
	ctx := context.Background()

	t.Run("all nodes", func(t *testing.T) {
		resp, err := svc.Usage(ctx, &NodesUsageRequest{})
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, "test", resp.ClusterName)
		assert.Contains(t, resp.Nodes, "n1")
	})

	t.Run("specific node", func(t *testing.T) {
		resp, err := svc.Usage(ctx, &NodesUsageRequest{NodeIds: []string{"node1"}})
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, int64(1234567890), resp.Nodes["n1"].Timestamp)
		assert.Equal(t, int64(1234567880), resp.Nodes["n1"].Since)
	})

	t.Run("specific node with metric", func(t *testing.T) {
		resp, err := svc.Usage(ctx, &NodesUsageRequest{NodeIds: []string{"node1"}, Metrics: []string{"rest_actions"}})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})
}

func TestUnitNodesService_ErrorPaths(t *testing.T) {
	ctx := context.Background()

	t.Run("HotThreads server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewNodesService(restyClient(srv), testLogger())
		_, err := s.HotThreads(ctx, nil)
		require.Error(t, err)
	})

	t.Run("ReloadSecureSettings server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewNodesService(restyClient(srv), testLogger())
		_, err := s.ReloadSecureSettings(ctx, nil)
		require.Error(t, err)
	})

	t.Run("ReloadSecureSettings unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewNodesService(restyClient(srv), testLogger())
		_, err := s.ReloadSecureSettings(ctx, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("Usage server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewNodesService(restyClient(srv), testLogger())
		_, err := s.Usage(ctx, &NodesUsageRequest{})
		require.Error(t, err)
	})

	t.Run("Usage unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewNodesService(restyClient(srv), testLogger())
		_, err := s.Usage(ctx, &NodesUsageRequest{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("Info server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewNodesService(restyClient(srv), testLogger())
		_, err := s.Info(ctx, &NodesInfoRequest{})
		require.Error(t, err)
	})

	t.Run("Info unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewNodesService(restyClient(srv), testLogger())
		_, err := s.Info(ctx, &NodesInfoRequest{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("Stats server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewNodesService(restyClient(srv), testLogger())
		_, err := s.Stats(ctx, &NodesStatsRequest{})
		require.Error(t, err)
	})

	t.Run("Stats unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewNodesService(restyClient(srv), testLogger())
		_, err := s.Stats(ctx, &NodesStatsRequest{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})
}

func TestUnitNodesService_NetworkErrors(t *testing.T) {
	ctx := context.Background()
	s := NewNodesService(deadClient(), testLogger())

	t.Run("Info network error", func(t *testing.T) {
		_, err := s.Info(ctx, &NodesInfoRequest{})
		require.Error(t, err)
	})

	t.Run("Stats network error", func(t *testing.T) {
		_, err := s.Stats(ctx, &NodesStatsRequest{})
		require.Error(t, err)
	})

	t.Run("HotThreads network error", func(t *testing.T) {
		_, err := s.HotThreads(ctx, nil)
		require.Error(t, err)
	})

	t.Run("ReloadSecureSettings network error", func(t *testing.T) {
		_, err := s.ReloadSecureSettings(ctx, nil)
		require.Error(t, err)
	})

	t.Run("Usage network error", func(t *testing.T) {
		_, err := s.Usage(ctx, &NodesUsageRequest{})
		require.Error(t, err)
	})
}
