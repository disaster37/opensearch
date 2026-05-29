// neural_service_test.go contains unit tests for DefaultNeuralService.
// Tests use httptest.NewServer to mock OpenSearch neural plugin endpoints.
// Each TestUnit* function covers one service method with success subtests,
// validation error subtests, and error branch subtests (server error,
// unmarshal error, network error).
package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUnitNeuralService_NeuralStats(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/_plugins/_neural/stats/model_inference", func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "GET", r.Method)
		jsonResponse(w, 200, `{"_nodes":{"total":1},"cluster_name":"test","model_inference":{"request_count":100}}`)
	})
	mux.HandleFunc("/_plugins/_neural/stats/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(404)
	})
	mux.HandleFunc("/_plugins/_neural/stats", func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "GET", r.Method)
		jsonResponse(w, 200, `{"_nodes":{"total":1},"cluster_name":"test","model_inference":{"request_count":100},"model_cache":{"hit_count":50}}`)
	})

	ts := httptest.NewServer(mux)
	defer ts.Close()

	svc := NewNeuralService(restyClient(ts), testLogger())
	ctx := context.Background()

	t.Run("all stats", func(t *testing.T) {
		resp, err := svc.NeuralStats(ctx, "")
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Contains(t, *resp, "cluster_name")
	})

	t.Run("specific stat", func(t *testing.T) {
		resp, err := svc.NeuralStats(ctx, "model_inference")
		require.NoError(t, err)
		require.NotNil(t, resp)
	})
}

func TestUnitNeuralService_NeuralStatsErrors(t *testing.T) {
	ctx := context.Background()

	t.Run("server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		svc := NewNeuralService(restyClient(srv), testLogger())
		_, err := svc.NeuralStats(ctx, "")
		require.Error(t, err)
	})

	t.Run("unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		svc := NewNeuralService(restyClient(srv), testLogger())
		_, err := svc.NeuralStats(ctx, "")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("network error", func(t *testing.T) {
		svc := NewNeuralService(deadClient(), testLogger())
		_, err := svc.NeuralStats(ctx, "")
		require.Error(t, err)
	})
}

func TestUnitNeuralService_NeuralWarmup(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "POST", r.Method)
		assert.Equal(t, "/_plugins/_neural/warmup/test-index", r.URL.Path)
		jsonResponse(w, 200, `{"_shards":{"total":2,"successful":2,"failed":0}}`)
	}))
	defer ts.Close()

	svc := NewNeuralService(restyClient(ts), testLogger())
	ctx := context.Background()

	resp, err := svc.NeuralWarmup(ctx, "test-index")
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.NotNil(t, resp.Shards)
	assert.Equal(t, 2, resp.Shards.Total)
	assert.Equal(t, 2, resp.Shards.Successful)
}

func TestUnitNeuralService_NeuralWarmupErrors(t *testing.T) {
	ctx := context.Background()

	t.Run("validation error", func(t *testing.T) {
		svc := NewNeuralService(deadClient(), testLogger())
		_, err := svc.NeuralWarmup(ctx, "")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "index is required")
	})

	t.Run("server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		svc := NewNeuralService(restyClient(srv), testLogger())
		_, err := svc.NeuralWarmup(ctx, "test-index")
		require.Error(t, err)
	})

	t.Run("unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		svc := NewNeuralService(restyClient(srv), testLogger())
		_, err := svc.NeuralWarmup(ctx, "test-index")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("network error", func(t *testing.T) {
		svc := NewNeuralService(deadClient(), testLogger())
		_, err := svc.NeuralWarmup(ctx, "test-index")
		require.Error(t, err)
	})
}

func TestUnitNeuralService_NeuralClearCache(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "POST", r.Method)
		assert.Equal(t, "/_plugins/_neural/clear_cache/test-index", r.URL.Path)
		jsonResponse(w, 200, `{"acknowledged":true}`)
	}))
	defer ts.Close()

	svc := NewNeuralService(restyClient(ts), testLogger())
	ctx := context.Background()

	resp, err := svc.NeuralClearCache(ctx, "test-index")
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.True(t, resp.Acknowledged)
}

func TestUnitNeuralService_NeuralClearCacheErrors(t *testing.T) {
	ctx := context.Background()

	t.Run("validation error", func(t *testing.T) {
		svc := NewNeuralService(deadClient(), testLogger())
		_, err := svc.NeuralClearCache(ctx, "")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "index is required")
	})

	t.Run("server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		svc := NewNeuralService(restyClient(srv), testLogger())
		_, err := svc.NeuralClearCache(ctx, "test-index")
		require.Error(t, err)
	})

	t.Run("unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		svc := NewNeuralService(restyClient(srv), testLogger())
		_, err := svc.NeuralClearCache(ctx, "test-index")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("network error", func(t *testing.T) {
		svc := NewNeuralService(deadClient(), testLogger())
		_, err := svc.NeuralClearCache(ctx, "test-index")
		require.Error(t, err)
	})
}
