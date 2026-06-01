// knn_service_test.go contains unit tests for DefaultKnnService.
// Tests use httptest.NewServer to mock OpenSearch k-NN plugin endpoints.
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

func TestUnitKnnService_KnnStats(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/_plugins/_knn/stats/graph_memory_usage", func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "GET", r.Method)
		jsonResponse(w, 200, `{"_nodes":{"total":1},"cluster_name":"test","graph_memory_usage":1024}`)
	})
	mux.HandleFunc("/_plugins/_knn/stats/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(404)
	})
	mux.HandleFunc("/_plugins/_knn/stats", func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "GET", r.Method)
		jsonResponse(w, 200, `{"_nodes":{"total":1},"cluster_name":"test","graph_memory_usage":1024,"cache_capacity_reached":false}`)
	})

	ts := httptest.NewServer(mux)
	defer ts.Close()

	svc := NewKnnService(restyClient(ts), testLogger())
	ctx := context.Background()

	t.Run("all stats", func(t *testing.T) {
		resp, err := svc.KnnStats(ctx, "")
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Contains(t, *resp, "cluster_name")
	})

	t.Run("specific stat", func(t *testing.T) {
		resp, err := svc.KnnStats(ctx, "graph_memory_usage")
		require.NoError(t, err)
		require.NotNil(t, resp)
	})
}

func TestUnitKnnService_KnnStatsErrors(t *testing.T) {
	ctx := context.Background()

	t.Run("server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		svc := NewKnnService(restyClient(srv), testLogger())
		_, err := svc.KnnStats(ctx, "")
		require.Error(t, err)
	})

	t.Run("unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		svc := NewKnnService(restyClient(srv), testLogger())
		_, err := svc.KnnStats(ctx, "")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("network error", func(t *testing.T) {
		svc := NewKnnService(deadClient(), testLogger())
		_, err := svc.KnnStats(ctx, "")
		require.Error(t, err)
	})
}

func TestUnitKnnService_KnnWarmup(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "GET", r.Method)
		assert.Equal(t, "/_plugins/_knn/warmup/test-index", r.URL.Path)
		jsonResponse(w, 200, `{"_shards":{"total":2,"successful":2,"failed":0}}`)
	}))
	defer ts.Close()

	svc := NewKnnService(restyClient(ts), testLogger())
	ctx := context.Background()

	resp, err := svc.KnnWarmup(ctx, "test-index")
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.NotNil(t, resp.Shards)
	assert.Equal(t, 2, resp.Shards.Total)
	assert.Equal(t, 2, resp.Shards.Successful)
}

func TestUnitKnnService_KnnWarmupErrors(t *testing.T) {
	ctx := context.Background()

	t.Run("validation error", func(t *testing.T) {
		svc := NewKnnService(deadClient(), testLogger())
		_, err := svc.KnnWarmup(ctx, "")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "index is required")
	})

	t.Run("server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		svc := NewKnnService(restyClient(srv), testLogger())
		_, err := svc.KnnWarmup(ctx, "test-index")
		require.Error(t, err)
	})

	t.Run("unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		svc := NewKnnService(restyClient(srv), testLogger())
		_, err := svc.KnnWarmup(ctx, "test-index")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("network error", func(t *testing.T) {
		svc := NewKnnService(deadClient(), testLogger())
		_, err := svc.KnnWarmup(ctx, "test-index")
		require.Error(t, err)
	})
}

func TestUnitKnnService_ClearCache(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "POST", r.Method)
		assert.Equal(t, "/_plugins/_knn/clear_cache/test-index", r.URL.Path)
		jsonResponse(w, 200, `{"acknowledged":true}`)
	}))
	defer ts.Close()

	svc := NewKnnService(restyClient(ts), testLogger())
	ctx := context.Background()

	resp, err := svc.ClearCache(ctx, "test-index")
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.True(t, resp.Acknowledged)
}

func TestUnitKnnService_ClearCacheErrors(t *testing.T) {
	ctx := context.Background()

	t.Run("validation error", func(t *testing.T) {
		svc := NewKnnService(deadClient(), testLogger())
		_, err := svc.ClearCache(ctx, "")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "index is required")
	})

	t.Run("server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		svc := NewKnnService(restyClient(srv), testLogger())
		_, err := svc.ClearCache(ctx, "test-index")
		require.Error(t, err)
	})

	t.Run("unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		svc := NewKnnService(restyClient(srv), testLogger())
		_, err := svc.ClearCache(ctx, "test-index")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("network error", func(t *testing.T) {
		svc := NewKnnService(deadClient(), testLogger())
		_, err := svc.ClearCache(ctx, "test-index")
		require.Error(t, err)
	})
}

func TestUnitKnnService_TrainModel(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "POST", r.Method)
		assert.Equal(t, "/_plugins/_knn/models/_train", r.URL.Path)
		jsonResponse(w, 200, `{"model_id":"model-123","status":"training"}`)
	}))
	defer ts.Close()

	svc := NewKnnService(restyClient(ts), testLogger())
	ctx := context.Background()

	resp, err := svc.TrainModel(ctx, map[string]any{
		"training_index":            "train-index",
		"training_field":            "vector_field",
		"dimension":                 2,
		"max_training_vector_count": 1200,
		"search_size":               50,
		"method":                    map[string]any{"name": "hnsw", "engine": "nmslib", "space_type": "l2"},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "model-123", (*resp)["model_id"])
	assert.Equal(t, "training", (*resp)["status"])
}

func TestUnitKnnService_TrainModelErrors(t *testing.T) {
	ctx := context.Background()

	t.Run("validation error", func(t *testing.T) {
		svc := NewKnnService(deadClient(), testLogger())
		_, err := svc.TrainModel(ctx, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "body is required")
	})

	t.Run("server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		svc := NewKnnService(restyClient(srv), testLogger())
		_, err := svc.TrainModel(ctx, map[string]any{"training_index": "idx"})
		require.Error(t, err)
	})

	t.Run("unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		svc := NewKnnService(restyClient(srv), testLogger())
		_, err := svc.TrainModel(ctx, map[string]any{"training_index": "idx"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("network error", func(t *testing.T) {
		svc := NewKnnService(deadClient(), testLogger())
		_, err := svc.TrainModel(ctx, map[string]any{"training_index": "idx"})
		require.Error(t, err)
	})
}

func TestUnitKnnService_GetModel(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "GET", r.Method)
		assert.Equal(t, "/_plugins/_knn/models/model-123", r.URL.Path)
		jsonResponse(w, 200, `{"model_id":"model-123","state":"created","dimension":2,"method":{"name":"hnsw"}}`)
	}))
	defer ts.Close()

	svc := NewKnnService(restyClient(ts), testLogger())
	ctx := context.Background()

	resp, err := svc.GetModel(ctx, "model-123")
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "model-123", (*resp)["model_id"])
	assert.Equal(t, "created", (*resp)["state"])
}

func TestUnitKnnService_GetModelErrors(t *testing.T) {
	ctx := context.Background()

	t.Run("validation error", func(t *testing.T) {
		svc := NewKnnService(deadClient(), testLogger())
		_, err := svc.GetModel(ctx, "")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "model id is required")
	})

	t.Run("server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		svc := NewKnnService(restyClient(srv), testLogger())
		_, err := svc.GetModel(ctx, "model-123")
		require.Error(t, err)
	})

	t.Run("unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		svc := NewKnnService(restyClient(srv), testLogger())
		_, err := svc.GetModel(ctx, "model-123")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("network error", func(t *testing.T) {
		svc := NewKnnService(deadClient(), testLogger())
		_, err := svc.GetModel(ctx, "model-123")
		require.Error(t, err)
	})
}

func TestUnitKnnService_SearchModels(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "POST", r.Method)
		assert.Equal(t, "/_plugins/_knn/models/_search", r.URL.Path)
		jsonResponse(w, 200, `{"took":5,"timed_out":false,"hits":{"total":{"value":1},"hits":[{"_id":"model-123","_source":{"model_id":"model-123","state":"created"}}]}}`)
	}))
	defer ts.Close()

	svc := NewKnnService(restyClient(ts), testLogger())
	ctx := context.Background()

	resp, err := svc.SearchModels(ctx, map[string]any{"query": map[string]any{"match_all": map[string]any{}}})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Contains(t, *resp, "hits")
}

func TestUnitKnnService_SearchModelsErrors(t *testing.T) {
	ctx := context.Background()

	t.Run("validation error", func(t *testing.T) {
		svc := NewKnnService(deadClient(), testLogger())
		_, err := svc.SearchModels(ctx, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "body is required")
	})

	t.Run("server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		svc := NewKnnService(restyClient(srv), testLogger())
		_, err := svc.SearchModels(ctx, map[string]any{"query": map[string]any{}})
		require.Error(t, err)
	})

	t.Run("unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		svc := NewKnnService(restyClient(srv), testLogger())
		_, err := svc.SearchModels(ctx, map[string]any{"query": map[string]any{}})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("network error", func(t *testing.T) {
		svc := NewKnnService(deadClient(), testLogger())
		_, err := svc.SearchModels(ctx, map[string]any{"query": map[string]any{}})
		require.Error(t, err)
	})
}

func TestUnitKnnService_DeleteModel(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "DELETE", r.Method)
		assert.Equal(t, "/_plugins/_knn/models/model-123", r.URL.Path)
		jsonResponse(w, 200, `{"acknowledged":true}`)
	}))
	defer ts.Close()

	svc := NewKnnService(restyClient(ts), testLogger())
	ctx := context.Background()

	resp, err := svc.DeleteModel(ctx, "model-123")
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.True(t, resp.Acknowledged)
}

func TestUnitKnnService_DeleteModelErrors(t *testing.T) {
	ctx := context.Background()

	t.Run("validation error", func(t *testing.T) {
		svc := NewKnnService(deadClient(), testLogger())
		_, err := svc.DeleteModel(ctx, "")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "model id is required")
	})

	t.Run("server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		svc := NewKnnService(restyClient(srv), testLogger())
		_, err := svc.DeleteModel(ctx, "model-123")
		require.Error(t, err)
	})

	t.Run("unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		svc := NewKnnService(restyClient(srv), testLogger())
		_, err := svc.DeleteModel(ctx, "model-123")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("network error", func(t *testing.T) {
		svc := NewKnnService(deadClient(), testLogger())
		_, err := svc.DeleteModel(ctx, "model-123")
		require.Error(t, err)
	})
}
