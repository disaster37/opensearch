// async_search_service_test.go contains unit tests for DefaultAsyncSearchService.
// Tests use httptest.NewServer to mock OpenSearch asynchronous search endpoints.
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

func TestUnitAsyncSearchService_Submit(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "POST", r.Method)
		assert.Equal(t, "/_plugins/_asynchronous_search", r.URL.Path)
		jsonResponse(w, 200, `{"id":"abc123","state":"RUNNING","start_time_in_millis":1700000000000,"expiration_time":"2024-01-01T00:00:00Z","response":{"hits":{"total":{"value":10}}}}`)
	}))
	defer ts.Close()

	svc := NewAsyncSearchService(restyClient(ts), testLogger())
	ctx := context.Background()

	resp, err := svc.Submit(ctx, map[string]any{"query": map[string]any{"match_all": map[string]any{}}}, map[string]string{"wait_for_completion_timeout": "1s"})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "abc123", resp.Id)
	assert.Equal(t, "RUNNING", resp.State)
	assert.Equal(t, int64(1700000000000), resp.StartInMillis)
}

func TestUnitAsyncSearchService_SubmitErrors(t *testing.T) {
	ctx := context.Background()

	t.Run("validation error", func(t *testing.T) {
		svc := NewAsyncSearchService(deadClient(), testLogger())
		_, err := svc.Submit(ctx, nil, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "body is required")
	})

	t.Run("server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		svc := NewAsyncSearchService(restyClient(srv), testLogger())
		_, err := svc.Submit(ctx, map[string]any{"query": map[string]any{}}, nil)
		require.Error(t, err)
	})

	t.Run("unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		svc := NewAsyncSearchService(restyClient(srv), testLogger())
		_, err := svc.Submit(ctx, map[string]any{"query": map[string]any{}}, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("network error", func(t *testing.T) {
		svc := NewAsyncSearchService(deadClient(), testLogger())
		_, err := svc.Submit(ctx, map[string]any{"query": map[string]any{}}, nil)
		require.Error(t, err)
	})
}

func TestUnitAsyncSearchService_AsyncGet(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "GET", r.Method)
		assert.Equal(t, "/_plugins/_asynchronous_search/abc123", r.URL.Path)
		jsonResponse(w, 200, `{"id":"abc123","state":"COMPLETE","start_time_in_millis":1700000000000,"expiration_time":"2024-01-01T00:00:00Z","response":{"hits":{"total":{"value":10}}}}`)
	}))
	defer ts.Close()

	svc := NewAsyncSearchService(restyClient(ts), testLogger())
	ctx := context.Background()

	resp, err := svc.AsyncGet(ctx, "abc123")
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "abc123", resp.Id)
	assert.Equal(t, "COMPLETE", resp.State)
}

func TestUnitAsyncSearchService_AsyncGetErrors(t *testing.T) {
	ctx := context.Background()

	t.Run("validation error", func(t *testing.T) {
		svc := NewAsyncSearchService(deadClient(), testLogger())
		_, err := svc.AsyncGet(ctx, "")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "id is required")
	})

	t.Run("server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		svc := NewAsyncSearchService(restyClient(srv), testLogger())
		_, err := svc.AsyncGet(ctx, "abc123")
		require.Error(t, err)
	})

	t.Run("unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		svc := NewAsyncSearchService(restyClient(srv), testLogger())
		_, err := svc.AsyncGet(ctx, "abc123")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("network error", func(t *testing.T) {
		svc := NewAsyncSearchService(deadClient(), testLogger())
		_, err := svc.AsyncGet(ctx, "abc123")
		require.Error(t, err)
	})
}

func TestUnitAsyncSearchService_AsyncDelete(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "DELETE", r.Method)
		assert.Equal(t, "/_plugins/_asynchronous_search/abc123", r.URL.Path)
		jsonResponse(w, 200, `{"acknowledged":true}`)
	}))
	defer ts.Close()

	svc := NewAsyncSearchService(restyClient(ts), testLogger())
	ctx := context.Background()

	resp, err := svc.AsyncDelete(ctx, "abc123")
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.True(t, resp.Acknowledged)
}

func TestUnitAsyncSearchService_AsyncDeleteErrors(t *testing.T) {
	ctx := context.Background()

	t.Run("validation error", func(t *testing.T) {
		svc := NewAsyncSearchService(deadClient(), testLogger())
		_, err := svc.AsyncDelete(ctx, "")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "id is required")
	})

	t.Run("server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		svc := NewAsyncSearchService(restyClient(srv), testLogger())
		_, err := svc.AsyncDelete(ctx, "abc123")
		require.Error(t, err)
	})

	t.Run("unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		svc := NewAsyncSearchService(restyClient(srv), testLogger())
		_, err := svc.AsyncDelete(ctx, "abc123")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("network error", func(t *testing.T) {
		svc := NewAsyncSearchService(deadClient(), testLogger())
		_, err := svc.AsyncDelete(ctx, "abc123")
		require.Error(t, err)
	})
}

func TestUnitAsyncSearchService_AsyncStats(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "GET", r.Method)
		assert.Equal(t, "/_plugins/_asynchronous_search/stats", r.URL.Path)
		jsonResponse(w, 200, `{"nodes":{"node1":{"submitted_queries":100,"completed_queries":95}}}`)
	}))
	defer ts.Close()

	svc := NewAsyncSearchService(restyClient(ts), testLogger())
	ctx := context.Background()

	resp, err := svc.AsyncStats(ctx)
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Contains(t, *resp, "nodes")
}

func TestUnitAsyncSearchService_AsyncStatsErrors(t *testing.T) {
	ctx := context.Background()

	t.Run("server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		svc := NewAsyncSearchService(restyClient(srv), testLogger())
		_, err := svc.AsyncStats(ctx)
		require.Error(t, err)
	})

	t.Run("unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		svc := NewAsyncSearchService(restyClient(srv), testLogger())
		_, err := svc.AsyncStats(ctx)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("network error", func(t *testing.T) {
		svc := NewAsyncSearchService(deadClient(), testLogger())
		_, err := svc.AsyncStats(ctx)
		require.Error(t, err)
	})
}
