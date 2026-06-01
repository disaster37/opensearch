package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUnitInfoService_Info(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "GET", r.Method)
		assert.Equal(t, "/", r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"name":"node-1","cluster_name":"test-cluster","cluster_uuid":"abc123","version":{"distribution":"opensearch","number":"2.11.0","build_type":"tar","build_hash":"abc","build_date":"2023-10-01","build_snapshot":false,"lucene_version":"9.7.0","minimum_wire_compatibility_version":"7.10.0","minimum_index_compatibility_version":"7.0.0"},"tagline":"The OpenSearch Project: https://opensearch.org/"}`))
	}))
	defer ts.Close()

	svc := NewInfoService(restyClient(ts), testLogger())
	ctx := context.Background()

	resp, err := svc.Info(ctx)
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "node-1", resp.Name)
	assert.Equal(t, "test-cluster", resp.ClusterName)
	assert.Equal(t, "abc123", resp.ClusterUUID)
	require.NotNil(t, resp.Version)
	assert.Equal(t, "opensearch", resp.Version.Distribution)
	assert.Equal(t, "2.11.0", resp.Version.Number)
	assert.Equal(t, "9.7.0", resp.Version.LuceneVersion)
}

func TestUnitInfoService_Ping(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "HEAD", r.Method)
		assert.Equal(t, "/", r.URL.Path)
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	svc := NewInfoService(restyClient(ts), testLogger())
	ctx := context.Background()

	ok, err := svc.Ping(ctx)
	require.NoError(t, err)
	assert.True(t, ok)
}

func TestUnitInfoService_PingNotOK(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer ts.Close()

	svc := NewInfoService(restyClient(ts), testLogger())
	ctx := context.Background()

	ok, err := svc.Ping(ctx)
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestUnitInfoService_InfoErrors(t *testing.T) {
	ctx := context.Background()

	t.Run("server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewInfoService(restyClient(srv), testLogger())
		_, err := s.Info(ctx)
		require.Error(t, err)
	})

	t.Run("unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewInfoService(restyClient(srv), testLogger())
		_, err := s.Info(ctx)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("network error", func(t *testing.T) {
		s := NewInfoService(deadClient(), testLogger())
		_, err := s.Info(ctx)
		require.Error(t, err)
	})
}

func TestUnitInfoService_PingErrors(t *testing.T) {
	ctx := context.Background()

	t.Run("network error", func(t *testing.T) {
		s := NewInfoService(deadClient(), testLogger())
		ok, err := s.Ping(ctx)
		require.Error(t, err)
		assert.False(t, ok)
	})
}
