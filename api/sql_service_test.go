// sql_service_test.go contains unit tests for DefaultSqlService.
// Tests use httptest.NewServer to mock OpenSearch SQL/PPL plugin endpoints.
// Each TestUnit* function covers one service method with success subtests,
// validation error subtests, server error subtests, bad JSON subtests,
// and network error subtests.
package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newSQLTestServer() *httptest.Server {
	mux := http.NewServeMux()

	mux.HandleFunc("/_plugins/_ppl", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"schema":[{"name":"host","type":"string"}],"datarows":[["host1"]],"total":1,"size":1,"status":200}`))
	})

	mux.HandleFunc("/_plugins/_ppl/_explain", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"root":{"name":"ProjectOperator","description":["host"],"children":[{"name":"OpenSearchIndexScan","description":["host"]}]}}`))
	})

	mux.HandleFunc("/_plugins/_sql", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"schema":[{"name":"host","type":"string"}],"datarows":[["host1"]],"total":1,"size":1,"status":200,"cursor":"abc123"}`))
	})

	mux.HandleFunc("/_plugins/_sql/_explain", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"root":{"name":"ProjectOperator","description":["host"],"children":[{"name":"OpenSearchIndexScan","description":["host"]}]}}`))
	})

	mux.HandleFunc("/_plugins/_sql/close", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"success":true}`))
	})

	return httptest.NewServer(mux)
}

// --- PPLQuery ---

func TestUnitSqlService_PPLQuery(t *testing.T) {
	srv := newSQLTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewSqlService(restyClient(srv), testLogger())

	resp, err := svc.PPLQuery(ctx, map[string]any{"query": "source=index | fields host"})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, int64(1), resp.Total)
	assert.Len(t, resp.Datapoints, 1)
}

func TestUnitSqlService_PPLQuery_NilBody(t *testing.T) {
	ctx := context.Background()
	svc := NewSqlService(deadClient(), testLogger())

	_, err := svc.PPLQuery(ctx, nil)
	require.Error(t, err)
}

func TestUnitSqlService_PPLQuery_ServerError(t *testing.T) {
	srv := errServer(500)
	defer srv.Close()
	ctx := context.Background()
	svc := NewSqlService(restyClient(srv), testLogger())

	_, err := svc.PPLQuery(ctx, map[string]any{"query": "test"})
	require.Error(t, err)
}

func TestUnitSqlService_PPLQuery_BadJSON(t *testing.T) {
	srv := badJSONServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewSqlService(restyClient(srv), testLogger())

	_, err := svc.PPLQuery(ctx, map[string]any{"query": "test"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}

func TestUnitSqlService_PPLQuery_NetworkError(t *testing.T) {
	ctx := context.Background()
	svc := NewSqlService(deadClient(), testLogger())

	_, err := svc.PPLQuery(ctx, map[string]any{"query": "test"})
	require.Error(t, err)
}

// --- PPLExplain ---

func TestUnitSqlService_PPLExplain(t *testing.T) {
	srv := newSQLTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewSqlService(restyClient(srv), testLogger())

	resp, err := svc.PPLExplain(ctx, map[string]any{"query": "source=index | fields host"})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.NotNil(t, resp.Root)
	assert.Equal(t, "ProjectOperator", resp.Root["name"])
}

func TestUnitSqlService_PPLExplain_NilBody(t *testing.T) {
	ctx := context.Background()
	svc := NewSqlService(deadClient(), testLogger())

	_, err := svc.PPLExplain(ctx, nil)
	require.Error(t, err)
}

func TestUnitSqlService_PPLExplain_ServerError(t *testing.T) {
	srv := errServer(500)
	defer srv.Close()
	ctx := context.Background()
	svc := NewSqlService(restyClient(srv), testLogger())

	_, err := svc.PPLExplain(ctx, map[string]any{"query": "test"})
	require.Error(t, err)
}

func TestUnitSqlService_PPLExplain_BadJSON(t *testing.T) {
	srv := badJSONServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewSqlService(restyClient(srv), testLogger())

	_, err := svc.PPLExplain(ctx, map[string]any{"query": "test"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}

func TestUnitSqlService_PPLExplain_NetworkError(t *testing.T) {
	ctx := context.Background()
	svc := NewSqlService(deadClient(), testLogger())

	_, err := svc.PPLExplain(ctx, map[string]any{"query": "test"})
	require.Error(t, err)
}

// --- SQLQuery ---

func TestUnitSqlService_SQLQuery(t *testing.T) {
	srv := newSQLTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewSqlService(restyClient(srv), testLogger())

	resp, err := svc.SQLQuery(ctx, map[string]any{"query": "SELECT host FROM index"}, "")
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, int64(1), resp.Total)
	require.NotNil(t, resp.Cursor)
	assert.Equal(t, "abc123", *resp.Cursor)
}

func TestUnitSqlService_SQLQueryWithFormat(t *testing.T) {
	srv := newSQLTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewSqlService(restyClient(srv), testLogger())

	resp, err := svc.SQLQuery(ctx, map[string]any{"query": "SELECT host FROM index"}, "json")
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, int64(1), resp.Total)
}

func TestUnitSqlService_SQLQuery_NilBody(t *testing.T) {
	ctx := context.Background()
	svc := NewSqlService(deadClient(), testLogger())

	_, err := svc.SQLQuery(ctx, nil, "")
	require.Error(t, err)
}

func TestUnitSqlService_SQLQuery_ServerError(t *testing.T) {
	srv := errServer(500)
	defer srv.Close()
	ctx := context.Background()
	svc := NewSqlService(restyClient(srv), testLogger())

	_, err := svc.SQLQuery(ctx, map[string]any{"query": "test"}, "")
	require.Error(t, err)
}

func TestUnitSqlService_SQLQuery_BadJSON(t *testing.T) {
	srv := badJSONServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewSqlService(restyClient(srv), testLogger())

	_, err := svc.SQLQuery(ctx, map[string]any{"query": "test"}, "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}

func TestUnitSqlService_SQLQuery_NetworkError(t *testing.T) {
	ctx := context.Background()
	svc := NewSqlService(deadClient(), testLogger())

	_, err := svc.SQLQuery(ctx, map[string]any{"query": "test"}, "")
	require.Error(t, err)
}

// --- SQLExplain ---

func TestUnitSqlService_SQLExplain(t *testing.T) {
	srv := newSQLTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewSqlService(restyClient(srv), testLogger())

	resp, err := svc.SQLExplain(ctx, map[string]any{"query": "SELECT host FROM index"})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.NotNil(t, resp.Root)
	assert.Equal(t, "ProjectOperator", resp.Root["name"])
}

func TestUnitSqlService_SQLExplain_NilBody(t *testing.T) {
	ctx := context.Background()
	svc := NewSqlService(deadClient(), testLogger())

	_, err := svc.SQLExplain(ctx, nil)
	require.Error(t, err)
}

func TestUnitSqlService_SQLExplain_ServerError(t *testing.T) {
	srv := errServer(500)
	defer srv.Close()
	ctx := context.Background()
	svc := NewSqlService(restyClient(srv), testLogger())

	_, err := svc.SQLExplain(ctx, map[string]any{"query": "test"})
	require.Error(t, err)
}

func TestUnitSqlService_SQLExplain_BadJSON(t *testing.T) {
	srv := badJSONServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewSqlService(restyClient(srv), testLogger())

	_, err := svc.SQLExplain(ctx, map[string]any{"query": "test"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}

func TestUnitSqlService_SQLExplain_NetworkError(t *testing.T) {
	ctx := context.Background()
	svc := NewSqlService(deadClient(), testLogger())

	_, err := svc.SQLExplain(ctx, map[string]any{"query": "test"})
	require.Error(t, err)
}

// --- SQLCloseCursor ---

func TestUnitSqlService_SQLCloseCursor(t *testing.T) {
	srv := newSQLTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewSqlService(restyClient(srv), testLogger())

	resp, err := svc.SQLCloseCursor(ctx, map[string]any{"cursor": "abc123"})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.True(t, resp.Success)
}

func TestUnitSqlService_SQLCloseCursor_NilBody(t *testing.T) {
	ctx := context.Background()
	svc := NewSqlService(deadClient(), testLogger())

	_, err := svc.SQLCloseCursor(ctx, nil)
	require.Error(t, err)
}

func TestUnitSqlService_SQLCloseCursor_ServerError(t *testing.T) {
	srv := errServer(500)
	defer srv.Close()
	ctx := context.Background()
	svc := NewSqlService(restyClient(srv), testLogger())

	_, err := svc.SQLCloseCursor(ctx, map[string]any{"cursor": "abc"})
	require.Error(t, err)
}

func TestUnitSqlService_SQLCloseCursor_BadJSON(t *testing.T) {
	srv := badJSONServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewSqlService(restyClient(srv), testLogger())

	_, err := svc.SQLCloseCursor(ctx, map[string]any{"cursor": "abc"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}

func TestUnitSqlService_SQLCloseCursor_NetworkError(t *testing.T) {
	ctx := context.Background()
	svc := NewSqlService(deadClient(), testLogger())

	_, err := svc.SQLCloseCursor(ctx, map[string]any{"cursor": "abc"})
	require.Error(t, err)
}
