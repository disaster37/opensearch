package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newRollupTestServer() *httptest.Server {
	mux := http.NewServeMux()

	mux.HandleFunc("/_plugins/_rollup/jobs/test_rollup", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodGet:
			w.Write([]byte(`{"_id":"test","_version":1,"_seq_no":0,"_primary_term":1,"rollup":{"rollup_id":"test_rollup","source_index":"src","target_index":"tgt"}}`))
		case http.MethodPut:
			w.Write([]byte(`{"_id":"test","_version":2,"_seq_no":1,"_primary_term":1,"rollup":{"rollup_id":"test_rollup","source_index":"src","target_index":"tgt"}}`))
		case http.MethodDelete:
			w.Write([]byte(`{"acknowledged":true}`))
		}
	})

	mux.HandleFunc("/_plugins/_rollup/jobs/test_rollup/_start", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"acknowledged":true}`))
	})

	mux.HandleFunc("/_plugins/_rollup/jobs/test_rollup/_stop", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"acknowledged":true}`))
	})

	mux.HandleFunc("/_plugins/_rollup/jobs/test_rollup/_explain", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"test_rollup":{"metadata":{"status":"RUNNING"}}}`))
	})

	return httptest.NewServer(mux)
}

func TestUnitRollupService_GetRollup(t *testing.T) {
	srv := newRollupTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewRollupService(restyClient(srv), testLogger())

	resp, err := svc.GetRollup(ctx, "test_rollup")
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "test", resp.Id)
	assert.Equal(t, int64(1), resp.Version)
	assert.Equal(t, "test_rollup", resp.Rollup.RollupId)
	assert.Equal(t, "src", resp.Rollup.SourceIndex)
}

func TestUnitRollupService_GetRollupEmptyId(t *testing.T) {
	ctx := context.Background()
	svc := NewRollupService(deadClient(), testLogger())

	_, err := svc.GetRollup(ctx, "")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "rollup id is required")
}

func TestUnitRollupService_PutRollup(t *testing.T) {
	srv := newRollupTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewRollupService(restyClient(srv), testLogger())

	body := &RollupJobBase{
		RollupId:    "test_rollup",
		SourceIndex: "src",
		TargetIndex: "tgt",
	}
	resp, err := svc.PutRollup(ctx, "test_rollup", body)
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, int64(2), resp.Version)
	assert.Equal(t, "test_rollup", resp.Rollup.RollupId)
}

func TestUnitRollupService_PutRollupEmptyId(t *testing.T) {
	ctx := context.Background()
	svc := NewRollupService(deadClient(), testLogger())

	_, err := svc.PutRollup(ctx, "", map[string]any{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "rollup id is required")
}

func TestUnitRollupService_PutRollupNilBody(t *testing.T) {
	ctx := context.Background()
	svc := NewRollupService(deadClient(), testLogger())

	_, err := svc.PutRollup(ctx, "test_rollup", nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "body is required")
}

func TestUnitRollupService_DeleteRollup(t *testing.T) {
	srv := newRollupTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewRollupService(restyClient(srv), testLogger())

	resp, err := svc.DeleteRollup(ctx, "test_rollup")
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.True(t, resp.Acknowledged)
}

func TestUnitRollupService_DeleteRollupEmptyId(t *testing.T) {
	ctx := context.Background()
	svc := NewRollupService(deadClient(), testLogger())

	_, err := svc.DeleteRollup(ctx, "")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "rollup id is required")
}

func TestUnitRollupService_StartRollup(t *testing.T) {
	srv := newRollupTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewRollupService(restyClient(srv), testLogger())

	resp, err := svc.StartRollup(ctx, "test_rollup")
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.True(t, resp.Acknowledged)
}

func TestUnitRollupService_StartRollupEmptyId(t *testing.T) {
	ctx := context.Background()
	svc := NewRollupService(deadClient(), testLogger())

	_, err := svc.StartRollup(ctx, "")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "rollup id is required")
}

func TestUnitRollupService_StopRollup(t *testing.T) {
	srv := newRollupTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewRollupService(restyClient(srv), testLogger())

	resp, err := svc.StopRollup(ctx, "test_rollup")
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.True(t, resp.Acknowledged)
}

func TestUnitRollupService_StopRollupEmptyId(t *testing.T) {
	ctx := context.Background()
	svc := NewRollupService(deadClient(), testLogger())

	_, err := svc.StopRollup(ctx, "")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "rollup id is required")
}

func TestUnitRollupService_ExplainRollup(t *testing.T) {
	srv := newRollupTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewRollupService(restyClient(srv), testLogger())

	resp, err := svc.ExplainRollup(ctx, "test_rollup")
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.NotNil(t, resp["test_rollup"])
	assert.Equal(t, "RUNNING", resp["test_rollup"].Metadata["status"])
}

func TestUnitRollupService_ExplainRollupEmptyId(t *testing.T) {
	ctx := context.Background()
	svc := NewRollupService(deadClient(), testLogger())

	_, err := svc.ExplainRollup(ctx, "")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "rollup id is required")
}

// --- Error path tests for RollupService ---

func TestUnitRollupService_ErrorPaths(t *testing.T) {
	ctx := context.Background()

	services := []struct {
		name string
		call func(svc RollupService) error
	}{
		{"GetRollup", func(s RollupService) error { _, err := s.GetRollup(ctx, "r"); return err }},
		{"PutRollup", func(s RollupService) error {
			_, err := s.PutRollup(ctx, "r", &RollupJobBase{SourceIndex: "s", TargetIndex: "t"})
			return err
		}},
		{"DeleteRollup", func(s RollupService) error { _, err := s.DeleteRollup(ctx, "r"); return err }},
		{"StartRollup", func(s RollupService) error { _, err := s.StartRollup(ctx, "r"); return err }},
		{"StopRollup", func(s RollupService) error { _, err := s.StopRollup(ctx, "r"); return err }},
		{"ExplainRollup", func(s RollupService) error { _, err := s.ExplainRollup(ctx, "r"); return err }},
	}

	for _, tc := range services {
		t.Run(tc.name+" server error", func(t *testing.T) {
			srv := errServer(500)
			defer srv.Close()
			svc := NewRollupService(restyClient(srv), testLogger())
			require.Error(t, tc.call(svc))
		})

		t.Run(tc.name+" unmarshal error", func(t *testing.T) {
			srv := badJSONServer()
			defer srv.Close()
			svc := NewRollupService(restyClient(srv), testLogger())
			err := tc.call(svc)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "unmarshal")
		})

		t.Run(tc.name+" network error", func(t *testing.T) {
			svc := NewRollupService(deadClient(), testLogger())
			require.Error(t, tc.call(svc))
		})
	}
}
