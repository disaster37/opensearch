package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/disaster37/opensearch/v4/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTransformTestServer() *httptest.Server {
	mux := http.NewServeMux()

	mux.HandleFunc("/_plugins/_transform/test_job/_explain", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"test_job":{"metadata_id":"meta1","transform_id":"test_job","last_updated_at":1000,"status":"started","failure_reason":"","stats":{"pages_processed":10,"documents_processed":100,"documents_indexed":50,"index_time_in_millis":500,"search_time_in_millis":200}}}`))
	})

	mux.HandleFunc("/_plugins/_transform/test_job/_start", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"acknowledged":true}`))
	})

	mux.HandleFunc("/_plugins/_transform/test_job/_stop", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"acknowledged":true}`))
	})

	mux.HandleFunc("/_plugins/_transform/test_job", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodGet:
			_, _ = w.Write([]byte(`{"_id":"test","_version":1,"_seq_no":0,"_primary_term":1,"transform":{"source_index":"source","target_index":"target"}}`))
		case http.MethodPut:
			_, _ = w.Write([]byte(`{"_id":"test","_version":2,"_seq_no":1,"_primary_term":1,"transform":{"source_index":"source","target_index":"target"}}`))
		case http.MethodDelete:
			_, _ = w.Write([]byte(`{"took":10,"errors":false,"items":[]}`))
		}
	})

	mux.HandleFunc("/_plugins/_transform/_preview", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"documents":[{"key":"value"}]}`))
	})

	mux.HandleFunc("/_plugins/_transform/_search", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"total_transforms":1,"transforms":[{"_id":"test","_version":1,"_seq_no":0,"_primary_term":1,"transform":{"source_index":"source","target_index":"target"}}]}`))
	})

	return httptest.NewServer(mux)
}

func TestUnitTransformService_GetJob(t *testing.T) {
	srv := newTransformTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewTransformService(restyClient(srv), testLogger())

	resp, err := svc.GetJob(ctx, "test_job")
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "test", resp.Id)
	assert.Equal(t, "source", resp.Transform.SourceIndex)
}

func TestUnitTransformService_PutJob(t *testing.T) {
	srv := newTransformTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewTransformService(restyClient(srv), testLogger())

	desc := "test job"
	resp, err := svc.PutJob(ctx, &TransformPutJobRequest{
		JobName: "test_job",
		Body: &TransformJobBase{
			SourceIndex: "source",
			TargetIndex: "target",
			Description: &desc,
			Schedule:    map[string]any{"interval": map[string]any{"period": 1, "unit": "Minutes"}},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, int64(2), resp.Version)
}

func TestUnitTransformService_PutJobWithVersion(t *testing.T) {
	srv := newTransformTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewTransformService(restyClient(srv), testLogger())

	seqNo := int64(0)
	primaryTerm := int64(1)
	resp, err := svc.PutJob(ctx, &TransformPutJobRequest{
		JobName: "test_job",
		Body: &TransformJobBase{
			SourceIndex: "source",
			TargetIndex: "target",
		},
		Version: &types.DocumentVersion{
			SeqNo:       &seqNo,
			PrimaryTerm: &primaryTerm,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
}

func TestUnitTransformService_DeleteJob(t *testing.T) {
	srv := newTransformTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewTransformService(restyClient(srv), testLogger())

	resp, err := svc.DeleteJob(ctx, "test_job")
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, int64(10), resp.Took)
	assert.False(t, resp.Errors)
}

func TestUnitTransformService_SearchJob(t *testing.T) {
	srv := newTransformTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewTransformService(restyClient(srv), testLogger())

	resp, err := svc.SearchJob(ctx, map[string]any{
		"query": map[string]any{"match_all": map[string]any{}},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, int64(1), resp.TotalTransforms)
	assert.Len(t, resp.Transforms, 1)
}

func TestUnitTransformService_ExplainJob(t *testing.T) {
	srv := newTransformTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewTransformService(restyClient(srv), testLogger())

	result, err := svc.ExplainJob(ctx, "test_job")
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Contains(t, result, "test_job")
	assert.Equal(t, "started", result["test_job"].Status)
	assert.Equal(t, int64(10), result["test_job"].Stats.PagesProcessed)
}

func TestUnitTransformService_PreviewJobResults(t *testing.T) {
	srv := newTransformTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewTransformService(restyClient(srv), testLogger())

	resp, err := svc.PreviewJobResults(ctx, map[string]any{
		"transform": map[string]any{"source_index": "source"},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Len(t, resp.Documents, 1)
}

func TestUnitTransformService_StartJob(t *testing.T) {
	srv := newTransformTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewTransformService(restyClient(srv), testLogger())

	resp, err := svc.StartJob(ctx, "test_job")
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.True(t, resp.Acknowledged)
}

func TestUnitTransformService_StopJob(t *testing.T) {
	srv := newTransformTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewTransformService(restyClient(srv), testLogger())

	resp, err := svc.StopJob(ctx, "test_job")
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.True(t, resp.Acknowledged)
}

func TestUnitTransformService_EmptyJobNameErrors(t *testing.T) {
	srv := newTransformTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewTransformService(restyClient(srv), testLogger())

	_, err := svc.GetJob(ctx, "")
	assert.Error(t, err)

	_, err = svc.DeleteJob(ctx, "")
	assert.Error(t, err)

	_, err = svc.ExplainJob(ctx, "")
	assert.Error(t, err)

	_, err = svc.StartJob(ctx, "")
	assert.Error(t, err)

	_, err = svc.StopJob(ctx, "")
	assert.Error(t, err)
}

func TestUnitTransformService_PutJobValidationError(t *testing.T) {
	ctx := context.Background()
	svc := NewTransformService(deadClient(), testLogger())

	_, err := svc.PutJob(ctx, &TransformPutJobRequest{})
	assert.Error(t, err)
}
