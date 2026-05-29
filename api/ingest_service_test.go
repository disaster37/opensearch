package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUnitIngestService_PutPipeline(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "PUT", r.Method)
		assert.Equal(t, "/_ingest/pipeline/my-pipeline", r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"acknowledged":true}`))
	}))
	defer ts.Close()

	svc := NewIngestService(restyClient(ts), testLogger())
	ctx := context.Background()

	resp, err := svc.PutPipeline(ctx, &IngestPutPipelineRequest{
		Id: "my-pipeline",
		Body: map[string]any{
			"description": "test pipeline",
			"processors":  []any{},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.True(t, resp.Acknowledged)
}

func TestUnitIngestService_GetPipeline(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/_ingest/pipeline/my-pipeline", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"my-pipeline":{"description":"test","processors":[{"set":{"field":"foo","value":"bar"}}]}}`))
	})
	mux.HandleFunc("/_ingest/pipeline", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"my-pipeline":{"description":"test","processors":[{"set":{"field":"foo","value":"bar"}}]},"other":{"description":"other","processors":[]}}`))
	})

	ts := httptest.NewServer(mux)
	defer ts.Close()

	svc := NewIngestService(restyClient(ts), testLogger())
	ctx := context.Background()

	t.Run("specific pipeline", func(t *testing.T) {
		resp, err := svc.GetPipeline(ctx, []string{"my-pipeline"})
		require.NoError(t, err)
		require.Contains(t, resp, "my-pipeline")
		assert.Equal(t, "test", resp["my-pipeline"].Description)
	})

	t.Run("all pipelines", func(t *testing.T) {
		resp, err := svc.GetPipeline(ctx, nil)
		require.NoError(t, err)
		assert.Len(t, resp, 2)
	})
}

func TestUnitIngestService_DeletePipeline(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "DELETE", r.Method)
		assert.Equal(t, "/_ingest/pipeline/my-pipeline", r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"acknowledged":true}`))
	}))
	defer ts.Close()

	svc := NewIngestService(restyClient(ts), testLogger())
	ctx := context.Background()

	resp, err := svc.DeletePipeline(ctx, "my-pipeline")
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.True(t, resp.Acknowledged)
}

func TestUnitIngestService_SimulatePipeline(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/_ingest/pipeline/my-pipeline/_simulate", func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "POST", r.Method)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"docs":[{"doc":{"_index":"test","_id":"1","_source":{"foo":"bar"}},"processor_results":[{"tag":"set-foo","doc":{"_index":"test","_id":"1","_source":{"foo":"bar"}}}]}]}`))
	})
	mux.HandleFunc("/_ingest/pipeline/_simulate", func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "POST", r.Method)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"docs":[{"doc":{"_index":"test","_id":"1","_source":{"foo":"bar"}},"processor_results":[]}]}`))
	})

	ts := httptest.NewServer(mux)
	defer ts.Close()

	svc := NewIngestService(restyClient(ts), testLogger())
	ctx := context.Background()

	t.Run("with pipeline id", func(t *testing.T) {
		resp, err := svc.SimulatePipeline(ctx, &IngestSimulatePipelineRequest{
			Id: "my-pipeline",
			Body: map[string]any{
				"docs": []any{
					map[string]any{"_index": "test", "_id": "1", "_source": map[string]any{"foo": "bar"}},
				},
			},
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Len(t, resp.Docs, 1)
	})

	t.Run("without pipeline id", func(t *testing.T) {
		resp, err := svc.SimulatePipeline(ctx, &IngestSimulatePipelineRequest{
			Body: map[string]any{
				"pipeline": map[string]any{
					"processors": []any{},
				},
				"docs": []any{
					map[string]any{"_index": "test", "_id": "1", "_source": map[string]any{"foo": "bar"}},
				},
			},
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Len(t, resp.Docs, 1)
	})
}

func TestUnitIngestService_ProcessorGrok(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "GET", r.Method)
		assert.Equal(t, "/_ingest/processor/grok", r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"patterns":{"BAC":"BAC\\|(?<bac_device>.*?)$","CISCOFW":"(?:\\w+)\\s+(?<cisco_action>\\w+)"}}`))
	}))
	defer ts.Close()

	svc := NewIngestService(restyClient(ts), testLogger())
	ctx := context.Background()

	resp, err := svc.ProcessorGrok(ctx)
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Contains(t, resp, "BAC")
	assert.Equal(t, []string{"BAC\\|(?<bac_device>.*?)$"}, resp["BAC"])
	assert.Contains(t, resp, "CISCOFW")
}

func TestUnitIngestService_ProcessorGrokErrors(t *testing.T) {
	ctx := context.Background()

	t.Run("server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewIngestService(restyClient(srv), testLogger())
		_, err := s.ProcessorGrok(ctx)
		require.Error(t, err)
	})

	t.Run("unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewIngestService(restyClient(srv), testLogger())
		_, err := s.ProcessorGrok(ctx)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("network error", func(t *testing.T) {
		s := NewIngestService(deadClient(), testLogger())
		_, err := s.ProcessorGrok(ctx)
		require.Error(t, err)
	})
}
