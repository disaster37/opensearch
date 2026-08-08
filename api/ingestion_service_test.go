// ingestion_service_test.go contains unit tests for DefaultIngestionService.
// Tests use httptest.NewServer to mock the pull-based ingestion control
// endpoints (pause/resume/state), GA in OpenSearch 3.6.0.
package api

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUnitIngestionService_Pause(t *testing.T) {
	respJSON := `{"acknowledged":true,"shards_acknowledged":true}`

	var capturedPath, capturedMethod, capturedTimeout string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedPath = r.URL.Path
		capturedMethod = r.Method
		capturedTimeout = r.URL.Query().Get("timeout")
		w.WriteHeader(200)
		_, _ = fmt.Fprint(w, respJSON)
	}))
	defer srv.Close()

	svc := NewIngestionService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success with params", func(t *testing.T) {
		capturedTimeout = ""
		resp, err := svc.Pause(ctx, "my-index", &IngestionStateParams{Timeout: "30s"})
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.True(t, resp.Acknowledged)
		assert.True(t, resp.ShardsAcknowledged)
		assert.Equal(t, "/my-index/ingestion/_pause", capturedPath)
		assert.Equal(t, http.MethodPost, capturedMethod)
		assert.Equal(t, "30s", capturedTimeout)
	})

	t.Run("success without params", func(t *testing.T) {
		resp, err := svc.Pause(ctx, "my-index")
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("empty index validation error", func(t *testing.T) {
		_, err := svc.Pause(ctx, "")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "index is required")
	})
}

func TestUnitIngestionService_Resume(t *testing.T) {
	respJSON := `{"acknowledged":true,"shards_acknowledged":true,"failures":{"my-index":[{"shard":0,"error":"none"}]}}`

	var capturedPath, capturedMethod string
	var capturedBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedPath = r.URL.Path
		capturedMethod = r.Method
		capturedBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(200)
		_, _ = fmt.Fprint(w, respJSON)
	}))
	defer srv.Close()

	svc := NewIngestionService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success with reset settings", func(t *testing.T) {
		capturedBody = nil
		resp, err := svc.Resume(ctx, &IngestionResumeRequest{
			Index: "my-index",
			ResetSettings: []*IngestionResetSettings{
				{Shard: 0, Mode: IngestionResetModeOffset, Value: "100"},
			},
			Params: &IngestionStateParams{Timeout: "30s"},
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.True(t, resp.Acknowledged)
		require.Contains(t, resp.Failures, "my-index")
		assert.Equal(t, "/my-index/ingestion/_resume", capturedPath)
		assert.Equal(t, http.MethodPost, capturedMethod)
		require.NotEmpty(t, capturedBody)
	})

	t.Run("success without reset settings omits body", func(t *testing.T) {
		capturedBody = nil
		resp, err := svc.Resume(ctx, &IngestionResumeRequest{Index: "my-index"})
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Empty(t, capturedBody)
	})

	t.Run("empty index validation error", func(t *testing.T) {
		_, err := svc.Resume(ctx, &IngestionResumeRequest{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "validation")
	})
}

func TestUnitIngestionService_GetState(t *testing.T) {
	respJSON := `{"ingestion_state":{"my-index":[{"shard":0,"poller_state":"PAUSED","error_policy":"block","poller_paused":true,"write_block_enabled":false,"batch_start_pointer":"ptr-1"}]},"next_page_token":"token-2","_shards":{"total":1,"successful":1,"failed":0}}`

	var capturedPath, capturedMethod string
	var capturedSize, capturedNextToken string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedPath = r.URL.Path
		capturedMethod = r.Method
		capturedSize = r.URL.Query().Get("size")
		capturedNextToken = r.URL.Query().Get("next_token")
		w.WriteHeader(200)
		_, _ = fmt.Fprint(w, respJSON)
	}))
	defer srv.Close()

	svc := NewIngestionService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success with params", func(t *testing.T) {
		size := 10
		resp, err := svc.GetState(ctx, &IngestionGetStateRequest{
			Index: "my-index",
			Params: &IngestionGetStateParams{
				Size:      &size,
				NextToken: "token-1",
			},
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Contains(t, resp.IngestionState, "my-index")
		require.Len(t, resp.IngestionState["my-index"], 1)
		assert.Equal(t, "PAUSED", resp.IngestionState["my-index"][0].PollerState)
		assert.True(t, resp.IngestionState["my-index"][0].PollerPaused)
		assert.Equal(t, "token-2", resp.NextPageToken)
		require.NotNil(t, resp.Shards)
		assert.Equal(t, "/my-index/ingestion/_state", capturedPath)
		assert.Equal(t, http.MethodGet, capturedMethod)
		assert.Equal(t, "10", capturedSize)
		assert.Equal(t, "token-1", capturedNextToken)
	})

	t.Run("success without params", func(t *testing.T) {
		resp, err := svc.GetState(ctx, &IngestionGetStateRequest{Index: "my-index"})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("empty index validation error", func(t *testing.T) {
		_, err := svc.GetState(ctx, &IngestionGetStateRequest{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "validation")
	})
}

func TestUnitIngestionService_ErrorPaths(t *testing.T) {
	ctx := context.Background()

	t.Run("Pause server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewIngestionService(restyClient(srv), testLogger())
		_, err := s.Pause(ctx, "idx")
		require.Error(t, err)
	})

	t.Run("Pause unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewIngestionService(restyClient(srv), testLogger())
		_, err := s.Pause(ctx, "idx")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("Resume server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewIngestionService(restyClient(srv), testLogger())
		_, err := s.Resume(ctx, &IngestionResumeRequest{Index: "idx"})
		require.Error(t, err)
	})

	t.Run("Resume unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewIngestionService(restyClient(srv), testLogger())
		_, err := s.Resume(ctx, &IngestionResumeRequest{Index: "idx"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("GetState server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewIngestionService(restyClient(srv), testLogger())
		_, err := s.GetState(ctx, &IngestionGetStateRequest{Index: "idx"})
		require.Error(t, err)
	})

	t.Run("GetState unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewIngestionService(restyClient(srv), testLogger())
		_, err := s.GetState(ctx, &IngestionGetStateRequest{Index: "idx"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})
}

func TestUnitIngestionService_NetworkErrors(t *testing.T) {
	ctx := context.Background()
	s := NewIngestionService(deadClient(), testLogger())

	t.Run("Pause network error", func(t *testing.T) {
		_, err := s.Pause(ctx, "idx")
		require.Error(t, err)
	})
	t.Run("Resume network error", func(t *testing.T) {
		_, err := s.Resume(ctx, &IngestionResumeRequest{Index: "idx"})
		require.Error(t, err)
	})
	t.Run("GetState network error", func(t *testing.T) {
		_, err := s.GetState(ctx, &IngestionGetStateRequest{Index: "idx"})
		require.Error(t, err)
	})
}
