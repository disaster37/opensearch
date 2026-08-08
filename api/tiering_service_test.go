// tiering_service_test.go contains unit tests for DefaultTieringService.
// Tests use httptest.NewServer to mock OpenSearch tiering endpoints
// (OpenSearch 3.7.0+, gated by the writable_warm_index feature flag).
package api

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUnitTieringService_GetStatus(t *testing.T) {
	respJSON := `{"tiering_status":{"index":"my-index","state":"RUNNING","source":"hot","target":"warm","start_time":1234567890,"shard_level_status":{"pending":0,"running":1,"succeeded":0,"total":1,"shard_relocation_status":[{"source_shard_id":0,"relocating_node_id":"node1"}]}}}`

	var capturedPath string
	var capturedDetailed string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedPath = r.URL.Path
		capturedDetailed = r.URL.Query().Get("detailed")
		w.WriteHeader(200)
		_, _ = fmt.Fprint(w, respJSON)
	}))
	defer srv.Close()

	svc := NewTieringService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success with detailed=true", func(t *testing.T) {
		capturedDetailed = ""
		resp, err := svc.GetStatus(ctx, "my-index", true)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.NotNil(t, resp.TieringStatus)
		assert.Equal(t, "my-index", resp.TieringStatus.Index)
		assert.Equal(t, "RUNNING", resp.TieringStatus.State)
		require.NotNil(t, resp.TieringStatus.ShardLevelStatus)
		assert.Equal(t, 1, resp.TieringStatus.ShardLevelStatus.Running)
		require.Len(t, resp.TieringStatus.ShardLevelStatus.ShardRelocationStatus, 1)
		assert.Equal(t, "node1", resp.TieringStatus.ShardLevelStatus.ShardRelocationStatus[0].RelocatingNodeId)
		assert.Equal(t, "/my-index/_tier", capturedPath)
		assert.Equal(t, "true", capturedDetailed)
	})

	t.Run("success with detailed=false omits param", func(t *testing.T) {
		capturedDetailed = "__sentinel__"
		resp, err := svc.GetStatus(ctx, "my-index", false)
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, "", capturedDetailed)
	})

	t.Run("empty index validation error", func(t *testing.T) {
		_, err := svc.GetStatus(ctx, "", true)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "index is required")
	})
}

func TestUnitTieringService_ListStatus(t *testing.T) {
	respJSON := `[{"index":"idx1","state":"RUNNING","source":"hot","target":"warm"},{"index":"idx2","state":"COMPLETED","source":"warm","target":"hot"}]`

	var capturedTarget string
	var capturedFormat string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedTarget = r.URL.Query().Get("target")
		capturedFormat = r.URL.Query().Get("format")
		w.WriteHeader(200)
		_, _ = fmt.Fprint(w, respJSON)
	}))
	defer srv.Close()

	svc := NewTieringService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success with hot target", func(t *testing.T) {
		capturedTarget = ""
		resp, err := svc.ListStatus(ctx, TierTargetHot)
		require.NoError(t, err)
		require.Len(t, resp, 2)
		assert.Equal(t, "idx1", resp[0].Index)
		assert.Equal(t, "_hot", capturedTarget)
		assert.Equal(t, "json", capturedFormat)
	})

	t.Run("success with warm target", func(t *testing.T) {
		resp, err := svc.ListStatus(ctx, TierTargetWarm)
		require.NoError(t, err)
		require.Len(t, resp, 2)
		assert.Equal(t, "_warm", capturedTarget)
	})

	t.Run("success without target", func(t *testing.T) {
		capturedTarget = "__sentinel__"
		resp, err := svc.ListStatus(ctx, "")
		require.NoError(t, err)
		require.Len(t, resp, 2)
		assert.Equal(t, "", capturedTarget)
	})

	t.Run("invalid target validation error", func(t *testing.T) {
		_, err := svc.ListStatus(ctx, "bogus")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid target")
	})
}

func TestUnitTieringService_HotToWarm(t *testing.T) {
	var capturedPath, capturedMethod, capturedTimeout string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedPath = r.URL.Path
		capturedMethod = r.Method
		capturedTimeout = r.URL.Query().Get("timeout")
		w.WriteHeader(200)
		_, _ = fmt.Fprint(w, `{"acknowledged":true}`)
	}))
	defer srv.Close()

	svc := NewTieringService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success with params", func(t *testing.T) {
		capturedTimeout = ""
		resp, err := svc.HotToWarm(ctx, "my-index", &TierOperationParams{Timeout: "30s"})
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.True(t, resp.Acknowledged)
		assert.Equal(t, "/my-index/_tier/warm", capturedPath)
		assert.Equal(t, http.MethodPost, capturedMethod)
		assert.Equal(t, "30s", capturedTimeout)
	})

	t.Run("success without params", func(t *testing.T) {
		resp, err := svc.HotToWarm(ctx, "my-index")
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("empty index validation error", func(t *testing.T) {
		_, err := svc.HotToWarm(ctx, "")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "index is required")
	})
}

func TestUnitTieringService_WarmToHot(t *testing.T) {
	var capturedPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedPath = r.URL.Path
		w.WriteHeader(200)
		_, _ = fmt.Fprint(w, `{"acknowledged":true}`)
	}))
	defer srv.Close()

	svc := NewTieringService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		resp, err := svc.WarmToHot(ctx, "my-index")
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, "/my-index/_tier/hot", capturedPath)
	})

	t.Run("empty index validation error", func(t *testing.T) {
		_, err := svc.WarmToHot(ctx, "")
		require.Error(t, err)
	})
}

func TestUnitTieringService_Cancel(t *testing.T) {
	var capturedPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedPath = r.URL.Path
		w.WriteHeader(200)
		_, _ = fmt.Fprint(w, `{"acknowledged":true}`)
	}))
	defer srv.Close()

	svc := NewTieringService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		resp, err := svc.Cancel(ctx, "my-index")
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, "/_tier/_cancel/my-index", capturedPath)
	})

	t.Run("empty index validation error", func(t *testing.T) {
		_, err := svc.Cancel(ctx, "")
		require.Error(t, err)
	})
}

func TestUnitTieringService_ErrorPaths(t *testing.T) {
	ctx := context.Background()

	t.Run("GetStatus server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewTieringService(restyClient(srv), testLogger())
		_, err := s.GetStatus(ctx, "idx", true)
		require.Error(t, err)
	})

	t.Run("GetStatus unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewTieringService(restyClient(srv), testLogger())
		_, err := s.GetStatus(ctx, "idx", true)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("ListStatus server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewTieringService(restyClient(srv), testLogger())
		_, err := s.ListStatus(ctx, "")
		require.Error(t, err)
	})

	t.Run("ListStatus unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewTieringService(restyClient(srv), testLogger())
		_, err := s.ListStatus(ctx, "")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("HotToWarm server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewTieringService(restyClient(srv), testLogger())
		_, err := s.HotToWarm(ctx, "idx")
		require.Error(t, err)
	})

	t.Run("HotToWarm unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewTieringService(restyClient(srv), testLogger())
		_, err := s.HotToWarm(ctx, "idx")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("WarmToHot server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewTieringService(restyClient(srv), testLogger())
		_, err := s.WarmToHot(ctx, "idx")
		require.Error(t, err)
	})

	t.Run("Cancel server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewTieringService(restyClient(srv), testLogger())
		_, err := s.Cancel(ctx, "idx")
		require.Error(t, err)
	})
}

func TestUnitTieringService_NetworkErrors(t *testing.T) {
	ctx := context.Background()
	s := NewTieringService(deadClient(), testLogger())

	t.Run("GetStatus network error", func(t *testing.T) {
		_, err := s.GetStatus(ctx, "idx", true)
		require.Error(t, err)
	})
	t.Run("ListStatus network error", func(t *testing.T) {
		_, err := s.ListStatus(ctx, "")
		require.Error(t, err)
	})
	t.Run("HotToWarm network error", func(t *testing.T) {
		_, err := s.HotToWarm(ctx, "idx")
		require.Error(t, err)
	})
	t.Run("WarmToHot network error", func(t *testing.T) {
		_, err := s.WarmToHot(ctx, "idx")
		require.Error(t, err)
	})
	t.Run("Cancel network error", func(t *testing.T) {
		_, err := s.Cancel(ctx, "idx")
		require.Error(t, err)
	})
}
