package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUnitSnapshotService_Create(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "PUT", r.Method)
		assert.Equal(t, "/_snapshot/my-repo/snap1", r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"accepted":true,"snapshot":{"snapshot":"snap1","uuid":"abc123","version_id":2,"version":"2.0.0","indices":["test-idx"],"state":"SUCCESS","shards":{"total":5,"successful":5,"failed":0}}}`))
	}))
	defer ts.Close()

	svc := NewSnapshotService(restyClient(ts), testLogger())
	ctx := context.Background()

	resp, err := svc.Create(ctx, &SnapshotCreateRequest{
		Repository: "my-repo",
		Snapshot:   "snap1",
		Body:       map[string]any{"indices": "test-idx"},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.NotNil(t, resp.Accepted)
	assert.True(t, *resp.Accepted)
	assert.Equal(t, "snap1", resp.Snapshot.Snapshot)
}

func TestUnitSnapshotService_Get(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/_snapshot/my-repo/snap1", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"snapshots":[{"snapshot":"snap1","uuid":"abc123","version_id":2,"version":"2.0.0","indices":["test-idx"],"state":"SUCCESS"}]}`))
	})
	mux.HandleFunc("/_snapshot/my-repo/_all", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"snapshots":[{"snapshot":"snap1","uuid":"abc123","version_id":2,"version":"2.0.0","indices":["test-idx"],"state":"SUCCESS"},{"snapshot":"snap2","uuid":"def456","version_id":2,"version":"2.0.0","indices":["test-idx"],"state":"SUCCESS"}]}`))
	})

	ts := httptest.NewServer(mux)
	defer ts.Close()

	svc := NewSnapshotService(restyClient(ts), testLogger())
	ctx := context.Background()

	t.Run("specific snapshot", func(t *testing.T) {
		resp, err := svc.Get(ctx, &SnapshotGetRequest{Repository: "my-repo", Snapshots: []string{"snap1"}})
		require.NoError(t, err)
		require.Len(t, resp.Snapshots, 1)
		assert.Equal(t, "snap1", resp.Snapshots[0].Snapshot)
	})

	t.Run("all snapshots", func(t *testing.T) {
		resp, err := svc.Get(ctx, &SnapshotGetRequest{Repository: "my-repo"})
		require.NoError(t, err)
		require.Len(t, resp.Snapshots, 2)
	})
}

func TestUnitSnapshotService_Delete(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "DELETE", r.Method)
		assert.Equal(t, "/_snapshot/my-repo/snap1", r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"acknowledged":true}`))
	}))
	defer ts.Close()

	svc := NewSnapshotService(restyClient(ts), testLogger())
	ctx := context.Background()

	resp, err := svc.Delete(ctx, &SnapshotDeleteRequest{Repository: "my-repo", Snapshot: "snap1"})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.True(t, resp.Acknowledged)
}

func TestUnitSnapshotService_Status(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/_snapshot/my-repo/snap1/_status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"snapshots":[{"snapshot":"snap1","repository":"my-repo","uuid":"abc123","state":"IN_PROGRESS","include_global_state":true}]}`))
	})
	mux.HandleFunc("/_snapshot/my-repo/_status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"snapshots":[{"snapshot":"snap1","repository":"my-repo","uuid":"abc123","state":"IN_PROGRESS","include_global_state":true}]}`))
	})

	ts := httptest.NewServer(mux)
	defer ts.Close()

	svc := NewSnapshotService(restyClient(ts), testLogger())
	ctx := context.Background()

	t.Run("specific snapshot status", func(t *testing.T) {
		resp, err := svc.Status(ctx, &SnapshotStatusRequest{Repository: "my-repo", Snapshots: []string{"snap1"}})
		require.NoError(t, err)
		require.Len(t, resp.Snapshots, 1)
		assert.Equal(t, "snap1", resp.Snapshots[0].Snapshot)
		assert.Equal(t, "IN_PROGRESS", resp.Snapshots[0].State)
	})

	t.Run("all snapshots status", func(t *testing.T) {
		resp, err := svc.Status(ctx, &SnapshotStatusRequest{Repository: "my-repo"})
		require.NoError(t, err)
		require.Len(t, resp.Snapshots, 1)
	})
}

func TestUnitSnapshotService_Restore(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "POST", r.Method)
		assert.Equal(t, "/_snapshot/my-repo/snap1/_restore", r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"accepted":true,"snapshot":{"snapshot":"snap1","indices":["test-idx"],"shards":{"total":5,"successful":5,"failed":0}}}`))
	}))
	defer ts.Close()

	svc := NewSnapshotService(restyClient(ts), testLogger())
	ctx := context.Background()

	resp, err := svc.Restore(ctx, &SnapshotRestoreRequest{
		Repository: "my-repo",
		Snapshot:   "snap1",
		Body:       map[string]any{"indices": "test-idx"},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.NotNil(t, resp.Accepted)
	assert.True(t, *resp.Accepted)
	assert.Equal(t, "snap1", resp.Snapshot.Snapshot)
}

func TestUnitSnapshotService_CreateRepository(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "PUT", r.Method)
		assert.Equal(t, "/_snapshot/my-repo", r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"acknowledged":true}`))
	}))
	defer ts.Close()

	svc := NewSnapshotService(restyClient(ts), testLogger())
	ctx := context.Background()

	resp, err := svc.CreateRepository(ctx, "my-repo", map[string]any{
		"type":     "fs",
		"settings": map[string]any{"location": "/backups"},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.True(t, resp.Acknowledged)
}

func TestUnitSnapshotService_GetRepository(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/_snapshot/my-repo", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"my-repo":{"type":"fs","settings":{"location":"/backups"}}}`))
	})
	mux.HandleFunc("/_snapshot", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"my-repo":{"type":"fs","settings":{"location":"/backups"}},"other-repo":{"type":"s3","settings":{"bucket":"my-bucket"}}}`))
	})

	ts := httptest.NewServer(mux)
	defer ts.Close()

	svc := NewSnapshotService(restyClient(ts), testLogger())
	ctx := context.Background()

	t.Run("specific repo", func(t *testing.T) {
		resp, err := svc.GetRepository(ctx, []string{"my-repo"})
		require.NoError(t, err)
		require.Contains(t, resp, "my-repo")
		assert.Equal(t, "fs", resp["my-repo"].Type)
	})

	t.Run("all repos", func(t *testing.T) {
		resp, err := svc.GetRepository(ctx, nil)
		require.NoError(t, err)
		assert.Len(t, resp, 2)
	})
}

func TestUnitSnapshotService_DeleteRepository(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "DELETE", r.Method)
		assert.Equal(t, "/_snapshot/my-repo", r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"acknowledged":true}`))
	}))
	defer ts.Close()

	svc := NewSnapshotService(restyClient(ts), testLogger())
	ctx := context.Background()

	resp, err := svc.DeleteRepository(ctx, "my-repo")
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.True(t, resp.Acknowledged)
}

func TestUnitSnapshotService_VerifyRepository(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "POST", r.Method)
		assert.Equal(t, "/_snapshot/my-repo/_verify", r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"nodes":{"n1":{"name":"node1"},"n2":{"name":"node2"}}}`))
	}))
	defer ts.Close()

	svc := NewSnapshotService(restyClient(ts), testLogger())
	ctx := context.Background()

	resp, err := svc.VerifyRepository(ctx, "my-repo")
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Len(t, resp.Nodes, 2)
	assert.Equal(t, "node1", resp.Nodes["n1"].Name)
}

func TestUnitSnapshotService_CleanupRepository(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "POST", r.Method)
		assert.Equal(t, "/_snapshot/my-repo/_cleanup", r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"results":{"deleted_bytes":1024,"deleted_blobs":5}}`))
	}))
	defer ts.Close()

	svc := NewSnapshotService(restyClient(ts), testLogger())
	ctx := context.Background()

	resp, err := svc.CleanupRepository(ctx, "my-repo")
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.NotNil(t, resp.Results)
	assert.Equal(t, int64(1024), resp.Results.DeletedBytes)
	assert.Equal(t, int64(5), resp.Results.DeletedBlobs)
}

func TestUnitSnapshotService_Clone(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "PUT", r.Method)
		assert.Equal(t, "/_snapshot/my-repo/snap1/_clone/snap1-clone", r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"acknowledged":true}`))
	}))
	defer ts.Close()

	svc := NewSnapshotService(restyClient(ts), testLogger())
	ctx := context.Background()

	resp, err := svc.Clone(ctx, &SnapshotCloneRequest{
		Repository:     "my-repo",
		Snapshot:       "snap1",
		TargetSnapshot: "snap1-clone",
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.True(t, resp.Acknowledged)

	t.Run("success with body", func(t *testing.T) {
		resp2, err := svc.Clone(ctx, &SnapshotCloneRequest{
			Repository:     "my-repo",
			Snapshot:       "snap1",
			TargetSnapshot: "snap1-clone",
			Body:           map[string]any{"indices": "idx-clone"},
		})
		require.NoError(t, err)
		require.NotNil(t, resp2)
		assert.True(t, resp2.Acknowledged)
	})
}

func TestUnitSnapshotService_CleanupRepositoryErrors(t *testing.T) {
	ctx := context.Background()

	t.Run("validation missing repository", func(t *testing.T) {
		srv := errServer(200)
		defer srv.Close()
		s := NewSnapshotService(restyClient(srv), testLogger())
		_, err := s.CleanupRepository(ctx, "")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "repository is required")
	})

	t.Run("server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewSnapshotService(restyClient(srv), testLogger())
		_, err := s.CleanupRepository(ctx, "my-repo")
		require.Error(t, err)
	})

	t.Run("unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewSnapshotService(restyClient(srv), testLogger())
		_, err := s.CleanupRepository(ctx, "my-repo")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("network error", func(t *testing.T) {
		s := NewSnapshotService(deadClient(), testLogger())
		_, err := s.CleanupRepository(ctx, "my-repo")
		require.Error(t, err)
	})
}

func TestUnitSnapshotService_CloneErrors(t *testing.T) {
	ctx := context.Background()

	t.Run("validation missing repository", func(t *testing.T) {
		srv := errServer(200)
		defer srv.Close()
		s := NewSnapshotService(restyClient(srv), testLogger())
		_, err := s.Clone(ctx, &SnapshotCloneRequest{Snapshot: "s", TargetSnapshot: "t"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "validation")
	})

	t.Run("validation missing snapshot", func(t *testing.T) {
		srv := errServer(200)
		defer srv.Close()
		s := NewSnapshotService(restyClient(srv), testLogger())
		_, err := s.Clone(ctx, &SnapshotCloneRequest{Repository: "r", TargetSnapshot: "t"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "validation")
	})

	t.Run("validation missing target", func(t *testing.T) {
		srv := errServer(200)
		defer srv.Close()
		s := NewSnapshotService(restyClient(srv), testLogger())
		_, err := s.Clone(ctx, &SnapshotCloneRequest{Repository: "r", Snapshot: "s"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "validation")
	})

	t.Run("server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewSnapshotService(restyClient(srv), testLogger())
		_, err := s.Clone(ctx, &SnapshotCloneRequest{Repository: "r", Snapshot: "s", TargetSnapshot: "t"})
		require.Error(t, err)
	})

	t.Run("unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewSnapshotService(restyClient(srv), testLogger())
		_, err := s.Clone(ctx, &SnapshotCloneRequest{Repository: "r", Snapshot: "s", TargetSnapshot: "t"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("network error", func(t *testing.T) {
		s := NewSnapshotService(deadClient(), testLogger())
		_, err := s.Clone(ctx, &SnapshotCloneRequest{Repository: "r", Snapshot: "s", TargetSnapshot: "t"})
		require.Error(t, err)
	})
}
