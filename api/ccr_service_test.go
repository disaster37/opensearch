package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newCcrTestServer() *httptest.Server {
	mux := http.NewServeMux()

	mux.HandleFunc("/_plugins/_replication/autofollow_stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"num_success_start_replications":0,"num_failed_start_replications":0,"num_failed_leader_calls":0,"failed_indices":[],"autofollow_stats":[]}`))
	})

	mux.HandleFunc("/_plugins/_replication/_autofollow", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodPost:
			w.Write([]byte(`{"acknowledged":true}`))
		case http.MethodDelete:
			w.Write([]byte(`{"acknowledged":true}`))
		}
	})

	mux.HandleFunc("/_plugins/_replication/test_rule/_start", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"acknowledged":true}`))
	})

	mux.HandleFunc("/_plugins/_replication/test_rule/_stop", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"acknowledged":true}`))
	})

	mux.HandleFunc("/_plugins/_replication/test_rule/_pause", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"acknowledged":true}`))
	})

	mux.HandleFunc("/_plugins/_replication/test_rule/_resume", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"acknowledged":true}`))
	})

	mux.HandleFunc("/_plugins/_replication/test_rule/_status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"status":"SYNCING","reason":"","leader_alias":"leader1","leader_index":"leader-index","follower_index":"follower-index","syncing_details":{"leader_checkpoint":100,"follower_checkpoint":95,"seq_no":100}}`))
	})

	mux.HandleFunc("/_plugins/_replication/test_rule/_update", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"acknowledged":true,"status":"UPDATED"}`))
	})

	mux.HandleFunc("/_plugins/_replication/follower_stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"operations_written":100,"operations_read":100,"failed_read_requests":0,"throttled_read_requests":0,"failed_write_requests":0,"throttled_write_requests":0,"follower_checkpoint":95,"leader_checkpoint":100,"total_write_time_millis":500,"num_syncing_indices":1,"num_bootstrapping_indices":0,"num_paused_indices":0,"num_failed_indices":0,"num_shard_tasks":1,"num_index_tasks":0,"index_stats":{}}`))
	})

	mux.HandleFunc("/_plugins/_replication/leader_stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"operations_read":200,"translog_size_bytes":1024,"operations_read_lucene":150,"operations_read_translog":50,"total_read_time_lucene_millis":300,"total_read_time_translog_millis":100,"bytes_read":2048,"num_replicated_indices":1,"index_stats":{}}`))
	})

	return httptest.NewServer(mux)
}

func TestUnitCcrService_AutoFollowStatus(t *testing.T) {
	srv := newCcrTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewCcrService(restyClient(srv), testLogger())

	resp, err := svc.AutoFollowStatus(ctx)
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, int64(0), resp.NumSuccessStartReplications)
}

func TestUnitCcrService_PostAutoFollow(t *testing.T) {
	srv := newCcrTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewCcrService(restyClient(srv), testLogger())

	resp, err := svc.PostAutoFollow(ctx, &CcrAutoFollowRule{
		LeaderAlias: "leader1",
		Name:        "test_rule",
		Pattern:     "leader-*",
		UseRoles: CcrRuleUseRoles{
			LeaderClusterRole:   "all_access",
			FollowerClusterRole: "all_access",
		},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.True(t, resp.Acknowledged)
}

func TestUnitCcrService_PostAutoFollowNilBodyError(t *testing.T) {
	ctx := context.Background()
	svc := NewCcrService(deadClient(), testLogger())

	_, err := svc.PostAutoFollow(ctx, nil)
	assert.Error(t, err)
}

func TestUnitCcrService_DeleteAutoFollow(t *testing.T) {
	srv := newCcrTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewCcrService(restyClient(srv), testLogger())

	resp, err := svc.DeleteAutoFollow(ctx, &CcrDeleteAutoFollowOptions{
		LeaderAlias: "leader1",
		Name:        "test_rule",
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.True(t, resp.Acknowledged)
}

func TestUnitCcrService_DeleteAutoFollowValidationError(t *testing.T) {
	ctx := context.Background()
	svc := NewCcrService(deadClient(), testLogger())

	_, err := svc.DeleteAutoFollow(ctx, &CcrDeleteAutoFollowOptions{})
	assert.Error(t, err)
}

func TestUnitCcrService_StartRule(t *testing.T) {
	srv := newCcrTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewCcrService(restyClient(srv), testLogger())

	resp, err := svc.StartRule(ctx, &CcrStartRuleRequest{
		Name: "test_rule",
		Body: &CcrRule{
			LeaderAlias: "leader1",
			LeaderIndex: "leader-index",
			UseRoles: CcrRuleUseRoles{
				LeaderClusterRole:   "all_access",
				FollowerClusterRole: "all_access",
			},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.True(t, resp.Acknowledged)
}

func TestUnitCcrService_StartRuleValidationError(t *testing.T) {
	ctx := context.Background()
	svc := NewCcrService(deadClient(), testLogger())

	_, err := svc.StartRule(ctx, &CcrStartRuleRequest{})
	assert.Error(t, err)
}

func TestUnitCcrService_StopRule(t *testing.T) {
	srv := newCcrTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewCcrService(restyClient(srv), testLogger())

	resp, err := svc.StopRule(ctx, "test_rule")
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.True(t, resp.Acknowledged)
}

func TestUnitCcrService_StopRuleEmptyError(t *testing.T) {
	ctx := context.Background()
	svc := NewCcrService(deadClient(), testLogger())

	_, err := svc.StopRule(ctx, "")
	assert.Error(t, err)
}

func TestUnitCcrService_PauseRule(t *testing.T) {
	srv := newCcrTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewCcrService(restyClient(srv), testLogger())

	resp, err := svc.PauseRule(ctx, "test_rule")
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.True(t, resp.Acknowledged)
}

func TestUnitCcrService_PauseRuleEmptyError(t *testing.T) {
	ctx := context.Background()
	svc := NewCcrService(deadClient(), testLogger())

	_, err := svc.PauseRule(ctx, "")
	assert.Error(t, err)
}

func TestUnitCcrService_ResumeRule(t *testing.T) {
	srv := newCcrTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewCcrService(restyClient(srv), testLogger())

	resp, err := svc.ResumeRule(ctx, "test_rule")
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.True(t, resp.Acknowledged)
}

func TestUnitCcrService_ResumeRuleEmptyError(t *testing.T) {
	ctx := context.Background()
	svc := NewCcrService(deadClient(), testLogger())

	_, err := svc.ResumeRule(ctx, "")
	assert.Error(t, err)
}

func TestUnitCcrService_StatusRule(t *testing.T) {
	srv := newCcrTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewCcrService(restyClient(srv), testLogger())

	resp, err := svc.StatusRule(ctx, "test_rule")
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "SYNCING", resp.Status)
	assert.Equal(t, "leader1", resp.LeaderAlias)
	assert.Equal(t, int64(100), resp.SyncingDetails.LeaderCheckpoint)
}

func TestUnitCcrService_StatusRuleEmptyError(t *testing.T) {
	ctx := context.Background()
	svc := NewCcrService(deadClient(), testLogger())

	_, err := svc.StatusRule(ctx, "")
	assert.Error(t, err)
}

func TestUnitCcrService_FollowerStats(t *testing.T) {
	srv := newCcrTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewCcrService(restyClient(srv), testLogger())

	resp, err := svc.FollowerStats(ctx)
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, int64(1), resp.NumSyncingIndices)
	assert.Equal(t, int64(100), resp.OperationsWritten)
}

func TestUnitCcrService_LeaderStats(t *testing.T) {
	srv := newCcrTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewCcrService(restyClient(srv), testLogger())

	resp, err := svc.LeaderStats(ctx)
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, int64(1), resp.NumReplicatedIndices)
	assert.Equal(t, int64(200), resp.OperationsRead)
}

func TestUnitCcrService_UpdateRule(t *testing.T) {
	srv := newCcrTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewCcrService(restyClient(srv), testLogger())

	resp, err := svc.UpdateRule(ctx, "test_rule", map[string]any{"pattern": "new-*"})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.True(t, resp.Acknowledged)
	assert.Equal(t, "UPDATED", resp.Status)
}

func TestUnitCcrService_UpdateRuleEmptyName(t *testing.T) {
	ctx := context.Background()
	svc := NewCcrService(deadClient(), testLogger())

	_, err := svc.UpdateRule(ctx, "", map[string]any{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "name is required")
}

func TestUnitCcrService_UpdateRuleNilBody(t *testing.T) {
	ctx := context.Background()
	svc := NewCcrService(deadClient(), testLogger())

	_, err := svc.UpdateRule(ctx, "test_rule", nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "body is required")
}
