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

func newIsmTestServer() *httptest.Server {
	mux := http.NewServeMux()

	mux.HandleFunc("/_plugins/_ism/policies/test_policy", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodGet:
			w.Write([]byte(`{"_id":"test","_version":1,"_seq_no":0,"_primary_term":1,"policy":{"policy_id":"test","policy":{},"schema_version":1,"last_updated_time":1000}}`))
		case http.MethodPut:
			w.Write([]byte(`{"_id":"test","_version":2,"_seq_no":1,"_primary_term":1,"policy":{"policy_id":"test","policy":{},"schema_version":1,"last_updated_time":2000}}`))
		case http.MethodDelete:
			result := `"_id"`
			_ = result
			w.Write([]byte(`{"_index":".opendistro-ism-config","_id":"test_policy","_version":2,"result":"deleted","forced_refresh":false,"_shards":{},"_seq_no":1,"_primary_term":1}`))
		}
	})

	mux.HandleFunc("/_plugins/_ism/explain/test_index", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"total_managed_indices":0}`))
	})

	mux.HandleFunc("/_plugins/_ism/add/test_index", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"updated_indices":1,"failed_indices":[],"failures":false}`))
	})

	mux.HandleFunc("/_plugins/_ism/remove/test_index", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"updated_indices":1,"failed_indices":[],"failures":false}`))
	})

	mux.HandleFunc("/_plugins/_ism/change_policy/test_index", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"updated_indices":1,"failed_indices":[{"index_name":"bad","reason":"not found"}],"failures":true}`))
	})

	mux.HandleFunc("/_plugins/_ism/retry/test_index", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"updated_indices":1,"failed_indices":[],"failures":false}`))
	})

	mux.HandleFunc("/_plugins/_ism/policies", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodGet {
			w.Write([]byte(`{"policies":[{"policy_id":"p1","default_state":"hot"}],"total_policies":1}`))
		}
	})

	return httptest.NewServer(mux)
}

func TestUnitIsmService_GetPolicy(t *testing.T) {
	srv := newIsmTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewIsmService(restyClient(srv), testLogger())

	resp, err := svc.GetPolicy(ctx, "test_policy")
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "test", resp.Id)
	assert.Equal(t, int64(1), resp.Version)
}

func TestUnitIsmService_PutPolicy(t *testing.T) {
	srv := newIsmTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewIsmService(restyClient(srv), testLogger())

	defaultState := "hot"
	resp, err := svc.PutPolicy(ctx, &IsmPutPolicyRequest{
		PolicyName: "test_policy",
		Body: &IsmPolicyBase{
			DefaultState: &defaultState,
			States: []IsmPolicyState{
				{Name: "hot"},
			},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, int64(2), resp.Version)
}

func TestUnitIsmService_PutPolicyWithVersion(t *testing.T) {
	srv := newIsmTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewIsmService(restyClient(srv), testLogger())

	seqNo := int64(0)
	primaryTerm := int64(1)
	defaultState := "hot"
	resp, err := svc.PutPolicy(ctx, &IsmPutPolicyRequest{
		PolicyName: "test_policy",
		Body: &IsmPolicyBase{
			DefaultState: &defaultState,
		},
		Version: &types.DocumentVersion{
			SeqNo:       &seqNo,
			PrimaryTerm: &primaryTerm,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
}

func TestUnitIsmService_DeletePolicy(t *testing.T) {
	srv := newIsmTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewIsmService(restyClient(srv), testLogger())

	resp, err := svc.DeletePolicy(ctx, "test_policy")
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "deleted", *resp.Result)
	assert.Equal(t, "test_policy", *resp.ID)
}

func TestUnitIsmService_ExplainPolicy(t *testing.T) {
	srv := newIsmTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewIsmService(restyClient(srv), testLogger())

	resp, err := svc.ExplainPolicy(ctx, "test_index")
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, int64(0), resp.TotalManagedIndices)
}

func TestUnitIsmService_EmptyPolicyNameErrors(t *testing.T) {
	srv := newIsmTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewIsmService(restyClient(srv), testLogger())

	_, err := svc.GetPolicy(ctx, "")
	assert.Error(t, err)

	_, err = svc.DeletePolicy(ctx, "")
	assert.Error(t, err)
}

func TestUnitIsmService_PutPolicyValidationError(t *testing.T) {
	ctx := context.Background()
	svc := NewIsmService(deadClient(), testLogger())

	_, err := svc.PutPolicy(ctx, &IsmPutPolicyRequest{})
	assert.Error(t, err)
}

func TestUnitIsmService_AddPolicy(t *testing.T) {
	srv := newIsmTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewIsmService(restyClient(srv), testLogger())

	resp, err := svc.AddPolicy(ctx, "test_index", map[string]any{"policy_id": "my_policy"})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, int64(1), resp.UpdatedIndices)
	assert.Empty(t, resp.FailedIndices)
}

func TestUnitIsmService_AddPolicyEmptyIndex(t *testing.T) {
	ctx := context.Background()
	svc := NewIsmService(deadClient(), testLogger())

	_, err := svc.AddPolicy(ctx, "", map[string]any{"policy_id": "p"})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "index is required")
}

func TestUnitIsmService_AddPolicyNilBody(t *testing.T) {
	ctx := context.Background()
	svc := NewIsmService(deadClient(), testLogger())

	_, err := svc.AddPolicy(ctx, "idx", nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "body is required")
}

func TestUnitIsmService_RemovePolicy(t *testing.T) {
	srv := newIsmTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewIsmService(restyClient(srv), testLogger())

	resp, err := svc.RemovePolicy(ctx, "test_index")
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, int64(1), resp.UpdatedIndices)
}

func TestUnitIsmService_RemovePolicyEmptyIndex(t *testing.T) {
	ctx := context.Background()
	svc := NewIsmService(deadClient(), testLogger())

	_, err := svc.RemovePolicy(ctx, "")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "index is required")
}

func TestUnitIsmService_ChangePolicy(t *testing.T) {
	srv := newIsmTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewIsmService(restyClient(srv), testLogger())

	resp, err := svc.ChangePolicy(ctx, "test_index", map[string]any{"policy_id": "new_policy"})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, int64(1), resp.UpdatedIndices)
	assert.Len(t, resp.FailedIndices, 1)
	assert.Equal(t, "bad", resp.FailedIndices[0].IndexName)
	assert.Equal(t, "not found", resp.FailedIndices[0].Reason)
}

func TestUnitIsmService_ChangePolicyEmptyIndex(t *testing.T) {
	ctx := context.Background()
	svc := NewIsmService(deadClient(), testLogger())

	_, err := svc.ChangePolicy(ctx, "", map[string]any{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "index is required")
}

func TestUnitIsmService_ChangePolicyNilBody(t *testing.T) {
	ctx := context.Background()
	svc := NewIsmService(deadClient(), testLogger())

	_, err := svc.ChangePolicy(ctx, "idx", nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "body is required")
}

func TestUnitIsmService_RetryFailedIndex(t *testing.T) {
	srv := newIsmTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewIsmService(restyClient(srv), testLogger())

	resp, err := svc.RetryFailedIndex(ctx, "test_index", map[string]any{"state": "hot"})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, int64(1), resp.UpdatedIndices)
}

func TestUnitIsmService_RetryFailedIndexNilBody(t *testing.T) {
	srv := newIsmTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewIsmService(restyClient(srv), testLogger())

	resp, err := svc.RetryFailedIndex(ctx, "test_index", nil)
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, int64(1), resp.UpdatedIndices)
}

func TestUnitIsmService_RetryFailedIndexEmptyIndex(t *testing.T) {
	ctx := context.Background()
	svc := NewIsmService(deadClient(), testLogger())

	_, err := svc.RetryFailedIndex(ctx, "", map[string]any{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "index is required")
}

func TestUnitIsmService_ListPolicies(t *testing.T) {
	srv := newIsmTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewIsmService(restyClient(srv), testLogger())

	resp, err := svc.ListPolicies(ctx)
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, int64(1), resp.TotalPolicies)
	assert.Len(t, resp.Policies, 1)
	assert.Equal(t, "p1", *resp.Policies[0].ID)
}
