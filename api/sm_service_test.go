package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/disaster37/opensearch/v4/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newSmTestServer() *httptest.Server {
	mux := http.NewServeMux()

	mux.HandleFunc("/_plugins/_sm/policies/test_sm", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		path := r.URL.Path
		if strings.HasSuffix(path, "/_explain") {
			w.Write([]byte(`{"policies":[]}`))
			return
		}
		switch r.Method {
		case http.MethodGet:
			w.Write([]byte(`{"_id":"test","_version":1,"_seq_no":0,"_primary_term":1,"sm_policy":{"policy_id":"test_sm","snapshot_config":{"repository":"my-repo"},"creation":{"schedule":{}}}}`))
		case http.MethodPut:
			w.Write([]byte(`{"_id":"test","_version":2,"_seq_no":1,"_primary_term":1,"sm_policy":{"policy_id":"test_sm","snapshot_config":{"repository":"my-repo"},"creation":{"schedule":{}}}}`))
		case http.MethodPost:
			w.Write([]byte(`{"_id":"test","_version":1,"_seq_no":0,"_primary_term":1,"sm_policy":{"policy_id":"test_sm","snapshot_config":{"repository":"my-repo"},"creation":{"schedule":{}}}}`))
		case http.MethodDelete:
			w.Write([]byte(`{"_index":".opendistro-sm-config","_id":"test_sm","_version":2,"result":"deleted","forced_refresh":false,"_shards":{},"_seq_no":1,"_primary_term":1}`))
		}
	})

	mux.HandleFunc("/_plugins/_sm/policies/test_sm/_explain", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"policies":[]}`))
	})

	mux.HandleFunc("/_plugins/_sm/policies/test_sm/_start", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"acknowledged":true,"status":"STARTED"}`))
	})

	mux.HandleFunc("/_plugins/_sm/policies/test_sm/_stop", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"acknowledged":true,"status":"STOPPED"}`))
	})

	mux.HandleFunc("/_plugins/_sm/policies", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodGet {
			w.Write([]byte(`{"policies":[{"policy_id":"p1","_seq_no":1,"_primary_term":1}],"total_policies":1}`))
		}
	})

	return httptest.NewServer(mux)
}

func TestUnitSmService_GetPolicy(t *testing.T) {
	srv := newSmTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewSmService(restyClient(srv), testLogger())

	resp, err := svc.GetPolicy(ctx, "test_sm")
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "test", resp.Id)
	assert.Equal(t, int64(1), resp.Version)
}

func TestUnitSmService_PutPolicy(t *testing.T) {
	srv := newSmTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewSmService(restyClient(srv), testLogger())

	resp, err := svc.PutPolicy(ctx, &SmPutPolicyRequest{
		PolicyName: "test_sm",
		Body: &SmPutPolicy{
			SnapshotConfig: SmPolicySnapshotConfig{Repository: "my-repo"},
			Creation: SmPolicyCreation{
				Schedule: map[string]any{"cron": map[string]any{"expression": "0 0 * * *"}},
			},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, int64(2), resp.Version)
}

func TestUnitSmService_PutPolicyWithVersion(t *testing.T) {
	srv := newSmTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewSmService(restyClient(srv), testLogger())

	seqNo := int64(0)
	primaryTerm := int64(1)
	resp, err := svc.PutPolicy(ctx, &SmPutPolicyRequest{
		PolicyName: "test_sm",
		Body: &SmPutPolicy{
			SnapshotConfig: SmPolicySnapshotConfig{Repository: "my-repo"},
			Creation: SmPolicyCreation{
				Schedule: map[string]any{},
			},
		},
		Version: &types.DocumentVersion{
			SeqNo:       &seqNo,
			PrimaryTerm: &primaryTerm,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
}

func TestUnitSmService_PostPolicy(t *testing.T) {
	srv := newSmTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewSmService(restyClient(srv), testLogger())

	resp, err := svc.PostPolicy(ctx, "test_sm", &SmPutPolicy{
		SnapshotConfig: SmPolicySnapshotConfig{Repository: "my-repo"},
		Creation:       SmPolicyCreation{Schedule: map[string]any{}},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "test", resp.Id)
}

func TestUnitSmService_DeletePolicy(t *testing.T) {
	srv := newSmTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewSmService(restyClient(srv), testLogger())

	resp, err := svc.DeletePolicy(ctx, "test_sm")
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "deleted", resp.Result)
}

func TestUnitSmService_ExplainPolicy(t *testing.T) {
	srv := newSmTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewSmService(restyClient(srv), testLogger())

	resp, err := svc.ExplainPolicy(ctx, []string{"test_sm"})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.NotNil(t, resp.Policies)
}

func TestUnitSmService_EmptyPolicyNameErrors(t *testing.T) {
	srv := newSmTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewSmService(restyClient(srv), testLogger())

	_, err := svc.GetPolicy(ctx, "")
	assert.Error(t, err)

	_, err = svc.PostPolicy(ctx, "", nil)
	assert.Error(t, err)

	_, err = svc.PostPolicy(ctx, "test", nil)
	assert.Error(t, err)

	_, err = svc.DeletePolicy(ctx, "")
	assert.Error(t, err)
}

func TestUnitSmService_PutPolicyValidationError(t *testing.T) {
	ctx := context.Background()
	svc := NewSmService(deadClient(), testLogger())

	_, err := svc.PutPolicy(ctx, &SmPutPolicyRequest{})
	assert.Error(t, err)
}

func TestUnitSmService_StartPolicy(t *testing.T) {
	srv := newSmTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewSmService(restyClient(srv), testLogger())

	resp, err := svc.StartPolicy(ctx, "test_sm")
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.True(t, resp.Acknowledged)
	assert.Equal(t, "STARTED", resp.Status)
}

func TestUnitSmService_StartPolicyEmptyError(t *testing.T) {
	ctx := context.Background()
	svc := NewSmService(deadClient(), testLogger())

	_, err := svc.StartPolicy(ctx, "")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "policy name is required")
}

func TestUnitSmService_StopPolicy(t *testing.T) {
	srv := newSmTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewSmService(restyClient(srv), testLogger())

	resp, err := svc.StopPolicy(ctx, "test_sm")
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.True(t, resp.Acknowledged)
	assert.Equal(t, "STOPPED", resp.Status)
}

func TestUnitSmService_StopPolicyEmptyError(t *testing.T) {
	ctx := context.Background()
	svc := NewSmService(deadClient(), testLogger())

	_, err := svc.StopPolicy(ctx, "")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "policy name is required")
}

func TestUnitSmService_ListPolicies(t *testing.T) {
	srv := newSmTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewSmService(restyClient(srv), testLogger())

	resp, err := svc.ListPolicies(ctx)
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, int64(1), resp.TotalPolicies)
	assert.Len(t, resp.Policies, 1)
	assert.Equal(t, "p1", *resp.Policies[0].Name)
}
