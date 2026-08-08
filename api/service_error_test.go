// service_error_test.go contains cross-service error propagation tests,
// node-model helper tests (HasRole, IsMaster, etc.), and edge-case tests
// for the parseErrorResponse / wrapUnmarshalError functions.
package api

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/disaster37/opensearch/v4/types"
	"github.com/go-resty/resty/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func longBodyServer() *httptest.Server {
	body := strings.Repeat("x", 600)
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		_, _ = fmt.Fprint(w, body)
	}))
}

func zeroStatusServer() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
		_, _ = fmt.Fprint(w, `{"error":{"type":"test","reason":"fail"},"status":0}`)
	}))
}

// --- Node Model Helpers ---

func TestNodesInfoNode_HasRole(t *testing.T) {
	node := &NodesInfoNode{Roles: []string{"cluster_manager", "data", "ingest"}}
	assert.True(t, node.HasRole("data"))
	assert.True(t, node.HasRole("cluster_manager"))
	assert.True(t, node.HasRole("ingest"))
	assert.False(t, node.HasRole("coordinating_only"))

	empty := &NodesInfoNode{Roles: nil}
	assert.False(t, empty.HasRole("data"))
}

func TestNodesInfoNode_IsMaster(t *testing.T) {
	master := &NodesInfoNode{Roles: []string{"cluster_manager", "data"}}
	assert.True(t, master.IsMaster())

	data := &NodesInfoNode{Roles: []string{"data"}}
	assert.False(t, data.IsMaster())
}

func TestNodesInfoNode_IsData(t *testing.T) {
	data := &NodesInfoNode{Roles: []string{"data"}}
	assert.True(t, data.IsData())

	master := &NodesInfoNode{Roles: []string{"cluster_manager"}}
	assert.False(t, master.IsData())
}

func TestNodesInfoNode_IsIngest(t *testing.T) {
	ingest := &NodesInfoNode{Roles: []string{"ingest"}}
	assert.True(t, ingest.IsIngest())

	data := &NodesInfoNode{Roles: []string{"data"}}
	assert.False(t, data.IsIngest())
}

// --- Error path tests for AlertingService ---

func TestAlertingServiceErrorPaths(t *testing.T) {
	ctx := context.Background()

	t.Run("GetMonitor server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		svc := NewAlertingService(restyClient(srv), testLogger())
		_, err := svc.GetMonitor(ctx, "mon1")
		require.Error(t, err)
	})

	t.Run("GetMonitor unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		svc := NewAlertingService(restyClient(srv), testLogger())
		_, err := svc.GetMonitor(ctx, "mon1")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("GetMonitor network error", func(t *testing.T) {
		svc := NewAlertingService(deadClient(), testLogger())
		_, err := svc.GetMonitor(ctx, "mon1")
		require.Error(t, err)
	})

	t.Run("PutMonitor server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		svc := NewAlertingService(restyClient(srv), testLogger())
		_, err := svc.PutMonitor(ctx, &AlertingPutMonitorRequest{MonitorId: "mon1", Body: &AlertingMonitor{Name: "t"}})
		require.Error(t, err)
	})

	t.Run("PutMonitor unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		svc := NewAlertingService(restyClient(srv), testLogger())
		_, err := svc.PutMonitor(ctx, &AlertingPutMonitorRequest{MonitorId: "mon1", Body: &AlertingMonitor{Name: "t"}})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("PutMonitor network error", func(t *testing.T) {
		svc := NewAlertingService(deadClient(), testLogger())
		_, err := svc.PutMonitor(ctx, &AlertingPutMonitorRequest{MonitorId: "mon1", Body: &AlertingMonitor{Name: "t"}})
		require.Error(t, err)
	})

	t.Run("PostMonitor server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		svc := NewAlertingService(restyClient(srv), testLogger())
		_, err := svc.PostMonitor(ctx, map[string]any{"name": "t"})
		require.Error(t, err)
	})

	t.Run("PostMonitor unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		svc := NewAlertingService(restyClient(srv), testLogger())
		_, err := svc.PostMonitor(ctx, map[string]any{"name": "t"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("PostMonitor network error", func(t *testing.T) {
		svc := NewAlertingService(deadClient(), testLogger())
		_, err := svc.PostMonitor(ctx, map[string]any{"name": "t"})
		require.Error(t, err)
	})

	t.Run("DeleteMonitor server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		svc := NewAlertingService(restyClient(srv), testLogger())
		_, err := svc.DeleteMonitor(ctx, "mon1")
		require.Error(t, err)
	})

	t.Run("DeleteMonitor unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		svc := NewAlertingService(restyClient(srv), testLogger())
		_, err := svc.DeleteMonitor(ctx, "mon1")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("DeleteMonitor network error", func(t *testing.T) {
		svc := NewAlertingService(deadClient(), testLogger())
		_, err := svc.DeleteMonitor(ctx, "mon1")
		require.Error(t, err)
	})

	t.Run("SearchMonitor server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		svc := NewAlertingService(restyClient(srv), testLogger())
		_, err := svc.SearchMonitor(ctx, map[string]any{})
		require.Error(t, err)
	})

	t.Run("SearchMonitor unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		svc := NewAlertingService(restyClient(srv), testLogger())
		_, err := svc.SearchMonitor(ctx, map[string]any{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("SearchMonitor network error", func(t *testing.T) {
		svc := NewAlertingService(deadClient(), testLogger())
		_, err := svc.SearchMonitor(ctx, map[string]any{})
		require.Error(t, err)
	})
}

// --- Error path tests for CatService ---

func TestCatServiceErrorPaths(t *testing.T) {
	ctx := context.Background()

	services := []struct {
		name string
		call func(svc CatService) error
	}{
		{"Indices", func(s CatService) error { _, err := s.Indices(ctx, nil); return err }},
		{"Indices with names", func(s CatService) error { _, err := s.Indices(ctx, []string{"idx"}); return err }},
		{"Shards", func(s CatService) error { _, err := s.Shards(ctx, nil); return err }},
		{"Shards with names", func(s CatService) error { _, err := s.Shards(ctx, []string{"idx"}); return err }},
		{"Aliases", func(s CatService) error { _, err := s.Aliases(ctx, nil); return err }},
		{"Aliases with names", func(s CatService) error { _, err := s.Aliases(ctx, []string{"a"}); return err }},
		{"Health", func(s CatService) error { _, err := s.Health(ctx); return err }},
		{"Count", func(s CatService) error { _, err := s.Count(ctx, nil); return err }},
		{"Count with indices", func(s CatService) error { _, err := s.Count(ctx, []string{"idx"}); return err }},
		{"Allocation", func(s CatService) error { _, err := s.Allocation(ctx, nil); return err }},
		{"Allocation with nodes", func(s CatService) error { _, err := s.Allocation(ctx, []string{"n1"}); return err }},
		{"Fielddata", func(s CatService) error { _, err := s.Fielddata(ctx, nil); return err }},
		{"Fielddata with fields", func(s CatService) error { _, err := s.Fielddata(ctx, []string{"f"}); return err }},
		{"Snapshots", func(s CatService) error { _, err := s.Snapshots(ctx, ""); return err }},
		{"Snapshots with repo", func(s CatService) error { _, err := s.Snapshots(ctx, "repo"); return err }},
		{"Master", func(s CatService) error { _, err := s.Master(ctx); return err }},
	}

	for _, tc := range services {
		t.Run(tc.name+" server error", func(t *testing.T) {
			srv := errServer(500)
			defer srv.Close()
			svc := NewCatService(restyClient(srv), testLogger())
			err := tc.call(svc)
			require.Error(t, err)
		})

		t.Run(tc.name+" unmarshal error", func(t *testing.T) {
			srv := badJSONServer()
			defer srv.Close()
			svc := NewCatService(restyClient(srv), testLogger())
			err := tc.call(svc)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "unmarshal")
		})

		t.Run(tc.name+" network error", func(t *testing.T) {
			svc := NewCatService(deadClient(), testLogger())
			err := tc.call(svc)
			require.Error(t, err)
		})
	}
}

// --- Error path tests for CcrService ---

func TestCcrServiceErrorPaths(t *testing.T) {
	ctx := context.Background()

	services := []struct {
		name string
		call func(svc CcrService) error
	}{
		{"AutoFollowStatus", func(s CcrService) error { _, err := s.AutoFollowStatus(ctx); return err }},
		{"PostAutoFollow", func(s CcrService) error {
			_, err := s.PostAutoFollow(ctx, &CcrAutoFollowRule{LeaderAlias: "l", Name: "n", Pattern: "p"})
			return err
		}},
		{"DeleteAutoFollow", func(s CcrService) error {
			_, err := s.DeleteAutoFollow(ctx, &CcrDeleteAutoFollowOptions{LeaderAlias: "l", Name: "n"})
			return err
		}},
		{"StartRule", func(s CcrService) error {
			_, err := s.StartRule(ctx, &CcrStartRuleRequest{Name: "r", Body: &CcrRule{LeaderAlias: "l", LeaderIndex: "i"}})
			return err
		}},
		{"StopRule", func(s CcrService) error { _, err := s.StopRule(ctx, "r"); return err }},
		{"PauseRule", func(s CcrService) error { _, err := s.PauseRule(ctx, "r"); return err }},
		{"ResumeRule", func(s CcrService) error { _, err := s.ResumeRule(ctx, "r"); return err }},
		{"StatusRule", func(s CcrService) error { _, err := s.StatusRule(ctx, "r"); return err }},
		{"FollowerStats", func(s CcrService) error { _, err := s.FollowerStats(ctx); return err }},
		{"LeaderStats", func(s CcrService) error { _, err := s.LeaderStats(ctx); return err }},
		{"UpdateRule", func(s CcrService) error { _, err := s.UpdateRule(ctx, "r", map[string]any{"pattern": "p"}); return err }},
	}

	for _, tc := range services {
		t.Run(tc.name+" server error", func(t *testing.T) {
			srv := errServer(500)
			defer srv.Close()
			svc := NewCcrService(restyClient(srv), testLogger())
			require.Error(t, tc.call(svc))
		})
		t.Run(tc.name+" unmarshal error", func(t *testing.T) {
			srv := badJSONServer()
			defer srv.Close()
			svc := NewCcrService(restyClient(srv), testLogger())
			err := tc.call(svc)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "unmarshal")
		})
		t.Run(tc.name+" network error", func(t *testing.T) {
			svc := NewCcrService(deadClient(), testLogger())
			require.Error(t, tc.call(svc))
		})
	}
}

// --- Error path tests for IngestService ---

func TestIngestServiceErrorPaths(t *testing.T) {
	ctx := context.Background()

	services := []struct {
		name string
		call func(svc IngestService) error
	}{
		{"PutPipeline", func(s IngestService) error {
			_, err := s.PutPipeline(ctx, &IngestPutPipelineRequest{Id: "p", Body: map[string]any{}})
			return err
		}},
		{"GetPipeline", func(s IngestService) error { _, err := s.GetPipeline(ctx, nil); return err }},
		{"GetPipeline with ids", func(s IngestService) error { _, err := s.GetPipeline(ctx, []string{"p"}); return err }},
		{"DeletePipeline", func(s IngestService) error { _, err := s.DeletePipeline(ctx, "p"); return err }},
		{"SimulatePipeline", func(s IngestService) error {
			_, err := s.SimulatePipeline(ctx, &IngestSimulatePipelineRequest{Body: map[string]any{}})
			return err
		}},
		{"SimulatePipeline with id", func(s IngestService) error {
			_, err := s.SimulatePipeline(ctx, &IngestSimulatePipelineRequest{Id: "p", Body: map[string]any{}})
			return err
		}},
	}

	for _, tc := range services {
		t.Run(tc.name+" server error", func(t *testing.T) {
			srv := errServer(500)
			defer srv.Close()
			svc := NewIngestService(restyClient(srv), testLogger())
			require.Error(t, tc.call(svc))
		})
		t.Run(tc.name+" unmarshal error", func(t *testing.T) {
			srv := badJSONServer()
			defer srv.Close()
			svc := NewIngestService(restyClient(srv), testLogger())
			err := tc.call(svc)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "unmarshal")
		})
		t.Run(tc.name+" network error", func(t *testing.T) {
			svc := NewIngestService(deadClient(), testLogger())
			require.Error(t, tc.call(svc))
		})
	}
}

// --- Error path tests for IsmService ---

func TestIsmServiceErrorPaths(t *testing.T) {
	ctx := context.Background()

	services := []struct {
		name string
		call func(svc IsmService) error
	}{
		{"GetPolicy", func(s IsmService) error { _, err := s.GetPolicy(ctx, "p"); return err }},
		{"PutPolicy", func(s IsmService) error {
			ds := "hot"
			_, err := s.PutPolicy(ctx, &IsmPutPolicyRequest{PolicyName: "p", Body: &IsmPolicyBase{DefaultState: &ds, States: []IsmPolicyState{{Name: "hot"}}}})
			return err
		}},
		{"DeletePolicy", func(s IsmService) error { _, err := s.DeletePolicy(ctx, "p"); return err }},
		{"ExplainPolicy", func(s IsmService) error { _, err := s.ExplainPolicy(ctx, "idx"); return err }},
		{"AddPolicy", func(s IsmService) error {
			_, err := s.AddPolicy(ctx, "idx", map[string]any{"policy_id": "p"})
			return err
		}},
		{"RemovePolicy", func(s IsmService) error { _, err := s.RemovePolicy(ctx, "idx"); return err }},
		{"ChangePolicy", func(s IsmService) error {
			_, err := s.ChangePolicy(ctx, "idx", map[string]any{"policy_id": "p"})
			return err
		}},
		{"RetryFailedIndex", func(s IsmService) error { _, err := s.RetryFailedIndex(ctx, "idx", nil); return err }},
		{"ListPolicies", func(s IsmService) error { _, err := s.ListPolicies(ctx); return err }},
	}

	for _, tc := range services {
		t.Run(tc.name+" server error", func(t *testing.T) {
			srv := errServer(500)
			defer srv.Close()
			svc := NewIsmService(restyClient(srv), testLogger())
			require.Error(t, tc.call(svc))
		})
		t.Run(tc.name+" unmarshal error", func(t *testing.T) {
			srv := badJSONServer()
			defer srv.Close()
			svc := NewIsmService(restyClient(srv), testLogger())
			err := tc.call(svc)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "unmarshal")
		})
		t.Run(tc.name+" network error", func(t *testing.T) {
			svc := NewIsmService(deadClient(), testLogger())
			require.Error(t, tc.call(svc))
		})
	}
}

// --- Error path tests for NodesService ---

func TestNodesServiceErrorPaths(t *testing.T) {
	ctx := context.Background()

	services := []struct {
		name string
		call func(svc NodesService) error
	}{
		{"Info", func(s NodesService) error { _, err := s.Info(ctx, &NodesInfoRequest{}); return err }},
		{"Info with nodes", func(s NodesService) error {
			_, err := s.Info(ctx, &NodesInfoRequest{NodeIds: []string{"n1"}})
			return err
		}},
		{"Info with metrics", func(s NodesService) error {
			_, err := s.Info(ctx, &NodesInfoRequest{Metrics: []string{"os"}})
			return err
		}},
		{"Stats all", func(s NodesService) error { _, err := s.Stats(ctx, &NodesStatsRequest{}); return err }},
		{"Stats with nodes", func(s NodesService) error {
			_, err := s.Stats(ctx, &NodesStatsRequest{NodeIds: []string{"n1"}})
			return err
		}},
		{"Stats with metrics", func(s NodesService) error {
			_, err := s.Stats(ctx, &NodesStatsRequest{Metrics: []string{"os"}})
			return err
		}},
		{"Stats with both", func(s NodesService) error {
			_, err := s.Stats(ctx, &NodesStatsRequest{NodeIds: []string{"n1"}, Metrics: []string{"os"}})
			return err
		}},
	}

	for _, tc := range services {
		t.Run(tc.name+" server error", func(t *testing.T) {
			srv := errServer(500)
			defer srv.Close()
			svc := NewNodesService(restyClient(srv), testLogger())
			require.Error(t, tc.call(svc))
		})
		t.Run(tc.name+" unmarshal error", func(t *testing.T) {
			srv := badJSONServer()
			defer srv.Close()
			svc := NewNodesService(restyClient(srv), testLogger())
			err := tc.call(svc)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "unmarshal")
		})
		t.Run(tc.name+" network error", func(t *testing.T) {
			svc := NewNodesService(deadClient(), testLogger())
			require.Error(t, tc.call(svc))
		})
	}
}

// --- Error path tests for ScriptService ---

func TestScriptServiceErrorPaths(t *testing.T) {
	ctx := context.Background()

	services := []struct {
		name string
		call func(svc ScriptService) error
	}{
		{"Get", func(s ScriptService) error { _, err := s.Get(ctx, "s1"); return err }},
		{"Put", func(s ScriptService) error {
			_, err := s.Put(ctx, &ScriptPutRequest{Id: "s1", Body: map[string]any{"script": map[string]any{"source": "return 1;"}}})
			return err
		}},
		{"Delete", func(s ScriptService) error { _, err := s.Delete(ctx, "s1"); return err }},
	}

	for _, tc := range services {
		t.Run(tc.name+" server error", func(t *testing.T) {
			srv := errServer(500)
			defer srv.Close()
			svc := NewScriptService(restyClient(srv), testLogger())
			require.Error(t, tc.call(svc))
		})
		t.Run(tc.name+" unmarshal error", func(t *testing.T) {
			srv := badJSONServer()
			defer srv.Close()
			svc := NewScriptService(restyClient(srv), testLogger())
			err := tc.call(svc)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "unmarshal")
		})
		t.Run(tc.name+" network error", func(t *testing.T) {
			svc := NewScriptService(deadClient(), testLogger())
			require.Error(t, tc.call(svc))
		})
	}
}

// --- Error path tests for SecurityService ---

func TestSecurityServiceErrorPaths(t *testing.T) {
	ctx := context.Background()

	services := []struct {
		name string
		call func(svc SecurityService) error
	}{
		{"GetRole", func(s SecurityService) error { _, err := s.GetRole(ctx, "r"); return err }},
		{"PutRole", func(s SecurityService) error { _, err := s.PutRole(ctx, "r", &SecurityPutRole{}); return err }},
		{"DeleteRole", func(s SecurityService) error { _, err := s.DeleteRole(ctx, "r"); return err }},
		{"GetRoleMapping", func(s SecurityService) error { _, err := s.GetRoleMapping(ctx, "r"); return err }},
		{"PutRoleMapping", func(s SecurityService) error {
			_, err := s.PutRoleMapping(ctx, "r", &SecurityPutRoleMapping{})
			return err
		}},
		{"DeleteRoleMapping", func(s SecurityService) error { _, err := s.DeleteRoleMapping(ctx, "r"); return err }},
		{"GetUser", func(s SecurityService) error { _, err := s.GetUser(ctx, "u"); return err }},
		{"PutUser", func(s SecurityService) error { _, err := s.PutUser(ctx, "u", &SecurityPutUser{}); return err }},
		{"DeleteUser", func(s SecurityService) error { _, err := s.DeleteUser(ctx, "u"); return err }},
		{"GetActionGroup", func(s SecurityService) error { _, err := s.GetActionGroup(ctx, "a"); return err }},
		{"PutActionGroup", func(s SecurityService) error {
			_, err := s.PutActionGroup(ctx, "a", &SecurityPutActionGroup{})
			return err
		}},
		{"DeleteActionGroup", func(s SecurityService) error { _, err := s.DeleteActionGroup(ctx, "a"); return err }},
		{"GetTenant", func(s SecurityService) error { _, err := s.GetTenant(ctx, "t"); return err }},
		{"PutTenant", func(s SecurityService) error { _, err := s.PutTenant(ctx, "t", &SecurityPutTenant{}); return err }},
		{"DeleteTenant", func(s SecurityService) error { _, err := s.DeleteTenant(ctx, "t"); return err }},
		{"GetDistinguishedName", func(s SecurityService) error { _, err := s.GetDistinguishedName(ctx, "d"); return err }},
		{"PutDistinguishedName", func(s SecurityService) error {
			_, err := s.PutDistinguishedName(ctx, "d", &SecurityDistinguishedName{})
			return err
		}},
		{"DeleteDistinguishedName", func(s SecurityService) error { _, err := s.DeleteDistinguishedName(ctx, "d"); return err }},
		{"FlushCache", func(s SecurityService) error { _, err := s.FlushCache(ctx); return err }},
		{"AuthInfo", func(s SecurityService) error { _, err := s.AuthInfo(ctx); return err }},
		{"GetConfig", func(s SecurityService) error { _, err := s.GetConfig(ctx); return err }},
		{"PutConfig", func(s SecurityService) error { _, err := s.PutConfig(ctx, map[string]any{}); return err }},
		{"GetAudit", func(s SecurityService) error { _, err := s.GetAudit(ctx); return err }},
		{"PutAudit", func(s SecurityService) error { _, err := s.PutAudit(ctx, map[string]any{}); return err }},
	}

	for _, tc := range services {
		t.Run(tc.name+" server error", func(t *testing.T) {
			srv := errServer(500)
			defer srv.Close()
			svc := NewSecurityService(restyClient(srv), testLogger())
			require.Error(t, tc.call(svc))
		})
		t.Run(tc.name+" unmarshal error", func(t *testing.T) {
			srv := badJSONServer()
			defer srv.Close()
			svc := NewSecurityService(restyClient(srv), testLogger())
			err := tc.call(svc)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "unmarshal")
		})
		t.Run(tc.name+" network error", func(t *testing.T) {
			svc := NewSecurityService(deadClient(), testLogger())
			require.Error(t, tc.call(svc))
		})
	}
}

// --- Error path tests for SmService ---

func TestSmServiceErrorPaths(t *testing.T) {
	ctx := context.Background()

	services := []struct {
		name string
		call func(svc SmService) error
	}{
		{"GetPolicy", func(s SmService) error { _, err := s.GetPolicy(ctx, "p"); return err }},
		{"PutPolicy", func(s SmService) error {
			_, err := s.PutPolicy(ctx, &SmPutPolicyRequest{
				PolicyName: "p",
				Body:       &SmPutPolicy{SnapshotConfig: SmPolicySnapshotConfig{Repository: "r"}, Creation: SmPolicyCreation{Schedule: map[string]any{}}},
			})
			return err
		}},
		{"PostPolicy", func(s SmService) error {
			_, err := s.PostPolicy(ctx, "p", &SmPutPolicy{SnapshotConfig: SmPolicySnapshotConfig{Repository: "r"}})
			return err
		}},
		{"DeletePolicy", func(s SmService) error { _, err := s.DeletePolicy(ctx, "p"); return err }},
		{"ExplainPolicy", func(s SmService) error { _, err := s.ExplainPolicy(ctx, []string{"p"}); return err }},
		{"StartPolicy", func(s SmService) error { _, err := s.StartPolicy(ctx, "p"); return err }},
		{"StopPolicy", func(s SmService) error { _, err := s.StopPolicy(ctx, "p"); return err }},
		{"ListPolicies", func(s SmService) error { _, err := s.ListPolicies(ctx); return err }},
	}

	for _, tc := range services {
		t.Run(tc.name+" server error", func(t *testing.T) {
			srv := errServer(500)
			defer srv.Close()
			svc := NewSmService(restyClient(srv), testLogger())
			require.Error(t, tc.call(svc))
		})
		t.Run(tc.name+" unmarshal error", func(t *testing.T) {
			srv := badJSONServer()
			defer srv.Close()
			svc := NewSmService(restyClient(srv), testLogger())
			err := tc.call(svc)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "unmarshal")
		})
		t.Run(tc.name+" network error", func(t *testing.T) {
			svc := NewSmService(deadClient(), testLogger())
			require.Error(t, tc.call(svc))
		})
	}
}

// --- Error path tests for SnapshotService ---

func TestSnapshotServiceErrorPaths(t *testing.T) {
	ctx := context.Background()

	services := []struct {
		name string
		call func(svc SnapshotService) error
	}{
		{"Create", func(s SnapshotService) error {
			_, err := s.Create(ctx, &SnapshotCreateRequest{Repository: "r", Snapshot: "s"})
			return err
		}},
		{"Create with body", func(s SnapshotService) error {
			_, err := s.Create(ctx, &SnapshotCreateRequest{Repository: "r", Snapshot: "s", Body: map[string]any{}})
			return err
		}},
		{"Get", func(s SnapshotService) error {
			_, err := s.Get(ctx, &SnapshotGetRequest{Repository: "r"})
			return err
		}},
		{"Get with snapshots", func(s SnapshotService) error {
			_, err := s.Get(ctx, &SnapshotGetRequest{Repository: "r", Snapshots: []string{"s"}})
			return err
		}},
		{"Delete", func(s SnapshotService) error {
			_, err := s.Delete(ctx, &SnapshotDeleteRequest{Repository: "r", Snapshot: "s"})
			return err
		}},
		{"Status", func(s SnapshotService) error {
			_, err := s.Status(ctx, &SnapshotStatusRequest{Repository: "r"})
			return err
		}},
		{"Status with snapshots", func(s SnapshotService) error {
			_, err := s.Status(ctx, &SnapshotStatusRequest{Repository: "r", Snapshots: []string{"s"}})
			return err
		}},
		{"Restore", func(s SnapshotService) error {
			_, err := s.Restore(ctx, &SnapshotRestoreRequest{Repository: "r", Snapshot: "s"})
			return err
		}},
		{"Restore with body", func(s SnapshotService) error {
			_, err := s.Restore(ctx, &SnapshotRestoreRequest{Repository: "r", Snapshot: "s", Body: map[string]any{}})
			return err
		}},
		{"CreateRepository", func(s SnapshotService) error {
			_, err := s.CreateRepository(ctx, "r", map[string]any{})
			return err
		}},
		{"GetRepository", func(s SnapshotService) error { _, err := s.GetRepository(ctx, nil); return err }},
		{"GetRepository with names", func(s SnapshotService) error { _, err := s.GetRepository(ctx, []string{"r"}); return err }},
		{"DeleteRepository", func(s SnapshotService) error { _, err := s.DeleteRepository(ctx, "r"); return err }},
		{"VerifyRepository", func(s SnapshotService) error { _, err := s.VerifyRepository(ctx, "r"); return err }},
	}

	for _, tc := range services {
		t.Run(tc.name+" server error", func(t *testing.T) {
			srv := errServer(500)
			defer srv.Close()
			svc := NewSnapshotService(restyClient(srv), testLogger())
			require.Error(t, tc.call(svc))
		})
		t.Run(tc.name+" unmarshal error", func(t *testing.T) {
			srv := badJSONServer()
			defer srv.Close()
			svc := NewSnapshotService(restyClient(srv), testLogger())
			err := tc.call(svc)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "unmarshal")
		})
		t.Run(tc.name+" network error", func(t *testing.T) {
			svc := NewSnapshotService(deadClient(), testLogger())
			require.Error(t, tc.call(svc))
		})
	}
}

// --- Error path tests for TasksService ---

func TestTasksServiceErrorPaths(t *testing.T) {
	ctx := context.Background()

	services := []struct {
		name string
		call func(svc TasksService) error
	}{
		{"List", func(s TasksService) error { _, err := s.List(ctx); return err }},
		{"Get", func(s TasksService) error { _, err := s.Get(ctx, "n1:100"); return err }},
		{"Cancel", func(s TasksService) error { _, err := s.Cancel(ctx, "n1:100"); return err }},
	}

	for _, tc := range services {
		t.Run(tc.name+" server error", func(t *testing.T) {
			srv := errServer(500)
			defer srv.Close()
			svc := NewTasksService(restyClient(srv), testLogger())
			require.Error(t, tc.call(svc))
		})
		t.Run(tc.name+" unmarshal error", func(t *testing.T) {
			srv := badJSONServer()
			defer srv.Close()
			svc := NewTasksService(restyClient(srv), testLogger())
			err := tc.call(svc)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "unmarshal")
		})
		t.Run(tc.name+" network error", func(t *testing.T) {
			svc := NewTasksService(deadClient(), testLogger())
			require.Error(t, tc.call(svc))
		})
	}
}

// --- Error path tests for TransformService ---

func TestTransformServiceErrorPaths(t *testing.T) {
	ctx := context.Background()

	services := []struct {
		name string
		call func(svc TransformService) error
	}{
		{"GetJob", func(s TransformService) error { _, err := s.GetJob(ctx, "j"); return err }},
		{"PutJob", func(s TransformService) error {
			desc := "d"
			_, err := s.PutJob(ctx, &TransformPutJobRequest{
				JobName: "j",
				Body:    &TransformJobBase{SourceIndex: "s", TargetIndex: "t", Description: &desc, Schedule: map[string]any{}},
			})
			return err
		}},
		{"DeleteJob", func(s TransformService) error { _, err := s.DeleteJob(ctx, "j"); return err }},
		{"SearchJob", func(s TransformService) error { _, err := s.SearchJob(ctx, map[string]any{}); return err }},
		{"ExplainJob", func(s TransformService) error { _, err := s.ExplainJob(ctx, "j"); return err }},
		{"PreviewJobResults", func(s TransformService) error { _, err := s.PreviewJobResults(ctx, map[string]any{}); return err }},
		{"StartJob", func(s TransformService) error { _, err := s.StartJob(ctx, "j"); return err }},
		{"StopJob", func(s TransformService) error { _, err := s.StopJob(ctx, "j"); return err }},
	}

	for _, tc := range services {
		t.Run(tc.name+" server error", func(t *testing.T) {
			srv := errServer(500)
			defer srv.Close()
			svc := NewTransformService(restyClient(srv), testLogger())
			require.Error(t, tc.call(svc))
		})
		t.Run(tc.name+" unmarshal error", func(t *testing.T) {
			srv := badJSONServer()
			defer srv.Close()
			svc := NewTransformService(restyClient(srv), testLogger())
			err := tc.call(svc)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "unmarshal")
		})
		t.Run(tc.name+" network error", func(t *testing.T) {
			svc := NewTransformService(deadClient(), testLogger())
			require.Error(t, tc.call(svc))
		})
	}
}

// --- Edge cases for errors.go ---

func TestParseErrorResponseZeroStatus(t *testing.T) {
	srv := zeroStatusServer()
	defer srv.Close()

	client := resty.New().SetBaseURL(srv.URL)
	resp, err := client.R().Get("/test")
	require.NoError(t, err)
	require.Equal(t, 500, resp.StatusCode())

	osErr := parseErrorResponse(resp)
	require.NotNil(t, osErr)
}

// TestParseErrorResponse504 verifies that an HTTP 504 Gateway Timeout
// response (the new OpenSearchTimeoutException status in OpenSearch 3.8.0,
// PR #22064) is parsed into an *types.OpenSearchError with StatusCode() == 504.
func TestParseErrorResponse504(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusGatewayTimeout)
		_, _ = fmt.Fprint(w, `{"error":{"type":"timeout_exception","reason":"request timed out"},"status":504}`)
	}))
	defer srv.Close()

	client := resty.New().SetBaseURL(srv.URL)
	resp, err := client.R().Get("/test")
	require.NoError(t, err)
	require.Equal(t, http.StatusGatewayTimeout, resp.StatusCode())

	osErr := parseErrorResponse(resp)
	require.NotNil(t, osErr)
	osError, ok := osErr.(*types.OpenSearchError)
	require.True(t, ok)
	assert.Equal(t, 504, osError.StatusCode())
}

func TestWrapUnmarshalErrorLongBody(t *testing.T) {
	srv := longBodyServer()
	defer srv.Close()

	client := resty.New().SetBaseURL(srv.URL)
	resp, err := client.R().Get("/test")
	require.NoError(t, err)

	logger := testLogger()
	resultErr := wrapUnmarshalError(logger, resp, fmt.Errorf("bad json"))
	require.Error(t, resultErr)
	assert.Contains(t, resultErr.Error(), "unmarshal response from GET")
	assert.Contains(t, resultErr.Error(), "bad json")
}

func TestIngestServiceValidationErrors(t *testing.T) {
	ctx := context.Background()
	svc := NewIngestService(deadClient(), testLogger())

	t.Run("PutPipeline empty id", func(t *testing.T) {
		_, err := svc.PutPipeline(ctx, &IngestPutPipelineRequest{Body: map[string]any{}})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "validation")
	})

	t.Run("DeletePipeline empty id", func(t *testing.T) {
		_, err := svc.DeletePipeline(ctx, "")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "pipeline id is required")
	})

	t.Run("SimulatePipeline nil body", func(t *testing.T) {
		_, err := svc.SimulatePipeline(ctx, &IngestSimulatePipelineRequest{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "validation")
	})
}

func TestScriptServiceValidationErrors(t *testing.T) {
	ctx := context.Background()
	svc := NewScriptService(deadClient(), testLogger())

	t.Run("Get empty id", func(t *testing.T) {
		_, err := svc.Get(ctx, "")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "script id is required")
	})

	t.Run("Put validation error", func(t *testing.T) {
		_, err := svc.Put(ctx, &ScriptPutRequest{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "validation")
	})

	t.Run("Delete empty id", func(t *testing.T) {
		_, err := svc.Delete(ctx, "")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "script id is required")
	})
}

func TestSnapshotServiceValidationErrors(t *testing.T) {
	ctx := context.Background()
	svc := NewSnapshotService(deadClient(), testLogger())

	t.Run("Create validation error", func(t *testing.T) {
		_, err := svc.Create(ctx, &SnapshotCreateRequest{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "validation")
	})

	t.Run("Get validation error", func(t *testing.T) {
		_, err := svc.Get(ctx, &SnapshotGetRequest{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "validation")
	})

	t.Run("Delete validation error", func(t *testing.T) {
		_, err := svc.Delete(ctx, &SnapshotDeleteRequest{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "validation")
	})

	t.Run("Status validation error", func(t *testing.T) {
		_, err := svc.Status(ctx, &SnapshotStatusRequest{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "validation")
	})

	t.Run("Restore validation error", func(t *testing.T) {
		_, err := svc.Restore(ctx, &SnapshotRestoreRequest{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "validation")
	})

	t.Run("CreateRepository empty repository", func(t *testing.T) {
		_, err := svc.CreateRepository(ctx, "", map[string]any{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "repository is required")
	})

	t.Run("CreateRepository nil body", func(t *testing.T) {
		_, err := svc.CreateRepository(ctx, "repo", nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "body is required")
	})

	t.Run("DeleteRepository empty", func(t *testing.T) {
		_, err := svc.DeleteRepository(ctx, "")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "repository is required")
	})

	t.Run("VerifyRepository empty", func(t *testing.T) {
		_, err := svc.VerifyRepository(ctx, "")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "repository is required")
	})
}

func TestTasksServiceValidationErrors(t *testing.T) {
	ctx := context.Background()
	svc := NewTasksService(deadClient(), testLogger())

	t.Run("Get empty id", func(t *testing.T) {
		_, err := svc.Get(ctx, "")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "task id is required")
	})

	t.Run("Cancel empty id", func(t *testing.T) {
		_, err := svc.Cancel(ctx, "")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "task id is required")
	})
}
