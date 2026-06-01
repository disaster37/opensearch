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

func newAlertingTestServer() *httptest.Server {
	mux := http.NewServeMux()

	mux.HandleFunc("/_plugins/_alerting/monitors/test_monitor", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodGet:
			w.Write([]byte(`{"_id":"test","_version":1,"_seq_no":0,"_primary_term":1,"monitor":{"type":"monitor","name":"test","monitor_type":"query_level_monitor","schedule":{},"inputs":[],"triggers":[]}}`))
		case http.MethodPut:
			w.Write([]byte(`{"_id":"test","_version":2,"_seq_no":1,"_primary_term":1,"monitor":{"type":"monitor","name":"test","monitor_type":"query_level_monitor","schedule":{},"inputs":[],"triggers":[]}}`))
		case http.MethodDelete:
			w.Write([]byte(`{"_index":".opendistro-alerting-config","_id":"test_monitor","_version":2,"result":"deleted"}`))
		}
	})

	mux.HandleFunc("/_plugins/_alerting/monitors", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodPost {
			w.Write([]byte(`{"_id":"new_monitor","_version":1,"_seq_no":0,"_primary_term":1,"monitor":{"type":"monitor","name":"test","monitor_type":"query_level_monitor","schedule":{},"inputs":[],"triggers":[]}}`))
		}
	})

	mux.HandleFunc("/_plugins/_alerting/monitors/_search", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"hits":{"hits":[{"_id":"test","_source":{"type":"monitor","name":"test","monitor_type":"query_level_monitor","schedule":{},"inputs":[],"triggers":[]}}]}}`))
	})

	mux.HandleFunc("/_plugins/_alerting/monitors/test_monitor/_execute", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"monitor_name":"test","period_start":"2024-01-01","period_end":"2024-01-02","input_results":{},"trigger_results":{}}`))
	})

	mux.HandleFunc("/_plugins/_alerting/monitors/test_monitor/_acknowledge/alerts", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"missing_alert_ids":[],"failed_alert_ids":[],"acknowledged_alerts":[{"_id":"alert1","_version":1}]}`))
	})

	mux.HandleFunc("/_plugins/_alerting/monitors/alerts", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"totalAlerts":1,"alerts":[{"id":"alert1","monitor_id":"mon1","monitor_name":"test","state":"ACTIVE"}]}`))
	})

	mux.HandleFunc("/_plugins/_alerting/monitors/findings/_search", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"total_findings":1,"findings":[{"id":"finding1","index":"test-index","monitor_id":"mon1","monitor_name":"test"}]}`))
	})

	mux.HandleFunc("/_plugins/_alerting/destinations", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"totalDestinations":1,"destinations":[{"id":"dest1","name":"slack","type":"slack"}]}`))
	})

	mux.HandleFunc("/_plugins/_alerting/destinations/test_dest", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"totalDestinations":1,"destinations":[{"id":"test_dest","name":"test","type":"slack"}]}`))
	})

	mux.HandleFunc("/_plugins/_alerting/workflows", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodPost {
			w.Write([]byte(`{"_id":"new_workflow","_version":1,"_seq_no":0,"_primary_term":1,"workflow":{"name":"test"}}`))
		}
	})

	mux.HandleFunc("/_plugins/_alerting/workflows/test_workflow", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodGet:
			w.Write([]byte(`{"_id":"test_workflow","_version":1,"_seq_no":0,"_primary_term":1,"workflow":{"name":"test"}}`))
		case http.MethodPut:
			w.Write([]byte(`{"_id":"test_workflow","_version":2,"_seq_no":1,"_primary_term":1,"workflow":{"name":"test"}}`))
		case http.MethodDelete:
			w.Write([]byte(`{"_index":".opendistro-alerting-config","_id":"test_workflow","_version":2,"result":"deleted"}`))
		}
	})

	mux.HandleFunc("/_plugins/_alerting/workflows/test_workflow/_execute", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"execution_id":"exec1"}`))
	})

	mux.HandleFunc("/_plugins/_alerting/workflows/alerts", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"totalAlerts":1,"alerts":[{"id":"alert1","monitor_id":"mon1","monitor_name":"test","state":"ACTIVE"}]}`))
	})

	mux.HandleFunc("/_plugins/_alerting/workflows/test_workflow/_acknowledge/alerts", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"missing_alert_ids":[],"failed_alert_ids":[],"acknowledged_alerts":[{"_id":"alert1","_version":1}]}`))
	})

	return httptest.NewServer(mux)
}

func TestUnitAlertingService_GetMonitor(t *testing.T) {
	srv := newAlertingTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewAlertingService(restyClient(srv), testLogger())

	resp, err := svc.GetMonitor(ctx, "test_monitor")
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "test", resp.Id)
	assert.Equal(t, "test", resp.Monitor.Name)
}

func TestUnitAlertingService_PutMonitor(t *testing.T) {
	srv := newAlertingTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewAlertingService(restyClient(srv), testLogger())

	resp, err := svc.PutMonitor(ctx, &AlertingPutMonitorRequest{
		MonitorId: "test_monitor",
		Body: &AlertingMonitor{
			Type:        "monitor",
			Name:        "test",
			MonitorType: "query_level_monitor",
			Schedule:    map[string]any{"period": map[string]any{"interval": 5, "unit": "MINUTES"}},
			Inputs:      []map[string]any{},
			Triggers:    []map[string]any{},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, int64(2), resp.Version)
}

func TestUnitAlertingService_PutMonitorWithVersion(t *testing.T) {
	srv := newAlertingTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewAlertingService(restyClient(srv), testLogger())

	seqNo := int64(0)
	primaryTerm := int64(1)
	resp, err := svc.PutMonitor(ctx, &AlertingPutMonitorRequest{
		MonitorId: "test_monitor",
		Body:      &AlertingMonitor{Name: "test"},
		Version: &types.DocumentVersion{
			SeqNo:       &seqNo,
			PrimaryTerm: &primaryTerm,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
}

func TestUnitAlertingService_PostMonitor(t *testing.T) {
	srv := newAlertingTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewAlertingService(restyClient(srv), testLogger())

	resp, err := svc.PostMonitor(ctx, &AlertingMonitor{
		Type:        "monitor",
		Name:        "test",
		MonitorType: "query_level_monitor",
		Schedule:    map[string]any{},
		Inputs:      []map[string]any{},
		Triggers:    []map[string]any{},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "new_monitor", resp.Id)
}

func TestUnitAlertingService_DeleteMonitor(t *testing.T) {
	srv := newAlertingTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewAlertingService(restyClient(srv), testLogger())

	resp, err := svc.DeleteMonitor(ctx, "test_monitor")
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "deleted", *resp.Result)
}

func TestUnitAlertingService_SearchMonitor(t *testing.T) {
	srv := newAlertingTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewAlertingService(restyClient(srv), testLogger())

	hits, err := svc.SearchMonitor(ctx, map[string]any{
		"query": map[string]any{"match_all": map[string]any{}},
	})
	require.NoError(t, err)
	require.NotNil(t, hits)
	assert.Len(t, hits, 1)
	assert.Equal(t, "test", hits[0].Id)
}

func TestUnitAlertingService_EmptyMonitorIdErrors(t *testing.T) {
	srv := newAlertingTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewAlertingService(restyClient(srv), testLogger())

	_, err := svc.GetMonitor(ctx, "")
	assert.Error(t, err)

	_, err = svc.DeleteMonitor(ctx, "")
	assert.Error(t, err)
}

func TestUnitAlertingService_PostMonitorNilBodyError(t *testing.T) {
	ctx := context.Background()
	svc := NewAlertingService(deadClient(), testLogger())

	_, err := svc.PostMonitor(ctx, nil)
	assert.Error(t, err)
}

func TestUnitAlertingService_PutMonitorValidationError(t *testing.T) {
	ctx := context.Background()
	svc := NewAlertingService(deadClient(), testLogger())

	_, err := svc.PutMonitor(ctx, &AlertingPutMonitorRequest{})
	assert.Error(t, err)
}

func TestUnitAlertingService_GetMonitor_ServerError(t *testing.T) {
	srv := errServer(500)
	defer srv.Close()
	ctx := context.Background()
	svc := NewAlertingService(restyClient(srv), testLogger())

	_, err := svc.GetMonitor(ctx, "test_monitor")
	require.Error(t, err)
}

func TestUnitAlertingService_GetMonitor_BadJSON(t *testing.T) {
	srv := badJSONServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewAlertingService(restyClient(srv), testLogger())

	_, err := svc.GetMonitor(ctx, "test_monitor")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}

func TestUnitAlertingService_GetMonitor_NetworkError(t *testing.T) {
	ctx := context.Background()
	svc := NewAlertingService(deadClient(), testLogger())

	_, err := svc.GetMonitor(ctx, "test_monitor")
	require.Error(t, err)
}

func TestUnitAlertingService_PutMonitor_ServerError(t *testing.T) {
	srv := errServer(500)
	defer srv.Close()
	ctx := context.Background()
	svc := NewAlertingService(restyClient(srv), testLogger())

	_, err := svc.PutMonitor(ctx, &AlertingPutMonitorRequest{MonitorId: "test", Body: map[string]any{}})
	require.Error(t, err)
}

func TestUnitAlertingService_PutMonitor_BadJSON(t *testing.T) {
	srv := badJSONServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewAlertingService(restyClient(srv), testLogger())

	_, err := svc.PutMonitor(ctx, &AlertingPutMonitorRequest{MonitorId: "test", Body: map[string]any{}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}

func TestUnitAlertingService_PutMonitor_NetworkError(t *testing.T) {
	ctx := context.Background()
	svc := NewAlertingService(deadClient(), testLogger())

	_, err := svc.PutMonitor(ctx, &AlertingPutMonitorRequest{MonitorId: "test", Body: map[string]any{}})
	require.Error(t, err)
}

func TestUnitAlertingService_PostMonitor_ServerError(t *testing.T) {
	srv := errServer(500)
	defer srv.Close()
	ctx := context.Background()
	svc := NewAlertingService(restyClient(srv), testLogger())

	_, err := svc.PostMonitor(ctx, map[string]any{"name": "test"})
	require.Error(t, err)
}

func TestUnitAlertingService_PostMonitor_BadJSON(t *testing.T) {
	srv := badJSONServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewAlertingService(restyClient(srv), testLogger())

	_, err := svc.PostMonitor(ctx, map[string]any{"name": "test"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}

func TestUnitAlertingService_PostMonitor_NetworkError(t *testing.T) {
	ctx := context.Background()
	svc := NewAlertingService(deadClient(), testLogger())

	_, err := svc.PostMonitor(ctx, map[string]any{"name": "test"})
	require.Error(t, err)
}

func TestUnitAlertingService_DeleteMonitor_ServerError(t *testing.T) {
	srv := errServer(500)
	defer srv.Close()
	ctx := context.Background()
	svc := NewAlertingService(restyClient(srv), testLogger())

	_, err := svc.DeleteMonitor(ctx, "test_monitor")
	require.Error(t, err)
}

func TestUnitAlertingService_DeleteMonitor_BadJSON(t *testing.T) {
	srv := badJSONServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewAlertingService(restyClient(srv), testLogger())

	_, err := svc.DeleteMonitor(ctx, "test_monitor")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}

func TestUnitAlertingService_DeleteMonitor_NetworkError(t *testing.T) {
	ctx := context.Background()
	svc := NewAlertingService(deadClient(), testLogger())

	_, err := svc.DeleteMonitor(ctx, "test_monitor")
	require.Error(t, err)
}

func TestUnitAlertingService_SearchMonitor_ServerError(t *testing.T) {
	srv := errServer(500)
	defer srv.Close()
	ctx := context.Background()
	svc := NewAlertingService(restyClient(srv), testLogger())

	_, err := svc.SearchMonitor(ctx, map[string]any{})
	require.Error(t, err)
}

func TestUnitAlertingService_SearchMonitor_BadJSON(t *testing.T) {
	srv := badJSONServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewAlertingService(restyClient(srv), testLogger())

	_, err := svc.SearchMonitor(ctx, map[string]any{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}

func TestUnitAlertingService_SearchMonitor_NetworkError(t *testing.T) {
	ctx := context.Background()
	svc := NewAlertingService(deadClient(), testLogger())

	_, err := svc.SearchMonitor(ctx, map[string]any{})
	require.Error(t, err)
}

// --- ExecuteMonitor ---

func TestUnitAlertingService_ExecuteMonitor(t *testing.T) {
	srv := newAlertingTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewAlertingService(restyClient(srv), testLogger())

	resp, err := svc.ExecuteMonitor(ctx, "test_monitor", nil)
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "test", resp.MonitorName)
	assert.Equal(t, "2024-01-01", resp.PeriodStart)
}

func TestUnitAlertingService_ExecuteMonitorWithBody(t *testing.T) {
	srv := newAlertingTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewAlertingService(restyClient(srv), testLogger())

	resp, err := svc.ExecuteMonitor(ctx, "test_monitor", map[string]any{"trigger_overrides": []any{}})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "test", resp.MonitorName)
}

func TestUnitAlertingService_ExecuteMonitor_EmptyId(t *testing.T) {
	ctx := context.Background()
	svc := NewAlertingService(deadClient(), testLogger())

	_, err := svc.ExecuteMonitor(ctx, "", nil)
	require.Error(t, err)
}

func TestUnitAlertingService_ExecuteMonitor_ServerError(t *testing.T) {
	srv := errServer(500)
	defer srv.Close()
	ctx := context.Background()
	svc := NewAlertingService(restyClient(srv), testLogger())

	_, err := svc.ExecuteMonitor(ctx, "test", nil)
	require.Error(t, err)
}

func TestUnitAlertingService_ExecuteMonitor_BadJSON(t *testing.T) {
	srv := badJSONServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewAlertingService(restyClient(srv), testLogger())

	_, err := svc.ExecuteMonitor(ctx, "test", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}

func TestUnitAlertingService_ExecuteMonitor_NetworkError(t *testing.T) {
	ctx := context.Background()
	svc := NewAlertingService(deadClient(), testLogger())

	_, err := svc.ExecuteMonitor(ctx, "test", nil)
	require.Error(t, err)
}

// --- AcknowledgeAlert ---

func TestUnitAlertingService_AcknowledgeAlert(t *testing.T) {
	srv := newAlertingTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewAlertingService(restyClient(srv), testLogger())

	resp, err := svc.AcknowledgeAlert(ctx, "test_monitor", map[string]any{"alerts": []string{"alert1"}})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Len(t, resp.AcknowledgedAlertIds, 1)
	assert.Equal(t, "alert1", resp.AcknowledgedAlertIds[0].Id)
}

func TestUnitAlertingService_AcknowledgeAlert_EmptyMonitorId(t *testing.T) {
	ctx := context.Background()
	svc := NewAlertingService(deadClient(), testLogger())

	_, err := svc.AcknowledgeAlert(ctx, "", map[string]any{})
	require.Error(t, err)
}

func TestUnitAlertingService_AcknowledgeAlert_NilBody(t *testing.T) {
	ctx := context.Background()
	svc := NewAlertingService(deadClient(), testLogger())

	_, err := svc.AcknowledgeAlert(ctx, "test_monitor", nil)
	require.Error(t, err)
}

func TestUnitAlertingService_AcknowledgeAlert_ServerError(t *testing.T) {
	srv := errServer(500)
	defer srv.Close()
	ctx := context.Background()
	svc := NewAlertingService(restyClient(srv), testLogger())

	_, err := svc.AcknowledgeAlert(ctx, "test", map[string]any{"alerts": []string{"a"}})
	require.Error(t, err)
}

func TestUnitAlertingService_AcknowledgeAlert_BadJSON(t *testing.T) {
	srv := badJSONServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewAlertingService(restyClient(srv), testLogger())

	_, err := svc.AcknowledgeAlert(ctx, "test", map[string]any{"alerts": []string{"a"}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}

func TestUnitAlertingService_AcknowledgeAlert_NetworkError(t *testing.T) {
	ctx := context.Background()
	svc := NewAlertingService(deadClient(), testLogger())

	_, err := svc.AcknowledgeAlert(ctx, "test", map[string]any{"alerts": []string{"a"}})
	require.Error(t, err)
}

// --- GetAlerts ---

func TestUnitAlertingService_GetAlerts(t *testing.T) {
	srv := newAlertingTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewAlertingService(restyClient(srv), testLogger())

	resp, err := svc.GetAlerts(ctx, nil)
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, 1, resp.TotalAlerts)
	assert.Len(t, resp.Alerts, 1)
	assert.Equal(t, "alert1", resp.Alerts[0].Id)
}

func TestUnitAlertingService_GetAlertsWithParams(t *testing.T) {
	srv := newAlertingTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewAlertingService(restyClient(srv), testLogger())

	resp, err := svc.GetAlerts(ctx, map[string]string{"size": "10", "sortOrder": "desc"})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, 1, resp.TotalAlerts)
}

func TestUnitAlertingService_GetAlerts_ServerError(t *testing.T) {
	srv := errServer(500)
	defer srv.Close()
	ctx := context.Background()
	svc := NewAlertingService(restyClient(srv), testLogger())

	_, err := svc.GetAlerts(ctx, nil)
	require.Error(t, err)
}

func TestUnitAlertingService_GetAlerts_BadJSON(t *testing.T) {
	srv := badJSONServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewAlertingService(restyClient(srv), testLogger())

	_, err := svc.GetAlerts(ctx, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}

func TestUnitAlertingService_GetAlerts_NetworkError(t *testing.T) {
	ctx := context.Background()
	svc := NewAlertingService(deadClient(), testLogger())

	_, err := svc.GetAlerts(ctx, nil)
	require.Error(t, err)
}

// --- GetFindings ---

func TestUnitAlertingService_GetFindings(t *testing.T) {
	srv := newAlertingTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewAlertingService(restyClient(srv), testLogger())

	resp, err := svc.GetFindings(ctx, nil)
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, 1, resp.TotalFindings)
	assert.Len(t, resp.Findings, 1)
	assert.Equal(t, "finding1", resp.Findings[0].Id)
}

func TestUnitAlertingService_GetFindingsWithParams(t *testing.T) {
	srv := newAlertingTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewAlertingService(restyClient(srv), testLogger())

	resp, err := svc.GetFindings(ctx, map[string]string{"size": "5"})
	require.NoError(t, err)
	require.NotNil(t, resp)
}

func TestUnitAlertingService_GetFindings_ServerError(t *testing.T) {
	srv := errServer(500)
	defer srv.Close()
	ctx := context.Background()
	svc := NewAlertingService(restyClient(srv), testLogger())

	_, err := svc.GetFindings(ctx, nil)
	require.Error(t, err)
}

func TestUnitAlertingService_GetFindings_BadJSON(t *testing.T) {
	srv := badJSONServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewAlertingService(restyClient(srv), testLogger())

	_, err := svc.GetFindings(ctx, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}

func TestUnitAlertingService_GetFindings_NetworkError(t *testing.T) {
	ctx := context.Background()
	svc := NewAlertingService(deadClient(), testLogger())

	_, err := svc.GetFindings(ctx, nil)
	require.Error(t, err)
}

// --- GetDestinations ---

func TestUnitAlertingService_GetDestinations(t *testing.T) {
	srv := newAlertingTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewAlertingService(restyClient(srv), testLogger())

	resp, err := svc.GetDestinations(ctx, "")
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, 1, resp.TotalDestinations)
	assert.Len(t, resp.Destinations, 1)
	assert.Equal(t, "dest1", resp.Destinations[0].Id)
}

func TestUnitAlertingService_GetDestinationsWithId(t *testing.T) {
	srv := newAlertingTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewAlertingService(restyClient(srv), testLogger())

	resp, err := svc.GetDestinations(ctx, "test_dest")
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, 1, resp.TotalDestinations)
	assert.Equal(t, "test_dest", resp.Destinations[0].Id)
}

func TestUnitAlertingService_GetDestinations_ServerError(t *testing.T) {
	srv := errServer(500)
	defer srv.Close()
	ctx := context.Background()
	svc := NewAlertingService(restyClient(srv), testLogger())

	_, err := svc.GetDestinations(ctx, "")
	require.Error(t, err)
}

func TestUnitAlertingService_GetDestinations_BadJSON(t *testing.T) {
	srv := badJSONServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewAlertingService(restyClient(srv), testLogger())

	_, err := svc.GetDestinations(ctx, "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}

func TestUnitAlertingService_GetDestinations_NetworkError(t *testing.T) {
	ctx := context.Background()
	svc := NewAlertingService(deadClient(), testLogger())

	_, err := svc.GetDestinations(ctx, "")
	require.Error(t, err)
}

// --- IndexWorkflow ---

func TestUnitAlertingService_IndexWorkflow_Create(t *testing.T) {
	srv := newAlertingTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewAlertingService(restyClient(srv), testLogger())

	resp, err := svc.IndexWorkflow(ctx, &AlertingIndexWorkflowRequest{
		Body: &AlertingWorkflow{Name: "test"},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "new_workflow", resp.Id)
}

func TestUnitAlertingService_IndexWorkflow_Update(t *testing.T) {
	srv := newAlertingTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewAlertingService(restyClient(srv), testLogger())

	resp, err := svc.IndexWorkflow(ctx, &AlertingIndexWorkflowRequest{
		WorkflowId: "test_workflow",
		Body:       &AlertingWorkflow{Name: "test"},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, int64(2), resp.Version)
}

func TestUnitAlertingService_IndexWorkflow_WithVersion(t *testing.T) {
	srv := newAlertingTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewAlertingService(restyClient(srv), testLogger())

	seqNo := int64(0)
	primaryTerm := int64(1)
	resp, err := svc.IndexWorkflow(ctx, &AlertingIndexWorkflowRequest{
		WorkflowId: "test_workflow",
		Body:       &AlertingWorkflow{Name: "test"},
		Version: &types.DocumentVersion{
			SeqNo:       &seqNo,
			PrimaryTerm: &primaryTerm,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
}

func TestUnitAlertingService_IndexWorkflow_ValidationError(t *testing.T) {
	ctx := context.Background()
	svc := NewAlertingService(deadClient(), testLogger())

	_, err := svc.IndexWorkflow(ctx, &AlertingIndexWorkflowRequest{})
	require.Error(t, err)
}

func TestUnitAlertingService_IndexWorkflow_ServerError(t *testing.T) {
	srv := errServer(500)
	defer srv.Close()
	ctx := context.Background()
	svc := NewAlertingService(restyClient(srv), testLogger())

	_, err := svc.IndexWorkflow(ctx, &AlertingIndexWorkflowRequest{
		Body: &AlertingWorkflow{Name: "test"},
	})
	require.Error(t, err)
}

func TestUnitAlertingService_IndexWorkflow_BadJSON(t *testing.T) {
	srv := badJSONServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewAlertingService(restyClient(srv), testLogger())

	_, err := svc.IndexWorkflow(ctx, &AlertingIndexWorkflowRequest{
		Body: &AlertingWorkflow{Name: "test"},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}

func TestUnitAlertingService_IndexWorkflow_NetworkError(t *testing.T) {
	ctx := context.Background()
	svc := NewAlertingService(deadClient(), testLogger())

	_, err := svc.IndexWorkflow(ctx, &AlertingIndexWorkflowRequest{
		Body: &AlertingWorkflow{Name: "test"},
	})
	require.Error(t, err)
}

// --- GetWorkflow ---

func TestUnitAlertingService_GetWorkflow(t *testing.T) {
	srv := newAlertingTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewAlertingService(restyClient(srv), testLogger())

	resp, err := svc.GetWorkflow(ctx, "test_workflow")
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "test_workflow", resp.Id)
	assert.Equal(t, "test", resp.Workflow.Name)
}

func TestUnitAlertingService_GetWorkflow_EmptyId(t *testing.T) {
	ctx := context.Background()
	svc := NewAlertingService(deadClient(), testLogger())

	_, err := svc.GetWorkflow(ctx, "")
	require.Error(t, err)
}

func TestUnitAlertingService_GetWorkflow_ServerError(t *testing.T) {
	srv := errServer(500)
	defer srv.Close()
	ctx := context.Background()
	svc := NewAlertingService(restyClient(srv), testLogger())

	_, err := svc.GetWorkflow(ctx, "test")
	require.Error(t, err)
}

func TestUnitAlertingService_GetWorkflow_BadJSON(t *testing.T) {
	srv := badJSONServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewAlertingService(restyClient(srv), testLogger())

	_, err := svc.GetWorkflow(ctx, "test")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}

func TestUnitAlertingService_GetWorkflow_NetworkError(t *testing.T) {
	ctx := context.Background()
	svc := NewAlertingService(deadClient(), testLogger())

	_, err := svc.GetWorkflow(ctx, "test")
	require.Error(t, err)
}

// --- DeleteWorkflow ---

func TestUnitAlertingService_DeleteWorkflow(t *testing.T) {
	srv := newAlertingTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewAlertingService(restyClient(srv), testLogger())

	resp, err := svc.DeleteWorkflow(ctx, "test_workflow")
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "deleted", *resp.Result)
}

func TestUnitAlertingService_DeleteWorkflow_EmptyId(t *testing.T) {
	ctx := context.Background()
	svc := NewAlertingService(deadClient(), testLogger())

	_, err := svc.DeleteWorkflow(ctx, "")
	require.Error(t, err)
}

func TestUnitAlertingService_DeleteWorkflow_ServerError(t *testing.T) {
	srv := errServer(500)
	defer srv.Close()
	ctx := context.Background()
	svc := NewAlertingService(restyClient(srv), testLogger())

	_, err := svc.DeleteWorkflow(ctx, "test")
	require.Error(t, err)
}

func TestUnitAlertingService_DeleteWorkflow_BadJSON(t *testing.T) {
	srv := badJSONServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewAlertingService(restyClient(srv), testLogger())

	_, err := svc.DeleteWorkflow(ctx, "test")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}

func TestUnitAlertingService_DeleteWorkflow_NetworkError(t *testing.T) {
	ctx := context.Background()
	svc := NewAlertingService(deadClient(), testLogger())

	_, err := svc.DeleteWorkflow(ctx, "test")
	require.Error(t, err)
}

// --- ExecuteWorkflow ---

func TestUnitAlertingService_ExecuteWorkflow(t *testing.T) {
	srv := newAlertingTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewAlertingService(restyClient(srv), testLogger())

	resp, err := svc.ExecuteWorkflow(ctx, "test_workflow", nil)
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "exec1", resp.ExecutionId)
}

func TestUnitAlertingService_ExecuteWorkflowWithBody(t *testing.T) {
	srv := newAlertingTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewAlertingService(restyClient(srv), testLogger())

	resp, err := svc.ExecuteWorkflow(ctx, "test_workflow", map[string]any{"param": "value"})
	require.NoError(t, err)
	require.NotNil(t, resp)
}

func TestUnitAlertingService_ExecuteWorkflow_EmptyId(t *testing.T) {
	ctx := context.Background()
	svc := NewAlertingService(deadClient(), testLogger())

	_, err := svc.ExecuteWorkflow(ctx, "", nil)
	require.Error(t, err)
}

func TestUnitAlertingService_ExecuteWorkflow_ServerError(t *testing.T) {
	srv := errServer(500)
	defer srv.Close()
	ctx := context.Background()
	svc := NewAlertingService(restyClient(srv), testLogger())

	_, err := svc.ExecuteWorkflow(ctx, "test", nil)
	require.Error(t, err)
}

func TestUnitAlertingService_ExecuteWorkflow_BadJSON(t *testing.T) {
	srv := badJSONServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewAlertingService(restyClient(srv), testLogger())

	_, err := svc.ExecuteWorkflow(ctx, "test", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}

func TestUnitAlertingService_ExecuteWorkflow_NetworkError(t *testing.T) {
	ctx := context.Background()
	svc := NewAlertingService(deadClient(), testLogger())

	_, err := svc.ExecuteWorkflow(ctx, "test", nil)
	require.Error(t, err)
}

// --- GetWorkflowAlerts ---

func TestUnitAlertingService_GetWorkflowAlerts(t *testing.T) {
	srv := newAlertingTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewAlertingService(restyClient(srv), testLogger())

	resp, err := svc.GetWorkflowAlerts(ctx, nil)
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, 1, resp.TotalAlerts)
	assert.Len(t, resp.Alerts, 1)
}

func TestUnitAlertingService_GetWorkflowAlertsWithParams(t *testing.T) {
	srv := newAlertingTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewAlertingService(restyClient(srv), testLogger())

	resp, err := svc.GetWorkflowAlerts(ctx, map[string]string{"size": "10"})
	require.NoError(t, err)
	require.NotNil(t, resp)
}

func TestUnitAlertingService_GetWorkflowAlerts_ServerError(t *testing.T) {
	srv := errServer(500)
	defer srv.Close()
	ctx := context.Background()
	svc := NewAlertingService(restyClient(srv), testLogger())

	_, err := svc.GetWorkflowAlerts(ctx, nil)
	require.Error(t, err)
}

func TestUnitAlertingService_GetWorkflowAlerts_BadJSON(t *testing.T) {
	srv := badJSONServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewAlertingService(restyClient(srv), testLogger())

	_, err := svc.GetWorkflowAlerts(ctx, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}

func TestUnitAlertingService_GetWorkflowAlerts_NetworkError(t *testing.T) {
	ctx := context.Background()
	svc := NewAlertingService(deadClient(), testLogger())

	_, err := svc.GetWorkflowAlerts(ctx, nil)
	require.Error(t, err)
}

// --- AcknowledgeChainedAlerts ---

func TestUnitAlertingService_AcknowledgeChainedAlerts(t *testing.T) {
	srv := newAlertingTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewAlertingService(restyClient(srv), testLogger())

	resp, err := svc.AcknowledgeChainedAlerts(ctx, "test_workflow", map[string]any{"alerts": []string{"alert1"}})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Len(t, resp.AcknowledgedAlertIds, 1)
	assert.Equal(t, "alert1", resp.AcknowledgedAlertIds[0].Id)
}

func TestUnitAlertingService_AcknowledgeChainedAlerts_EmptyWorkflowId(t *testing.T) {
	ctx := context.Background()
	svc := NewAlertingService(deadClient(), testLogger())

	_, err := svc.AcknowledgeChainedAlerts(ctx, "", map[string]any{})
	require.Error(t, err)
}

func TestUnitAlertingService_AcknowledgeChainedAlerts_NilBody(t *testing.T) {
	ctx := context.Background()
	svc := NewAlertingService(deadClient(), testLogger())

	_, err := svc.AcknowledgeChainedAlerts(ctx, "test_workflow", nil)
	require.Error(t, err)
}

func TestUnitAlertingService_AcknowledgeChainedAlerts_ServerError(t *testing.T) {
	srv := errServer(500)
	defer srv.Close()
	ctx := context.Background()
	svc := NewAlertingService(restyClient(srv), testLogger())

	_, err := svc.AcknowledgeChainedAlerts(ctx, "test", map[string]any{"alerts": []string{"a"}})
	require.Error(t, err)
}

func TestUnitAlertingService_AcknowledgeChainedAlerts_BadJSON(t *testing.T) {
	srv := badJSONServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewAlertingService(restyClient(srv), testLogger())

	_, err := svc.AcknowledgeChainedAlerts(ctx, "test", map[string]any{"alerts": []string{"a"}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}

func TestUnitAlertingService_AcknowledgeChainedAlerts_NetworkError(t *testing.T) {
	ctx := context.Background()
	svc := NewAlertingService(deadClient(), testLogger())

	_, err := svc.AcknowledgeChainedAlerts(ctx, "test", map[string]any{"alerts": []string{"a"}})
	require.Error(t, err)
}
