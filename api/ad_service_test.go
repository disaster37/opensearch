// ad_service_test.go contains unit tests for DefaultAdService.
// Tests use httptest.NewServer to mock OpenSearch Anomaly Detection plugin endpoints.
// Each TestUnit* function covers one service method with success subtests,
// server error subtests, bad JSON subtests, network error subtests,
// and validation / required-parameter error subtests.
package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newAdTestServer() *httptest.Server {
	mux := http.NewServeMux()

	mux.HandleFunc("/_plugins/_anomaly_detection/detectors", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodPost {
			_, _ = w.Write([]byte(`{"_id":"new_detector","_version":1,"_seq_no":0,"_primary_term":1,"anomaly_detector":{"name":"test-detector"}}`))
		}
	})

	mux.HandleFunc("/_plugins/_anomaly_detection/detectors/test_detector", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodGet:
			_, _ = w.Write([]byte(`{"_id":"test_detector","_version":1,"_seq_no":0,"_primary_term":1,"anomaly_detector":{"name":"test-detector"}}`))
		case http.MethodPut:
			_, _ = w.Write([]byte(`{"_id":"test_detector","_version":2,"_seq_no":1,"_primary_term":1,"anomaly_detector":{"name":"test-detector-updated"}}`))
		case http.MethodDelete:
			_, _ = w.Write([]byte(`{"_index":".opendistro-anomaly-results","_id":"test_detector","_version":2,"result":"deleted"}`))
		}
	})

	mux.HandleFunc("/_plugins/_anomaly_detection/detectors/test_detector/_run", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"anomaly_grade":0.5,"confidence":0.9,"data_start_time":"2024-01-01","data_end_time":"2024-01-02","features":[{"feature_name":"f1"}]}`))
	})

	mux.HandleFunc("/_plugins/_anomaly_detection/detectors/_preview", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodPost {
			_, _ = w.Write([]byte(`{"anomaly_result":[{"anomaly_grade":0.5,"confidence":0.9}]}`))
		}
	})

	mux.HandleFunc("/_plugins/_anomaly_detection/detectors/test_detector/_preview", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodPost {
			_, _ = w.Write([]byte(`{"anomaly_result":[{"anomaly_grade":0.7,"confidence":0.8}]}`))
		}
	})

	mux.HandleFunc("/_plugins/_anomaly_detection/detectors/_search", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"total_anomaly_detectors":1,"anomaly_detectors":[{"_id":"test_detector","detector":{"name":"test-detector"}}]}`))
	})

	mux.HandleFunc("/_plugins/_anomaly_detection/detectors/results/_search", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"total_results":1,"results":[{"anomaly_grade":0.5}]}`))
	})

	mux.HandleFunc("/_plugins/_anomaly_detection/detectors/test_detector/results/_topAnomalies", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"total_results":1,"results":[{"anomaly_grade":0.9}]}`))
	})

	mux.HandleFunc("/_plugins/_anomaly_detection/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"detector_count":5,"anomaly_detectors_index_status":"green"}`))
	})

	mux.HandleFunc("/_plugins/_anomaly_detection/stats/detector_count", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"detector_count":5}`))
	})

	mux.HandleFunc("/_plugins/_anomaly_detection/detectors/_validate", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodPost {
			_, _ = w.Write([]byte(`{"message":"Valid detector configuration"}`))
		}
	})

	return httptest.NewServer(mux)
}

func TestUnitAdService_IndexDetector_Create(t *testing.T) {
	srv := newAdTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewAdService(restyClient(srv), testLogger())

	resp, err := svc.IndexDetector(ctx, &AdIndexDetectorRequest{
		Body: &AdDetector{Name: "test-detector"},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "new_detector", resp.Id)
	assert.Equal(t, "test-detector", resp.Detector.Name)
}

func TestUnitAdService_IndexDetector_Update(t *testing.T) {
	srv := newAdTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewAdService(restyClient(srv), testLogger())

	resp, err := svc.IndexDetector(ctx, &AdIndexDetectorRequest{
		DetectorId: "test_detector",
		Body:       &AdDetector{Name: "test-detector-updated"},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, int64(2), resp.Version)
	assert.Equal(t, "test-detector-updated", resp.Detector.Name)
}

func TestUnitAdService_IndexDetector_ValidationError(t *testing.T) {
	srv := newAdTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewAdService(restyClient(srv), testLogger())

	_, err := svc.IndexDetector(ctx, &AdIndexDetectorRequest{})
	require.Error(t, err)
}

func TestUnitAdService_IndexDetector_ServerError(t *testing.T) {
	srv := errServer(500)
	defer srv.Close()
	ctx := context.Background()
	svc := NewAdService(restyClient(srv), testLogger())

	_, err := svc.IndexDetector(ctx, &AdIndexDetectorRequest{
		Body: &AdDetector{Name: "test"},
	})
	require.Error(t, err)
}

func TestUnitAdService_IndexDetector_BadJSON(t *testing.T) {
	srv := badJSONServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewAdService(restyClient(srv), testLogger())

	_, err := svc.IndexDetector(ctx, &AdIndexDetectorRequest{
		Body: &AdDetector{Name: "test"},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}

func TestUnitAdService_IndexDetector_NetworkError(t *testing.T) {
	ctx := context.Background()
	svc := NewAdService(deadClient(), testLogger())

	_, err := svc.IndexDetector(ctx, &AdIndexDetectorRequest{
		Body: &AdDetector{Name: "test"},
	})
	require.Error(t, err)
}

func TestUnitAdService_GetDetector(t *testing.T) {
	srv := newAdTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewAdService(restyClient(srv), testLogger())

	resp, err := svc.GetDetector(ctx, "test_detector")
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "test_detector", resp.Id)
	assert.Equal(t, "test-detector", resp.Detector.Name)
}

func TestUnitAdService_GetDetector_EmptyId(t *testing.T) {
	ctx := context.Background()
	svc := NewAdService(deadClient(), testLogger())

	_, err := svc.GetDetector(ctx, "")
	require.Error(t, err)
}

func TestUnitAdService_GetDetector_ServerError(t *testing.T) {
	srv := errServer(500)
	defer srv.Close()
	ctx := context.Background()
	svc := NewAdService(restyClient(srv), testLogger())

	_, err := svc.GetDetector(ctx, "test_detector")
	require.Error(t, err)
}

func TestUnitAdService_GetDetector_BadJSON(t *testing.T) {
	srv := badJSONServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewAdService(restyClient(srv), testLogger())

	_, err := svc.GetDetector(ctx, "test_detector")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}

func TestUnitAdService_GetDetector_NetworkError(t *testing.T) {
	ctx := context.Background()
	svc := NewAdService(deadClient(), testLogger())

	_, err := svc.GetDetector(ctx, "test_detector")
	require.Error(t, err)
}

func TestUnitAdService_DeleteDetector(t *testing.T) {
	srv := newAdTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewAdService(restyClient(srv), testLogger())

	resp, err := svc.DeleteDetector(ctx, "test_detector")
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "deleted", *resp.Result)
}

func TestUnitAdService_DeleteDetector_EmptyId(t *testing.T) {
	ctx := context.Background()
	svc := NewAdService(deadClient(), testLogger())

	_, err := svc.DeleteDetector(ctx, "")
	require.Error(t, err)
}

func TestUnitAdService_DeleteDetector_ServerError(t *testing.T) {
	srv := errServer(500)
	defer srv.Close()
	ctx := context.Background()
	svc := NewAdService(restyClient(srv), testLogger())

	_, err := svc.DeleteDetector(ctx, "test_detector")
	require.Error(t, err)
}

func TestUnitAdService_DeleteDetector_BadJSON(t *testing.T) {
	srv := badJSONServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewAdService(restyClient(srv), testLogger())

	_, err := svc.DeleteDetector(ctx, "test_detector")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}

func TestUnitAdService_DeleteDetector_NetworkError(t *testing.T) {
	ctx := context.Background()
	svc := NewAdService(deadClient(), testLogger())

	_, err := svc.DeleteDetector(ctx, "test_detector")
	require.Error(t, err)
}

func TestUnitAdService_ExecuteDetector(t *testing.T) {
	srv := newAdTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewAdService(restyClient(srv), testLogger())

	resp, err := svc.ExecuteDetector(ctx, "test_detector", nil)
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, 0.5, resp.AnomalyGrade)
	assert.Equal(t, 0.9, resp.Confidence)
}

func TestUnitAdService_ExecuteDetector_WithBody(t *testing.T) {
	srv := newAdTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewAdService(restyClient(srv), testLogger())

	resp, err := svc.ExecuteDetector(ctx, "test_detector", map[string]any{"param": "value"})
	require.NoError(t, err)
	require.NotNil(t, resp)
}

func TestUnitAdService_ExecuteDetector_EmptyId(t *testing.T) {
	ctx := context.Background()
	svc := NewAdService(deadClient(), testLogger())

	_, err := svc.ExecuteDetector(ctx, "", nil)
	require.Error(t, err)
}

func TestUnitAdService_ExecuteDetector_ServerError(t *testing.T) {
	srv := errServer(500)
	defer srv.Close()
	ctx := context.Background()
	svc := NewAdService(restyClient(srv), testLogger())

	_, err := svc.ExecuteDetector(ctx, "test_detector", nil)
	require.Error(t, err)
}

func TestUnitAdService_ExecuteDetector_BadJSON(t *testing.T) {
	srv := badJSONServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewAdService(restyClient(srv), testLogger())

	_, err := svc.ExecuteDetector(ctx, "test_detector", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}

func TestUnitAdService_ExecuteDetector_NetworkError(t *testing.T) {
	ctx := context.Background()
	svc := NewAdService(deadClient(), testLogger())

	_, err := svc.ExecuteDetector(ctx, "test_detector", nil)
	require.Error(t, err)
}

func TestUnitAdService_PreviewDetector(t *testing.T) {
	srv := newAdTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewAdService(restyClient(srv), testLogger())

	resp, err := svc.PreviewDetector(ctx, "", map[string]any{"detector": map[string]any{"name": "test"}})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Len(t, resp.AnomalyResult, 1)
}

func TestUnitAdService_PreviewDetector_WithDetectorId(t *testing.T) {
	srv := newAdTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewAdService(restyClient(srv), testLogger())

	resp, err := svc.PreviewDetector(ctx, "test_detector", map[string]any{"detector": map[string]any{"name": "test"}})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Len(t, resp.AnomalyResult, 1)
}

func TestUnitAdService_PreviewDetector_NilBody(t *testing.T) {
	ctx := context.Background()
	svc := NewAdService(deadClient(), testLogger())

	_, err := svc.PreviewDetector(ctx, "test_detector", nil)
	require.Error(t, err)
}

func TestUnitAdService_PreviewDetector_ServerError(t *testing.T) {
	srv := errServer(500)
	defer srv.Close()
	ctx := context.Background()
	svc := NewAdService(restyClient(srv), testLogger())

	_, err := svc.PreviewDetector(ctx, "", map[string]any{"detector": map[string]any{"name": "test"}})
	require.Error(t, err)
}

func TestUnitAdService_PreviewDetector_BadJSON(t *testing.T) {
	srv := badJSONServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewAdService(restyClient(srv), testLogger())

	_, err := svc.PreviewDetector(ctx, "", map[string]any{"detector": map[string]any{"name": "test"}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}

func TestUnitAdService_PreviewDetector_NetworkError(t *testing.T) {
	ctx := context.Background()
	svc := NewAdService(deadClient(), testLogger())

	_, err := svc.PreviewDetector(ctx, "", map[string]any{"detector": map[string]any{"name": "test"}})
	require.Error(t, err)
}

func TestUnitAdService_SearchDetectors(t *testing.T) {
	srv := newAdTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewAdService(restyClient(srv), testLogger())

	resp, err := svc.SearchDetectors(ctx, map[string]any{"query": map[string]any{"match_all": map[string]any{}}})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, int64(1), resp.Total)
	assert.Len(t, resp.Detectors, 1)
	assert.Equal(t, "test_detector", resp.Detectors[0].Id)
}

func TestUnitAdService_SearchDetectors_NilBody(t *testing.T) {
	ctx := context.Background()
	svc := NewAdService(deadClient(), testLogger())

	_, err := svc.SearchDetectors(ctx, nil)
	require.Error(t, err)
}

func TestUnitAdService_SearchDetectors_ServerError(t *testing.T) {
	srv := errServer(500)
	defer srv.Close()
	ctx := context.Background()
	svc := NewAdService(restyClient(srv), testLogger())

	_, err := svc.SearchDetectors(ctx, map[string]any{"query": map[string]any{"match_all": map[string]any{}}})
	require.Error(t, err)
}

func TestUnitAdService_SearchDetectors_BadJSON(t *testing.T) {
	srv := badJSONServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewAdService(restyClient(srv), testLogger())

	_, err := svc.SearchDetectors(ctx, map[string]any{"query": map[string]any{"match_all": map[string]any{}}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}

func TestUnitAdService_SearchDetectors_NetworkError(t *testing.T) {
	ctx := context.Background()
	svc := NewAdService(deadClient(), testLogger())

	_, err := svc.SearchDetectors(ctx, map[string]any{"query": map[string]any{"match_all": map[string]any{}}})
	require.Error(t, err)
}

func TestUnitAdService_SearchResults(t *testing.T) {
	srv := newAdTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewAdService(restyClient(srv), testLogger())

	resp, err := svc.SearchResults(ctx, map[string]any{"query": map[string]any{"match_all": map[string]any{}}})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, int64(1), resp.Total)
	assert.Len(t, resp.Results, 1)
}

func TestUnitAdService_SearchResults_NilBody(t *testing.T) {
	ctx := context.Background()
	svc := NewAdService(deadClient(), testLogger())

	_, err := svc.SearchResults(ctx, nil)
	require.Error(t, err)
}

func TestUnitAdService_SearchResults_ServerError(t *testing.T) {
	srv := errServer(500)
	defer srv.Close()
	ctx := context.Background()
	svc := NewAdService(restyClient(srv), testLogger())

	_, err := svc.SearchResults(ctx, map[string]any{"query": map[string]any{"match_all": map[string]any{}}})
	require.Error(t, err)
}

func TestUnitAdService_SearchResults_BadJSON(t *testing.T) {
	srv := badJSONServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewAdService(restyClient(srv), testLogger())

	_, err := svc.SearchResults(ctx, map[string]any{"query": map[string]any{"match_all": map[string]any{}}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}

func TestUnitAdService_SearchResults_NetworkError(t *testing.T) {
	ctx := context.Background()
	svc := NewAdService(deadClient(), testLogger())

	_, err := svc.SearchResults(ctx, map[string]any{"query": map[string]any{"match_all": map[string]any{}}})
	require.Error(t, err)
}

func TestUnitAdService_SearchTopResults(t *testing.T) {
	srv := newAdTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewAdService(restyClient(srv), testLogger())

	resp, err := svc.SearchTopResults(ctx, "test_detector", map[string]any{"size": 10})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, int64(1), resp.Total)
	assert.Len(t, resp.Results, 1)
}

func TestUnitAdService_SearchTopResults_EmptyId(t *testing.T) {
	ctx := context.Background()
	svc := NewAdService(deadClient(), testLogger())

	_, err := svc.SearchTopResults(ctx, "", map[string]any{})
	require.Error(t, err)
}

func TestUnitAdService_SearchTopResults_NilBody(t *testing.T) {
	ctx := context.Background()
	svc := NewAdService(deadClient(), testLogger())

	_, err := svc.SearchTopResults(ctx, "test_detector", nil)
	require.Error(t, err)
}

func TestUnitAdService_SearchTopResults_ServerError(t *testing.T) {
	srv := errServer(500)
	defer srv.Close()
	ctx := context.Background()
	svc := NewAdService(restyClient(srv), testLogger())

	_, err := svc.SearchTopResults(ctx, "test_detector", map[string]any{})
	require.Error(t, err)
}

func TestUnitAdService_SearchTopResults_BadJSON(t *testing.T) {
	srv := badJSONServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewAdService(restyClient(srv), testLogger())

	_, err := svc.SearchTopResults(ctx, "test_detector", map[string]any{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}

func TestUnitAdService_SearchTopResults_NetworkError(t *testing.T) {
	ctx := context.Background()
	svc := NewAdService(deadClient(), testLogger())

	_, err := svc.SearchTopResults(ctx, "test_detector", map[string]any{})
	require.Error(t, err)
}

func TestUnitAdService_AdStats(t *testing.T) {
	srv := newAdTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewAdService(restyClient(srv), testLogger())

	resp, err := svc.AdStats(ctx, "")
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, 5.0, (*resp)["detector_count"])
}

func TestUnitAdService_AdStats_WithStat(t *testing.T) {
	srv := newAdTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewAdService(restyClient(srv), testLogger())

	resp, err := svc.AdStats(ctx, "detector_count")
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, 5.0, (*resp)["detector_count"])
}

func TestUnitAdService_AdStats_ServerError(t *testing.T) {
	srv := errServer(500)
	defer srv.Close()
	ctx := context.Background()
	svc := NewAdService(restyClient(srv), testLogger())

	_, err := svc.AdStats(ctx, "")
	require.Error(t, err)
}

func TestUnitAdService_AdStats_BadJSON(t *testing.T) {
	srv := badJSONServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewAdService(restyClient(srv), testLogger())

	_, err := svc.AdStats(ctx, "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}

func TestUnitAdService_AdStats_NetworkError(t *testing.T) {
	ctx := context.Background()
	svc := NewAdService(deadClient(), testLogger())

	_, err := svc.AdStats(ctx, "")
	require.Error(t, err)
}

func TestUnitAdService_ValidateDetector(t *testing.T) {
	srv := newAdTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewAdService(restyClient(srv), testLogger())

	resp, err := svc.ValidateDetector(ctx, map[string]any{
		"detector": map[string]any{"name": "test", "indices": []string{"idx"}},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "Valid detector configuration", *resp.Message)
}

func TestUnitAdService_ValidateDetector_NilBody(t *testing.T) {
	ctx := context.Background()
	svc := NewAdService(deadClient(), testLogger())

	_, err := svc.ValidateDetector(ctx, nil)
	require.Error(t, err)
}

func TestUnitAdService_ValidateDetector_ServerError(t *testing.T) {
	srv := errServer(500)
	defer srv.Close()
	ctx := context.Background()
	svc := NewAdService(restyClient(srv), testLogger())

	_, err := svc.ValidateDetector(ctx, map[string]any{"detector": map[string]any{"name": "test"}})
	require.Error(t, err)
}

func TestUnitAdService_ValidateDetector_BadJSON(t *testing.T) {
	srv := badJSONServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewAdService(restyClient(srv), testLogger())

	_, err := svc.ValidateDetector(ctx, map[string]any{"detector": map[string]any{"name": "test"}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}

func TestUnitAdService_ValidateDetector_NetworkError(t *testing.T) {
	ctx := context.Background()
	svc := NewAdService(deadClient(), testLogger())

	_, err := svc.ValidateDetector(ctx, map[string]any{"detector": map[string]any{"name": "test"}})
	require.Error(t, err)
}
