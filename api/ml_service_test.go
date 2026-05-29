// ml_service_test.go contains unit tests for DefaultMlService.
// Tests use httptest.NewServer to mock OpenSearch ML Commons plugin endpoints.
// Each TestUnit* function covers one service method with success subtests,
// validation error subtests, server error tests, bad JSON tests, and network error tests.
package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newMlTestServer() *httptest.Server {
	mux := http.NewServeMux()

	mux.HandleFunc("/_plugins/_ml/models/_register", func(w http.ResponseWriter, r *http.Request) {
		jsonResponse(w, 200, `{"task_id":"task1","task_type":"REGISTER_MODEL","status":"CREATED"}`)
	})

	mux.HandleFunc("/_plugins/_ml/models/_search", func(w http.ResponseWriter, r *http.Request) {
		jsonResponse(w, 200, `{"hits":{"total":{"value":1},"hits":[{"_id":"m1"}]}}`)
	})

	mux.HandleFunc("/_plugins/_ml/models/", func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		switch r.Method {
		case http.MethodGet:
			jsonResponse(w, 200, `{"model_id":"m1","name":"test-model","status":"DEPLOYED"}`)
		case http.MethodPut:
			jsonResponse(w, 200, `{"model_id":"m1","name":"updated-model"}`)
		case http.MethodDelete:
			jsonResponse(w, 200, `{"acknowledged":true}`)
		case http.MethodPost:
			if strings.HasSuffix(path, "/_predict") {
				jsonResponse(w, 200, `{"inference_results":[{"output":[{"name":"response","result":"ok"}]}]}`)
			} else if strings.HasSuffix(path, "/_deploy") {
				jsonResponse(w, 200, `{"task_id":"task1","status":"COMPLETED"}`)
			} else {
				jsonResponse(w, 200, `{}`)
			}
		}
	})

	mux.HandleFunc("/_plugins/_ml/_train_predict/", func(w http.ResponseWriter, r *http.Request) {
		jsonResponse(w, 200, `{"inference_results":[{"output":[{"name":"response","result":"ok"}]}]}`)
	})

	mux.HandleFunc("/_plugins/_ml/_train/", func(w http.ResponseWriter, r *http.Request) {
		jsonResponse(w, 200, `{"task_id":"train1"}`)
	})

	mux.HandleFunc("/_plugins/_ml/_execute/", func(w http.ResponseWriter, r *http.Request) {
		jsonResponse(w, 200, `{"status":"COMPLETED"}`)
	})

	mux.HandleFunc("/_plugins/_ml/stats/", func(w http.ResponseWriter, r *http.Request) {
		jsonResponse(w, 200, `{"ml_model_index_status":"green"}`)
	})

	mux.HandleFunc("/_plugins/_ml/stats", func(w http.ResponseWriter, r *http.Request) {
		jsonResponse(w, 200, `{"ml_model_index_status":"green"}`)
	})

	mux.HandleFunc("/_plugins/_ml/tasks/", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			jsonResponse(w, 200, `{"task_id":"task1","state":"COMPLETED"}`)
		case http.MethodDelete:
			jsonResponse(w, 200, `{"acknowledged":true}`)
		}
	})

	mux.HandleFunc("/_plugins/_ml/connectors/_create", func(w http.ResponseWriter, r *http.Request) {
		jsonResponse(w, 200, `{"connector_id":"conn1"}`)
	})

	mux.HandleFunc("/_plugins/_ml/connectors/_search", func(w http.ResponseWriter, r *http.Request) {
		jsonResponse(w, 200, `{"hits":{"total":{"value":1},"hits":[{"_id":"conn1"}]}}`)
	})

	mux.HandleFunc("/_plugins/_ml/connectors/", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			jsonResponse(w, 200, `{"connector_id":"conn1","name":"test-connector"}`)
		case http.MethodDelete:
			jsonResponse(w, 200, `{"acknowledged":true}`)
		}
	})

	mux.HandleFunc("/_plugins/_ml/agents/_register", func(w http.ResponseWriter, r *http.Request) {
		jsonResponse(w, 200, `{"agent_id":"agent1"}`)
	})

	mux.HandleFunc("/_plugins/_ml/agents/_search", func(w http.ResponseWriter, r *http.Request) {
		jsonResponse(w, 200, `{"hits":{"total":{"value":1},"hits":[{"_id":"agent1"}]}}`)
	})

	mux.HandleFunc("/_plugins/_ml/agents/", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			jsonResponse(w, 200, `{"agent_id":"agent1","name":"test-agent"}`)
		case http.MethodDelete:
			jsonResponse(w, 200, `{"acknowledged":true}`)
		}
	})

	mux.HandleFunc("/_plugins/_ml/model_groups/_register", func(w http.ResponseWriter, r *http.Request) {
		jsonResponse(w, 200, `{"model_group_id":"mg1","status":"CREATED"}`)
	})

	mux.HandleFunc("/_plugins/_ml/model_groups/", func(w http.ResponseWriter, r *http.Request) {
		jsonResponse(w, 200, `{"acknowledged":true}`)
	})

	mux.HandleFunc("/_plugins/_ml/profile/", func(w http.ResponseWriter, r *http.Request) {
		jsonResponse(w, 200, `{"nodes":{"node1":{"models":{"m1":{"model_state":"DEPLOYED"}}}}}`)
	})

	mux.HandleFunc("/_plugins/_ml/profile", func(w http.ResponseWriter, r *http.Request) {
		jsonResponse(w, 200, `{"nodes":{"node1":{}}}`)
	})

	mux.HandleFunc("/_plugins/_ml/tools/_execute/", func(w http.ResponseWriter, r *http.Request) {
		jsonResponse(w, 200, `{"result":"tool output"}`)
	})

	mux.HandleFunc("/_plugins/_ml/tools", func(w http.ResponseWriter, r *http.Request) {
		jsonResponse(w, 200, `{"ConnectorTool":"connector tool desc","VectorDBTool":"vectordb tool desc"}`)
	})

	return httptest.NewServer(mux)
}

func TestUnitMlService_RegisterModel(t *testing.T) {
	srv := newMlTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewMlService(restyClient(srv), testLogger())

	resp, err := svc.RegisterModel(ctx, map[string]any{"name": "test-model"})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "task1", resp.TaskId)
	assert.Equal(t, "REGISTER_MODEL", resp.TaskType)
}

func TestUnitMlService_DeployModel(t *testing.T) {
	srv := newMlTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewMlService(restyClient(srv), testLogger())

	resp, err := svc.DeployModel(ctx, "m1")
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "task1", resp.TaskId)
}

func TestUnitMlService_UndeployModel(t *testing.T) {
	srv := newMlTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewMlService(restyClient(srv), testLogger())

	resp, err := svc.UndeployModel(ctx, "m1")
	require.NoError(t, err)
	require.NotNil(t, resp)
}

func TestUnitMlService_GetModel(t *testing.T) {
	srv := newMlTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewMlService(restyClient(srv), testLogger())

	resp, err := svc.GetModel(ctx, "m1")
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "m1", resp["model_id"])
}

func TestUnitMlService_SearchModels(t *testing.T) {
	srv := newMlTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewMlService(restyClient(srv), testLogger())

	resp, err := svc.SearchModels(ctx, map[string]any{"query": map[string]any{"match_all": map[string]any{}}})
	require.NoError(t, err)
	require.NotNil(t, resp)
}

func TestUnitMlService_UpdateModel(t *testing.T) {
	srv := newMlTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewMlService(restyClient(srv), testLogger())

	resp, err := svc.UpdateModel(ctx, "m1", map[string]any{"name": "updated"})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "updated-model", resp["name"])
}

func TestUnitMlService_DeleteModel(t *testing.T) {
	srv := newMlTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewMlService(restyClient(srv), testLogger())

	resp, err := svc.DeleteModel(ctx, "m1")
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.True(t, resp.Acknowledged)
}

func TestUnitMlService_Predict(t *testing.T) {
	srv := newMlTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewMlService(restyClient(srv), testLogger())

	resp, err := svc.Predict(ctx, "m1", map[string]any{"parameters": map[string]any{"input": "test"}})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Len(t, resp.InferenceResults, 1)
}

func TestUnitMlService_Train(t *testing.T) {
	srv := newMlTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewMlService(restyClient(srv), testLogger())

	resp, err := svc.Train(ctx, "kmeans", map[string]any{"parameters": map[string]any{"centroids": 3}})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "train1", resp.TaskId)
}

func TestUnitMlService_TrainAndPredict(t *testing.T) {
	srv := newMlTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewMlService(restyClient(srv), testLogger())

	resp, err := svc.TrainAndPredict(ctx, "kmeans", map[string]any{"parameters": map[string]any{"centroids": 3}})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Len(t, resp.InferenceResults, 1)
}

func TestUnitMlService_Execute(t *testing.T) {
	srv := newMlTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewMlService(restyClient(srv), testLogger())

	resp, err := svc.Execute(ctx, "anomaly_localization", map[string]any{"index_name": "test"})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "COMPLETED", resp["status"])
}

func TestUnitMlService_MlStats(t *testing.T) {
	srv := newMlTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewMlService(restyClient(srv), testLogger())

	resp, err := svc.MlStats(ctx, "")
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "green", resp["ml_model_index_status"])

	resp, err = svc.MlStats(ctx, "ml_model_index_status")
	require.NoError(t, err)
	require.NotNil(t, resp)
}

func TestUnitMlService_GetTask(t *testing.T) {
	srv := newMlTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewMlService(restyClient(srv), testLogger())

	resp, err := svc.GetTask(ctx, "task1")
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "task1", resp["task_id"])
}

func TestUnitMlService_DeleteTask(t *testing.T) {
	srv := newMlTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewMlService(restyClient(srv), testLogger())

	resp, err := svc.DeleteTask(ctx, "task1")
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.True(t, resp.Acknowledged)
}

func TestUnitMlService_CreateConnector(t *testing.T) {
	srv := newMlTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewMlService(restyClient(srv), testLogger())

	resp, err := svc.CreateConnector(ctx, map[string]any{"name": "test-connector"})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "conn1", resp.ConnectorId)
}

func TestUnitMlService_GetConnector(t *testing.T) {
	srv := newMlTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewMlService(restyClient(srv), testLogger())

	resp, err := svc.GetConnector(ctx, "conn1")
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "conn1", resp["connector_id"])
}

func TestUnitMlService_DeleteConnector(t *testing.T) {
	srv := newMlTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewMlService(restyClient(srv), testLogger())

	resp, err := svc.DeleteConnector(ctx, "conn1")
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.True(t, resp.Acknowledged)
}

func TestUnitMlService_SearchConnectors(t *testing.T) {
	srv := newMlTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewMlService(restyClient(srv), testLogger())

	resp, err := svc.SearchConnectors(ctx, map[string]any{"query": map[string]any{"match_all": map[string]any{}}})
	require.NoError(t, err)
	require.NotNil(t, resp)
}

func TestUnitMlService_RegisterAgent(t *testing.T) {
	srv := newMlTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewMlService(restyClient(srv), testLogger())

	resp, err := svc.RegisterAgent(ctx, map[string]any{"name": "test-agent"})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "agent1", resp.AgentId)
}

func TestUnitMlService_GetAgent(t *testing.T) {
	srv := newMlTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewMlService(restyClient(srv), testLogger())

	resp, err := svc.GetAgent(ctx, "agent1")
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "agent1", resp["agent_id"])
}

func TestUnitMlService_DeleteAgent(t *testing.T) {
	srv := newMlTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewMlService(restyClient(srv), testLogger())

	resp, err := svc.DeleteAgent(ctx, "agent1")
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.True(t, resp.Acknowledged)
}

func TestUnitMlService_SearchAgents(t *testing.T) {
	srv := newMlTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewMlService(restyClient(srv), testLogger())

	resp, err := svc.SearchAgents(ctx, map[string]any{"query": map[string]any{"match_all": map[string]any{}}})
	require.NoError(t, err)
	require.NotNil(t, resp)
}

func TestUnitMlService_DeleteModelGroup(t *testing.T) {
	srv := newMlTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewMlService(restyClient(srv), testLogger())

	resp, err := svc.DeleteModelGroup(ctx, "mg1")
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.True(t, resp.Acknowledged)
}

func TestUnitMlService_RegisterModelGroup(t *testing.T) {
	srv := newMlTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewMlService(restyClient(srv), testLogger())

	resp, err := svc.RegisterModelGroup(ctx, map[string]any{"name": "test-group"})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "mg1", resp.ModelGroupId)
}

func TestUnitMlService_Profile(t *testing.T) {
	srv := newMlTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewMlService(restyClient(srv), testLogger())

	resp, err := svc.Profile(ctx, "")
	require.NoError(t, err)
	require.NotNil(t, resp)

	resp, err = svc.Profile(ctx, "models/m1")
	require.NoError(t, err)
	require.NotNil(t, resp)
}

func TestUnitMlService_ListTools(t *testing.T) {
	srv := newMlTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewMlService(restyClient(srv), testLogger())

	resp, err := svc.ListTools(ctx)
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Contains(t, resp, "ConnectorTool")
}

func TestUnitMlService_ExecuteTool(t *testing.T) {
	srv := newMlTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewMlService(restyClient(srv), testLogger())

	resp, err := svc.ExecuteTool(ctx, "ConnectorTool", map[string]any{"input": "test"})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "tool output", resp["result"])
}

func TestUnitMlService_ValidationErrors(t *testing.T) {
	ctx := context.Background()
	svc := NewMlService(deadClient(), testLogger())

	_, err := svc.DeployModel(ctx, "")
	assert.Error(t, err)

	_, err = svc.UndeployModel(ctx, "")
	assert.Error(t, err)

	_, err = svc.GetModel(ctx, "")
	assert.Error(t, err)

	_, err = svc.UpdateModel(ctx, "", nil)
	assert.Error(t, err)

	_, err = svc.DeleteModel(ctx, "")
	assert.Error(t, err)

	_, err = svc.Predict(ctx, "", nil)
	assert.Error(t, err)

	_, err = svc.Train(ctx, "", nil)
	assert.Error(t, err)

	_, err = svc.TrainAndPredict(ctx, "", nil)
	assert.Error(t, err)

	_, err = svc.Execute(ctx, "", nil)
	assert.Error(t, err)

	_, err = svc.GetTask(ctx, "")
	assert.Error(t, err)

	_, err = svc.DeleteTask(ctx, "")
	assert.Error(t, err)

	_, err = svc.GetConnector(ctx, "")
	assert.Error(t, err)

	_, err = svc.DeleteConnector(ctx, "")
	assert.Error(t, err)

	_, err = svc.GetAgent(ctx, "")
	assert.Error(t, err)

	_, err = svc.DeleteAgent(ctx, "")
	assert.Error(t, err)

	_, err = svc.DeleteModelGroup(ctx, "")
	assert.Error(t, err)

	_, err = svc.ExecuteTool(ctx, "", nil)
	assert.Error(t, err)
}

func TestUnitMlService_RegisterModel_ServerError(t *testing.T) {
	srv := errServer(500)
	defer srv.Close()
	ctx := context.Background()
	svc := NewMlService(restyClient(srv), testLogger())
	_, err := svc.RegisterModel(ctx, map[string]any{})
	require.Error(t, err)
}

func TestUnitMlService_RegisterModel_BadJSON(t *testing.T) {
	srv := badJSONServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewMlService(restyClient(srv), testLogger())
	_, err := svc.RegisterModel(ctx, map[string]any{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}

func TestUnitMlService_RegisterModel_NetworkError(t *testing.T) {
	ctx := context.Background()
	svc := NewMlService(deadClient(), testLogger())
	_, err := svc.RegisterModel(ctx, map[string]any{})
	require.Error(t, err)
}

func TestUnitMlService_DeployModel_ServerError(t *testing.T) {
	srv := errServer(500)
	defer srv.Close()
	ctx := context.Background()
	svc := NewMlService(restyClient(srv), testLogger())
	_, err := svc.DeployModel(ctx, "m1")
	require.Error(t, err)
}

func TestUnitMlService_DeployModel_BadJSON(t *testing.T) {
	srv := badJSONServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewMlService(restyClient(srv), testLogger())
	_, err := svc.DeployModel(ctx, "m1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}

func TestUnitMlService_DeployModel_NetworkError(t *testing.T) {
	ctx := context.Background()
	svc := NewMlService(deadClient(), testLogger())
	_, err := svc.DeployModel(ctx, "m1")
	require.Error(t, err)
}

func TestUnitMlService_UndeployModel_ServerError(t *testing.T) {
	srv := errServer(500)
	defer srv.Close()
	ctx := context.Background()
	svc := NewMlService(restyClient(srv), testLogger())
	_, err := svc.UndeployModel(ctx, "m1")
	require.Error(t, err)
}

func TestUnitMlService_UndeployModel_BadJSON(t *testing.T) {
	srv := badJSONServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewMlService(restyClient(srv), testLogger())
	_, err := svc.UndeployModel(ctx, "m1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}

func TestUnitMlService_UndeployModel_NetworkError(t *testing.T) {
	ctx := context.Background()
	svc := NewMlService(deadClient(), testLogger())
	_, err := svc.UndeployModel(ctx, "m1")
	require.Error(t, err)
}

func TestUnitMlService_GetModel_ServerError(t *testing.T) {
	srv := errServer(500)
	defer srv.Close()
	ctx := context.Background()
	svc := NewMlService(restyClient(srv), testLogger())
	_, err := svc.GetModel(ctx, "m1")
	require.Error(t, err)
}

func TestUnitMlService_GetModel_BadJSON(t *testing.T) {
	srv := badJSONServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewMlService(restyClient(srv), testLogger())
	_, err := svc.GetModel(ctx, "m1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}

func TestUnitMlService_GetModel_NetworkError(t *testing.T) {
	ctx := context.Background()
	svc := NewMlService(deadClient(), testLogger())
	_, err := svc.GetModel(ctx, "m1")
	require.Error(t, err)
}

func TestUnitMlService_SearchModels_ServerError(t *testing.T) {
	srv := errServer(500)
	defer srv.Close()
	ctx := context.Background()
	svc := NewMlService(restyClient(srv), testLogger())
	_, err := svc.SearchModels(ctx, map[string]any{})
	require.Error(t, err)
}

func TestUnitMlService_SearchModels_BadJSON(t *testing.T) {
	srv := badJSONServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewMlService(restyClient(srv), testLogger())
	_, err := svc.SearchModels(ctx, map[string]any{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}

func TestUnitMlService_SearchModels_NetworkError(t *testing.T) {
	ctx := context.Background()
	svc := NewMlService(deadClient(), testLogger())
	_, err := svc.SearchModels(ctx, map[string]any{})
	require.Error(t, err)
}

func TestUnitMlService_UpdateModel_ServerError(t *testing.T) {
	srv := errServer(500)
	defer srv.Close()
	ctx := context.Background()
	svc := NewMlService(restyClient(srv), testLogger())
	_, err := svc.UpdateModel(ctx, "m1", map[string]any{})
	require.Error(t, err)
}

func TestUnitMlService_UpdateModel_BadJSON(t *testing.T) {
	srv := badJSONServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewMlService(restyClient(srv), testLogger())
	_, err := svc.UpdateModel(ctx, "m1", map[string]any{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}

func TestUnitMlService_UpdateModel_NetworkError(t *testing.T) {
	ctx := context.Background()
	svc := NewMlService(deadClient(), testLogger())
	_, err := svc.UpdateModel(ctx, "m1", map[string]any{})
	require.Error(t, err)
}

func TestUnitMlService_DeleteModel_ServerError(t *testing.T) {
	srv := errServer(500)
	defer srv.Close()
	ctx := context.Background()
	svc := NewMlService(restyClient(srv), testLogger())
	_, err := svc.DeleteModel(ctx, "m1")
	require.Error(t, err)
}

func TestUnitMlService_DeleteModel_BadJSON(t *testing.T) {
	srv := badJSONServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewMlService(restyClient(srv), testLogger())
	_, err := svc.DeleteModel(ctx, "m1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}

func TestUnitMlService_DeleteModel_NetworkError(t *testing.T) {
	ctx := context.Background()
	svc := NewMlService(deadClient(), testLogger())
	_, err := svc.DeleteModel(ctx, "m1")
	require.Error(t, err)
}

func TestUnitMlService_Predict_ServerError(t *testing.T) {
	srv := errServer(500)
	defer srv.Close()
	ctx := context.Background()
	svc := NewMlService(restyClient(srv), testLogger())
	_, err := svc.Predict(ctx, "m1", map[string]any{})
	require.Error(t, err)
}

func TestUnitMlService_Predict_BadJSON(t *testing.T) {
	srv := badJSONServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewMlService(restyClient(srv), testLogger())
	_, err := svc.Predict(ctx, "m1", map[string]any{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}

func TestUnitMlService_Predict_NetworkError(t *testing.T) {
	ctx := context.Background()
	svc := NewMlService(deadClient(), testLogger())
	_, err := svc.Predict(ctx, "m1", map[string]any{})
	require.Error(t, err)
}

func TestUnitMlService_Train_ServerError(t *testing.T) {
	srv := errServer(500)
	defer srv.Close()
	ctx := context.Background()
	svc := NewMlService(restyClient(srv), testLogger())
	_, err := svc.Train(ctx, "kmeans", map[string]any{})
	require.Error(t, err)
}

func TestUnitMlService_Train_BadJSON(t *testing.T) {
	srv := badJSONServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewMlService(restyClient(srv), testLogger())
	_, err := svc.Train(ctx, "kmeans", map[string]any{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}

func TestUnitMlService_Train_NetworkError(t *testing.T) {
	ctx := context.Background()
	svc := NewMlService(deadClient(), testLogger())
	_, err := svc.Train(ctx, "kmeans", map[string]any{})
	require.Error(t, err)
}

func TestUnitMlService_TrainAndPredict_ServerError(t *testing.T) {
	srv := errServer(500)
	defer srv.Close()
	ctx := context.Background()
	svc := NewMlService(restyClient(srv), testLogger())
	_, err := svc.TrainAndPredict(ctx, "kmeans", map[string]any{})
	require.Error(t, err)
}

func TestUnitMlService_TrainAndPredict_BadJSON(t *testing.T) {
	srv := badJSONServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewMlService(restyClient(srv), testLogger())
	_, err := svc.TrainAndPredict(ctx, "kmeans", map[string]any{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}

func TestUnitMlService_TrainAndPredict_NetworkError(t *testing.T) {
	ctx := context.Background()
	svc := NewMlService(deadClient(), testLogger())
	_, err := svc.TrainAndPredict(ctx, "kmeans", map[string]any{})
	require.Error(t, err)
}

func TestUnitMlService_Execute_ServerError(t *testing.T) {
	srv := errServer(500)
	defer srv.Close()
	ctx := context.Background()
	svc := NewMlService(restyClient(srv), testLogger())
	_, err := svc.Execute(ctx, "anomaly_localization", map[string]any{})
	require.Error(t, err)
}

func TestUnitMlService_Execute_BadJSON(t *testing.T) {
	srv := badJSONServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewMlService(restyClient(srv), testLogger())
	_, err := svc.Execute(ctx, "anomaly_localization", map[string]any{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}

func TestUnitMlService_Execute_NetworkError(t *testing.T) {
	ctx := context.Background()
	svc := NewMlService(deadClient(), testLogger())
	_, err := svc.Execute(ctx, "anomaly_localization", map[string]any{})
	require.Error(t, err)
}

func TestUnitMlService_MlStats_ServerError(t *testing.T) {
	srv := errServer(500)
	defer srv.Close()
	ctx := context.Background()
	svc := NewMlService(restyClient(srv), testLogger())
	_, err := svc.MlStats(ctx, "")
	require.Error(t, err)
}

func TestUnitMlService_MlStats_BadJSON(t *testing.T) {
	srv := badJSONServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewMlService(restyClient(srv), testLogger())
	_, err := svc.MlStats(ctx, "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}

func TestUnitMlService_MlStats_NetworkError(t *testing.T) {
	ctx := context.Background()
	svc := NewMlService(deadClient(), testLogger())
	_, err := svc.MlStats(ctx, "")
	require.Error(t, err)
}

func TestUnitMlService_GetTask_ServerError(t *testing.T) {
	srv := errServer(500)
	defer srv.Close()
	ctx := context.Background()
	svc := NewMlService(restyClient(srv), testLogger())
	_, err := svc.GetTask(ctx, "task1")
	require.Error(t, err)
}

func TestUnitMlService_GetTask_BadJSON(t *testing.T) {
	srv := badJSONServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewMlService(restyClient(srv), testLogger())
	_, err := svc.GetTask(ctx, "task1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}

func TestUnitMlService_GetTask_NetworkError(t *testing.T) {
	ctx := context.Background()
	svc := NewMlService(deadClient(), testLogger())
	_, err := svc.GetTask(ctx, "task1")
	require.Error(t, err)
}

func TestUnitMlService_DeleteTask_ServerError(t *testing.T) {
	srv := errServer(500)
	defer srv.Close()
	ctx := context.Background()
	svc := NewMlService(restyClient(srv), testLogger())
	_, err := svc.DeleteTask(ctx, "task1")
	require.Error(t, err)
}

func TestUnitMlService_DeleteTask_BadJSON(t *testing.T) {
	srv := badJSONServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewMlService(restyClient(srv), testLogger())
	_, err := svc.DeleteTask(ctx, "task1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}

func TestUnitMlService_DeleteTask_NetworkError(t *testing.T) {
	ctx := context.Background()
	svc := NewMlService(deadClient(), testLogger())
	_, err := svc.DeleteTask(ctx, "task1")
	require.Error(t, err)
}

func TestUnitMlService_CreateConnector_ServerError(t *testing.T) {
	srv := errServer(500)
	defer srv.Close()
	ctx := context.Background()
	svc := NewMlService(restyClient(srv), testLogger())
	_, err := svc.CreateConnector(ctx, map[string]any{})
	require.Error(t, err)
}

func TestUnitMlService_CreateConnector_BadJSON(t *testing.T) {
	srv := badJSONServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewMlService(restyClient(srv), testLogger())
	_, err := svc.CreateConnector(ctx, map[string]any{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}

func TestUnitMlService_CreateConnector_NetworkError(t *testing.T) {
	ctx := context.Background()
	svc := NewMlService(deadClient(), testLogger())
	_, err := svc.CreateConnector(ctx, map[string]any{})
	require.Error(t, err)
}

func TestUnitMlService_GetConnector_ServerError(t *testing.T) {
	srv := errServer(500)
	defer srv.Close()
	ctx := context.Background()
	svc := NewMlService(restyClient(srv), testLogger())
	_, err := svc.GetConnector(ctx, "conn1")
	require.Error(t, err)
}

func TestUnitMlService_GetConnector_BadJSON(t *testing.T) {
	srv := badJSONServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewMlService(restyClient(srv), testLogger())
	_, err := svc.GetConnector(ctx, "conn1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}

func TestUnitMlService_GetConnector_NetworkError(t *testing.T) {
	ctx := context.Background()
	svc := NewMlService(deadClient(), testLogger())
	_, err := svc.GetConnector(ctx, "conn1")
	require.Error(t, err)
}

func TestUnitMlService_DeleteConnector_ServerError(t *testing.T) {
	srv := errServer(500)
	defer srv.Close()
	ctx := context.Background()
	svc := NewMlService(restyClient(srv), testLogger())
	_, err := svc.DeleteConnector(ctx, "conn1")
	require.Error(t, err)
}

func TestUnitMlService_DeleteConnector_BadJSON(t *testing.T) {
	srv := badJSONServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewMlService(restyClient(srv), testLogger())
	_, err := svc.DeleteConnector(ctx, "conn1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}

func TestUnitMlService_DeleteConnector_NetworkError(t *testing.T) {
	ctx := context.Background()
	svc := NewMlService(deadClient(), testLogger())
	_, err := svc.DeleteConnector(ctx, "conn1")
	require.Error(t, err)
}

func TestUnitMlService_SearchConnectors_ServerError(t *testing.T) {
	srv := errServer(500)
	defer srv.Close()
	ctx := context.Background()
	svc := NewMlService(restyClient(srv), testLogger())
	_, err := svc.SearchConnectors(ctx, map[string]any{})
	require.Error(t, err)
}

func TestUnitMlService_SearchConnectors_BadJSON(t *testing.T) {
	srv := badJSONServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewMlService(restyClient(srv), testLogger())
	_, err := svc.SearchConnectors(ctx, map[string]any{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}

func TestUnitMlService_SearchConnectors_NetworkError(t *testing.T) {
	ctx := context.Background()
	svc := NewMlService(deadClient(), testLogger())
	_, err := svc.SearchConnectors(ctx, map[string]any{})
	require.Error(t, err)
}

func TestUnitMlService_RegisterAgent_ServerError(t *testing.T) {
	srv := errServer(500)
	defer srv.Close()
	ctx := context.Background()
	svc := NewMlService(restyClient(srv), testLogger())
	_, err := svc.RegisterAgent(ctx, map[string]any{})
	require.Error(t, err)
}

func TestUnitMlService_RegisterAgent_BadJSON(t *testing.T) {
	srv := badJSONServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewMlService(restyClient(srv), testLogger())
	_, err := svc.RegisterAgent(ctx, map[string]any{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}

func TestUnitMlService_RegisterAgent_NetworkError(t *testing.T) {
	ctx := context.Background()
	svc := NewMlService(deadClient(), testLogger())
	_, err := svc.RegisterAgent(ctx, map[string]any{})
	require.Error(t, err)
}

func TestUnitMlService_GetAgent_ServerError(t *testing.T) {
	srv := errServer(500)
	defer srv.Close()
	ctx := context.Background()
	svc := NewMlService(restyClient(srv), testLogger())
	_, err := svc.GetAgent(ctx, "agent1")
	require.Error(t, err)
}

func TestUnitMlService_GetAgent_BadJSON(t *testing.T) {
	srv := badJSONServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewMlService(restyClient(srv), testLogger())
	_, err := svc.GetAgent(ctx, "agent1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}

func TestUnitMlService_GetAgent_NetworkError(t *testing.T) {
	ctx := context.Background()
	svc := NewMlService(deadClient(), testLogger())
	_, err := svc.GetAgent(ctx, "agent1")
	require.Error(t, err)
}

func TestUnitMlService_DeleteAgent_ServerError(t *testing.T) {
	srv := errServer(500)
	defer srv.Close()
	ctx := context.Background()
	svc := NewMlService(restyClient(srv), testLogger())
	_, err := svc.DeleteAgent(ctx, "agent1")
	require.Error(t, err)
}

func TestUnitMlService_DeleteAgent_BadJSON(t *testing.T) {
	srv := badJSONServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewMlService(restyClient(srv), testLogger())
	_, err := svc.DeleteAgent(ctx, "agent1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}

func TestUnitMlService_DeleteAgent_NetworkError(t *testing.T) {
	ctx := context.Background()
	svc := NewMlService(deadClient(), testLogger())
	_, err := svc.DeleteAgent(ctx, "agent1")
	require.Error(t, err)
}

func TestUnitMlService_SearchAgents_ServerError(t *testing.T) {
	srv := errServer(500)
	defer srv.Close()
	ctx := context.Background()
	svc := NewMlService(restyClient(srv), testLogger())
	_, err := svc.SearchAgents(ctx, map[string]any{})
	require.Error(t, err)
}

func TestUnitMlService_SearchAgents_BadJSON(t *testing.T) {
	srv := badJSONServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewMlService(restyClient(srv), testLogger())
	_, err := svc.SearchAgents(ctx, map[string]any{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}

func TestUnitMlService_SearchAgents_NetworkError(t *testing.T) {
	ctx := context.Background()
	svc := NewMlService(deadClient(), testLogger())
	_, err := svc.SearchAgents(ctx, map[string]any{})
	require.Error(t, err)
}

func TestUnitMlService_DeleteModelGroup_ServerError(t *testing.T) {
	srv := errServer(500)
	defer srv.Close()
	ctx := context.Background()
	svc := NewMlService(restyClient(srv), testLogger())
	_, err := svc.DeleteModelGroup(ctx, "mg1")
	require.Error(t, err)
}

func TestUnitMlService_DeleteModelGroup_BadJSON(t *testing.T) {
	srv := badJSONServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewMlService(restyClient(srv), testLogger())
	_, err := svc.DeleteModelGroup(ctx, "mg1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}

func TestUnitMlService_DeleteModelGroup_NetworkError(t *testing.T) {
	ctx := context.Background()
	svc := NewMlService(deadClient(), testLogger())
	_, err := svc.DeleteModelGroup(ctx, "mg1")
	require.Error(t, err)
}

func TestUnitMlService_RegisterModelGroup_ServerError(t *testing.T) {
	srv := errServer(500)
	defer srv.Close()
	ctx := context.Background()
	svc := NewMlService(restyClient(srv), testLogger())
	_, err := svc.RegisterModelGroup(ctx, map[string]any{})
	require.Error(t, err)
}

func TestUnitMlService_RegisterModelGroup_BadJSON(t *testing.T) {
	srv := badJSONServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewMlService(restyClient(srv), testLogger())
	_, err := svc.RegisterModelGroup(ctx, map[string]any{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}

func TestUnitMlService_RegisterModelGroup_NetworkError(t *testing.T) {
	ctx := context.Background()
	svc := NewMlService(deadClient(), testLogger())
	_, err := svc.RegisterModelGroup(ctx, map[string]any{})
	require.Error(t, err)
}

func TestUnitMlService_Profile_ServerError(t *testing.T) {
	srv := errServer(500)
	defer srv.Close()
	ctx := context.Background()
	svc := NewMlService(restyClient(srv), testLogger())
	_, err := svc.Profile(ctx, "")
	require.Error(t, err)
}

func TestUnitMlService_Profile_BadJSON(t *testing.T) {
	srv := badJSONServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewMlService(restyClient(srv), testLogger())
	_, err := svc.Profile(ctx, "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}

func TestUnitMlService_Profile_NetworkError(t *testing.T) {
	ctx := context.Background()
	svc := NewMlService(deadClient(), testLogger())
	_, err := svc.Profile(ctx, "")
	require.Error(t, err)
}

func TestUnitMlService_ListTools_ServerError(t *testing.T) {
	srv := errServer(500)
	defer srv.Close()
	ctx := context.Background()
	svc := NewMlService(restyClient(srv), testLogger())
	_, err := svc.ListTools(ctx)
	require.Error(t, err)
}

func TestUnitMlService_ListTools_BadJSON(t *testing.T) {
	srv := badJSONServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewMlService(restyClient(srv), testLogger())
	_, err := svc.ListTools(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}

func TestUnitMlService_ListTools_NetworkError(t *testing.T) {
	ctx := context.Background()
	svc := NewMlService(deadClient(), testLogger())
	_, err := svc.ListTools(ctx)
	require.Error(t, err)
}

func TestUnitMlService_ExecuteTool_ServerError(t *testing.T) {
	srv := errServer(500)
	defer srv.Close()
	ctx := context.Background()
	svc := NewMlService(restyClient(srv), testLogger())
	_, err := svc.ExecuteTool(ctx, "ConnectorTool", map[string]any{})
	require.Error(t, err)
}

func TestUnitMlService_ExecuteTool_BadJSON(t *testing.T) {
	srv := badJSONServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewMlService(restyClient(srv), testLogger())
	_, err := svc.ExecuteTool(ctx, "ConnectorTool", map[string]any{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}

func TestUnitMlService_ExecuteTool_NetworkError(t *testing.T) {
	ctx := context.Background()
	svc := NewMlService(deadClient(), testLogger())
	_, err := svc.ExecuteTool(ctx, "ConnectorTool", map[string]any{})
	require.Error(t, err)
}
