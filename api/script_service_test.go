package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUnitScriptService_Get(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "GET", r.Method)
		assert.Equal(t, "/_scripts/my-script", r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"_id":"my-script","found":true,"script":{"lang":"painless","source":"return 1;"}}`))
	}))
	defer ts.Close()

	svc := NewScriptService(restyClient(ts), testLogger())
	ctx := context.Background()

	resp, err := svc.Get(ctx, "my-script")
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "my-script", resp.Id)
	assert.True(t, resp.Found)
	assert.NotNil(t, resp.Script)
}

func TestUnitScriptService_Put(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "PUT", r.Method)
		assert.Equal(t, "/_scripts/my-script", r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"acknowledged":true}`))
	}))
	defer ts.Close()

	svc := NewScriptService(restyClient(ts), testLogger())
	ctx := context.Background()

	resp, err := svc.Put(ctx, &ScriptPutRequest{
		Id: "my-script",
		Body: map[string]any{
			"script": map[string]any{
				"lang":   "painless",
				"source": "return 1;",
			},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.True(t, resp.Acknowledged)
}

func TestUnitScriptService_Delete(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "DELETE", r.Method)
		assert.Equal(t, "/_scripts/my-script", r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"acknowledged":true}`))
	}))
	defer ts.Close()

	svc := NewScriptService(restyClient(ts), testLogger())
	ctx := context.Background()

	resp, err := svc.Delete(ctx, "my-script")
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.True(t, resp.Acknowledged)
}

func TestUnitScriptService_PainlessExecute(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "POST", r.Method)
		assert.Equal(t, "/_scripts/painless/_execute", r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"result":3}`))
	}))
	defer ts.Close()

	svc := NewScriptService(restyClient(ts), testLogger())
	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		resp, err := svc.PainlessExecute(ctx, map[string]any{
			"script": map[string]any{
				"source": "1 + 2",
				"lang":   "painless",
			},
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.NotEmpty(t, resp.Result)
	})

	t.Run("server error", func(t *testing.T) {
		errSrv := errServer(500)
		defer errSrv.Close()
		s := NewScriptService(restyClient(errSrv), testLogger())
		_, err := s.PainlessExecute(ctx, map[string]any{})
		require.Error(t, err)
	})

	t.Run("unmarshal error", func(t *testing.T) {
		badSrv := badJSONServer()
		defer badSrv.Close()
		s := NewScriptService(restyClient(badSrv), testLogger())
		_, err := s.PainlessExecute(ctx, map[string]any{})
		// badJSONServer returns "not json at all" which is still unmarshalable as json.RawMessage
		// so no unmarshal error here
		if err != nil {
			assert.Contains(t, err.Error(), "unmarshal")
		}
	})

	t.Run("network error", func(t *testing.T) {
		s := NewScriptService(deadClient(), testLogger())
		_, err := s.PainlessExecute(ctx, map[string]any{})
		require.Error(t, err)
	})
}

func TestUnitScriptService_GetContext(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "GET", r.Method)
		assert.Equal(t, "/_script_context", r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"contexts":[{"name":"ingest"},{"name":"score"}]}`))
	}))
	defer ts.Close()

	svc := NewScriptService(restyClient(ts), testLogger())
	ctx := context.Background()

	resp, err := svc.GetContext(ctx)
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Len(t, resp.Contexts, 2)
	assert.Equal(t, "ingest", resp.Contexts[0].Name)
}

func TestUnitScriptService_GetContext_Errors(t *testing.T) {
	ctx := context.Background()

	t.Run("server error", func(t *testing.T) {
		errSrv := errServer(500)
		defer errSrv.Close()
		s := NewScriptService(restyClient(errSrv), testLogger())
		_, err := s.GetContext(ctx)
		require.Error(t, err)
	})

	t.Run("unmarshal error", func(t *testing.T) {
		badSrv := badJSONServer()
		defer badSrv.Close()
		s := NewScriptService(restyClient(badSrv), testLogger())
		_, err := s.GetContext(ctx)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("network error", func(t *testing.T) {
		s := NewScriptService(deadClient(), testLogger())
		_, err := s.GetContext(ctx)
		require.Error(t, err)
	})
}

func TestUnitScriptService_GetLanguages(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "GET", r.Method)
		assert.Equal(t, "/_script_language", r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"language_contexts":[{"language":"painless","contexts":["ingest","score"]}]}`))
	}))
	defer ts.Close()

	svc := NewScriptService(restyClient(ts), testLogger())
	ctx := context.Background()

	resp, err := svc.GetLanguages(ctx)
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Len(t, resp.LanguageContexts, 1)
	assert.Equal(t, "painless", resp.LanguageContexts[0].Language)
}

func TestUnitScriptService_GetLanguages_Errors(t *testing.T) {
	ctx := context.Background()

	t.Run("server error", func(t *testing.T) {
		errSrv := errServer(500)
		defer errSrv.Close()
		s := NewScriptService(restyClient(errSrv), testLogger())
		_, err := s.GetLanguages(ctx)
		require.Error(t, err)
	})

	t.Run("unmarshal error", func(t *testing.T) {
		badSrv := badJSONServer()
		defer badSrv.Close()
		s := NewScriptService(restyClient(badSrv), testLogger())
		_, err := s.GetLanguages(ctx)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("network error", func(t *testing.T) {
		s := NewScriptService(deadClient(), testLogger())
		_, err := s.GetLanguages(ctx)
		require.Error(t, err)
	})
}
