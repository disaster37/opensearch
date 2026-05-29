package api

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUnitSearchServiceSearch(t *testing.T) {
	respJSON := `{"took":5,"timed_out":false,"_shards":{"total":1,"successful":1,"skipped":0,"failed":0},"hits":{"total":{"value":0,"relation":"eq"},"max_score":null,"hits":[]}}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/_search") {
			w.WriteHeader(200)
			fmt.Fprint(w, respJSON)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewSearchService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success with indices", func(t *testing.T) {
		resp, err := svc.Search(ctx, &SearchRequest{Indices: []string{"idx1"}, Body: map[string]any{"query": map[string]any{"match_all": map[string]any{}}}})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("success without indices", func(t *testing.T) {
		resp, err := svc.Search(ctx, &SearchRequest{Body: map[string]any{"query": map[string]any{"match_all": map[string]any{}}}})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("success with params", func(t *testing.T) {
		resp, err := svc.Search(ctx, &SearchRequest{Indices: []string{"idx1"}, Body: map[string]any{}, Params: map[string]string{"size": "10"}})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("success with multiple indices", func(t *testing.T) {
		resp, err := svc.Search(ctx, &SearchRequest{Indices: []string{"idx1", "idx2"}, Body: map[string]any{}})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})
}

func TestUnitSearchServiceMultiSearch(t *testing.T) {
	respJSON := `{"responses":[{"took":5,"timed_out":false,"_shards":{"total":1,"successful":1,"skipped":0,"failed":0},"hits":{"total":{"value":0,"relation":"eq"},"max_score":null,"hits":[]}}]}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == "/_msearch" {
			w.WriteHeader(200)
			fmt.Fprint(w, respJSON)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewSearchService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		body := []any{
			map[string]any{"index": "idx1"},
			map[string]any{"query": map[string]any{"match_all": map[string]any{}}},
		}
		resp, err := svc.MultiSearch(ctx, body)
		require.NoError(t, err)
		require.NotNil(t, resp)
	})
}

func TestUnitSearchServiceCount(t *testing.T) {
	respJSON := `{"count":42,"_shards":{"total":1,"successful":1,"skipped":0,"failed":0}}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/_count") {
			w.WriteHeader(200)
			fmt.Fprint(w, respJSON)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewSearchService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success with indices and body", func(t *testing.T) {
		count, err := svc.Count(ctx, []string{"idx1"}, map[string]any{"query": map[string]any{"match_all": map[string]any{}}})
		require.NoError(t, err)
		assert.Equal(t, int64(42), count)
	})

	t.Run("success without indices", func(t *testing.T) {
		count, err := svc.Count(ctx, []string{}, nil)
		require.NoError(t, err)
		assert.Equal(t, int64(42), count)
	})

	t.Run("success without body but with indices", func(t *testing.T) {
		count, err := svc.Count(ctx, []string{"idx1"}, nil)
		require.NoError(t, err)
		assert.Equal(t, int64(42), count)
	})

	t.Run("success with multiple indices", func(t *testing.T) {
		count, err := svc.Count(ctx, []string{"idx1", "idx2"}, nil)
		require.NoError(t, err)
		assert.Equal(t, int64(42), count)
	})
}

func TestUnitSearchServiceScroll(t *testing.T) {
	respJSON := `{"took":5,"timed_out":false,"_shards":{"total":1,"successful":1,"skipped":0,"failed":0},"hits":{"total":{"value":0,"relation":"eq"},"max_score":null,"hits":[]}}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/_search/scroll" {
			w.WriteHeader(200)
			fmt.Fprint(w, respJSON)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewSearchService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		resp, err := svc.Scroll(ctx, &ScrollRequest{ScrollId: "abc123", KeepAlive: "10m"})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("success default keepalive", func(t *testing.T) {
		resp, err := svc.Scroll(ctx, &ScrollRequest{ScrollId: "abc123"})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("validation error missing scroll id", func(t *testing.T) {
		resp, err := svc.Scroll(ctx, &ScrollRequest{})
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Contains(t, err.Error(), "validation")
	})
}

func TestUnitSearchServiceClearScroll(t *testing.T) {
	respJSON := `{"succeeded":true,"num_freed":1}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete && r.URL.Path == "/_search/scroll" {
			w.WriteHeader(200)
			fmt.Fprint(w, respJSON)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewSearchService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		resp, err := svc.ClearScroll(ctx, []string{"scroll_id_1"})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("validation error empty scroll ids", func(t *testing.T) {
		resp, err := svc.ClearScroll(ctx, []string{})
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Contains(t, err.Error(), "scroll_ids is required")
	})

	t.Run("validation error nil scroll ids", func(t *testing.T) {
		resp, err := svc.ClearScroll(ctx, nil)
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Contains(t, err.Error(), "scroll_ids is required")
	})
}

func TestUnitSearchServiceValidate(t *testing.T) {
	respJSON := `{"valid":true,"_shards":{"total":1,"successful":1,"failed":0}}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/_validate/query") {
			w.WriteHeader(200)
			fmt.Fprint(w, respJSON)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewSearchService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success with indices", func(t *testing.T) {
		resp, err := svc.Validate(ctx, []string{"idx1"}, map[string]any{"query": map[string]any{"match_all": map[string]any{}}})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("success without indices", func(t *testing.T) {
		resp, err := svc.Validate(ctx, []string{}, nil)
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("success with nil indices", func(t *testing.T) {
		resp, err := svc.Validate(ctx, nil, nil)
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("success with multiple indices", func(t *testing.T) {
		resp, err := svc.Validate(ctx, []string{"idx1", "idx2"}, map[string]any{"query": map[string]any{"match_all": map[string]any{}}})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})
}

func TestUnitSearchServiceSearchShards(t *testing.T) {
	respJSON := `{"nodes":{},"indices":{},"shards":[]}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/_search_shards") {
			w.WriteHeader(200)
			fmt.Fprint(w, respJSON)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewSearchService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		resp, err := svc.SearchShards(ctx, []string{"idx1"})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("success multiple indices", func(t *testing.T) {
		resp, err := svc.SearchShards(ctx, []string{"idx1", "idx2"})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("validation error empty indices", func(t *testing.T) {
		resp, err := svc.SearchShards(ctx, []string{})
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Contains(t, err.Error(), "indices is required")
	})
}

func TestUnitSearchServiceFieldCaps(t *testing.T) {
	respJSON := `{"indices":["idx1"],"fields":{}}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/_field_caps") {
			if strings.Contains(r.URL.Path, "notexist") {
				w.WriteHeader(404)
				fmt.Fprint(w, `{"error":"not found"}`)
				return
			}
			w.WriteHeader(200)
			fmt.Fprint(w, respJSON)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewSearchService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success with indices", func(t *testing.T) {
		resp, err := svc.FieldCaps(ctx, &FieldCapsRequest{Indices: []string{"idx1"}, Fields: []string{"field1"}})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("success without indices", func(t *testing.T) {
		resp, err := svc.FieldCaps(ctx, &FieldCapsRequest{})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("success 404 returns empty response", func(t *testing.T) {
		resp, err := svc.FieldCaps(ctx, &FieldCapsRequest{Indices: []string{"notexist"}})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("success with multiple fields", func(t *testing.T) {
		resp, err := svc.FieldCaps(ctx, &FieldCapsRequest{Indices: []string{"idx1"}, Fields: []string{"f1", "f2"}})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})
}

func TestUnitSearchServiceErrorPaths(t *testing.T) {
	ctx := context.Background()

	t.Run("Search server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewSearchService(restyClient(srv), testLogger())
		_, err := s.Search(ctx, &SearchRequest{Indices: []string{"idx"}, Body: map[string]any{}})
		require.Error(t, err)
	})

	t.Run("Search unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewSearchService(restyClient(srv), testLogger())
		_, err := s.Search(ctx, &SearchRequest{Body: map[string]any{}})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("MultiSearch server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewSearchService(restyClient(srv), testLogger())
		_, err := s.MultiSearch(ctx, []any{})
		require.Error(t, err)
	})

	t.Run("MultiSearch unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewSearchService(restyClient(srv), testLogger())
		_, err := s.MultiSearch(ctx, []any{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("Count server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewSearchService(restyClient(srv), testLogger())
		_, err := s.Count(ctx, []string{"idx"}, nil)
		require.Error(t, err)
	})

	t.Run("Count unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewSearchService(restyClient(srv), testLogger())
		_, err := s.Count(ctx, nil, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("Scroll server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewSearchService(restyClient(srv), testLogger())
		_, err := s.Scroll(ctx, &ScrollRequest{ScrollId: "abc"})
		require.Error(t, err)
	})

	t.Run("Scroll unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewSearchService(restyClient(srv), testLogger())
		_, err := s.Scroll(ctx, &ScrollRequest{ScrollId: "abc"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("ClearScroll server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewSearchService(restyClient(srv), testLogger())
		_, err := s.ClearScroll(ctx, []string{"id1"})
		require.Error(t, err)
	})

	t.Run("ClearScroll unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewSearchService(restyClient(srv), testLogger())
		_, err := s.ClearScroll(ctx, []string{"id1"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("Validate server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewSearchService(restyClient(srv), testLogger())
		_, err := s.Validate(ctx, []string{"idx"}, nil)
		require.Error(t, err)
	})

	t.Run("Validate unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewSearchService(restyClient(srv), testLogger())
		_, err := s.Validate(ctx, []string{"idx"}, map[string]any{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("SearchShards server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewSearchService(restyClient(srv), testLogger())
		_, err := s.SearchShards(ctx, []string{"idx"})
		require.Error(t, err)
	})

	t.Run("SearchShards unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewSearchService(restyClient(srv), testLogger())
		_, err := s.SearchShards(ctx, []string{"idx"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("FieldCaps server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewSearchService(restyClient(srv), testLogger())
		_, err := s.FieldCaps(ctx, &FieldCapsRequest{Indices: []string{"idx"}})
		require.Error(t, err)
	})

	t.Run("FieldCaps unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewSearchService(restyClient(srv), testLogger())
		_, err := s.FieldCaps(ctx, &FieldCapsRequest{Indices: []string{"idx"}})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("SearchTemplate server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewSearchService(restyClient(srv), testLogger())
		_, err := s.SearchTemplate(ctx, &SearchTemplateRequest{Body: map[string]any{}})
		require.Error(t, err)
	})

	t.Run("SearchTemplate unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewSearchService(restyClient(srv), testLogger())
		_, err := s.SearchTemplate(ctx, &SearchTemplateRequest{Body: map[string]any{}})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("MultiSearchTemplate server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewSearchService(restyClient(srv), testLogger())
		_, err := s.MultiSearchTemplate(ctx, &MultiSearchTemplateRequest{Body: []any{}})
		require.Error(t, err)
	})

	t.Run("MultiSearchTemplate unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewSearchService(restyClient(srv), testLogger())
		_, err := s.MultiSearchTemplate(ctx, &MultiSearchTemplateRequest{Body: []any{}})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("RenderSearchTemplate server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewSearchService(restyClient(srv), testLogger())
		_, err := s.RenderSearchTemplate(ctx, &RenderSearchTemplateRequest{Id: "t"})
		require.Error(t, err)
	})

	t.Run("RenderSearchTemplate unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewSearchService(restyClient(srv), testLogger())
		_, err := s.RenderSearchTemplate(ctx, &RenderSearchTemplateRequest{Id: "t"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("RankEval server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewSearchService(restyClient(srv), testLogger())
		_, err := s.RankEval(ctx, &RankEvalRequest{Body: map[string]any{}})
		require.Error(t, err)
	})

	t.Run("RankEval unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewSearchService(restyClient(srv), testLogger())
		_, err := s.RankEval(ctx, &RankEvalRequest{Body: map[string]any{}})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("CreatePIT server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewSearchService(restyClient(srv), testLogger())
		_, err := s.CreatePIT(ctx, &CreatePITRequest{Indices: []string{"idx"}, KeepAlive: "5m"})
		require.Error(t, err)
	})

	t.Run("CreatePIT unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewSearchService(restyClient(srv), testLogger())
		_, err := s.CreatePIT(ctx, &CreatePITRequest{Indices: []string{"idx"}, KeepAlive: "5m"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("DeletePIT server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewSearchService(restyClient(srv), testLogger())
		_, err := s.DeletePIT(ctx, &DeletePITRequest{PitIds: []string{"p1"}})
		require.Error(t, err)
	})

	t.Run("DeletePIT unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewSearchService(restyClient(srv), testLogger())
		_, err := s.DeletePIT(ctx, &DeletePITRequest{PitIds: []string{"p1"}})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("GetAllPITs server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewSearchService(restyClient(srv), testLogger())
		_, err := s.GetAllPITs(ctx)
		require.Error(t, err)
	})

	t.Run("GetAllPITs unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewSearchService(restyClient(srv), testLogger())
		_, err := s.GetAllPITs(ctx)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("DeleteAllPITs server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewSearchService(restyClient(srv), testLogger())
		_, err := s.DeleteAllPITs(ctx)
		require.Error(t, err)
	})

	t.Run("DeleteAllPITs unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewSearchService(restyClient(srv), testLogger())
		_, err := s.DeleteAllPITs(ctx)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})
}

func TestUnitSearchServiceNetworkErrors(t *testing.T) {
	ctx := context.Background()
	s := NewSearchService(deadClient(), testLogger())

	t.Run("Search network error", func(t *testing.T) {
		_, err := s.Search(ctx, &SearchRequest{Body: map[string]any{}})
		require.Error(t, err)
	})

	t.Run("MultiSearch network error", func(t *testing.T) {
		_, err := s.MultiSearch(ctx, []any{})
		require.Error(t, err)
	})

	t.Run("Count network error", func(t *testing.T) {
		_, err := s.Count(ctx, nil, nil)
		require.Error(t, err)
	})

	t.Run("Scroll network error", func(t *testing.T) {
		_, err := s.Scroll(ctx, &ScrollRequest{ScrollId: "abc"})
		require.Error(t, err)
	})

	t.Run("ClearScroll network error", func(t *testing.T) {
		_, err := s.ClearScroll(ctx, []string{"id"})
		require.Error(t, err)
	})

	t.Run("Validate network error", func(t *testing.T) {
		_, err := s.Validate(ctx, []string{"idx"}, nil)
		require.Error(t, err)
	})

	t.Run("SearchShards network error", func(t *testing.T) {
		_, err := s.SearchShards(ctx, []string{"idx"})
		require.Error(t, err)
	})

	t.Run("FieldCaps network error", func(t *testing.T) {
		_, err := s.FieldCaps(ctx, &FieldCapsRequest{Indices: []string{"idx"}})
		require.Error(t, err)
	})

	t.Run("SearchTemplate network error", func(t *testing.T) {
		_, err := s.SearchTemplate(ctx, &SearchTemplateRequest{Body: map[string]any{}})
		require.Error(t, err)
	})

	t.Run("MultiSearchTemplate network error", func(t *testing.T) {
		_, err := s.MultiSearchTemplate(ctx, &MultiSearchTemplateRequest{Body: []any{}})
		require.Error(t, err)
	})

	t.Run("RenderSearchTemplate network error", func(t *testing.T) {
		_, err := s.RenderSearchTemplate(ctx, &RenderSearchTemplateRequest{})
		require.Error(t, err)
	})

	t.Run("RankEval network error", func(t *testing.T) {
		_, err := s.RankEval(ctx, &RankEvalRequest{Body: map[string]any{}})
		require.Error(t, err)
	})

	t.Run("CreatePIT network error", func(t *testing.T) {
		_, err := s.CreatePIT(ctx, &CreatePITRequest{Indices: []string{"idx"}, KeepAlive: "5m"})
		require.Error(t, err)
	})

	t.Run("DeletePIT network error", func(t *testing.T) {
		_, err := s.DeletePIT(ctx, &DeletePITRequest{PitIds: []string{"pit1"}})
		require.Error(t, err)
	})

	t.Run("GetAllPITs network error", func(t *testing.T) {
		_, err := s.GetAllPITs(ctx)
		require.Error(t, err)
	})

	t.Run("DeleteAllPITs network error", func(t *testing.T) {
		_, err := s.DeleteAllPITs(ctx)
		require.Error(t, err)
	})
}

func TestUnitSearchServiceSearchTemplate(t *testing.T) {
	respJSON := `{"took":5,"timed_out":false,"_shards":{"total":1,"successful":1,"skipped":0,"failed":0},"hits":{"total":{"value":0,"relation":"eq"},"max_score":null,"hits":[]}}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/_search/template") {
			w.WriteHeader(200)
			fmt.Fprint(w, respJSON)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewSearchService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success with indices", func(t *testing.T) {
		resp, err := svc.SearchTemplate(ctx, &SearchTemplateRequest{Indices: []string{"idx1"}, Body: map[string]any{"id": "my-template"}})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("success without indices", func(t *testing.T) {
		resp, err := svc.SearchTemplate(ctx, &SearchTemplateRequest{Body: map[string]any{"id": "my-template"}})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("success with params", func(t *testing.T) {
		resp, err := svc.SearchTemplate(ctx, &SearchTemplateRequest{Indices: []string{"idx1"}, Body: map[string]any{"id": "t"}, Params: map[string]string{"search_type": "query_then_fetch"}})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("validation error missing body", func(t *testing.T) {
		resp, err := svc.SearchTemplate(ctx, &SearchTemplateRequest{})
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Contains(t, err.Error(), "validation")
	})
}

func TestUnitSearchServiceMultiSearchTemplate(t *testing.T) {
	respJSON := `{"responses":[{"took":5,"timed_out":false,"_shards":{"total":1,"successful":1,"skipped":0,"failed":0},"hits":{"total":{"value":0,"relation":"eq"},"max_score":null,"hits":[]}}]}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/_msearch/template") {
			w.WriteHeader(200)
			fmt.Fprint(w, respJSON)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewSearchService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success with indices", func(t *testing.T) {
		resp, err := svc.MultiSearchTemplate(ctx, &MultiSearchTemplateRequest{Indices: []string{"idx1"}, Body: []any{}})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("success without indices", func(t *testing.T) {
		resp, err := svc.MultiSearchTemplate(ctx, &MultiSearchTemplateRequest{Body: []any{}})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("success with params", func(t *testing.T) {
		resp, err := svc.MultiSearchTemplate(ctx, &MultiSearchTemplateRequest{Body: []any{}, Params: map[string]string{"search_type": "query_then_fetch"}})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("validation error missing body", func(t *testing.T) {
		resp, err := svc.MultiSearchTemplate(ctx, &MultiSearchTemplateRequest{})
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Contains(t, err.Error(), "validation")
	})
}

func TestUnitSearchServiceRenderSearchTemplate(t *testing.T) {
	respJSON := `{"template_output":{"query":{"match_all":{}}}}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/_render/template") {
			w.WriteHeader(200)
			fmt.Fprint(w, respJSON)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewSearchService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success with id", func(t *testing.T) {
		resp, err := svc.RenderSearchTemplate(ctx, &RenderSearchTemplateRequest{Id: "my-template", Body: map[string]any{"params": map[string]any{}}})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("success without id", func(t *testing.T) {
		resp, err := svc.RenderSearchTemplate(ctx, &RenderSearchTemplateRequest{Body: map[string]any{"source": "{}"}})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("success without body", func(t *testing.T) {
		resp, err := svc.RenderSearchTemplate(ctx, &RenderSearchTemplateRequest{Id: "my-template"})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})
}

func TestUnitSearchServiceRankEval(t *testing.T) {
	respJSON := `{"rank_eval_score":0.95,"details":{},"failures":{}}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/_rank_eval") {
			w.WriteHeader(200)
			fmt.Fprint(w, respJSON)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewSearchService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success with indices", func(t *testing.T) {
		resp, err := svc.RankEval(ctx, &RankEvalRequest{Indices: []string{"idx1"}, Body: map[string]any{}})
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, 0.95, resp.RankEvalScore)
	})

	t.Run("success without indices", func(t *testing.T) {
		resp, err := svc.RankEval(ctx, &RankEvalRequest{Body: map[string]any{}})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("success with params", func(t *testing.T) {
		resp, err := svc.RankEval(ctx, &RankEvalRequest{Body: map[string]any{}, Params: map[string]string{"search_type": "query_then_fetch"}})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("validation error missing body", func(t *testing.T) {
		resp, err := svc.RankEval(ctx, &RankEvalRequest{})
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Contains(t, err.Error(), "validation")
	})
}

func TestUnitSearchServicePIT(t *testing.T) {
	createRespJSON := `{"pit_id":"abc123","creation_time":1609459200000,"_shards":{"total":1,"successful":1,"failed":0}}`
	listRespJSON := `{"pits":[{"pit_id":"abc123","creation_time":1609459200000,"keep_alive":300000}]}`
	deleteRespJSON := `{"pits":[{"pit_id":"abc123","successful":true}]}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/_search/point_in_time") {
			w.WriteHeader(200)
			fmt.Fprint(w, createRespJSON)
			return
		}
		if r.Method == http.MethodGet && r.URL.Path == "/_search/point_in_time/_all" {
			w.WriteHeader(200)
			fmt.Fprint(w, listRespJSON)
			return
		}
		if r.Method == http.MethodDelete && r.URL.Path == "/_search/point_in_time" {
			w.WriteHeader(200)
			fmt.Fprint(w, deleteRespJSON)
			return
		}
		if r.Method == http.MethodDelete && r.URL.Path == "/_search/point_in_time/_all" {
			w.WriteHeader(200)
			fmt.Fprint(w, deleteRespJSON)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewSearchService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("CreatePIT success", func(t *testing.T) {
		resp, err := svc.CreatePIT(ctx, &CreatePITRequest{Indices: []string{"idx1"}, KeepAlive: "5m"})
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, "abc123", resp.PitId)
	})

	t.Run("CreatePIT success with params", func(t *testing.T) {
		resp, err := svc.CreatePIT(ctx, &CreatePITRequest{Indices: []string{"idx1"}, KeepAlive: "5m", Params: map[string]string{"allow_partial_pit_creation": "true"}})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("CreatePIT validation error missing indices", func(t *testing.T) {
		resp, err := svc.CreatePIT(ctx, &CreatePITRequest{KeepAlive: "5m"})
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Contains(t, err.Error(), "validation")
	})

	t.Run("CreatePIT validation error missing keep alive", func(t *testing.T) {
		resp, err := svc.CreatePIT(ctx, &CreatePITRequest{Indices: []string{"idx1"}})
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Contains(t, err.Error(), "validation")
	})

	t.Run("DeletePIT success", func(t *testing.T) {
		resp, err := svc.DeletePIT(ctx, &DeletePITRequest{PitIds: []string{"abc123"}})
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.True(t, resp.Pits[0].Successful)
	})

	t.Run("DeletePIT validation error empty pit ids", func(t *testing.T) {
		resp, err := svc.DeletePIT(ctx, &DeletePITRequest{})
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Contains(t, err.Error(), "validation")
	})

	t.Run("GetAllPITs success", func(t *testing.T) {
		resp, err := svc.GetAllPITs(ctx)
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Len(t, resp.Pits, 1)
		assert.Equal(t, "abc123", resp.Pits[0].PitId)
	})

	t.Run("DeleteAllPITs success", func(t *testing.T) {
		resp, err := svc.DeleteAllPITs(ctx)
		require.NoError(t, err)
		require.NotNil(t, resp)
	})
}
