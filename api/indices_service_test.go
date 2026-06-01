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

func TestUnitIndicesServiceCreate(t *testing.T) {
	respJSON := `{"acknowledged":true,"shards_acknowledged":true,"index":"test"}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut && r.URL.Path == "/myindex" {
			w.WriteHeader(200)
			_, _ = fmt.Fprint(w, respJSON)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewIndicesService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		body := map[string]any{"settings": map[string]any{"number_of_shards": 1}}
		resp, err := svc.Create(ctx, "myindex", body)
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("success without body", func(t *testing.T) {
		resp, err := svc.Create(ctx, "myindex", nil)
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("validation error empty index", func(t *testing.T) {
		resp, err := svc.Create(ctx, "", nil)
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Contains(t, err.Error(), "index is required")
	})
}

func TestUnitIndicesServiceDelete(t *testing.T) {
	respJSON := `{"acknowledged":true}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, "/") {
			w.WriteHeader(200)
			_, _ = fmt.Fprint(w, respJSON)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewIndicesService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		resp, err := svc.Delete(ctx, []string{"myindex"})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("success multiple indices", func(t *testing.T) {
		resp, err := svc.Delete(ctx, []string{"idx1", "idx2"})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("validation error empty indices", func(t *testing.T) {
		resp, err := svc.Delete(ctx, []string{})
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Contains(t, err.Error(), "indices is required")
	})
}

func TestUnitIndicesServiceGet(t *testing.T) {
	respJSON := `{"myindex":{"aliases":{},"mappings":{},"settings":{"index":{"number_of_shards":"1"}},"warmers":{}}}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path != "/" && r.URL.Path != "" {
			w.WriteHeader(200)
			_, _ = fmt.Fprint(w, respJSON)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewIndicesService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		resp, err := svc.Get(ctx, []string{"myindex"})
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Contains(t, resp, "myindex")
	})

	t.Run("success multiple indices", func(t *testing.T) {
		resp, err := svc.Get(ctx, []string{"idx1", "idx2"})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("validation error empty indices", func(t *testing.T) {
		resp, err := svc.Get(ctx, []string{})
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Contains(t, err.Error(), "indices is required")
	})
}

func TestUnitIndicesServiceExists(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			if strings.Contains(r.URL.Path, "notexist") {
				w.WriteHeader(404)
				return
			}
			if strings.Contains(r.URL.Path, "error") {
				w.WriteHeader(500)
				return
			}
			w.WriteHeader(200)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewIndicesService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("exists", func(t *testing.T) {
		exists, err := svc.Exists(ctx, []string{"myindex"})
		require.NoError(t, err)
		assert.True(t, exists)
	})

	t.Run("not exists", func(t *testing.T) {
		exists, err := svc.Exists(ctx, []string{"notexist"})
		require.NoError(t, err)
		assert.False(t, exists)
	})

	t.Run("unexpected status", func(t *testing.T) {
		exists, err := svc.Exists(ctx, []string{"error"})
		require.Error(t, err)
		assert.False(t, exists)
		assert.Contains(t, err.Error(), "unexpected HTTP status")
	})

	t.Run("validation error empty indices", func(t *testing.T) {
		exists, err := svc.Exists(ctx, []string{})
		require.Error(t, err)
		assert.False(t, exists)
		assert.Contains(t, err.Error(), "indices is required")
	})

	t.Run("exists multiple indices", func(t *testing.T) {
		exists, err := svc.Exists(ctx, []string{"myindex", "myindex"})
		require.NoError(t, err)
		assert.True(t, exists)
	})
}

func TestUnitIndicesServiceOpen(t *testing.T) {
	respJSON := `{"acknowledged":true,"shards_acknowledged":true}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/myindex/_open" {
			w.WriteHeader(200)
			_, _ = fmt.Fprint(w, respJSON)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewIndicesService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		resp, err := svc.Open(ctx, "myindex")
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("validation error empty index", func(t *testing.T) {
		resp, err := svc.Open(ctx, "")
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Contains(t, err.Error(), "index is required")
	})
}

func TestUnitIndicesServiceClose(t *testing.T) {
	respJSON := `{"acknowledged":true,"shards_acknowledged":true}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/myindex/_close" {
			w.WriteHeader(200)
			_, _ = fmt.Fprint(w, respJSON)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewIndicesService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		resp, err := svc.Close(ctx, "myindex")
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("validation error empty index", func(t *testing.T) {
		resp, err := svc.Close(ctx, "")
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Contains(t, err.Error(), "index is required")
	})
}

func TestUnitIndicesServiceRollover(t *testing.T) {
	respJSON := `{"old_index":"my-alias-000001","new_index":"my-alias-000002","rolled_over":true,"dry_run":false,"acknowledged":true,"shards_acknowledged":true,"conditions":{}}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/_rollover") {
			w.WriteHeader(200)
			_, _ = fmt.Fprint(w, respJSON)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewIndicesService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		body := map[string]any{"conditions": map[string]any{"max_age": "7d"}}
		resp, err := svc.Rollover(ctx, "my-alias", body)
		require.NoError(t, err)
		assert.True(t, resp.RolledOver)
	})

	t.Run("success without body", func(t *testing.T) {
		resp, err := svc.Rollover(ctx, "my-alias", nil)
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("validation error empty alias", func(t *testing.T) {
		resp, err := svc.Rollover(ctx, "", nil)
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Contains(t, err.Error(), "alias is required")
	})
}

func TestUnitIndicesServiceShrink(t *testing.T) {
	respJSON := `{"acknowledged":true,"shards_acknowledged":true}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && strings.Contains(r.URL.Path, "/_shrink/") {
			w.WriteHeader(200)
			_, _ = fmt.Fprint(w, respJSON)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewIndicesService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		resp, err := svc.Shrink(ctx, &ShrinkRequest{Source: "source-idx", Target: "target-idx"})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("success with body", func(t *testing.T) {
		body := map[string]any{"settings": map[string]any{"index.number_of_shards": 1}}
		resp, err := svc.Shrink(ctx, &ShrinkRequest{Source: "source-idx", Target: "target-idx", Body: body})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("validation error missing source", func(t *testing.T) {
		resp, err := svc.Shrink(ctx, &ShrinkRequest{Target: "target-idx"})
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Contains(t, err.Error(), "validation")
	})

	t.Run("validation error missing target", func(t *testing.T) {
		resp, err := svc.Shrink(ctx, &ShrinkRequest{Source: "source-idx"})
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Contains(t, err.Error(), "validation")
	})
}

func TestUnitIndicesServiceFlush(t *testing.T) {
	respJSON := `{"_shards":{"total":10,"successful":5,"failed":0}}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/_flush") {
			w.WriteHeader(200)
			_, _ = fmt.Fprint(w, respJSON)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewIndicesService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success with indices", func(t *testing.T) {
		resp, err := svc.Flush(ctx, []string{"myindex"})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("success without indices", func(t *testing.T) {
		resp, err := svc.Flush(ctx, nil)
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("success with multiple indices", func(t *testing.T) {
		resp, err := svc.Flush(ctx, []string{"idx1", "idx2"})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})
}

func TestUnitIndicesServiceRefresh(t *testing.T) {
	respJSON := `{"_shards":{"total":10,"successful":5,"failed":0}}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/_refresh") {
			w.WriteHeader(200)
			_, _ = fmt.Fprint(w, respJSON)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewIndicesService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success with indices", func(t *testing.T) {
		resp, err := svc.Refresh(ctx, []string{"myindex"})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("success without indices", func(t *testing.T) {
		resp, err := svc.Refresh(ctx, nil)
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("success with multiple indices", func(t *testing.T) {
		resp, err := svc.Refresh(ctx, []string{"idx1", "idx2"})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})
}

func TestUnitIndicesServiceForcemerge(t *testing.T) {
	respJSON := `{"_shards":{"total":10,"successful":5,"failed":0}}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/_forcemerge") {
			w.WriteHeader(200)
			_, _ = fmt.Fprint(w, respJSON)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewIndicesService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success with indices", func(t *testing.T) {
		resp, err := svc.Forcemerge(ctx, []string{"myindex"})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("success without indices", func(t *testing.T) {
		resp, err := svc.Forcemerge(ctx, nil)
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("success with multiple indices", func(t *testing.T) {
		resp, err := svc.Forcemerge(ctx, []string{"idx1", "idx2"})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})
}

func TestUnitIndicesServiceFreeze(t *testing.T) {
	respJSON := `{"acknowledged":true}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/myindex/_freeze" {
			w.WriteHeader(200)
			_, _ = fmt.Fprint(w, respJSON)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewIndicesService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		resp, err := svc.Freeze(ctx, "myindex")
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("validation error empty index", func(t *testing.T) {
		resp, err := svc.Freeze(ctx, "")
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Contains(t, err.Error(), "index is required")
	})
}

func TestUnitIndicesServiceUnfreeze(t *testing.T) {
	respJSON := `{"acknowledged":true}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/myindex/_unfreeze" {
			w.WriteHeader(200)
			_, _ = fmt.Fprint(w, respJSON)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewIndicesService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		resp, err := svc.Unfreeze(ctx, "myindex")
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("validation error empty index", func(t *testing.T) {
		resp, err := svc.Unfreeze(ctx, "")
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Contains(t, err.Error(), "index is required")
	})
}

func TestUnitIndicesServiceClearCache(t *testing.T) {
	respJSON := `{"_shards":{"total":10,"successful":5,"failed":0}}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/_cache/clear") {
			w.WriteHeader(200)
			_, _ = fmt.Fprint(w, respJSON)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewIndicesService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success with indices", func(t *testing.T) {
		resp, err := svc.ClearCache(ctx, []string{"myindex"})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("success without indices", func(t *testing.T) {
		resp, err := svc.ClearCache(ctx, nil)
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("success with multiple indices", func(t *testing.T) {
		resp, err := svc.ClearCache(ctx, []string{"idx1", "idx2"})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})
}

func TestUnitIndicesServiceStats(t *testing.T) {
	respJSON := `{"_shards":{"total":10,"successful":5,"failed":0},"_all":{"primaries":{},"total":{}},"indices":{}}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/_stats") {
			w.WriteHeader(200)
			_, _ = fmt.Fprint(w, respJSON)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewIndicesService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success no indices no metrics", func(t *testing.T) {
		resp, err := svc.Stats(ctx, &IndicesStatsRequest{})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("success with indices only", func(t *testing.T) {
		resp, err := svc.Stats(ctx, &IndicesStatsRequest{Indices: []string{"myindex"}})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("success with metrics only", func(t *testing.T) {
		resp, err := svc.Stats(ctx, &IndicesStatsRequest{Metrics: []string{"docs"}})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("success with indices and metrics", func(t *testing.T) {
		resp, err := svc.Stats(ctx, &IndicesStatsRequest{Indices: []string{"myindex"}, Metrics: []string{"docs", "store"}})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("success with multiple indices and metrics", func(t *testing.T) {
		resp, err := svc.Stats(ctx, &IndicesStatsRequest{Indices: []string{"idx1", "idx2"}, Metrics: []string{"docs"}})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})
}

func TestUnitIndicesServiceSegments(t *testing.T) {
	respJSON := `{"_shards":{"total":10,"successful":5,"failed":0},"indices":{}}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/_segments") {
			w.WriteHeader(200)
			_, _ = fmt.Fprint(w, respJSON)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewIndicesService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success with indices", func(t *testing.T) {
		resp, err := svc.Segments(ctx, []string{"myindex"})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("success without indices", func(t *testing.T) {
		resp, err := svc.Segments(ctx, nil)
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("success with multiple indices", func(t *testing.T) {
		resp, err := svc.Segments(ctx, []string{"idx1", "idx2"})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})
}

func TestUnitIndicesServiceAnalyze(t *testing.T) {
	respJSON := `{"tokens":[{"token":"hello","start_offset":0,"end_offset":5,"type":"<ALPHANUM>","position":0}],"detail":{"custom_analyzer":false}}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/_analyze") {
			w.WriteHeader(200)
			_, _ = fmt.Fprint(w, respJSON)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewIndicesService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success with index", func(t *testing.T) {
		body := map[string]any{"analyzer": "standard", "text": "Hello World"}
		resp, err := svc.Analyze(ctx, "myindex", body)
		require.NoError(t, err)
		require.Len(t, resp.Tokens, 1)
	})

	t.Run("success without index", func(t *testing.T) {
		body := map[string]any{"analyzer": "standard", "text": "Hello World"}
		resp, err := svc.Analyze(ctx, "", body)
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("success without body", func(t *testing.T) {
		resp, err := svc.Analyze(ctx, "myindex", nil)
		require.NoError(t, err)
		require.NotNil(t, resp)
	})
}

func TestUnitIndicesServicePutAlias(t *testing.T) {
	respJSON := `{"acknowledged":true}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut && strings.Contains(r.URL.Path, "/_alias/") {
			w.WriteHeader(200)
			_, _ = fmt.Fprint(w, respJSON)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewIndicesService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		resp, err := svc.PutAlias(ctx, &PutAliasRequest{Index: "myindex", Alias: "myalias"})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("success with body", func(t *testing.T) {
		body := map[string]any{"filter": map[string]any{"term": map[string]any{"status": "active"}}}
		resp, err := svc.PutAlias(ctx, &PutAliasRequest{Index: "myindex", Alias: "myalias", Body: body})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("validation error missing index", func(t *testing.T) {
		resp, err := svc.PutAlias(ctx, &PutAliasRequest{Alias: "myalias"})
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Contains(t, err.Error(), "validation")
	})

	t.Run("validation error missing alias", func(t *testing.T) {
		resp, err := svc.PutAlias(ctx, &PutAliasRequest{Index: "myindex"})
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Contains(t, err.Error(), "validation")
	})
}

func TestUnitIndicesServiceGetAliases(t *testing.T) {
	respJSON := `{"myindex":{"aliases":{"myalias":{}},"mappings":{},"settings":{},"warmers":{}}}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/_alias") {
			w.WriteHeader(200)
			_, _ = fmt.Fprint(w, respJSON)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewIndicesService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success with indices", func(t *testing.T) {
		resp, err := svc.GetAliases(ctx, []string{"myindex"})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("success without indices", func(t *testing.T) {
		resp, err := svc.GetAliases(ctx, nil)
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("success with multiple indices", func(t *testing.T) {
		resp, err := svc.GetAliases(ctx, []string{"idx1", "idx2"})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})
}

func TestUnitIndicesServiceGetSettings(t *testing.T) {
	respJSON := `{"myindex":{"settings":{"index":{"number_of_shards":"1","number_of_replicas":"1"}}}}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/_settings") {
			w.WriteHeader(200)
			_, _ = fmt.Fprint(w, respJSON)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewIndicesService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success with indices", func(t *testing.T) {
		resp, err := svc.GetSettings(ctx, []string{"myindex"})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("success without indices", func(t *testing.T) {
		resp, err := svc.GetSettings(ctx, nil)
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("success with multiple indices", func(t *testing.T) {
		resp, err := svc.GetSettings(ctx, []string{"idx1", "idx2"})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})
}

func TestUnitIndicesServicePutSettings(t *testing.T) {
	respJSON := `{"acknowledged":true}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut && strings.HasSuffix(r.URL.Path, "/_settings") {
			w.WriteHeader(200)
			_, _ = fmt.Fprint(w, respJSON)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewIndicesService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success with indices", func(t *testing.T) {
		body := map[string]any{"index": map[string]any{"number_of_replicas": 2}}
		resp, err := svc.PutSettings(ctx, []string{"myindex"}, body)
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("success without indices", func(t *testing.T) {
		body := map[string]any{"index": map[string]any{"number_of_replicas": 2}}
		resp, err := svc.PutSettings(ctx, nil, body)
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("success without body", func(t *testing.T) {
		resp, err := svc.PutSettings(ctx, []string{"myindex"}, nil)
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("success with multiple indices", func(t *testing.T) {
		body := map[string]any{"index": map[string]any{"refresh_interval": "30s"}}
		resp, err := svc.PutSettings(ctx, []string{"idx1", "idx2"}, body)
		require.NoError(t, err)
		require.NotNil(t, resp)
	})
}

func TestUnitIndicesServiceGetMapping(t *testing.T) {
	respJSON := `{"myindex":{"mappings":{"properties":{"field":{"type":"text"}}}}}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/_mapping") {
			w.WriteHeader(200)
			_, _ = fmt.Fprint(w, respJSON)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewIndicesService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success with indices", func(t *testing.T) {
		resp, err := svc.GetMapping(ctx, []string{"myindex"})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("success without indices", func(t *testing.T) {
		resp, err := svc.GetMapping(ctx, nil)
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("success with multiple indices", func(t *testing.T) {
		resp, err := svc.GetMapping(ctx, []string{"idx1", "idx2"})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})
}

func TestUnitIndicesServicePutMapping(t *testing.T) {
	respJSON := `{"acknowledged":true}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut && strings.HasSuffix(r.URL.Path, "/_mapping") {
			w.WriteHeader(200)
			_, _ = fmt.Fprint(w, respJSON)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewIndicesService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		body := map[string]any{"properties": map[string]any{"new_field": map[string]any{"type": "keyword"}}}
		resp, err := svc.PutMapping(ctx, &PutMappingRequest{Indices: []string{"myindex"}, Body: body})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("success with multiple indices", func(t *testing.T) {
		body := map[string]any{"properties": map[string]any{"new_field": map[string]any{"type": "keyword"}}}
		resp, err := svc.PutMapping(ctx, &PutMappingRequest{Indices: []string{"idx1", "idx2"}, Body: body})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("validation error missing indices", func(t *testing.T) {
		body := map[string]any{"properties": map[string]any{"field": map[string]any{"type": "text"}}}
		resp, err := svc.PutMapping(ctx, &PutMappingRequest{Body: body})
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Contains(t, err.Error(), "validation")
	})

	t.Run("validation error empty indices", func(t *testing.T) {
		body := map[string]any{"properties": map[string]any{"field": map[string]any{"type": "text"}}}
		resp, err := svc.PutMapping(ctx, &PutMappingRequest{Indices: []string{}, Body: body})
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Contains(t, err.Error(), "validation")
	})

	t.Run("validation error missing body", func(t *testing.T) {
		resp, err := svc.PutMapping(ctx, &PutMappingRequest{Indices: []string{"myindex"}})
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Contains(t, err.Error(), "validation")
	})
}

func TestUnitIndicesServiceGetFieldMapping(t *testing.T) {
	respJSON := `{"myindex":{"mappings":{"field":{"full_name":"field","mapping":{"type":"text"}}}}}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/_mapping/field/") {
			w.WriteHeader(200)
			_, _ = fmt.Fprint(w, respJSON)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewIndicesService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success with indices", func(t *testing.T) {
		resp, err := svc.GetFieldMapping(ctx, &GetFieldMappingRequest{Indices: []string{"myindex"}, Fields: []string{"field"}})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("success without indices", func(t *testing.T) {
		resp, err := svc.GetFieldMapping(ctx, &GetFieldMappingRequest{Fields: []string{"field"}})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("success with multiple fields", func(t *testing.T) {
		resp, err := svc.GetFieldMapping(ctx, &GetFieldMappingRequest{Fields: []string{"field1", "field2"}})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("validation error missing fields", func(t *testing.T) {
		resp, err := svc.GetFieldMapping(ctx, &GetFieldMappingRequest{})
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Contains(t, err.Error(), "validation")
	})

	t.Run("validation error empty fields", func(t *testing.T) {
		resp, err := svc.GetFieldMapping(ctx, &GetFieldMappingRequest{Fields: []string{}})
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Contains(t, err.Error(), "validation")
	})
}

func TestUnitIndicesServiceGetTemplate(t *testing.T) {
	respJSON := `{"my_template":{"index_patterns":["myindex-*"],"order":0,"settings":{"index":{"number_of_shards":"1"}},"mappings":{},"aliases":{}}}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/_template") {
			w.WriteHeader(200)
			_, _ = fmt.Fprint(w, respJSON)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewIndicesService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success with names", func(t *testing.T) {
		resp, err := svc.GetTemplate(ctx, []string{"my_template"})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("success without names", func(t *testing.T) {
		resp, err := svc.GetTemplate(ctx, nil)
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("success with multiple names", func(t *testing.T) {
		resp, err := svc.GetTemplate(ctx, []string{"t1", "t2"})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})
}

func TestUnitIndicesServicePutTemplate(t *testing.T) {
	respJSON := `{"acknowledged":true}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/_template/") {
			w.WriteHeader(200)
			_, _ = fmt.Fprint(w, respJSON)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewIndicesService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		body := map[string]any{"index_patterns": []string{"myindex-*"}, "settings": map[string]any{"number_of_shards": 1}}
		resp, err := svc.PutTemplate(ctx, &PutTemplateRequest{Name: "my_template", Body: body})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("validation error missing name", func(t *testing.T) {
		body := map[string]any{"index_patterns": []string{"myindex-*"}}
		resp, err := svc.PutTemplate(ctx, &PutTemplateRequest{Body: body})
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Contains(t, err.Error(), "validation")
	})

	t.Run("validation error missing body", func(t *testing.T) {
		resp, err := svc.PutTemplate(ctx, &PutTemplateRequest{Name: "my_template"})
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Contains(t, err.Error(), "validation")
	})
}

func TestUnitIndicesServiceExistsTemplate(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead && r.URL.Path == "/_template/my_template" {
			w.WriteHeader(200)
			return
		}
		if r.Method == http.MethodHead && r.URL.Path == "/_template/notexist" {
			w.WriteHeader(404)
			return
		}
		if r.Method == http.MethodHead && r.URL.Path == "/_template/error" {
			w.WriteHeader(500)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewIndicesService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("exists", func(t *testing.T) {
		exists, err := svc.ExistsTemplate(ctx, "my_template")
		require.NoError(t, err)
		assert.True(t, exists)
	})

	t.Run("not exists", func(t *testing.T) {
		exists, err := svc.ExistsTemplate(ctx, "notexist")
		require.NoError(t, err)
		assert.False(t, exists)
	})

	t.Run("unexpected status", func(t *testing.T) {
		exists, err := svc.ExistsTemplate(ctx, "error")
		require.Error(t, err)
		assert.False(t, exists)
		assert.Contains(t, err.Error(), "unexpected HTTP status")
	})

	t.Run("validation error empty name", func(t *testing.T) {
		exists, err := svc.ExistsTemplate(ctx, "")
		require.Error(t, err)
		assert.False(t, exists)
		assert.Contains(t, err.Error(), "name is required")
	})
}

func TestUnitIndicesServiceDeleteTemplate(t *testing.T) {
	respJSON := `{"acknowledged":true}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, "/_template/") {
			w.WriteHeader(200)
			_, _ = fmt.Fprint(w, respJSON)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewIndicesService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		resp, err := svc.DeleteTemplate(ctx, "my_template")
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("validation error empty name", func(t *testing.T) {
		resp, err := svc.DeleteTemplate(ctx, "")
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Contains(t, err.Error(), "name is required")
	})
}

func TestUnitIndicesServicePutIndexTemplate(t *testing.T) {
	respJSON := `{"acknowledged":true}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/_index_template/") {
			w.WriteHeader(200)
			_, _ = fmt.Fprint(w, respJSON)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewIndicesService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		body := map[string]any{"index_patterns": []string{"myindex-*"}, "template": map[string]any{"settings": map[string]any{"number_of_shards": 1}}}
		resp, err := svc.PutIndexTemplate(ctx, &PutIndexTemplateRequest{Name: "my_template", Body: body})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("validation error missing name", func(t *testing.T) {
		body := map[string]any{"index_patterns": []string{"myindex-*"}}
		resp, err := svc.PutIndexTemplate(ctx, &PutIndexTemplateRequest{Body: body})
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Contains(t, err.Error(), "validation")
	})

	t.Run("validation error missing body", func(t *testing.T) {
		resp, err := svc.PutIndexTemplate(ctx, &PutIndexTemplateRequest{Name: "my_template"})
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Contains(t, err.Error(), "validation")
	})
}

func TestUnitIndicesServiceGetIndexTemplate(t *testing.T) {
	respJSON := `{"index_templates":[{"name":"my_template","index_template":{"index_patterns":["myindex-*"],"template":{"settings":{"number_of_shards":"1"}},"priority":200}}]}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/_index_template") {
			w.WriteHeader(200)
			_, _ = fmt.Fprint(w, respJSON)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewIndicesService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success with names", func(t *testing.T) {
		resp, err := svc.GetIndexTemplate(ctx, []string{"my_template"})
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Len(t, resp.IndexTemplates, 1)
	})

	t.Run("success without names", func(t *testing.T) {
		resp, err := svc.GetIndexTemplate(ctx, nil)
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("success with multiple names", func(t *testing.T) {
		resp, err := svc.GetIndexTemplate(ctx, []string{"t1", "t2"})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})
}

func TestUnitIndicesServiceDeleteIndexTemplate(t *testing.T) {
	respJSON := `{"acknowledged":true}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, "/_index_template/") {
			w.WriteHeader(200)
			_, _ = fmt.Fprint(w, respJSON)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewIndicesService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		resp, err := svc.DeleteIndexTemplate(ctx, "my_template")
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("validation error empty name", func(t *testing.T) {
		resp, err := svc.DeleteIndexTemplate(ctx, "")
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Contains(t, err.Error(), "name is required")
	})
}

func TestUnitIndicesServicePutComponentTemplate(t *testing.T) {
	respJSON := `{"acknowledged":true}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/_component_template/") {
			w.WriteHeader(200)
			_, _ = fmt.Fprint(w, respJSON)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewIndicesService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		body := map[string]any{"template": map[string]any{"settings": map[string]any{"number_of_shards": 1}}}
		resp, err := svc.PutComponentTemplate(ctx, &PutComponentTemplateRequest{Name: "my_comp", Body: body})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("validation error missing name", func(t *testing.T) {
		body := map[string]any{"template": map[string]any{}}
		resp, err := svc.PutComponentTemplate(ctx, &PutComponentTemplateRequest{Body: body})
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Contains(t, err.Error(), "validation")
	})

	t.Run("validation error missing body", func(t *testing.T) {
		resp, err := svc.PutComponentTemplate(ctx, &PutComponentTemplateRequest{Name: "my_comp"})
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Contains(t, err.Error(), "validation")
	})
}

func TestUnitIndicesServiceGetComponentTemplate(t *testing.T) {
	respJSON := `{"component_templates":[{"name":"my_comp","component_template":{"template":{"settings":{"number_of_shards":"1"}}}}]}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/_component_template") {
			w.WriteHeader(200)
			_, _ = fmt.Fprint(w, respJSON)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewIndicesService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success with names", func(t *testing.T) {
		resp, err := svc.GetComponentTemplate(ctx, []string{"my_comp"})
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Len(t, resp.ComponentTemplates, 1)
	})

	t.Run("success without names", func(t *testing.T) {
		resp, err := svc.GetComponentTemplate(ctx, nil)
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("success with multiple names", func(t *testing.T) {
		resp, err := svc.GetComponentTemplate(ctx, []string{"t1", "t2"})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})
}

func TestUnitIndicesServiceDeleteComponentTemplate(t *testing.T) {
	respJSON := `{"acknowledged":true}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, "/_component_template/") {
			w.WriteHeader(200)
			_, _ = fmt.Fprint(w, respJSON)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewIndicesService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		resp, err := svc.DeleteComponentTemplate(ctx, "my_comp")
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("validation error empty name", func(t *testing.T) {
		resp, err := svc.DeleteComponentTemplate(ctx, "")
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Contains(t, err.Error(), "name is required")
	})
}

func TestUnitIndicesServiceCreateDataStream(t *testing.T) {
	respJSON := `{"acknowledged":true}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/_data_stream/") {
			w.WriteHeader(200)
			_, _ = fmt.Fprint(w, respJSON)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewIndicesService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		resp, err := svc.CreateDataStream(ctx, "my-data-stream")
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("validation error empty name", func(t *testing.T) {
		resp, err := svc.CreateDataStream(ctx, "")
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Contains(t, err.Error(), "name is required")
	})
}

func TestUnitIndicesServiceGetDataStream(t *testing.T) {
	respJSON := `{"data_streams":[{"name":"my-data-stream","timestamp_field":{"name":"@timestamp"},"indices":[{"index_name":".ds-my-data-stream-000001","index_uuid":"abc"}],"generation":1,"status":"GREEN","template":"my-template"}]}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/_data_stream") {
			w.WriteHeader(200)
			_, _ = fmt.Fprint(w, respJSON)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewIndicesService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success with names", func(t *testing.T) {
		resp, err := svc.GetDataStream(ctx, []string{"my-data-stream"})
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Len(t, resp.DataStreams, 1)
	})

	t.Run("success without names", func(t *testing.T) {
		resp, err := svc.GetDataStream(ctx, nil)
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("success with multiple names", func(t *testing.T) {
		resp, err := svc.GetDataStream(ctx, []string{"ds1", "ds2"})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})
}

func TestUnitIndicesServiceDeleteDataStream(t *testing.T) {
	respJSON := `{"acknowledged":true}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, "/_data_stream/") {
			w.WriteHeader(200)
			_, _ = fmt.Fprint(w, respJSON)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewIndicesService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		resp, err := svc.DeleteDataStream(ctx, []string{"my-data-stream"})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("success with multiple names", func(t *testing.T) {
		resp, err := svc.DeleteDataStream(ctx, []string{"ds1", "ds2"})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("validation error empty names", func(t *testing.T) {
		resp, err := svc.DeleteDataStream(ctx, []string{})
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Contains(t, err.Error(), "names is required")
	})
}

func TestUnitIndicesServiceAddBlock(t *testing.T) {
	respJSON := `{"acknowledged":true,"shards_acknowledged":true}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut && strings.Contains(r.URL.Path, "/_block/") {
			w.WriteHeader(200)
			_, _ = fmt.Fprint(w, respJSON)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewIndicesService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		resp, err := svc.AddBlock(ctx, &AddBlockRequest{Indices: []string{"myindex"}, Block: "write"})
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.True(t, resp.Acknowledged)
	})

	t.Run("success multiple indices", func(t *testing.T) {
		resp, err := svc.AddBlock(ctx, &AddBlockRequest{Indices: []string{"idx1", "idx2"}, Block: "read"})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("validation error empty indices", func(t *testing.T) {
		resp, err := svc.AddBlock(ctx, &AddBlockRequest{Indices: []string{}, Block: "write"})
		require.Error(t, err)
		assert.Nil(t, resp)
	})

	t.Run("validation error empty block", func(t *testing.T) {
		resp, err := svc.AddBlock(ctx, &AddBlockRequest{Indices: []string{"myindex"}, Block: ""})
		require.Error(t, err)
		assert.Nil(t, resp)
	})
}

func TestUnitIndicesServiceClone(t *testing.T) {
	respJSON := `{"acknowledged":true}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && strings.Contains(r.URL.Path, "/_clone/") {
			w.WriteHeader(200)
			_, _ = fmt.Fprint(w, respJSON)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewIndicesService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		resp, err := svc.Clone(ctx, &CloneRequest{Source: "src", Target: "tgt"})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("success with body", func(t *testing.T) {
		body := map[string]any{"settings": map[string]any{"index.number_of_shards": 1}}
		resp, err := svc.Clone(ctx, &CloneRequest{Source: "src", Target: "tgt", Body: body})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("validation error empty source", func(t *testing.T) {
		resp, err := svc.Clone(ctx, &CloneRequest{Source: "", Target: "tgt"})
		require.Error(t, err)
		assert.Nil(t, resp)
	})

	t.Run("validation error empty target", func(t *testing.T) {
		resp, err := svc.Clone(ctx, &CloneRequest{Source: "src", Target: ""})
		require.Error(t, err)
		assert.Nil(t, resp)
	})
}

func TestUnitIndicesServiceSplit(t *testing.T) {
	respJSON := `{"acknowledged":true}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && strings.Contains(r.URL.Path, "/_split/") {
			w.WriteHeader(200)
			_, _ = fmt.Fprint(w, respJSON)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewIndicesService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		resp, err := svc.Split(ctx, &SplitRequest{Source: "src", Target: "tgt"})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("success with body", func(t *testing.T) {
		body := map[string]any{"settings": map[string]any{"index.number_of_shards": 2}}
		resp, err := svc.Split(ctx, &SplitRequest{Source: "src", Target: "tgt", Body: body})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("validation error empty source", func(t *testing.T) {
		resp, err := svc.Split(ctx, &SplitRequest{Source: "", Target: "tgt"})
		require.Error(t, err)
		assert.Nil(t, resp)
	})

	t.Run("validation error empty target", func(t *testing.T) {
		resp, err := svc.Split(ctx, &SplitRequest{Source: "src", Target: ""})
		require.Error(t, err)
		assert.Nil(t, resp)
	})
}

func TestUnitIndicesServiceDeleteAlias(t *testing.T) {
	respJSON := `{"acknowledged":true}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete && strings.Contains(r.URL.Path, "/_alias/") {
			w.WriteHeader(200)
			_, _ = fmt.Fprint(w, respJSON)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewIndicesService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		resp, err := svc.DeleteAlias(ctx, &DeleteAliasRequest{Indices: []string{"myindex"}, Names: []string{"myalias"}})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("success multiple indices and names", func(t *testing.T) {
		resp, err := svc.DeleteAlias(ctx, &DeleteAliasRequest{Indices: []string{"idx1", "idx2"}, Names: []string{"a1", "a2"}})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("validation error empty indices", func(t *testing.T) {
		resp, err := svc.DeleteAlias(ctx, &DeleteAliasRequest{Indices: []string{}, Names: []string{"a"}})
		require.Error(t, err)
		assert.Nil(t, resp)
	})

	t.Run("validation error empty names", func(t *testing.T) {
		resp, err := svc.DeleteAlias(ctx, &DeleteAliasRequest{Indices: []string{"i"}, Names: []string{}})
		require.Error(t, err)
		assert.Nil(t, resp)
	})
}

func TestUnitIndicesServiceExistsAlias(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			if strings.Contains(r.URL.Path, "notexist") {
				w.WriteHeader(404)
				return
			}
			if strings.Contains(r.URL.Path, "error") {
				w.WriteHeader(500)
				return
			}
			w.WriteHeader(200)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewIndicesService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("exists with indices", func(t *testing.T) {
		exists, err := svc.ExistsAlias(ctx, []string{"myindex"}, "myalias")
		require.NoError(t, err)
		assert.True(t, exists)
	})

	t.Run("exists without indices", func(t *testing.T) {
		exists, err := svc.ExistsAlias(ctx, nil, "myalias")
		require.NoError(t, err)
		assert.True(t, exists)
	})

	t.Run("not exists", func(t *testing.T) {
		exists, err := svc.ExistsAlias(ctx, []string{"myindex"}, "notexist")
		require.NoError(t, err)
		assert.False(t, exists)
	})

	t.Run("unexpected status", func(t *testing.T) {
		exists, err := svc.ExistsAlias(ctx, []string{"myindex"}, "error")
		require.Error(t, err)
		assert.False(t, exists)
		assert.Contains(t, err.Error(), "unexpected HTTP status")
	})

	t.Run("validation error empty name", func(t *testing.T) {
		exists, err := svc.ExistsAlias(ctx, []string{"myindex"}, "")
		require.Error(t, err)
		assert.False(t, exists)
		assert.Contains(t, err.Error(), "name is required")
	})
}

func TestUnitIndicesServiceExistsIndexTemplate(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead && r.URL.Path == "/_index_template/my_template" {
			w.WriteHeader(200)
			return
		}
		if r.Method == http.MethodHead && r.URL.Path == "/_index_template/notexist" {
			w.WriteHeader(404)
			return
		}
		if r.Method == http.MethodHead && r.URL.Path == "/_index_template/error" {
			w.WriteHeader(500)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewIndicesService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("exists", func(t *testing.T) {
		exists, err := svc.ExistsIndexTemplate(ctx, "my_template")
		require.NoError(t, err)
		assert.True(t, exists)
	})

	t.Run("not exists", func(t *testing.T) {
		exists, err := svc.ExistsIndexTemplate(ctx, "notexist")
		require.NoError(t, err)
		assert.False(t, exists)
	})

	t.Run("unexpected status", func(t *testing.T) {
		exists, err := svc.ExistsIndexTemplate(ctx, "error")
		require.Error(t, err)
		assert.False(t, exists)
		assert.Contains(t, err.Error(), "unexpected HTTP status")
	})

	t.Run("validation error empty name", func(t *testing.T) {
		exists, err := svc.ExistsIndexTemplate(ctx, "")
		require.Error(t, err)
		assert.False(t, exists)
		assert.Contains(t, err.Error(), "name is required")
	})
}

func TestUnitIndicesServiceRecovery(t *testing.T) {
	respJSON := `{"myindex":{"shards":[{"id":0,"type":"peer","stage":"done","primary":true}]}}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/_recovery") {
			w.WriteHeader(200)
			_, _ = fmt.Fprint(w, respJSON)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewIndicesService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success with indices", func(t *testing.T) {
		resp, err := svc.Recovery(ctx, []string{"myindex"})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("success without indices", func(t *testing.T) {
		resp, err := svc.Recovery(ctx, nil)
		require.NoError(t, err)
		require.NotNil(t, resp)
	})
}

func TestUnitIndicesServiceShardStores(t *testing.T) {
	respJSON := `{"myindex":{"shards":{"0":{"stores":[{"node_name":"node1"}]}}}}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/_shard_stores") {
			w.WriteHeader(200)
			_, _ = fmt.Fprint(w, respJSON)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewIndicesService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success with indices", func(t *testing.T) {
		resp, err := svc.ShardStores(ctx, []string{"myindex"})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("success without indices", func(t *testing.T) {
		resp, err := svc.ShardStores(ctx, nil)
		require.NoError(t, err)
		require.NotNil(t, resp)
	})
}

func TestUnitIndicesServiceUpdateAliases(t *testing.T) {
	respJSON := `{"acknowledged":true}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/_aliases" {
			w.WriteHeader(200)
			_, _ = fmt.Fprint(w, respJSON)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewIndicesService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		body := map[string]any{"actions": []any{map[string]any{"add": map[string]any{"index": "myindex", "alias": "myalias"}}}}
		resp, err := svc.UpdateAliases(ctx, body)
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("validation error nil body", func(t *testing.T) {
		resp, err := svc.UpdateAliases(ctx, nil)
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Contains(t, err.Error(), "body is required")
	})
}

func TestUnitIndicesServiceResolveIndex(t *testing.T) {
	respJSON := `{"indices":[{"name":"myindex","aliases":["myalias"],"attributes":["open"]}],"aliases":[{"name":"myalias","indices":["myindex"]}],"data_streams":[]}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/_resolve/index/") {
			w.WriteHeader(200)
			_, _ = fmt.Fprint(w, respJSON)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewIndicesService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		resp, err := svc.ResolveIndex(ctx, "my*")
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Len(t, resp.Indices, 1)
	})

	t.Run("validation error empty name", func(t *testing.T) {
		resp, err := svc.ResolveIndex(ctx, "")
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Contains(t, err.Error(), "name is required")
	})
}

func TestUnitIndicesServiceSimulateIndexTemplate(t *testing.T) {
	respJSON := `{"template":{"settings":{"index":{"number_of_shards":"1"}},"mappings":{},"aliases":{}},"overlapping":[]}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/_index_template/_simulate_index/") {
			w.WriteHeader(200)
			_, _ = fmt.Fprint(w, respJSON)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewIndicesService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		resp, err := svc.SimulateIndexTemplate(ctx, &SimulateIndexTemplateRequest{Name: "my_template"})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("success with body", func(t *testing.T) {
		body := map[string]any{"template": map[string]any{"settings": map[string]any{}}}
		resp, err := svc.SimulateIndexTemplate(ctx, &SimulateIndexTemplateRequest{Name: "my_template", Body: body})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("validation error empty name", func(t *testing.T) {
		resp, err := svc.SimulateIndexTemplate(ctx, &SimulateIndexTemplateRequest{Name: ""})
		require.Error(t, err)
		assert.Nil(t, resp)
	})
}

func TestUnitIndicesServiceSimulateTemplate(t *testing.T) {
	respJSON := `{"template":{"settings":{},"mappings":{},"aliases":{}},"overlapping":[]}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/_index_template/_simulate") {
			w.WriteHeader(200)
			_, _ = fmt.Fprint(w, respJSON)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewIndicesService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success without name", func(t *testing.T) {
		body := map[string]any{"template": map[string]any{"settings": map[string]any{}}}
		resp, err := svc.SimulateTemplate(ctx, &SimulateTemplateRequest{Body: body})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("success with name", func(t *testing.T) {
		resp, err := svc.SimulateTemplate(ctx, &SimulateTemplateRequest{Name: "my_template"})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("success with name and body", func(t *testing.T) {
		body := map[string]any{"template": map[string]any{}}
		resp, err := svc.SimulateTemplate(ctx, &SimulateTemplateRequest{Name: "my_template", Body: body})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})
}

func TestUnitIndicesServiceDataStreamsStats(t *testing.T) {
	respJSON := `{"_shards":{"total":1,"successful":1,"failed":0},"data_stream_count":1,"backing_indices":1,"total_store_size_bytes":1024,"data_streams":[{"data_stream":"my-ds","backing_indices":1}]}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/_stats") && strings.Contains(r.URL.Path, "/_data_stream") {
			w.WriteHeader(200)
			_, _ = fmt.Fprint(w, respJSON)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewIndicesService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success without names", func(t *testing.T) {
		resp, err := svc.DataStreamsStats(ctx, nil)
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, 1, resp.DataStreamCount)
	})

	t.Run("success with names", func(t *testing.T) {
		resp, err := svc.DataStreamsStats(ctx, []string{"my-ds"})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("success with multiple names", func(t *testing.T) {
		resp, err := svc.DataStreamsStats(ctx, []string{"ds1", "ds2"})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})
}

func TestUnitIndicesServiceErrorPaths(t *testing.T) {
	ctx := context.Background()

	t.Run("Create server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewIndicesService(restyClient(srv), testLogger())
		_, err := s.Create(ctx, "myindex", nil)
		require.Error(t, err)
	})

	t.Run("Create unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewIndicesService(restyClient(srv), testLogger())
		_, err := s.Create(ctx, "myindex", nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("Delete server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewIndicesService(restyClient(srv), testLogger())
		_, err := s.Delete(ctx, []string{"myindex"})
		require.Error(t, err)
	})

	t.Run("Delete unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewIndicesService(restyClient(srv), testLogger())
		_, err := s.Delete(ctx, []string{"myindex"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("Get server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewIndicesService(restyClient(srv), testLogger())
		_, err := s.Get(ctx, []string{"myindex"})
		require.Error(t, err)
	})

	t.Run("Get unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewIndicesService(restyClient(srv), testLogger())
		_, err := s.Get(ctx, []string{"myindex"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("Open server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewIndicesService(restyClient(srv), testLogger())
		_, err := s.Open(ctx, "myindex")
		require.Error(t, err)
	})

	t.Run("Open unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewIndicesService(restyClient(srv), testLogger())
		_, err := s.Open(ctx, "myindex")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("Close server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewIndicesService(restyClient(srv), testLogger())
		_, err := s.Close(ctx, "myindex")
		require.Error(t, err)
	})

	t.Run("Close unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewIndicesService(restyClient(srv), testLogger())
		_, err := s.Close(ctx, "myindex")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("Rollover server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewIndicesService(restyClient(srv), testLogger())
		_, err := s.Rollover(ctx, "alias", nil)
		require.Error(t, err)
	})

	t.Run("Rollover unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewIndicesService(restyClient(srv), testLogger())
		_, err := s.Rollover(ctx, "alias", nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("Shrink server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewIndicesService(restyClient(srv), testLogger())
		_, err := s.Shrink(ctx, &ShrinkRequest{Source: "src", Target: "tgt"})
		require.Error(t, err)
	})

	t.Run("Shrink unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewIndicesService(restyClient(srv), testLogger())
		_, err := s.Shrink(ctx, &ShrinkRequest{Source: "src", Target: "tgt"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("Flush server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewIndicesService(restyClient(srv), testLogger())
		_, err := s.Flush(ctx, []string{"idx"})
		require.Error(t, err)
	})

	t.Run("Flush unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewIndicesService(restyClient(srv), testLogger())
		_, err := s.Flush(ctx, []string{"idx"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("Refresh server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewIndicesService(restyClient(srv), testLogger())
		_, err := s.Refresh(ctx, []string{"idx"})
		require.Error(t, err)
	})

	t.Run("Refresh unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewIndicesService(restyClient(srv), testLogger())
		_, err := s.Refresh(ctx, []string{"idx"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("Forcemerge server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewIndicesService(restyClient(srv), testLogger())
		_, err := s.Forcemerge(ctx, []string{"idx"})
		require.Error(t, err)
	})

	t.Run("Forcemerge unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewIndicesService(restyClient(srv), testLogger())
		_, err := s.Forcemerge(ctx, []string{"idx"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("Freeze server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewIndicesService(restyClient(srv), testLogger())
		_, err := s.Freeze(ctx, "myindex")
		require.Error(t, err)
	})

	t.Run("Freeze unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewIndicesService(restyClient(srv), testLogger())
		_, err := s.Freeze(ctx, "myindex")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("Unfreeze server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewIndicesService(restyClient(srv), testLogger())
		_, err := s.Unfreeze(ctx, "myindex")
		require.Error(t, err)
	})

	t.Run("Unfreeze unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewIndicesService(restyClient(srv), testLogger())
		_, err := s.Unfreeze(ctx, "myindex")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("ClearCache server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewIndicesService(restyClient(srv), testLogger())
		_, err := s.ClearCache(ctx, []string{"idx"})
		require.Error(t, err)
	})

	t.Run("ClearCache unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewIndicesService(restyClient(srv), testLogger())
		_, err := s.ClearCache(ctx, []string{"idx"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("Stats server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewIndicesService(restyClient(srv), testLogger())
		_, err := s.Stats(ctx, &IndicesStatsRequest{})
		require.Error(t, err)
	})

	t.Run("Stats unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewIndicesService(restyClient(srv), testLogger())
		_, err := s.Stats(ctx, &IndicesStatsRequest{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("Segments server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewIndicesService(restyClient(srv), testLogger())
		_, err := s.Segments(ctx, []string{"idx"})
		require.Error(t, err)
	})

	t.Run("Segments unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewIndicesService(restyClient(srv), testLogger())
		_, err := s.Segments(ctx, []string{"idx"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("Analyze server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewIndicesService(restyClient(srv), testLogger())
		_, err := s.Analyze(ctx, "myindex", nil)
		require.Error(t, err)
	})

	t.Run("Analyze unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewIndicesService(restyClient(srv), testLogger())
		_, err := s.Analyze(ctx, "myindex", nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("PutAlias server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewIndicesService(restyClient(srv), testLogger())
		_, err := s.PutAlias(ctx, &PutAliasRequest{Index: "myindex", Alias: "a"})
		require.Error(t, err)
	})

	t.Run("PutAlias unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewIndicesService(restyClient(srv), testLogger())
		_, err := s.PutAlias(ctx, &PutAliasRequest{Index: "myindex", Alias: "a"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("GetAliases server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewIndicesService(restyClient(srv), testLogger())
		_, err := s.GetAliases(ctx, []string{"idx"})
		require.Error(t, err)
	})

	t.Run("GetAliases unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewIndicesService(restyClient(srv), testLogger())
		_, err := s.GetAliases(ctx, []string{"idx"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("GetSettings server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewIndicesService(restyClient(srv), testLogger())
		_, err := s.GetSettings(ctx, []string{"idx"})
		require.Error(t, err)
	})

	t.Run("GetSettings unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewIndicesService(restyClient(srv), testLogger())
		_, err := s.GetSettings(ctx, []string{"idx"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("PutSettings server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewIndicesService(restyClient(srv), testLogger())
		_, err := s.PutSettings(ctx, nil, nil)
		require.Error(t, err)
	})

	t.Run("PutSettings unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewIndicesService(restyClient(srv), testLogger())
		_, err := s.PutSettings(ctx, nil, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("GetMapping server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewIndicesService(restyClient(srv), testLogger())
		_, err := s.GetMapping(ctx, []string{"idx"})
		require.Error(t, err)
	})

	t.Run("GetMapping unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewIndicesService(restyClient(srv), testLogger())
		_, err := s.GetMapping(ctx, []string{"idx"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("PutMapping server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewIndicesService(restyClient(srv), testLogger())
		_, err := s.PutMapping(ctx, &PutMappingRequest{Indices: []string{"idx"}, Body: map[string]any{}})
		require.Error(t, err)
	})

	t.Run("PutMapping unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewIndicesService(restyClient(srv), testLogger())
		_, err := s.PutMapping(ctx, &PutMappingRequest{Indices: []string{"idx"}, Body: map[string]any{}})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("GetFieldMapping server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewIndicesService(restyClient(srv), testLogger())
		_, err := s.GetFieldMapping(ctx, &GetFieldMappingRequest{Fields: []string{"f"}})
		require.Error(t, err)
	})

	t.Run("GetFieldMapping unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewIndicesService(restyClient(srv), testLogger())
		_, err := s.GetFieldMapping(ctx, &GetFieldMappingRequest{Fields: []string{"f"}})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("GetTemplate server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewIndicesService(restyClient(srv), testLogger())
		_, err := s.GetTemplate(ctx, []string{"t"})
		require.Error(t, err)
	})

	t.Run("GetTemplate unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewIndicesService(restyClient(srv), testLogger())
		_, err := s.GetTemplate(ctx, []string{"t"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("PutTemplate server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewIndicesService(restyClient(srv), testLogger())
		_, err := s.PutTemplate(ctx, &PutTemplateRequest{Name: "t", Body: map[string]any{}})
		require.Error(t, err)
	})

	t.Run("PutTemplate unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewIndicesService(restyClient(srv), testLogger())
		_, err := s.PutTemplate(ctx, &PutTemplateRequest{Name: "t", Body: map[string]any{}})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("DeleteTemplate server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewIndicesService(restyClient(srv), testLogger())
		_, err := s.DeleteTemplate(ctx, "t")
		require.Error(t, err)
	})

	t.Run("DeleteTemplate unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewIndicesService(restyClient(srv), testLogger())
		_, err := s.DeleteTemplate(ctx, "t")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("PutIndexTemplate server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewIndicesService(restyClient(srv), testLogger())
		_, err := s.PutIndexTemplate(ctx, &PutIndexTemplateRequest{Name: "t", Body: map[string]any{}})
		require.Error(t, err)
	})

	t.Run("PutIndexTemplate unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewIndicesService(restyClient(srv), testLogger())
		_, err := s.PutIndexTemplate(ctx, &PutIndexTemplateRequest{Name: "t", Body: map[string]any{}})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("GetIndexTemplate server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewIndicesService(restyClient(srv), testLogger())
		_, err := s.GetIndexTemplate(ctx, []string{"t"})
		require.Error(t, err)
	})

	t.Run("GetIndexTemplate unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewIndicesService(restyClient(srv), testLogger())
		_, err := s.GetIndexTemplate(ctx, []string{"t"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("DeleteIndexTemplate server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewIndicesService(restyClient(srv), testLogger())
		_, err := s.DeleteIndexTemplate(ctx, "t")
		require.Error(t, err)
	})

	t.Run("DeleteIndexTemplate unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewIndicesService(restyClient(srv), testLogger())
		_, err := s.DeleteIndexTemplate(ctx, "t")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("PutComponentTemplate server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewIndicesService(restyClient(srv), testLogger())
		_, err := s.PutComponentTemplate(ctx, &PutComponentTemplateRequest{Name: "t", Body: map[string]any{}})
		require.Error(t, err)
	})

	t.Run("PutComponentTemplate unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewIndicesService(restyClient(srv), testLogger())
		_, err := s.PutComponentTemplate(ctx, &PutComponentTemplateRequest{Name: "t", Body: map[string]any{}})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("GetComponentTemplate server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewIndicesService(restyClient(srv), testLogger())
		_, err := s.GetComponentTemplate(ctx, []string{"t"})
		require.Error(t, err)
	})

	t.Run("GetComponentTemplate unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewIndicesService(restyClient(srv), testLogger())
		_, err := s.GetComponentTemplate(ctx, []string{"t"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("DeleteComponentTemplate server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewIndicesService(restyClient(srv), testLogger())
		_, err := s.DeleteComponentTemplate(ctx, "t")
		require.Error(t, err)
	})

	t.Run("DeleteComponentTemplate unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewIndicesService(restyClient(srv), testLogger())
		_, err := s.DeleteComponentTemplate(ctx, "t")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("CreateDataStream server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewIndicesService(restyClient(srv), testLogger())
		_, err := s.CreateDataStream(ctx, "ds")
		require.Error(t, err)
	})

	t.Run("CreateDataStream unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewIndicesService(restyClient(srv), testLogger())
		_, err := s.CreateDataStream(ctx, "ds")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("GetDataStream server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewIndicesService(restyClient(srv), testLogger())
		_, err := s.GetDataStream(ctx, []string{"ds"})
		require.Error(t, err)
	})

	t.Run("GetDataStream unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewIndicesService(restyClient(srv), testLogger())
		_, err := s.GetDataStream(ctx, []string{"ds"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("DeleteDataStream server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewIndicesService(restyClient(srv), testLogger())
		_, err := s.DeleteDataStream(ctx, []string{"ds"})
		require.Error(t, err)
	})

	t.Run("DeleteDataStream unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewIndicesService(restyClient(srv), testLogger())
		_, err := s.DeleteDataStream(ctx, []string{"ds"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("AddBlock server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewIndicesService(restyClient(srv), testLogger())
		_, err := s.AddBlock(ctx, &AddBlockRequest{Indices: []string{"i"}, Block: "write"})
		require.Error(t, err)
	})

	t.Run("AddBlock unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewIndicesService(restyClient(srv), testLogger())
		_, err := s.AddBlock(ctx, &AddBlockRequest{Indices: []string{"i"}, Block: "write"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("Clone server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewIndicesService(restyClient(srv), testLogger())
		_, err := s.Clone(ctx, &CloneRequest{Source: "s", Target: "t"})
		require.Error(t, err)
	})

	t.Run("Clone unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewIndicesService(restyClient(srv), testLogger())
		_, err := s.Clone(ctx, &CloneRequest{Source: "s", Target: "t"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("Split server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewIndicesService(restyClient(srv), testLogger())
		_, err := s.Split(ctx, &SplitRequest{Source: "s", Target: "t"})
		require.Error(t, err)
	})

	t.Run("Split unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewIndicesService(restyClient(srv), testLogger())
		_, err := s.Split(ctx, &SplitRequest{Source: "s", Target: "t"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("DeleteAlias server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewIndicesService(restyClient(srv), testLogger())
		_, err := s.DeleteAlias(ctx, &DeleteAliasRequest{Indices: []string{"i"}, Names: []string{"a"}})
		require.Error(t, err)
	})

	t.Run("DeleteAlias unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewIndicesService(restyClient(srv), testLogger())
		_, err := s.DeleteAlias(ctx, &DeleteAliasRequest{Indices: []string{"i"}, Names: []string{"a"}})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("ExistsAlias server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewIndicesService(restyClient(srv), testLogger())
		_, err := s.ExistsAlias(ctx, []string{"i"}, "a")
		require.Error(t, err)
	})

	t.Run("ExistsIndexTemplate server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewIndicesService(restyClient(srv), testLogger())
		_, err := s.ExistsIndexTemplate(ctx, "t")
		require.Error(t, err)
	})

	t.Run("Recovery server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewIndicesService(restyClient(srv), testLogger())
		_, err := s.Recovery(ctx, []string{"i"})
		require.Error(t, err)
	})

	t.Run("Recovery unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewIndicesService(restyClient(srv), testLogger())
		_, err := s.Recovery(ctx, []string{"i"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("ShardStores server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewIndicesService(restyClient(srv), testLogger())
		_, err := s.ShardStores(ctx, []string{"i"})
		require.Error(t, err)
	})

	t.Run("ShardStores unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewIndicesService(restyClient(srv), testLogger())
		_, err := s.ShardStores(ctx, []string{"i"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("UpdateAliases server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewIndicesService(restyClient(srv), testLogger())
		_, err := s.UpdateAliases(ctx, map[string]any{"actions": []any{}})
		require.Error(t, err)
	})

	t.Run("UpdateAliases unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewIndicesService(restyClient(srv), testLogger())
		_, err := s.UpdateAliases(ctx, map[string]any{"actions": []any{}})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("ResolveIndex server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewIndicesService(restyClient(srv), testLogger())
		_, err := s.ResolveIndex(ctx, "test")
		require.Error(t, err)
	})

	t.Run("ResolveIndex unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewIndicesService(restyClient(srv), testLogger())
		_, err := s.ResolveIndex(ctx, "test")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("SimulateIndexTemplate server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewIndicesService(restyClient(srv), testLogger())
		_, err := s.SimulateIndexTemplate(ctx, &SimulateIndexTemplateRequest{Name: "t"})
		require.Error(t, err)
	})

	t.Run("SimulateIndexTemplate unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewIndicesService(restyClient(srv), testLogger())
		_, err := s.SimulateIndexTemplate(ctx, &SimulateIndexTemplateRequest{Name: "t"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("SimulateTemplate server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewIndicesService(restyClient(srv), testLogger())
		_, err := s.SimulateTemplate(ctx, &SimulateTemplateRequest{Name: "t"})
		require.Error(t, err)
	})

	t.Run("SimulateTemplate unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewIndicesService(restyClient(srv), testLogger())
		_, err := s.SimulateTemplate(ctx, &SimulateTemplateRequest{Name: "t"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("DataStreamsStats server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewIndicesService(restyClient(srv), testLogger())
		_, err := s.DataStreamsStats(ctx, []string{"ds"})
		require.Error(t, err)
	})

	t.Run("DataStreamsStats unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewIndicesService(restyClient(srv), testLogger())
		_, err := s.DataStreamsStats(ctx, []string{"ds"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})
}
func TestUnitIndicesServiceNetworkErrors(t *testing.T) {
	ctx := context.Background()
	s := NewIndicesService(deadClient(), testLogger())

	t.Run("Create", func(t *testing.T) { _, err := s.Create(ctx, "i", nil); require.Error(t, err) })
	t.Run("Delete", func(t *testing.T) { _, err := s.Delete(ctx, []string{"i"}); require.Error(t, err) })
	t.Run("Get", func(t *testing.T) { _, err := s.Get(ctx, []string{"i"}); require.Error(t, err) })
	t.Run("Exists", func(t *testing.T) { _, err := s.Exists(ctx, []string{"i"}); require.Error(t, err) })
	t.Run("ExistsTemplate", func(t *testing.T) { _, err := s.ExistsTemplate(ctx, "t"); require.Error(t, err) })
	t.Run("Open", func(t *testing.T) { _, err := s.Open(ctx, "i"); require.Error(t, err) })
	t.Run("Close", func(t *testing.T) { _, err := s.Close(ctx, "i"); require.Error(t, err) })
	t.Run("Rollover", func(t *testing.T) { _, err := s.Rollover(ctx, "a", nil); require.Error(t, err) })
	t.Run("Shrink", func(t *testing.T) {
		_, err := s.Shrink(ctx, &ShrinkRequest{Source: "s", Target: "t"})
		require.Error(t, err)
	})
	t.Run("Flush", func(t *testing.T) { _, err := s.Flush(ctx, []string{"i"}); require.Error(t, err) })
	t.Run("Refresh", func(t *testing.T) { _, err := s.Refresh(ctx, []string{"i"}); require.Error(t, err) })
	t.Run("Forcemerge", func(t *testing.T) { _, err := s.Forcemerge(ctx, []string{"i"}); require.Error(t, err) })
	t.Run("Freeze", func(t *testing.T) { _, err := s.Freeze(ctx, "i"); require.Error(t, err) })
	t.Run("Unfreeze", func(t *testing.T) { _, err := s.Unfreeze(ctx, "i"); require.Error(t, err) })
	t.Run("ClearCache", func(t *testing.T) { _, err := s.ClearCache(ctx, []string{"i"}); require.Error(t, err) })
	t.Run("Stats", func(t *testing.T) { _, err := s.Stats(ctx, &IndicesStatsRequest{}); require.Error(t, err) })
	t.Run("Segments", func(t *testing.T) { _, err := s.Segments(ctx, []string{"i"}); require.Error(t, err) })
	t.Run("Analyze", func(t *testing.T) { _, err := s.Analyze(ctx, "i", nil); require.Error(t, err) })
	t.Run("PutAlias", func(t *testing.T) {
		_, err := s.PutAlias(ctx, &PutAliasRequest{Index: "i", Alias: "a"})
		require.Error(t, err)
	})
	t.Run("GetAliases", func(t *testing.T) { _, err := s.GetAliases(ctx, []string{"i"}); require.Error(t, err) })
	t.Run("GetSettings", func(t *testing.T) { _, err := s.GetSettings(ctx, []string{"i"}); require.Error(t, err) })
	t.Run("PutSettings", func(t *testing.T) { _, err := s.PutSettings(ctx, []string{"i"}, nil); require.Error(t, err) })
	t.Run("GetMapping", func(t *testing.T) { _, err := s.GetMapping(ctx, []string{"i"}); require.Error(t, err) })
	t.Run("PutMapping", func(t *testing.T) {
		_, err := s.PutMapping(ctx, &PutMappingRequest{Indices: []string{"i"}, Body: map[string]any{}})
		require.Error(t, err)
	})
	t.Run("GetFieldMapping", func(t *testing.T) {
		_, err := s.GetFieldMapping(ctx, &GetFieldMappingRequest{Fields: []string{"f"}})
		require.Error(t, err)
	})
	t.Run("GetTemplate", func(t *testing.T) { _, err := s.GetTemplate(ctx, []string{"t"}); require.Error(t, err) })
	t.Run("PutTemplate", func(t *testing.T) {
		_, err := s.PutTemplate(ctx, &PutTemplateRequest{Name: "t", Body: map[string]any{}})
		require.Error(t, err)
	})
	t.Run("DeleteTemplate", func(t *testing.T) { _, err := s.DeleteTemplate(ctx, "t"); require.Error(t, err) })
	t.Run("PutIndexTemplate", func(t *testing.T) {
		_, err := s.PutIndexTemplate(ctx, &PutIndexTemplateRequest{Name: "t", Body: map[string]any{}})
		require.Error(t, err)
	})
	t.Run("GetIndexTemplate", func(t *testing.T) { _, err := s.GetIndexTemplate(ctx, []string{"t"}); require.Error(t, err) })
	t.Run("DeleteIndexTemplate", func(t *testing.T) { _, err := s.DeleteIndexTemplate(ctx, "t"); require.Error(t, err) })
	t.Run("PutComponentTemplate", func(t *testing.T) {
		_, err := s.PutComponentTemplate(ctx, &PutComponentTemplateRequest{Name: "t", Body: map[string]any{}})
		require.Error(t, err)
	})
	t.Run("GetComponentTemplate", func(t *testing.T) { _, err := s.GetComponentTemplate(ctx, []string{"t"}); require.Error(t, err) })
	t.Run("DeleteComponentTemplate", func(t *testing.T) { _, err := s.DeleteComponentTemplate(ctx, "t"); require.Error(t, err) })
	t.Run("CreateDataStream", func(t *testing.T) { _, err := s.CreateDataStream(ctx, "ds"); require.Error(t, err) })
	t.Run("GetDataStream", func(t *testing.T) { _, err := s.GetDataStream(ctx, []string{"ds"}); require.Error(t, err) })
	t.Run("DeleteDataStream", func(t *testing.T) { _, err := s.DeleteDataStream(ctx, []string{"ds"}); require.Error(t, err) })
	t.Run("AddBlock", func(t *testing.T) {
		_, err := s.AddBlock(ctx, &AddBlockRequest{Indices: []string{"i"}, Block: "write"})
		require.Error(t, err)
	})
	t.Run("Clone", func(t *testing.T) {
		_, err := s.Clone(ctx, &CloneRequest{Source: "s", Target: "t"})
		require.Error(t, err)
	})
	t.Run("Split", func(t *testing.T) {
		_, err := s.Split(ctx, &SplitRequest{Source: "s", Target: "t"})
		require.Error(t, err)
	})
	t.Run("DeleteAlias", func(t *testing.T) {
		_, err := s.DeleteAlias(ctx, &DeleteAliasRequest{Indices: []string{"i"}, Names: []string{"a"}})
		require.Error(t, err)
	})
	t.Run("ExistsAlias", func(t *testing.T) { _, err := s.ExistsAlias(ctx, []string{"i"}, "a"); require.Error(t, err) })
	t.Run("ExistsIndexTemplate", func(t *testing.T) { _, err := s.ExistsIndexTemplate(ctx, "t"); require.Error(t, err) })
	t.Run("Recovery", func(t *testing.T) { _, err := s.Recovery(ctx, []string{"i"}); require.Error(t, err) })
	t.Run("ShardStores", func(t *testing.T) { _, err := s.ShardStores(ctx, []string{"i"}); require.Error(t, err) })
	t.Run("UpdateAliases", func(t *testing.T) {
		_, err := s.UpdateAliases(ctx, map[string]any{"actions": []any{}})
		require.Error(t, err)
	})
	t.Run("ResolveIndex", func(t *testing.T) { _, err := s.ResolveIndex(ctx, "n"); require.Error(t, err) })
	t.Run("SimulateIndexTemplate", func(t *testing.T) {
		_, err := s.SimulateIndexTemplate(ctx, &SimulateIndexTemplateRequest{Name: "t"})
		require.Error(t, err)
	})
	t.Run("SimulateTemplate", func(t *testing.T) {
		_, err := s.SimulateTemplate(ctx, &SimulateTemplateRequest{Name: "t"})
		require.Error(t, err)
	})
	t.Run("DataStreamsStats", func(t *testing.T) { _, err := s.DataStreamsStats(ctx, []string{"ds"}); require.Error(t, err) })
}
