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

func TestUnitDocumentServiceIndex(t *testing.T) {
	respJSON := `{"_index":"idx","_id":"1","_version":1,"result":"created","_shards":{"total":2,"successful":1,"failed":0},"_seq_no":0,"_primary_term":1}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut && r.URL.Path == "/myindex/_doc/1" {
			w.WriteHeader(201)
			fmt.Fprint(w, respJSON)
			return
		}
		if r.Method == http.MethodPost && r.URL.Path == "/myindex/_doc" {
			w.WriteHeader(201)
			fmt.Fprint(w, respJSON)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewDocumentService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success with ID", func(t *testing.T) {
		resp, err := svc.Index(ctx, &IndexRequest{Index: "myindex", Id: "1", Body: map[string]any{"field": "value"}})
		require.NoError(t, err)
		assert.Equal(t, "idx", resp.Index)
		assert.Equal(t, "1", resp.Id)
		assert.Equal(t, "created", resp.Result)
	})

	t.Run("success without ID", func(t *testing.T) {
		resp, err := svc.Index(ctx, &IndexRequest{Index: "myindex", Body: map[string]any{"field": "value"}})
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, "1", resp.Id)
	})

	t.Run("validation error missing index", func(t *testing.T) {
		resp, err := svc.Index(ctx, &IndexRequest{})
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Contains(t, err.Error(), "validation")
	})

	t.Run("success with params and ID", func(t *testing.T) {
		resp, err := svc.Index(ctx, &IndexRequest{Index: "myindex", Id: "1", Body: map[string]any{"field": "value"}, Params: map[string]string{"refresh": "true"}})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("Index network error", func(t *testing.T) {
		s := NewDocumentService(deadClient(), testLogger())
		_, err := s.Index(ctx, &IndexRequest{Index: "idx", Id: "1", Body: map[string]any{}})
		require.Error(t, err)
	})

	t.Run("Index HTTP error", func(t *testing.T) {
		errSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(500)
			fmt.Fprint(w, `{"error":{"type":"internal","reason":"test"},"status":500}`)
		}))
		defer errSrv.Close()
		s := NewDocumentService(restyClient(errSrv), testLogger())
		_, err := s.Index(ctx, &IndexRequest{Index: "myindex", Id: "1", Body: map[string]any{}})
		require.Error(t, err)
	})
}

func TestUnitDocumentServiceGet(t *testing.T) {
	respJSON := `{"_index":"idx","_id":"1","_version":1,"_seq_no":0,"_primary_term":1,"found":true,"_source":{"field":"value"}}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/myindex/_doc/") {
			w.WriteHeader(200)
			fmt.Fprint(w, respJSON)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewDocumentService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		resp, err := svc.Get(ctx, &GetRequest{Index: "myindex", Id: "1"})
		require.NoError(t, err)
		assert.True(t, resp.Found)
		assert.Equal(t, "1", resp.Id)
	})

	t.Run("success with params", func(t *testing.T) {
		resp, err := svc.Get(ctx, &GetRequest{Index: "myindex", Id: "1", Params: map[string]string{"_source": "true"}})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("validation error missing index", func(t *testing.T) {
		resp, err := svc.Get(ctx, &GetRequest{Id: "1"})
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Contains(t, err.Error(), "validation")
	})

	t.Run("validation error missing id", func(t *testing.T) {
		resp, err := svc.Get(ctx, &GetRequest{Index: "myindex"})
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Contains(t, err.Error(), "validation")
	})
}

func TestUnitDocumentServiceMultiGet(t *testing.T) {
	respJSON := `{"docs":[{"_index":"idx","_id":"1","found":true},{"_index":"idx","_id":"2","found":true}]}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == "/_mget" {
			w.WriteHeader(200)
			fmt.Fprint(w, respJSON)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewDocumentService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		items := []*MultiGetItem{
			{Index: "idx", Id: "1"},
			{Index: "idx", Id: "2"},
		}
		resp, err := svc.MultiGet(ctx, items)
		require.NoError(t, err)
		require.Len(t, resp.Docs, 2)
		assert.True(t, resp.Docs[0].Found)
	})

	t.Run("validation error empty items", func(t *testing.T) {
		resp, err := svc.MultiGet(ctx, []*MultiGetItem{})
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Contains(t, err.Error(), "items is required")
	})

	t.Run("validation error nil items", func(t *testing.T) {
		resp, err := svc.MultiGet(ctx, nil)
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Contains(t, err.Error(), "items is required")
	})
}

func TestUnitDocumentServiceDelete(t *testing.T) {
	respJSON := `{"_index":"idx","_id":"1","_version":2,"result":"deleted","_shards":{"total":2,"successful":1,"failed":0},"_seq_no":1,"_primary_term":1}`
	deletedJSON := `{"_index":"idx","_id":"999","_version":0,"result":"not_found","_shards":{"total":2,"successful":1,"failed":0},"_seq_no":0,"_primary_term":0}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete && r.URL.Path == "/myindex/_doc/1" {
			w.WriteHeader(200)
			fmt.Fprint(w, respJSON)
			return
		}
		if r.Method == http.MethodDelete && r.URL.Path == "/myindex/_doc/999" {
			w.WriteHeader(404)
			fmt.Fprint(w, deletedJSON)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewDocumentService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		resp, err := svc.Delete(ctx, &DeleteRequest{Index: "myindex", Id: "1"})
		require.NoError(t, err)
		assert.Equal(t, "deleted", resp.Result)
		assert.Equal(t, "1", resp.Id)
	})

	t.Run("not found returns error", func(t *testing.T) {
		resp, err := svc.Delete(ctx, &DeleteRequest{Index: "myindex", Id: "999"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "document not found")
		assert.NotNil(t, resp)
	})

	t.Run("validation error missing index", func(t *testing.T) {
		resp, err := svc.Delete(ctx, &DeleteRequest{Id: "1"})
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Contains(t, err.Error(), "validation")
	})

	t.Run("validation error missing id", func(t *testing.T) {
		resp, err := svc.Delete(ctx, &DeleteRequest{Index: "myindex"})
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Contains(t, err.Error(), "validation")
	})
}

func TestUnitDocumentServiceDeleteByQuery(t *testing.T) {
	respJSON := `{"took":10,"timed_out":false,"total":5,"deleted":5,"batches":1,"version_conflicts":0,"noops":0,"retries":{"bulk":0,"search":0},"throttled":"0s","throttled_millis":0,"requests_per_second":-1,"throttled_until":"0s","throttled_until_millis":0,"failures":[]}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/_delete_by_query") {
			w.WriteHeader(200)
			fmt.Fprint(w, respJSON)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewDocumentService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		body := map[string]any{"query": map[string]any{"match_all": map[string]any{}}}
		resp, err := svc.DeleteByQuery(ctx, []string{"myindex"}, body)
		require.NoError(t, err)
		assert.Equal(t, int64(5), resp.Deleted)
		assert.Equal(t, int64(5), resp.Total)
	})

	t.Run("success multiple indices", func(t *testing.T) {
		body := map[string]any{"query": map[string]any{"match_all": map[string]any{}}}
		resp, err := svc.DeleteByQuery(ctx, []string{"idx1", "idx2"}, body)
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("validation error empty indices", func(t *testing.T) {
		resp, err := svc.DeleteByQuery(ctx, []string{}, nil)
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Contains(t, err.Error(), "indices is required")
	})
}

func TestUnitDocumentServiceUpdate(t *testing.T) {
	respJSON := `{"_index":"idx","_id":"1","_version":2,"result":"updated","_shards":{"total":2,"successful":1,"failed":0},"_seq_no":1,"_primary_term":1}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/myindex/_update/1" {
			w.WriteHeader(200)
			fmt.Fprint(w, respJSON)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewDocumentService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		body := map[string]any{"doc": map[string]any{"field": "updated"}}
		resp, err := svc.Update(ctx, &UpdateRequest{Index: "myindex", Id: "1", Body: body})
		require.NoError(t, err)
		assert.Equal(t, "updated", resp.Result)
	})

	t.Run("success with params", func(t *testing.T) {
		body := map[string]any{"doc": map[string]any{"field": "updated"}}
		resp, err := svc.Update(ctx, &UpdateRequest{Index: "myindex", Id: "1", Body: body, Params: map[string]string{"refresh": "true"}})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("validation error missing index", func(t *testing.T) {
		resp, err := svc.Update(ctx, &UpdateRequest{Id: "1", Body: map[string]any{}})
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Contains(t, err.Error(), "validation")
	})

	t.Run("validation error missing id", func(t *testing.T) {
		resp, err := svc.Update(ctx, &UpdateRequest{Index: "myindex", Body: map[string]any{}})
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Contains(t, err.Error(), "validation")
	})
}

func TestUnitDocumentServiceUpdateByQuery(t *testing.T) {
	respJSON := `{"took":10,"timed_out":false,"total":3,"updated":3,"batches":1,"version_conflicts":0,"noops":0,"retries":{"bulk":0,"search":0},"throttled":"0s","throttled_millis":0,"requests_per_second":-1,"throttled_until":"0s","throttled_until_millis":0,"failures":[]}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/_update_by_query") {
			w.WriteHeader(200)
			fmt.Fprint(w, respJSON)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewDocumentService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		body := map[string]any{"script": map[string]any{"source": "ctx._source.counter++"}, "query": map[string]any{"match_all": map[string]any{}}}
		resp, err := svc.UpdateByQuery(ctx, []string{"myindex"}, body)
		require.NoError(t, err)
		assert.Equal(t, int64(3), resp.Updated)
	})

	t.Run("validation error empty indices", func(t *testing.T) {
		resp, err := svc.UpdateByQuery(ctx, []string{}, nil)
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Contains(t, err.Error(), "indices is required")
	})
}

func TestUnitDocumentServiceBulk(t *testing.T) {
	respJSON := `{"took":30,"errors":false,"items":[{"index":{"_index":"idx","_id":"1","_version":1,"result":"created","status":201,"_shards":{"total":2,"successful":1,"failed":0},"_seq_no":0,"_primary_term":1}}]}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && (strings.HasSuffix(r.URL.Path, "/_bulk")) {
			w.WriteHeader(200)
			fmt.Fprint(w, respJSON)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewDocumentService(restyClient(srv), testLogger())
	ctx := context.Background()

	bulkBody := `{"index":{"_id":"1"}}
{"field":"value"}
`
	t.Run("success with index", func(t *testing.T) {
		resp, err := svc.Bulk(ctx, "myindex", bulkBody)
		require.NoError(t, err)
		assert.False(t, resp.Errors)
		assert.Len(t, resp.Items, 1)
	})

	t.Run("success without index", func(t *testing.T) {
		resp, err := svc.Bulk(ctx, "", bulkBody)
		require.NoError(t, err)
		require.NotNil(t, resp)
	})
}

func TestUnitDocumentServiceExists(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead && r.URL.Path == "/myindex/_doc/1" {
			w.WriteHeader(200)
			return
		}
		if r.Method == http.MethodHead && r.URL.Path == "/myindex/_doc/999" {
			w.WriteHeader(404)
			return
		}
		if r.Method == http.MethodHead && r.URL.Path == "/myindex/_doc/500" {
			w.WriteHeader(500)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewDocumentService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("exists", func(t *testing.T) {
		exists, err := svc.Exists(ctx, "myindex", "1")
		require.NoError(t, err)
		assert.True(t, exists)
	})

	t.Run("not found", func(t *testing.T) {
		exists, err := svc.Exists(ctx, "myindex", "999")
		require.NoError(t, err)
		assert.False(t, exists)
	})

	t.Run("unexpected status code", func(t *testing.T) {
		exists, err := svc.Exists(ctx, "myindex", "500")
		require.Error(t, err)
		assert.False(t, exists)
		assert.Contains(t, err.Error(), "unexpected status code")
	})

	t.Run("validation error empty index", func(t *testing.T) {
		exists, err := svc.Exists(ctx, "", "1")
		require.Error(t, err)
		assert.False(t, exists)
		assert.Contains(t, err.Error(), "index is required")
	})

	t.Run("validation error empty id", func(t *testing.T) {
		exists, err := svc.Exists(ctx, "myindex", "")
		require.Error(t, err)
		assert.False(t, exists)
		assert.Contains(t, err.Error(), "id is required")
	})
}

func TestUnitDocumentServiceExplain(t *testing.T) {
	respJSON := `{"_index":"myindex","_type":"_doc","_id":"1","matched":true,"explanation":{"value":1.0,"description":"weight"}}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == "/myindex/_explain/1" {
			w.WriteHeader(200)
			fmt.Fprint(w, respJSON)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewDocumentService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		body := map[string]any{"query": map[string]any{"match": map[string]any{"field": "value"}}}
		resp, err := svc.Explain(ctx, "myindex", "1", body)
		require.NoError(t, err)
		assert.True(t, resp.Matched)
		assert.Equal(t, "1", resp.Id)
	})

	t.Run("validation error empty index", func(t *testing.T) {
		resp, err := svc.Explain(ctx, "", "1", nil)
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Contains(t, err.Error(), "index is required")
	})

	t.Run("validation error empty id", func(t *testing.T) {
		resp, err := svc.Explain(ctx, "myindex", "", nil)
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Contains(t, err.Error(), "id is required")
	})
}

func TestUnitDocumentServiceTermVectors(t *testing.T) {
	respJSON := `{"_index":"myindex","_type":"_doc","_id":"1","_version":1,"found":true,"took":5,"term_vectors":{"content":{"field_statistics":{"doc_count":10,"sum_doc_freq":100,"sum_ttf":200},"terms":{"hello":{"doc_freq":5,"term_freq":3,"ttf":10,"tokens":[{"start_offset":0,"end_offset":5,"position":0}]}}}}}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == "/myindex/_termvectors/1" {
			w.WriteHeader(200)
			fmt.Fprint(w, respJSON)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewDocumentService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success with body", func(t *testing.T) {
		body := map[string]any{"fields": []string{"content"}}
		resp, err := svc.TermVectors(ctx, "myindex", "1", body)
		require.NoError(t, err)
		assert.True(t, resp.Found)
		assert.NotNil(t, resp.TermVectors)
	})

	t.Run("success without body", func(t *testing.T) {
		resp, err := svc.TermVectors(ctx, "myindex", "1", nil)
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("validation error empty index", func(t *testing.T) {
		resp, err := svc.TermVectors(ctx, "", "1", nil)
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Contains(t, err.Error(), "index is required")
	})

	t.Run("validation error empty id", func(t *testing.T) {
		resp, err := svc.TermVectors(ctx, "myindex", "", nil)
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Contains(t, err.Error(), "id is required")
	})
}

func TestUnitDocumentServiceMultiTermVectors(t *testing.T) {
	respJSON := `{"docs":[{"_index":"myindex","_id":"1","_version":1,"found":true,"took":5}]}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/_mtermvectors") {
			w.WriteHeader(200)
			fmt.Fprint(w, respJSON)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewDocumentService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success with index and body", func(t *testing.T) {
		body := map[string]any{"docs": []map[string]any{{"_index": "myindex", "_id": "1"}}}
		resp, err := svc.MultiTermVectors(ctx, "myindex", body)
		require.NoError(t, err)
		require.Len(t, resp.Docs, 1)
	})

	t.Run("success without index", func(t *testing.T) {
		body := map[string]any{"docs": []map[string]any{{"_index": "myindex", "_id": "1"}}}
		resp, err := svc.MultiTermVectors(ctx, "", body)
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("success without body", func(t *testing.T) {
		resp, err := svc.MultiTermVectors(ctx, "", nil)
		require.NoError(t, err)
		require.NotNil(t, resp)
	})
}

func TestUnitDocumentServiceReindex(t *testing.T) {
	respJSON := `{"took":100,"timed_out":false,"total":10,"created":10,"updated":0,"deleted":0,"batches":1,"version_conflicts":0,"noops":0,"retries":{"bulk":0,"search":0},"throttled":"0s","throttled_millis":0,"requests_per_second":-1,"throttled_until":"0s","throttled_until_millis":0,"failures":[]}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/_reindex" {
			w.WriteHeader(200)
			fmt.Fprint(w, respJSON)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewDocumentService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		body := map[string]any{
			"source": map[string]any{"index": "old_index"},
			"dest":   map[string]any{"index": "new_index"},
		}
		resp, err := svc.Reindex(ctx, body)
		require.NoError(t, err)
		assert.Equal(t, int64(10), resp.Created)
		assert.Equal(t, int64(10), resp.Total)
	})
}

func TestUnitDocumentServiceErrorPaths(t *testing.T) {
	ctx := context.Background()

	t.Run("Index server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewDocumentService(restyClient(srv), testLogger())
		_, err := s.Index(ctx, &IndexRequest{Index: "myindex", Id: "1", Body: map[string]any{"f": "v"}})
		require.Error(t, err)
	})

	t.Run("Index unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewDocumentService(restyClient(srv), testLogger())
		_, err := s.Index(ctx, &IndexRequest{Index: "myindex", Id: "1", Body: map[string]any{"f": "v"}})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("Get server error", func(t *testing.T) {
		srv := errServer(404)
		defer srv.Close()
		s := NewDocumentService(restyClient(srv), testLogger())
		_, err := s.Get(ctx, &GetRequest{Index: "myindex", Id: "1"})
		require.Error(t, err)
	})

	t.Run("Get unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewDocumentService(restyClient(srv), testLogger())
		_, err := s.Get(ctx, &GetRequest{Index: "myindex", Id: "1"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("MultiGet server error", func(t *testing.T) {
		srv := errServer(404)
		defer srv.Close()
		s := NewDocumentService(restyClient(srv), testLogger())
		_, err := s.MultiGet(ctx, []*MultiGetItem{{Index: "idx", Id: "1"}})
		require.Error(t, err)
	})

	t.Run("MultiGet unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewDocumentService(restyClient(srv), testLogger())
		_, err := s.MultiGet(ctx, []*MultiGetItem{{Index: "idx", Id: "1"}})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("Delete server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewDocumentService(restyClient(srv), testLogger())
		_, err := s.Delete(ctx, &DeleteRequest{Index: "myindex", Id: "1"})
		require.Error(t, err)
	})

	t.Run("Delete unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewDocumentService(restyClient(srv), testLogger())
		_, err := s.Delete(ctx, &DeleteRequest{Index: "myindex", Id: "1"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("DeleteByQuery server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewDocumentService(restyClient(srv), testLogger())
		_, err := s.DeleteByQuery(ctx, []string{"idx"}, map[string]any{})
		require.Error(t, err)
	})

	t.Run("DeleteByQuery unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewDocumentService(restyClient(srv), testLogger())
		_, err := s.DeleteByQuery(ctx, []string{"idx"}, map[string]any{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("Update server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewDocumentService(restyClient(srv), testLogger())
		_, err := s.Update(ctx, &UpdateRequest{Index: "myindex", Id: "1", Body: map[string]any{}})
		require.Error(t, err)
	})

	t.Run("Update unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewDocumentService(restyClient(srv), testLogger())
		_, err := s.Update(ctx, &UpdateRequest{Index: "myindex", Id: "1", Body: map[string]any{}})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("UpdateByQuery server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewDocumentService(restyClient(srv), testLogger())
		_, err := s.UpdateByQuery(ctx, []string{"idx"}, map[string]any{})
		require.Error(t, err)
	})

	t.Run("UpdateByQuery unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewDocumentService(restyClient(srv), testLogger())
		_, err := s.UpdateByQuery(ctx, []string{"idx"}, map[string]any{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("Bulk server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewDocumentService(restyClient(srv), testLogger())
		_, err := s.Bulk(ctx, "idx", "{}")
		require.Error(t, err)
	})

	t.Run("Bulk unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewDocumentService(restyClient(srv), testLogger())
		_, err := s.Bulk(ctx, "idx", "{}")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("Explain server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewDocumentService(restyClient(srv), testLogger())
		_, err := s.Explain(ctx, "myindex", "1", map[string]any{})
		require.Error(t, err)
	})

	t.Run("Explain unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewDocumentService(restyClient(srv), testLogger())
		_, err := s.Explain(ctx, "myindex", "1", map[string]any{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("TermVectors server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewDocumentService(restyClient(srv), testLogger())
		_, err := s.TermVectors(ctx, "myindex", "1", nil)
		require.Error(t, err)
	})

	t.Run("TermVectors unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewDocumentService(restyClient(srv), testLogger())
		_, err := s.TermVectors(ctx, "myindex", "1", nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("MultiTermVectors server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewDocumentService(restyClient(srv), testLogger())
		_, err := s.MultiTermVectors(ctx, "myindex", nil)
		require.Error(t, err)
	})

	t.Run("MultiTermVectors unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewDocumentService(restyClient(srv), testLogger())
		_, err := s.MultiTermVectors(ctx, "myindex", nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("Reindex server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewDocumentService(restyClient(srv), testLogger())
		_, err := s.Reindex(ctx, map[string]any{})
		require.Error(t, err)
	})

	t.Run("Reindex unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewDocumentService(restyClient(srv), testLogger())
		_, err := s.Reindex(ctx, map[string]any{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("Create validation error missing index", func(t *testing.T) {
		srv := errServer(200)
		defer srv.Close()
		s := NewDocumentService(restyClient(srv), testLogger())
		_, err := s.Create(ctx, &CreateRequest{Id: "1", Body: map[string]any{}})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "validation")
	})

	t.Run("Create validation error missing id", func(t *testing.T) {
		srv := errServer(200)
		defer srv.Close()
		s := NewDocumentService(restyClient(srv), testLogger())
		_, err := s.Create(ctx, &CreateRequest{Index: "idx", Body: map[string]any{}})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "validation")
	})

	t.Run("Create validation error missing body", func(t *testing.T) {
		srv := errServer(200)
		defer srv.Close()
		s := NewDocumentService(restyClient(srv), testLogger())
		_, err := s.Create(ctx, &CreateRequest{Index: "idx", Id: "1"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "validation")
	})

	t.Run("Create unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewDocumentService(restyClient(srv), testLogger())
		_, err := s.Create(ctx, &CreateRequest{Index: "idx", Id: "1", Body: map[string]any{}})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("GetSource missing index", func(t *testing.T) {
		s := NewDocumentService(deadClient(), testLogger())
		_, err := s.GetSource(ctx, "", "1")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "index is required")
	})

	t.Run("GetSource missing id", func(t *testing.T) {
		s := NewDocumentService(deadClient(), testLogger())
		_, err := s.GetSource(ctx, "idx", "")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "id is required")
	})

	t.Run("GetSource unmarshal error", func(t *testing.T) {
		// GetSource returns raw body so unmarshal error path is N/A;
		// however, a network error is tested in TestUnitDocumentServiceNetworkErrors
	})

	t.Run("ExistsSource missing index", func(t *testing.T) {
		s := NewDocumentService(deadClient(), testLogger())
		_, err := s.ExistsSource(ctx, "", "1")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "index is required")
	})

	t.Run("ExistsSource missing id", func(t *testing.T) {
		s := NewDocumentService(deadClient(), testLogger())
		_, err := s.ExistsSource(ctx, "idx", "")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "id is required")
	})

	t.Run("ReindexRethrottle validation error missing task id", func(t *testing.T) {
		srv := errServer(200)
		defer srv.Close()
		s := NewDocumentService(restyClient(srv), testLogger())
		_, err := s.ReindexRethrottle(ctx, &RethrottleRequest{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "validation")
	})

	t.Run("ReindexRethrottle unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewDocumentService(restyClient(srv), testLogger())
		_, err := s.ReindexRethrottle(ctx, &RethrottleRequest{TaskId: "t1"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("DeleteByQueryRethrottle unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewDocumentService(restyClient(srv), testLogger())
		_, err := s.DeleteByQueryRethrottle(ctx, &RethrottleRequest{TaskId: "t1"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("UpdateByQueryRethrottle unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewDocumentService(restyClient(srv), testLogger())
		_, err := s.UpdateByQueryRethrottle(ctx, &RethrottleRequest{TaskId: "t1"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})
}

func TestUnitDocumentServiceNetworkErrors(t *testing.T) {
	ctx := context.Background()
	s := NewDocumentService(deadClient(), testLogger())

	t.Run("Get network error", func(t *testing.T) {
		_, err := s.Get(ctx, &GetRequest{Index: "idx", Id: "1"})
		require.Error(t, err)
	})

	t.Run("MultiGet network error", func(t *testing.T) {
		_, err := s.MultiGet(ctx, []*MultiGetItem{{Index: "idx", Id: "1"}})
		require.Error(t, err)
	})

	t.Run("Delete network error", func(t *testing.T) {
		_, err := s.Delete(ctx, &DeleteRequest{Index: "idx", Id: "1"})
		require.Error(t, err)
	})

	t.Run("DeleteByQuery network error", func(t *testing.T) {
		_, err := s.DeleteByQuery(ctx, []string{"idx"}, map[string]any{})
		require.Error(t, err)
	})

	t.Run("Update network error", func(t *testing.T) {
		_, err := s.Update(ctx, &UpdateRequest{Index: "idx", Id: "1", Body: map[string]any{}})
		require.Error(t, err)
	})

	t.Run("UpdateByQuery network error", func(t *testing.T) {
		_, err := s.UpdateByQuery(ctx, []string{"idx"}, map[string]any{})
		require.Error(t, err)
	})

	t.Run("Bulk network error", func(t *testing.T) {
		_, err := s.Bulk(ctx, "idx", "{}")
		require.Error(t, err)
	})

	t.Run("Exists network error", func(t *testing.T) {
		_, err := s.Exists(ctx, "idx", "1")
		require.Error(t, err)
	})

	t.Run("Explain network error", func(t *testing.T) {
		_, err := s.Explain(ctx, "idx", "1", nil)
		require.Error(t, err)
	})

	t.Run("TermVectors network error", func(t *testing.T) {
		_, err := s.TermVectors(ctx, "idx", "1", nil)
		require.Error(t, err)
	})

	t.Run("MultiTermVectors network error", func(t *testing.T) {
		_, err := s.MultiTermVectors(ctx, "idx", nil)
		require.Error(t, err)
	})

	t.Run("Reindex network error", func(t *testing.T) {
		_, err := s.Reindex(ctx, map[string]any{})
		require.Error(t, err)
	})

	t.Run("Create network error", func(t *testing.T) {
		_, err := s.Create(ctx, &CreateRequest{Index: "idx", Id: "1", Body: map[string]any{}})
		require.Error(t, err)
	})

	t.Run("GetSource network error", func(t *testing.T) {
		_, err := s.GetSource(ctx, "idx", "1")
		require.Error(t, err)
	})

	t.Run("ExistsSource network error", func(t *testing.T) {
		_, err := s.ExistsSource(ctx, "idx", "1")
		require.Error(t, err)
	})

	t.Run("ReindexRethrottle network error", func(t *testing.T) {
		_, err := s.ReindexRethrottle(ctx, &RethrottleRequest{TaskId: "task1"})
		require.Error(t, err)
	})

	t.Run("DeleteByQueryRethrottle network error", func(t *testing.T) {
		_, err := s.DeleteByQueryRethrottle(ctx, &RethrottleRequest{TaskId: "task1"})
		require.Error(t, err)
	})

	t.Run("UpdateByQueryRethrottle network error", func(t *testing.T) {
		_, err := s.UpdateByQueryRethrottle(ctx, &RethrottleRequest{TaskId: "task1"})
		require.Error(t, err)
	})
}

func TestUnitDocumentServiceCreate(t *testing.T) {
	respJSON := `{"_index":"idx","_id":"1","_version":1,"result":"created","_shards":{"total":2,"successful":1,"failed":0},"_seq_no":0,"_primary_term":1}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut && r.URL.Path == "/myindex/_create/1" {
			w.WriteHeader(201)
			fmt.Fprint(w, respJSON)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewDocumentService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		resp, err := svc.Create(ctx, &CreateRequest{Index: "myindex", Id: "1", Body: map[string]any{"field": "value"}})
		require.NoError(t, err)
		assert.Equal(t, "idx", resp.Index)
		assert.Equal(t, "1", resp.Id)
		assert.Equal(t, "created", resp.Result)
	})

	t.Run("success with params", func(t *testing.T) {
		resp, err := svc.Create(ctx, &CreateRequest{Index: "myindex", Id: "1", Body: map[string]any{"field": "value"}, Params: map[string]string{"refresh": "true"}})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("server error", func(t *testing.T) {
		errSrv := errServer(409)
		defer errSrv.Close()
		s := NewDocumentService(restyClient(errSrv), testLogger())
		_, err := s.Create(ctx, &CreateRequest{Index: "idx", Id: "1", Body: map[string]any{}})
		require.Error(t, err)
	})

	t.Run("unmarshal error", func(t *testing.T) {
		badSrv := badJSONServer()
		defer badSrv.Close()
		s := NewDocumentService(restyClient(badSrv), testLogger())
		_, err := s.Create(ctx, &CreateRequest{Index: "idx", Id: "1", Body: map[string]any{}})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})
}

func TestUnitDocumentServiceGetSource(t *testing.T) {
	respJSON := `{"field":"value","nested":{"a":1}}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == "/myindex/_source/1" {
			w.WriteHeader(200)
			fmt.Fprint(w, respJSON)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewDocumentService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		resp, err := svc.GetSource(ctx, "myindex", "1")
		require.NoError(t, err)
		assert.NotEmpty(t, resp)
	})

	t.Run("server error", func(t *testing.T) {
		errSrv := errServer(404)
		defer errSrv.Close()
		s := NewDocumentService(restyClient(errSrv), testLogger())
		_, err := s.GetSource(ctx, "idx", "1")
		require.Error(t, err)
	})
}

func TestUnitDocumentServiceExistsSource(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead && r.URL.Path == "/myindex/_source/1" {
			w.WriteHeader(200)
			return
		}
		if r.Method == http.MethodHead && r.URL.Path == "/myindex/_source/2" {
			w.WriteHeader(404)
			return
		}
		w.WriteHeader(500)
	}))
	defer srv.Close()

	svc := NewDocumentService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("exists", func(t *testing.T) {
		exists, err := svc.ExistsSource(ctx, "myindex", "1")
		require.NoError(t, err)
		assert.True(t, exists)
	})

	t.Run("does not exist", func(t *testing.T) {
		exists, err := svc.ExistsSource(ctx, "myindex", "2")
		require.NoError(t, err)
		assert.False(t, exists)
	})
}

func TestUnitDocumentServiceReindexRethrottle(t *testing.T) {
	respJSON := `{"nodes":{}}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && strings.Contains(r.URL.Path, "/_reindex/") && strings.HasSuffix(r.URL.Path, "/_rethrottle") {
			w.WriteHeader(200)
			fmt.Fprint(w, respJSON)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewDocumentService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		resp, err := svc.ReindexRethrottle(ctx, &RethrottleRequest{TaskId: "task1", RequestsPerSecond: 5.0})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("server error", func(t *testing.T) {
		errSrv := errServer(500)
		defer errSrv.Close()
		s := NewDocumentService(restyClient(errSrv), testLogger())
		_, err := s.ReindexRethrottle(ctx, &RethrottleRequest{TaskId: "task1"})
		require.Error(t, err)
	})
}

func TestUnitDocumentServiceDeleteByQueryRethrottle(t *testing.T) {
	respJSON := `{"nodes":{}}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && strings.Contains(r.URL.Path, "/_delete_by_query/") {
			w.WriteHeader(200)
			fmt.Fprint(w, respJSON)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewDocumentService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		resp, err := svc.DeleteByQueryRethrottle(ctx, &RethrottleRequest{TaskId: "task1", RequestsPerSecond: 2.0})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("server error", func(t *testing.T) {
		errSrv := errServer(500)
		defer errSrv.Close()
		s := NewDocumentService(restyClient(errSrv), testLogger())
		_, err := s.DeleteByQueryRethrottle(ctx, &RethrottleRequest{TaskId: "task1"})
		require.Error(t, err)
	})

	t.Run("validation error missing task id", func(t *testing.T) {
		_, err := svc.DeleteByQueryRethrottle(ctx, &RethrottleRequest{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "validation")
	})
}

func TestUnitDocumentServiceUpdateByQueryRethrottle(t *testing.T) {
	respJSON := `{"nodes":{}}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && strings.Contains(r.URL.Path, "/_update_by_query/") {
			w.WriteHeader(200)
			fmt.Fprint(w, respJSON)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewDocumentService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		resp, err := svc.UpdateByQueryRethrottle(ctx, &RethrottleRequest{TaskId: "task1", RequestsPerSecond: 10.0})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("server error", func(t *testing.T) {
		errSrv := errServer(500)
		defer errSrv.Close()
		s := NewDocumentService(restyClient(errSrv), testLogger())
		_, err := s.UpdateByQueryRethrottle(ctx, &RethrottleRequest{TaskId: "task1"})
		require.Error(t, err)
	})

	t.Run("validation error missing task id", func(t *testing.T) {
		_, err := svc.UpdateByQueryRethrottle(ctx, &RethrottleRequest{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "validation")
	})
}

func TestUnitDocumentServiceExistsSourceUnknownStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
	}))
	defer srv.Close()

	svc := NewDocumentService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("unexpected status", func(t *testing.T) {
		exists, err := svc.ExistsSource(ctx, "myindex", "1")
		require.Error(t, err)
		assert.False(t, exists)
		assert.Contains(t, err.Error(), "unexpected status code")
	})
}
