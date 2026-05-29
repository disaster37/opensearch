package api

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUnitCatService_Indices(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/_cat/indices", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`[{"health":"green","status":"open","index":"test-idx","uuid":"abc123","pri":"1","rep":"0","docs.count":"100","docs.deleted":"0","store.size":"1kb","pri.store.size":"1kb","creation.date":"1700000000000","creation.date.string":"2023-11-14","completion.size":"0b","pri.completion.size":"0b","fielddata.memory_size":"0b","pri.fielddata.memory_size":"0b","fielddata.evictions":"0","pri.fielddata.evictions":"0","query_cache.memory_size":"0b","pri.query_cache.memory_size":"0b","query_cache.evictions":"0","pri.query_cache.evictions":"0","request_cache.memory_size":"0b","pri.request_cache.memory_size":"0b","request_cache.evictions":"0","pri.request_cache.evictions":"0","request_cache.hit_count":"0","pri.request_cache.hit_count":"0","request_cache.miss_count":"0","pri.request_cache.miss_count":"0","flush.total":"1","pri.flush.total":"1","flush.total_time":"1s","pri.flush.total_time":"1s","get.current":"0","pri.get.current":"0","get.time":"0s","pri.get.time":"0s","get.total":"0","pri.get.total":"0","get.exists_time":"0s","pri.get.exists_time":"0s","get.exists_total":"0","pri.get.exists_total":"0","get.missing_time":"0s","pri.get.missing_time":"0s","get.missing_total":"0","pri.get.missing_total":"0","indexing.delete_current":"0","pri.indexing.delete_current":"0","indexing.delete_time":"0s","pri.indexing.delete_time":"0s","indexing.delete_total":"0","pri.indexing.delete_total":"0","indexing.index_current":"0","pri.indexing.index_current":"0","indexing.index_time":"0s","pri.indexing.index_time":"0s","indexing.index_total":"0","pri.indexing.index_total":"0","indexing.index_failed":"0","pri.indexing.index_failed":"0","merges.current":"0","pri.merges.current":"0","merges.current_docs":"0","pri.merges.current_docs":"0","merges.current_size":"0b","pri.merges.current_size":"0b","merges.total":"0","pri.merges.total":"0","merges.total_docs":"0","pri.merges.total_docs":"0","merges.total_size":"0b","pri.merges.total_size":"0b","merges.total_time":"0s","pri.merges.total_time":"0s","refresh.total":"1","pri.refresh.total":"1","refresh.external_total":"0","pri.refresh.external_total":"0","refresh.time":"0s","pri.refresh.time":"0s","refresh.external_time":"0s","pri.refresh.external_time":"0s","refresh.listeners":"0","pri.refresh.listeners":"0","search.fetch_current":"0","pri.search.fetch_current":"0","search.fetch_time":"0s","pri.search.fetch_time":"0s","search.fetch_total":"0","pri.search.fetch_total":"0","search.open_contexts":"0","pri.search.open_contexts":"0","search.query_current":"0","pri.search.query_current":"0","search.query_time":"0s","pri.search.query_time":"0s","search.query_total":"0","pri.search.query_total":"0","search.query_failed":"0","pri.search.query_failed":"0","search.scroll_current":"0","pri.search.scroll_current":"0","search.scroll_time":"0s","pri.search.scroll_time":"0s","search.scroll_total":"0","pri.search.scroll_total":"0","search.throttled":"false","segments.count":"1","pri.segments.count":"1","segments.memory":"0b","pri.segments.memory":"0b","segments.index_writer_memory":"0b","pri.segments.index_writer_memory":"0b","segments.version_map_memory":"0b","pri.segments.version_map_memory":"0b","segments.fixed_bitset_memory":"0b","pri.segments.fixed_bitset_memory":"0b","warmer.current":"0","pri.warmer.current":"0","warmer.total":"0","pri.warmer.total":"0","warmer.total_time":"0s","pri.warmer.total_time":"0s","suggest.current":"0","pri.suggest.current":"0","suggest.time":"0s","pri.suggest.time":"0s","suggest.total":"0","pri.suggest.total":"0","memory.total":"0b","pri.memory.total":"0b","last_index_request_timestamp":"0","last_index_request_timestamp_string":""}]`))
	})
	mux.HandleFunc("/_cat/indices/test-idx", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`[{"health":"green","status":"open","index":"test-idx","uuid":"abc123","pri":"1","rep":"0","docs.count":"100","docs.deleted":"0","store.size":"1kb","pri.store.size":"1kb","creation.date":"1700000000000","creation.date.string":"2023-11-14","completion.size":"0b","pri.completion.size":"0b","fielddata.memory_size":"0b","pri.fielddata.memory_size":"0b","fielddata.evictions":"0","pri.fielddata.evictions":"0","query_cache.memory_size":"0b","pri.query_cache.memory_size":"0b","query_cache.evictions":"0","pri.query_cache.evictions":"0","request_cache.memory_size":"0b","pri.request_cache.memory_size":"0b","request_cache.evictions":"0","pri.request_cache.evictions":"0","request_cache.hit_count":"0","pri.request_cache.hit_count":"0","request_cache.miss_count":"0","pri.request_cache.miss_count":"0","flush.total":"1","pri.flush.total":"1","flush.total_time":"1s","pri.flush.total_time":"1s","get.current":"0","pri.get.current":"0","get.time":"0s","pri.get.time":"0s","get.total":"0","pri.get.total":"0","get.exists_time":"0s","pri.get.exists_time":"0s","get.exists_total":"0","pri.get.exists_total":"0","get.missing_time":"0s","pri.get.missing_time":"0s","get.missing_total":"0","pri.get.missing_total":"0","indexing.delete_current":"0","pri.indexing.delete_current":"0","indexing.delete_time":"0s","pri.indexing.delete_time":"0s","indexing.delete_total":"0","pri.indexing.delete_total":"0","indexing.index_current":"0","pri.indexing.index_current":"0","indexing.index_time":"0s","pri.indexing.index_time":"0s","indexing.index_total":"0","pri.indexing.index_total":"0","indexing.index_failed":"0","pri.indexing.index_failed":"0","merges.current":"0","pri.merges.current":"0","merges.current_docs":"0","pri.merges.current_docs":"0","merges.current_size":"0b","pri.merges.current_size":"0b","merges.total":"0","pri.merges.total":"0","merges.total_docs":"0","pri.merges.total_docs":"0","merges.total_size":"0b","pri.merges.total_size":"0b","merges.total_time":"0s","pri.merges.total_time":"0s","refresh.total":"1","pri.refresh.total":"1","refresh.external_total":"0","pri.refresh.external_total":"0","refresh.time":"0s","pri.refresh.time":"0s","refresh.external_time":"0s","pri.refresh.external_time":"0s","refresh.listeners":"0","pri.refresh.listeners":"0","search.fetch_current":"0","pri.search.fetch_current":"0","search.fetch_time":"0s","pri.search.fetch_time":"0s","search.fetch_total":"0","pri.search.fetch_total":"0","search.open_contexts":"0","pri.search.open_contexts":"0","search.query_current":"0","pri.search.query_current":"0","search.query_time":"0s","pri.search.query_time":"0s","search.query_total":"0","pri.search.query_total":"0","search.query_failed":"0","pri.search.query_failed":"0","search.scroll_current":"0","pri.search.scroll_current":"0","search.scroll_time":"0s","pri.search.scroll_time":"0s","search.scroll_total":"0","pri.search.scroll_total":"0","search.throttled":"false","segments.count":"1","pri.segments.count":"1","segments.memory":"0b","pri.segments.memory":"0b","segments.index_writer_memory":"0b","pri.segments.index_writer_memory":"0b","segments.version_map_memory":"0b","pri.segments.version_map_memory":"0b","segments.fixed_bitset_memory":"0b","pri.segments.fixed_bitset_memory":"0b","warmer.current":"0","pri.warmer.current":"0","warmer.total":"0","pri.warmer.total":"0","warmer.total_time":"0s","pri.warmer.total_time":"0s","suggest.current":"0","pri.suggest.current":"0","suggest.time":"0s","pri.suggest.time":"0s","suggest.total":"0","pri.suggest.total":"0","memory.total":"0b","pri.memory.total":"0b","last_index_request_timestamp":"0","last_index_request_timestamp_string":""}]`))
	})

	ts := httptest.NewServer(mux)
	defer ts.Close()

	svc := NewCatService(restyClient(ts), testLogger())
	ctx := context.Background()

	t.Run("all indices", func(t *testing.T) {
		resp, err := svc.Indices(ctx, nil)
		require.NoError(t, err)
		require.Len(t, resp, 1)
		assert.Equal(t, "test-idx", resp[0].Index)
		assert.Equal(t, "green", resp[0].Health)
	})

	t.Run("specific index", func(t *testing.T) {
		resp, err := svc.Indices(ctx, []string{"test-idx"})
		require.NoError(t, err)
		require.Len(t, resp, 1)
		assert.Equal(t, "test-idx", resp[0].Index)
	})
}

func TestUnitCatService_Shards(t *testing.T) {
	mux := http.NewServeMux()
	handler := func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`[{"index":"test-idx","shard":"0","prirep":"p","state":"STARTED","docs":"100","store":"1kb","ip":"127.0.0.1","node":"node1","uuid":"abc"}]`))
	}
	mux.HandleFunc("/_cat/shards", handler)
	mux.HandleFunc("/_cat/shards/test-idx", handler)

	ts := httptest.NewServer(mux)
	defer ts.Close()

	svc := NewCatService(restyClient(ts), testLogger())
	ctx := context.Background()

	t.Run("all shards", func(t *testing.T) {
		resp, err := svc.Shards(ctx, nil)
		require.NoError(t, err)
		require.Len(t, resp, 1)
		assert.Equal(t, "test-idx", resp[0].Index)
		assert.Equal(t, "STARTED", resp[0].State)
	})

	t.Run("specific index", func(t *testing.T) {
		resp, err := svc.Shards(ctx, []string{"test-idx"})
		require.NoError(t, err)
		require.Len(t, resp, 1)
	})
}

func TestUnitCatService_Aliases(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/_cat/aliases", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`[{"alias":"my-alias","index":"test-idx","filter":"-","routing.index":"-","routing.search":"-","is_write_index":"-"}]`))
	})

	ts := httptest.NewServer(mux)
	defer ts.Close()

	svc := NewCatService(restyClient(ts), testLogger())
	ctx := context.Background()

	resp, err := svc.Aliases(ctx, nil)
	require.NoError(t, err)
	require.Len(t, resp, 1)
	assert.Equal(t, "my-alias", resp[0].Alias)
	assert.Equal(t, "test-idx", resp[0].Index)
}

func TestUnitCatService_Health(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`[{"epoch":"1700000000","timestamp":"12:00:00","cluster":"test-cluster","status":"green","node.total":"1","node.data":"1","shards":"5","pri":"5","relo":"0","init":"0","unassign":"0","pending_tasks":"0","max_task_wait_time":"-1ms","active_shards_percent":"100.0%","discovered_cluster_manager":"true"}]`))
	}))
	defer ts.Close()

	svc := NewCatService(restyClient(ts), testLogger())
	ctx := context.Background()

	resp, err := svc.Health(ctx)
	require.NoError(t, err)
	require.Len(t, resp, 1)
	assert.Equal(t, "test-cluster", resp[0].Cluster)
	assert.Equal(t, "green", resp[0].Status)
	assert.Equal(t, 1, resp[0].NodeTotal)
}

func TestUnitCatService_Count(t *testing.T) {
	mux := http.NewServeMux()
	handler := func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`[{"epoch":"1700000000","timestamp":"12:00:00","count":"500"}]`))
	}
	mux.HandleFunc("/_cat/count", handler)
	mux.HandleFunc("/_cat/count/test-idx", handler)

	ts := httptest.NewServer(mux)
	defer ts.Close()

	svc := NewCatService(restyClient(ts), testLogger())
	ctx := context.Background()

	t.Run("all indices", func(t *testing.T) {
		resp, err := svc.Count(ctx, nil)
		require.NoError(t, err)
		require.Len(t, resp, 1)
		assert.Equal(t, 500, resp[0].Count)
	})

	t.Run("specific index", func(t *testing.T) {
		resp, err := svc.Count(ctx, []string{"test-idx"})
		require.NoError(t, err)
		require.Len(t, resp, 1)
	})
}

func TestUnitCatService_Allocation(t *testing.T) {
	mux := http.NewServeMux()
	handler := func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`[{"shards":"5","disk.indices":"1gb","disk.used":"10gb","disk.avail":"90gb","disk.total":"100gb","disk.percent":"10","host":"127.0.0.1","ip":"127.0.0.1","node":"node1"}]`))
	}
	mux.HandleFunc("/_cat/allocation", handler)
	mux.HandleFunc("/_cat/allocation/node1", handler)

	ts := httptest.NewServer(mux)
	defer ts.Close()

	svc := NewCatService(restyClient(ts), testLogger())
	ctx := context.Background()

	t.Run("all nodes", func(t *testing.T) {
		resp, err := svc.Allocation(ctx, nil)
		require.NoError(t, err)
		require.Len(t, resp, 1)
		assert.Equal(t, "node1", resp[0].Node)
		assert.Equal(t, 5, resp[0].Shards)
		assert.Equal(t, 10, resp[0].DiskPercent)
	})

	t.Run("specific node", func(t *testing.T) {
		resp, err := svc.Allocation(ctx, []string{"node1"})
		require.NoError(t, err)
		require.Len(t, resp, 1)
	})
}

func TestUnitCatService_Fielddata(t *testing.T) {
	mux := http.NewServeMux()
	handler := func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`[{"id":"n1","host":"127.0.0.1","ip":"127.0.0.1","node":"node1","field":"my-field","size":"1mb"}]`))
	}
	mux.HandleFunc("/_cat/fielddata", handler)
	mux.HandleFunc("/_cat/fielddata/my-field", handler)

	ts := httptest.NewServer(mux)
	defer ts.Close()

	svc := NewCatService(restyClient(ts), testLogger())
	ctx := context.Background()

	t.Run("all fields", func(t *testing.T) {
		resp, err := svc.Fielddata(ctx, nil)
		require.NoError(t, err)
		require.Len(t, resp, 1)
		assert.Equal(t, "my-field", resp[0].Field)
	})

	t.Run("specific field", func(t *testing.T) {
		resp, err := svc.Fielddata(ctx, []string{"my-field"})
		require.NoError(t, err)
		require.Len(t, resp, 1)
	})
}

func TestUnitCatService_Snapshots(t *testing.T) {
	mux := http.NewServeMux()
	handler := func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`[{"id":"snap1","repository":"my-repo","status":"SUCCESS","start_epoch":"1700000000","start_time":"12:00:00","end_epoch":"1700000100","end_time":"12:01:40","duration":"100s","indices":"test-idx","successful_shards":"5","failed_shards":"0","total_shards":"5","reason":""}]`))
	}
	mux.HandleFunc("/_cat/snapshots", handler)
	mux.HandleFunc("/_cat/snapshots/my-repo", handler)

	ts := httptest.NewServer(mux)
	defer ts.Close()

	svc := NewCatService(restyClient(ts), testLogger())
	ctx := context.Background()

	t.Run("all repos", func(t *testing.T) {
		resp, err := svc.Snapshots(ctx, "")
		require.NoError(t, err)
		require.Len(t, resp, 1)
		assert.Equal(t, "snap1", resp[0].ID)
		assert.Equal(t, "SUCCESS", resp[0].Status)
	})

	t.Run("specific repo", func(t *testing.T) {
		resp, err := svc.Snapshots(ctx, "my-repo")
		require.NoError(t, err)
		require.Len(t, resp, 1)
		assert.Equal(t, "my-repo", resp[0].Repository)
	})
}

func TestUnitCatService_Master(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`[{"id":"n1","host":"127.0.0.1","ip":"127.0.0.1","node":"master-node"}]`))
	}))
	defer ts.Close()

	svc := NewCatService(restyClient(ts), testLogger())
	ctx := context.Background()

	resp, err := svc.Master(ctx)
	require.NoError(t, err)
	require.Len(t, resp, 1)
	assert.Equal(t, "master-node", resp[0].Node)
	assert.Equal(t, "n1", resp[0].ID)
}

func TestUnitCatService_Help(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/_cat" {
			w.WriteHeader(200)
			fmt.Fprint(w, "/_cat/aliases\n/_cat/allocation\n/_cat/health\n")
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewCatService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		resp, err := svc.Help(ctx)
		require.NoError(t, err)
		assert.Contains(t, resp, "/_cat/aliases")
		assert.Contains(t, resp, "/_cat/health")
	})
}

func TestUnitCatService_NodeAttrs(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/_cat/nodeattrs" && r.URL.Query().Get("format") == "json" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(200)
			fmt.Fprint(w, `[{"node":"node1","host":"127.0.0.1","ip":"10.0.0.1","attr":"rack","value":"a"}]`)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewCatService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		resp, err := svc.NodeAttrs(ctx)
		require.NoError(t, err)
		require.Len(t, resp, 1)
		assert.Equal(t, "node1", resp[0].Node)
		assert.Equal(t, "rack", resp[0].Attr)
		assert.Equal(t, "a", resp[0].Value)
	})
}

func TestUnitCatService_CatNodes(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/_cat/nodes" && r.URL.Query().Get("format") == "json" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(200)
			fmt.Fprint(w, `[{"ip":"10.0.0.1","heap.percent":"50","ram.percent":"70","cpu":"10","load_1m":"0.5","load_5m":"0.4","load_15m":"0.3","node.role":"dimr","master":"*","name":"node1","jdk":"17","version":"2.11.0","disk.used_percent":"20"}]`)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewCatService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		resp, err := svc.CatNodes(ctx)
		require.NoError(t, err)
		require.Len(t, resp, 1)
		assert.Equal(t, "node1", resp[0].Name)
		assert.Equal(t, "10.0.0.1", resp[0].IP)
		assert.Equal(t, "2.11.0", resp[0].Version)
	})
}

func TestUnitCatService_CatPendingTasks(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/_cat/pending_tasks" && r.URL.Query().Get("format") == "json" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(200)
			fmt.Fprint(w, `[{"insertOrder":"1","timeInQueue":"100ms","priority":"HIGH","source":"create-index"}]`)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewCatService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		resp, err := svc.CatPendingTasks(ctx)
		require.NoError(t, err)
		require.Len(t, resp, 1)
		assert.Equal(t, "HIGH", resp[0].Priority)
		assert.Equal(t, "create-index", resp[0].Source)
	})
}

func TestUnitCatService_Plugins(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/_cat/plugins" && r.URL.Query().Get("format") == "json" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(200)
			fmt.Fprint(w, `[{"name":"analysis-icu","component":"analysis-icu","version":"2.11.0","description":"ICU analyzer plugin"}]`)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewCatService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		resp, err := svc.Plugins(ctx)
		require.NoError(t, err)
		require.Len(t, resp, 1)
		assert.Equal(t, "analysis-icu", resp[0].Name)
		assert.Equal(t, "2.11.0", resp[0].Version)
	})
}

func TestUnitCatService_CatRecovery(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("format") != "json" {
			w.WriteHeader(404)
			return
		}
		if r.URL.Path == "/_cat/recovery" || r.URL.Path == "/_cat/recovery/test-idx" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(200)
			fmt.Fprint(w, `[{"index":"test-idx","shard":"0","start_time":"12:00:00","time":"10s","type":"existing_store","stage":"done","source_host":"127.0.0.1","source_node":"node1","target_host":"127.0.0.1","target_node":"node1","repository":"","snapshot":"","files":"5","files_recovered":"5","files_percent":"100%","bytes":"1000","bytes_recovered":"1000","bytes_percent":"100%","translog_ops":"10","translog_ops_recovered":"10","translog_ops_percent":"100%"}]`)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewCatService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("all indices", func(t *testing.T) {
		resp, err := svc.CatRecovery(ctx, nil)
		require.NoError(t, err)
		require.Len(t, resp, 1)
		assert.Equal(t, "test-idx", resp[0].Index)
		assert.Equal(t, "done", resp[0].Stage)
	})

	t.Run("specific index", func(t *testing.T) {
		resp, err := svc.CatRecovery(ctx, []string{"test-idx"})
		require.NoError(t, err)
		require.Len(t, resp, 1)
		assert.Equal(t, "test-idx", resp[0].Index)
	})
}

func TestUnitCatService_Repositories(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/_cat/repositories" && r.URL.Query().Get("format") == "json" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(200)
			fmt.Fprint(w, `[{"id":"my-repo","type":"fs"}]`)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewCatService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		resp, err := svc.Repositories(ctx)
		require.NoError(t, err)
		require.Len(t, resp, 1)
		assert.Equal(t, "my-repo", resp[0].ID)
		assert.Equal(t, "fs", resp[0].Type)
	})
}

func TestUnitCatService_Segments(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("format") != "json" {
			w.WriteHeader(404)
			return
		}
		if r.URL.Path == "/_cat/segments" || r.URL.Path == "/_cat/segments/test-idx" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(200)
			fmt.Fprint(w, `[{"index":"test-idx","shard":"0","prirep":"p","ip":"127.0.0.1","id":"abc","segment":"_0","version":"9.7.0","compound":"true","size":"1kb","docs.count":"10","size.memory":"512"}]`)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewCatService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("all indices", func(t *testing.T) {
		resp, err := svc.Segments(ctx, nil)
		require.NoError(t, err)
		require.Len(t, resp, 1)
		assert.Equal(t, "test-idx", resp[0].Index)
		assert.Equal(t, "_0", resp[0].Segment)
	})

	t.Run("specific index", func(t *testing.T) {
		resp, err := svc.Segments(ctx, []string{"test-idx"})
		require.NoError(t, err)
		require.Len(t, resp, 1)
		assert.Equal(t, "test-idx", resp[0].Index)
	})
}

func TestUnitCatService_SegmentReplication(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("format") != "json" {
			w.WriteHeader(404)
			return
		}
		if r.URL.Path == "/_cat/segment_replication" || r.URL.Path == "/_cat/segment_replication/test-idx" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(200)
			fmt.Fprint(w, `[{"shard":"test-idx:0","checkpoint":"1","replicating.time_taken":"100ms","get_changes.time_taken":"50ms","total.time_taken":"150ms","accepted.translog_ops":"5"}]`)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewCatService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("all indices", func(t *testing.T) {
		resp, err := svc.SegmentReplication(ctx, nil)
		require.NoError(t, err)
		require.Len(t, resp, 1)
		assert.Equal(t, "test-idx:0", resp[0].Shard)
		assert.Equal(t, "1", resp[0].Checkpoint)
	})

	t.Run("specific index", func(t *testing.T) {
		resp, err := svc.SegmentReplication(ctx, []string{"test-idx"})
		require.NoError(t, err)
		require.Len(t, resp, 1)
		assert.Equal(t, "test-idx:0", resp[0].Shard)
	})
}

func TestUnitCatService_CatTasks(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/_cat/tasks" && r.URL.Query().Get("format") == "json" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(200)
			fmt.Fprint(w, `[{"id":"1","action":"indices:data/read/search","task_id":"abc123","parent_task_id":"-","node_id":"n1","node_ip":"127.0.0.1","running_time":"100ms","type":"transport","description":"search"}]`)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewCatService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		resp, err := svc.CatTasks(ctx)
		require.NoError(t, err)
		require.Len(t, resp, 1)
		assert.Equal(t, "indices:data/read/search", resp[0].Action)
		assert.Equal(t, "abc123", resp[0].TaskID)
	})
}

func TestUnitCatService_Templates(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("format") != "json" {
			w.WriteHeader(404)
			return
		}
		if r.URL.Path == "/_cat/templates" || r.URL.Path == "/_cat/templates/my-template" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(200)
			fmt.Fprint(w, `[{"name":"my-template","index_patterns":"[test-*]","order":"0","version":"1"}]`)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewCatService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("all templates", func(t *testing.T) {
		resp, err := svc.Templates(ctx, "")
		require.NoError(t, err)
		require.Len(t, resp, 1)
		assert.Equal(t, "my-template", resp[0].Name)
		assert.Equal(t, "[test-*]", resp[0].IndexPatterns)
	})

	t.Run("specific template", func(t *testing.T) {
		resp, err := svc.Templates(ctx, "my-template")
		require.NoError(t, err)
		require.Len(t, resp, 1)
		assert.Equal(t, "my-template", resp[0].Name)
	})
}

func TestUnitCatService_ThreadPool(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("format") != "json" {
			w.WriteHeader(404)
			return
		}
		if r.URL.Path == "/_cat/thread_pool" || r.URL.Path == "/_cat/thread_pool/bulk" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(200)
			fmt.Fprint(w, `[{"node_name":"node1","name":"bulk","active":"0","pool_size":"5","queue":"0","queue_size":"200","rejected":"0","largest":"5","completed":"1000","type":"fixed"}]`)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewCatService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("all thread pools", func(t *testing.T) {
		resp, err := svc.ThreadPool(ctx, nil)
		require.NoError(t, err)
		require.Len(t, resp, 1)
		assert.Equal(t, "node1", resp[0].NodeName)
		assert.Equal(t, "bulk", resp[0].Name)
		assert.Equal(t, "fixed", resp[0].Type)
	})

	t.Run("specific pattern", func(t *testing.T) {
		resp, err := svc.ThreadPool(ctx, []string{"bulk"})
		require.NoError(t, err)
		require.Len(t, resp, 1)
		assert.Equal(t, "bulk", resp[0].Name)
	})
}

func TestUnitCatService_ClusterManager(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/_cat/cluster_manager" && r.URL.Query().Get("format") == "json" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(200)
			fmt.Fprint(w, `[{"ip":"10.0.0.1","id":"n1","host":"127.0.0.1","node":"manager-node"}]`)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()

	svc := NewCatService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		resp, err := svc.ClusterManager(ctx)
		require.NoError(t, err)
		require.Len(t, resp, 1)
		assert.Equal(t, "manager-node", resp[0].Node)
		assert.Equal(t, "n1", resp[0].ID)
		assert.Equal(t, "10.0.0.1", resp[0].IP)
	})
}

func TestUnitCatService_ErrorPaths(t *testing.T) {
	ctx := context.Background()

	t.Run("Indices server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewCatService(restyClient(srv), testLogger())
		_, err := s.Indices(ctx, nil)
		require.Error(t, err)
	})

	t.Run("Indices unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewCatService(restyClient(srv), testLogger())
		_, err := s.Indices(ctx, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("Shards server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewCatService(restyClient(srv), testLogger())
		_, err := s.Shards(ctx, nil)
		require.Error(t, err)
	})

	t.Run("Shards unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewCatService(restyClient(srv), testLogger())
		_, err := s.Shards(ctx, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("Aliases server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewCatService(restyClient(srv), testLogger())
		_, err := s.Aliases(ctx, nil)
		require.Error(t, err)
	})

	t.Run("Aliases unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewCatService(restyClient(srv), testLogger())
		_, err := s.Aliases(ctx, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("Health server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewCatService(restyClient(srv), testLogger())
		_, err := s.Health(ctx)
		require.Error(t, err)
	})

	t.Run("Health unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewCatService(restyClient(srv), testLogger())
		_, err := s.Health(ctx)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("Count server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewCatService(restyClient(srv), testLogger())
		_, err := s.Count(ctx, nil)
		require.Error(t, err)
	})

	t.Run("Count unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewCatService(restyClient(srv), testLogger())
		_, err := s.Count(ctx, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("Allocation server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewCatService(restyClient(srv), testLogger())
		_, err := s.Allocation(ctx, nil)
		require.Error(t, err)
	})

	t.Run("Allocation unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewCatService(restyClient(srv), testLogger())
		_, err := s.Allocation(ctx, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("Fielddata server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewCatService(restyClient(srv), testLogger())
		_, err := s.Fielddata(ctx, nil)
		require.Error(t, err)
	})

	t.Run("Fielddata unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewCatService(restyClient(srv), testLogger())
		_, err := s.Fielddata(ctx, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("Snapshots server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewCatService(restyClient(srv), testLogger())
		_, err := s.Snapshots(ctx, "")
		require.Error(t, err)
	})

	t.Run("Snapshots unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewCatService(restyClient(srv), testLogger())
		_, err := s.Snapshots(ctx, "")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("Master server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewCatService(restyClient(srv), testLogger())
		_, err := s.Master(ctx)
		require.Error(t, err)
	})

	t.Run("Master unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewCatService(restyClient(srv), testLogger())
		_, err := s.Master(ctx)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("Help server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewCatService(restyClient(srv), testLogger())
		_, err := s.Help(ctx)
		require.Error(t, err)
	})

	t.Run("NodeAttrs server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewCatService(restyClient(srv), testLogger())
		_, err := s.NodeAttrs(ctx)
		require.Error(t, err)
	})

	t.Run("NodeAttrs unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewCatService(restyClient(srv), testLogger())
		_, err := s.NodeAttrs(ctx)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("CatNodes server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewCatService(restyClient(srv), testLogger())
		_, err := s.CatNodes(ctx)
		require.Error(t, err)
	})

	t.Run("CatNodes unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewCatService(restyClient(srv), testLogger())
		_, err := s.CatNodes(ctx)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("CatPendingTasks server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewCatService(restyClient(srv), testLogger())
		_, err := s.CatPendingTasks(ctx)
		require.Error(t, err)
	})

	t.Run("CatPendingTasks unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewCatService(restyClient(srv), testLogger())
		_, err := s.CatPendingTasks(ctx)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("Plugins server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewCatService(restyClient(srv), testLogger())
		_, err := s.Plugins(ctx)
		require.Error(t, err)
	})

	t.Run("Plugins unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewCatService(restyClient(srv), testLogger())
		_, err := s.Plugins(ctx)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("CatRecovery server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewCatService(restyClient(srv), testLogger())
		_, err := s.CatRecovery(ctx, nil)
		require.Error(t, err)
	})

	t.Run("CatRecovery unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewCatService(restyClient(srv), testLogger())
		_, err := s.CatRecovery(ctx, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("Repositories server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewCatService(restyClient(srv), testLogger())
		_, err := s.Repositories(ctx)
		require.Error(t, err)
	})

	t.Run("Repositories unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewCatService(restyClient(srv), testLogger())
		_, err := s.Repositories(ctx)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("Segments server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewCatService(restyClient(srv), testLogger())
		_, err := s.Segments(ctx, nil)
		require.Error(t, err)
	})

	t.Run("Segments unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewCatService(restyClient(srv), testLogger())
		_, err := s.Segments(ctx, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("SegmentReplication server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewCatService(restyClient(srv), testLogger())
		_, err := s.SegmentReplication(ctx, nil)
		require.Error(t, err)
	})

	t.Run("SegmentReplication unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewCatService(restyClient(srv), testLogger())
		_, err := s.SegmentReplication(ctx, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("CatTasks server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewCatService(restyClient(srv), testLogger())
		_, err := s.CatTasks(ctx)
		require.Error(t, err)
	})

	t.Run("CatTasks unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewCatService(restyClient(srv), testLogger())
		_, err := s.CatTasks(ctx)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("Templates server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewCatService(restyClient(srv), testLogger())
		_, err := s.Templates(ctx, "")
		require.Error(t, err)
	})

	t.Run("Templates unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewCatService(restyClient(srv), testLogger())
		_, err := s.Templates(ctx, "")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("ThreadPool server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewCatService(restyClient(srv), testLogger())
		_, err := s.ThreadPool(ctx, nil)
		require.Error(t, err)
	})

	t.Run("ThreadPool unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewCatService(restyClient(srv), testLogger())
		_, err := s.ThreadPool(ctx, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})

	t.Run("ClusterManager server error", func(t *testing.T) {
		srv := errServer(500)
		defer srv.Close()
		s := NewCatService(restyClient(srv), testLogger())
		_, err := s.ClusterManager(ctx)
		require.Error(t, err)
	})

	t.Run("ClusterManager unmarshal error", func(t *testing.T) {
		srv := badJSONServer()
		defer srv.Close()
		s := NewCatService(restyClient(srv), testLogger())
		_, err := s.ClusterManager(ctx)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal")
	})
}

func TestUnitCatService_NetworkErrors(t *testing.T) {
	ctx := context.Background()
	s := NewCatService(deadClient(), testLogger())

	t.Run("Indices network error", func(t *testing.T) {
		_, err := s.Indices(ctx, nil)
		require.Error(t, err)
	})

	t.Run("Shards network error", func(t *testing.T) {
		_, err := s.Shards(ctx, nil)
		require.Error(t, err)
	})

	t.Run("Aliases network error", func(t *testing.T) {
		_, err := s.Aliases(ctx, nil)
		require.Error(t, err)
	})

	t.Run("Health network error", func(t *testing.T) {
		_, err := s.Health(ctx)
		require.Error(t, err)
	})

	t.Run("Count network error", func(t *testing.T) {
		_, err := s.Count(ctx, nil)
		require.Error(t, err)
	})

	t.Run("Allocation network error", func(t *testing.T) {
		_, err := s.Allocation(ctx, nil)
		require.Error(t, err)
	})

	t.Run("Fielddata network error", func(t *testing.T) {
		_, err := s.Fielddata(ctx, nil)
		require.Error(t, err)
	})

	t.Run("Snapshots network error", func(t *testing.T) {
		_, err := s.Snapshots(ctx, "")
		require.Error(t, err)
	})

	t.Run("Master network error", func(t *testing.T) {
		_, err := s.Master(ctx)
		require.Error(t, err)
	})

	t.Run("Help network error", func(t *testing.T) {
		_, err := s.Help(ctx)
		require.Error(t, err)
	})

	t.Run("NodeAttrs network error", func(t *testing.T) {
		_, err := s.NodeAttrs(ctx)
		require.Error(t, err)
	})

	t.Run("CatNodes network error", func(t *testing.T) {
		_, err := s.CatNodes(ctx)
		require.Error(t, err)
	})

	t.Run("CatPendingTasks network error", func(t *testing.T) {
		_, err := s.CatPendingTasks(ctx)
		require.Error(t, err)
	})

	t.Run("Plugins network error", func(t *testing.T) {
		_, err := s.Plugins(ctx)
		require.Error(t, err)
	})

	t.Run("CatRecovery network error", func(t *testing.T) {
		_, err := s.CatRecovery(ctx, nil)
		require.Error(t, err)
	})

	t.Run("Repositories network error", func(t *testing.T) {
		_, err := s.Repositories(ctx)
		require.Error(t, err)
	})

	t.Run("Segments network error", func(t *testing.T) {
		_, err := s.Segments(ctx, nil)
		require.Error(t, err)
	})

	t.Run("SegmentReplication network error", func(t *testing.T) {
		_, err := s.SegmentReplication(ctx, nil)
		require.Error(t, err)
	})

	t.Run("CatTasks network error", func(t *testing.T) {
		_, err := s.CatTasks(ctx)
		require.Error(t, err)
	})

	t.Run("Templates network error", func(t *testing.T) {
		_, err := s.Templates(ctx, "")
		require.Error(t, err)
	})

	t.Run("ThreadPool network error", func(t *testing.T) {
		_, err := s.ThreadPool(ctx, nil)
		require.Error(t, err)
	})

	t.Run("ClusterManager network error", func(t *testing.T) {
		_, err := s.ClusterManager(ctx)
		require.Error(t, err)
	})
}
