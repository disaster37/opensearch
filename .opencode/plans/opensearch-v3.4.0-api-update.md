# OpenSearch 3.4.0 → 3.8.0 API Update — Implementation Plan

Target: `github.com/disaster37/opensearch/v4` (Go 1.26+), tracking OpenSearch releases
**3.4.0, 3.5.0, 3.6.0, 3.7.0, 3.8.0** (latest = 3.8.0, published 2026-08-05).

---

## 1. Goal & Scope

1. Add every REST endpoint that was **added** in OpenSearch 3.4.0–3.8.0 and is missing
   from this client.
2. Fix/extend client surfaces affected by **behavior or parameter changes** in those releases.
3. Keep the layered architecture (`types/` → `querydsl/` → `api/` → root) and all
   CONTRIBUTING.md conventions (5 runtime deps, testify only, 100% coverage, httptest
   unit tests, `//go:build integration` tests).

Out of scope: gRPC/Arrow transport, server settings with no REST surface, plugin repos
not bundled with the release distribution (see §9 for investigated-and-excluded items).

---

## 2. Discovery Method & Sources

This planning environment has **no shell/Bash tool**, so `git clone` of
`opensearch-project/OpenSearch`, `opensearch-api-specification`, and
`documentation-website` could not be executed. Equivalent evidence was gathered via
GitHub REST/raw endpoints (release tags, PR file diffs, source at tag `3.8.0`):

- Release notes: `api.github.com/repos/opensearch-project/OpenSearch/releases/tags/{3.4.0…3.8.0}` (all five fetched in full).
- REST handler + action sources at tag `3.8.0` (raw.githubusercontent.com), e.g.
  `RestModifyDataStreamsAction.java`, `RestGetTieringStatusAction.java`,
  `RestListTieringStatusAction.java`, `RestPruneBlockCacheAction.java`,
  `TieringStatus.java`, `PruneBlockCacheResponse.java`.
- PR diffs for: #22487 (modify data stream), #21220/#21295 (tiering), #21705
  (block cache prune + file cache stats), #20606 (bitmap64), #20472
  (multivalue_doc_count), #19798 (X-Request-Id), #21660 (analytics ppl explain),
  #21637 (native memory stats), #21559 (hunspell hot-reload).
- `opensearch-api-specification` repo tree + `spec/namespaces/ingestion.yaml`,
  `spec/schemas/ingestion._common.yaml`, `tests/default/ism/refresh_search_analyzers.yaml`.
- rest-api-spec at tag 3.8.0: `indices.modify_data_stream.json` (confirms path/params).

Every endpoint below was verified against **merged source at tag 3.8.0**, not just
release-note text. Where release notes and merged code disagreed, merged code wins
(see PruneBlockCache `cache` param note in §3.4).

---

## 3. Per-Release Findings (REST-relevant)

### 3.1 OpenSearch 3.4.0 (2025-12)

No new REST endpoints. Changes are internal (gRPC query coverage, filter-rewrite
aggregation optimizations, stats-class builder refactors, `index.creation_date`
settable at create/restore, `error_trace` on bulk). All request bodies are passed as
`any` by this client and generic params are unaffected → **no client change**.

### 3.2 OpenSearch 3.5.0 (2026-02)

| Change | Evidence | Client impact |
|---|---|---|
| `X-Request-Id` header to uniquely identify a search request (+ `traceparent` propagation, slow-log `request-id`) | PR #19798, issue #18512 | **New**: optional request header support on Search (§6 T6) |
| `hyperloglog` (hll) field mapper for cardinality rollups | PR #20129 | Mapping JSON passed as `any` → no change |
| HTTP/3 server-side support | PR #20017 | Server transport only → no change |

### 3.3 OpenSearch 3.6.0 (2026-04)

| Change | Evidence | Client impact |
|---|---|---|
| **bitmap64**: `terms` query accepts `"value_type": "bitmap"` for `long` fields (base64 Roaring64 portable encoding); terms-lookup variant supports `"store": true` on `binary` fields | PR #20606, rest test `search/381_bitmap_filtering_long.yml` | **New**: `TermsQuery.ValueType` + `TermsLookup.Store` in querydsl (§6 T1) |
| Pull-based ingestion promoted to **public API** (experimental tag removed). Endpoints (x-version-added 3.1): `POST /{index}/ingestion/_pause`, `POST /{index}/ingestion/_resume`, `GET /{index}/ingestion/_state` | PR #20704; api-spec `spec/namespaces/ingestion.yaml` | **New**: `Ingestion` service (§6 T8) |
| `X-Request-Id` format restrictions removed, max length configurable | PR #21048 | Client must not validate format (T6) |
| `search_settings` on WLM workload groups | PR #20536 | WLM service absent from client; WLM predates 3.4.0 → excluded (§9) |

### 3.4 OpenSearch 3.7.0 (2026-06)

| Change | Evidence | Client impact |
|---|---|---|
| **Tiering APIs** (WritableWarm, gated by `WRITABLE_WARM_INDEX_EXPERIMENTAL_FLAG`): `GET /{index}/_tier?detailed=` ; `GET /_tier/all?target=_hot\|_warm` (cat-style); `POST /{index}/_tier/warm`; `POST /{index}/_tier/hot`; `POST /_tier/_cancel/{index}` | PRs #21220, #21295; sources `RestGetTieringStatusAction.java`, `RestListTieringStatusAction.java`, `RestBaseTierAction.java`, `RestCancelTierAction.java`, `TieringStatus.java` at 3.8.0 | **New**: `Tiering` service (§6 T4) |
| **Block cache prune**: `POST /_blockcache/prune` with `nodes`, `timeout` params | PR #21705; merged `RestPruneBlockCacheAction.java` at 3.8.0. Note: dev notes mentioned `?cache=disk`, but the **merged** handler only accepts `nodes` + `timeout` and prunes all registered block caches | **New**: `Cluster().PruneBlockCache` (§6 T5) |
| Node stats: `GET _nodes/stats/file_cache?detailed=true` exposes `aggregate_file_cache` incl. `block_cache` sub-stats on warm nodes | PR #21705 (`BlockCacheStatsIT`) | **New**: `Detailed` param on `NodesStatsRequest` + `FileCache` model field (§6 T7) |
| Node stats: `native_memory` section (`total_estimated_bytes`, `analytics_backend.{allocated_bytes,resident_bytes}`) + task cancellation stats | PR #21637 (`AnalyticsBackendNativeMemoryStatsXContentTests` verifies exact JSON keys) | **New**: `NativeMemory` model field (§6 T7) |
| `/_analytics/ppl` + `/_analytics/ppl/_explain` (DataFusion analytics engine) | PR #21660 — handler lives in `sandbox/plugins/test-ppl-frontend` | **Excluded**: sandbox plugins are not shipped in release distributions (§9) |
| Hunspell dictionary hot-reload via `_refresh_search_analyzers` | PR #21559 extends the reload mechanism; endpoint `POST /_plugins/_refresh_search_analyzers/{index}` (api-spec test `tests/default/ism/refresh_search_analyzers.yaml`) is registered by the ISM/index-management plugin and predates 3.4.0 | **Optional gap fix**: `ISM().RefreshSearchAnalyzers` (§6 T9) |

### 3.5 OpenSearch 3.8.0 (2026-08)

| Change | Evidence | Client impact |
|---|---|---|
| **`POST /_data_stream/_modify`** — add/remove backing indices of a data stream (experimental). Params: `cluster_manager_timeout`, `timeout`. Body (required): `{"actions":[{"add_backing_index"\|"remove_backing_index":{"data_stream":"…","index":"…"}}]}`. Response: `{"acknowledged":true}` | PR #22487; `rest-api-spec/api/indices.modify_data_stream.json`; `RestModifyDataStreamsAction.parseActions` at 3.8.0 | **New**: `Indices().ModifyDataStream` (§6 T3) |
| **`multivalue_doc_count` aggregation** — counts docs with ≥2 values on a field. DSL like `value_count`; supports `field`, `script`, `missing` (no `format`); result serialized as `InternalValueCount` (`{"value":N}`) | PR #20472; `MultiValueDocCountAggregationBuilder` (`declareFields(PARSER, true, true, false)`, `.addResultReader(InternalValueCount::new)`) | **New**: querydsl builder + `Aggregations` accessor (§6 T2) |
| `OpenSearchTimeoutException` returns **HTTP 504** instead of 500 | PR #22064 | Behavior verification + regression tests; no code change (§6 T10) |
| Malformed mappings on create index → 400 instead of 500 | PR #22371 | Surfaces as `OpenSearchError` status 400; no change |
| Mustache partial resolution disabled in search templates; stricter `scroll_id` validation; rollover `checkBlock` scoped to write index | PRs #22438, #22396, #21838 | Server-side semantics; bodies/params unchanged → no change (documented in godoc of affected methods) |
| `ingestion-hive` plugin; HTTP/3 client via JDK HttpClient; delayed-allocation cluster default | release notes | No REST surface for this client |

---

## 4. Gap Analysis (existing client vs. findings)

Existing surface verified by reading all 24 services (~250 methods). Relevant gaps:

| # | Finding (§3) | Existing client state | Verdict |
|---|---|---|---|
| G1 | `POST /_data_stream/_modify` (3.8.0) | `IndicesService` has Create/Get/Delete/DataStreamsStats only | **Add method** |
| G2 | Tiering API family (3.7.0) | No tiering methods anywhere | **Add service** |
| G3 | `POST /_blockcache/prune` (3.7.0) | Not present (no filecache prune either — pre-existing gap, out of scope) | **Add method** |
| G4 | `multivalue_doc_count` agg (3.8.0) | Not in querydsl | **Add builder + accessor** |
| G5 | `terms` `value_type` / lookup `store` (3.6.0 bitmap64) | `TermsQuery` has no ValueType; `TermsLookup` has no Store | **Extend builders** |
| G6 | `X-Request-Id` (3.5.0) | No header support on `SearchRequest` | **Extend request** |
| G7 | nodes stats `detailed` param + `native_memory`/`file_cache` (3.7.0) | `NodesStatsRequest{NodeIds,Metrics}` only; model lacks both fields | **Extend request + model** |
| G8 | Ingestion pause/resume/state (GA in 3.6.0) | No ingestion service | **Add service** |
| G9 | `_refresh_search_analyzers` (extended 3.7.0) | Not present | **Add method (P2)** |
| G10 | 504-on-timeout (3.8.0) | `DefaultRetryConditions` already retries 5xx≠501 (incl. 504); `OpenSearchError.StatusCode()` exposes it | **Tests only** |

No existing method signatures break; all additions are backward compatible.

---

## 5. Design Decisions

1. **Tiering = new service** (`api/tiering_service.go` + `api/tiering_model.go`,
   accessor `Client.Tiering()`). It is a distinct core API group (`/_tier/...`,
   `/{index}/_tier/...`) and matches the "new API group = new pair of files + accessor"
   rule in CONTRIBUTING.md.
2. **Ingestion = new service** (`api/ingestion_service.go` + `api/ingestion_model.go`,
   accessor `Client.Ingestion()`). Endpoints predate 3.4.0 but became a **public,
   non-experimental API within the window** (3.6.0 #20704), and the api-spec repo
   defines them formally.
3. **PruneBlockCache goes in `ClusterService`** (cluster-admin operation,
   `cluster:admin/blockcache/prune`), not NodesService.
4. **ModifyDataStream goes in `IndicesService`** next to the other data-stream methods.
5. **`X-Request-Id` is request-scoped on `SearchRequest` only** (the server feature
   targets search requests per #19798). Implemented as an HTTP header via resty
   `SetHeader`; no format validation (server ≥3.6.0 accepts arbitrary strings up to a
   configurable max length; older servers accept 32-char hex).
6. **Loose typing where server shape is plugin/flag dependent**: `NodesStatsNode.FileCache`
   is `map[string]any` (contents vary with `detailed`, warm role, and block-cache
   plugins). `native_memory` is fully typed (shape verified from server XContent tests).
   Task-cancellation node-stats sub-fields are left untyped (Go decoder ignores unknown
   fields; no verified shape needed for correctness).
7. **Feature-flagged/experimental endpoints** (tiering, modify data stream): implemented
   normally; integration tests degrade to `t.Skip` when the cluster lacks the feature.
8. **No new dependencies.** Bitmap64 integration tests use hardcoded base64 Roaring
   bitmaps copied from OpenSearch's own yaml rest tests — no roaring library added.
9. Priority: P1 = G1–G7, G10; P2 = G8, G9.

---

## 6. Implementation Tasks (ordered)

### T1 — querydsl: bitmap64 support in terms query (G5, 3.6.0) — P1

Files: `querydsl/search_queries_terms.go`, `querydsl/search_terms_lookup.go`,
tests in `querydsl/common_test.go` (or `search_queries_builder_test.go`).

1. `TermsQuery`: add field `ValueType string` and setter:

```go
// WithValueType sets the value type hint for the terms values, e.g. "bitmap"
// to interpret values as base64-encoded serialized Roaring bitmaps
// (32-bit for integer fields, 64-bit for long fields since OpenSearch 3.6.0).
func (q *TermsQuery) WithValueType(valueType string) *TermsQuery {
	q.ValueType = valueType
	return q
}
```

In `Source()`, emit `params["value_type"] = q.ValueType` when non-empty — in **both**
the lookup branch and the values branch (bitmap lookup uses
`{"terms":{"field":{"index":...,"id":...,"path":...,"store":true},"value_type":"bitmap"}}`).

2. `TermsLookup`: add unexported `store *bool`, setter, and Source emission:

```go
// Store indicates the lookup path points at a stored binary field
// (used with bitmap value_type since OpenSearch 3.6.0).
func (t *TermsLookup) Store(store bool) *TermsLookup {
	t.store = &store
	return t
}
// in Source(): if t.store != nil { src["store"] = *t.store }
```

Unit tests: table-driven `Source()` assertions for (a) values + value_type,
(b) lookup + store:true + value_type, (c) no value_type → key absent.

Integration test (in `api/search_service_integration_test.go`,
`TestSearchService_TermsBitmap64`): create `test-search-bitmap64` index mapping
`employee_id: long`; index docs `{1: 1000000000001, 2: 2000000000002, 3: 3000000000003}`;
search with `NewTermsQuery("employee_id", "AgAAAAAAAADoAAAAOjAAAAEAAACl1AAAEAAAAAEQ0QEAADowAAABAAAASqkAABAAAAACIA==").WithValueType("bitmap")`
(constant copied verbatim from OpenSearch rest test `381_bitmap_filtering_long.yml`;
it encodes {1000000000001, 2000000000002}); assert 2 hits (ids 1, 2). Register
`t.Cleanup` index delete.

### T2 — querydsl: multivalue_doc_count aggregation (G4, 3.8.0) — P1

Files: new `querydsl/search_aggs_metrics_multivalue_doc_count.go`,
`querydsl/search_aggs.go` (accessor), tests in
`querydsl/search_aggs_builders_test.go`, integration in `api/search_service_integration_test.go`.

```go
// MultiValueDocCountAggregation counts documents that hold two or more values
// for the aggregated field (added in OpenSearch 3.8.0).
//
// JSON output shape:
//
//	{"multivalue_doc_count": {"field": "field_name"}}
type MultiValueDocCountAggregation struct {
	Field   string
	Script  *Script
	Missing any
	SubAggs map[string]Aggregation
	Meta    map[string]any
}

func NewMultiValueDocCountAggregation() *MultiValueDocCountAggregation {
	return &MultiValueDocCountAggregation{}
}
// WithField / WithScript / WithMissing / WithSubAggs / WithMeta — pointer receivers,
// same style as ValueCountAggregation.

func (a MultiValueDocCountAggregation) Source() (any, error) {
	body := map[string]any{}
	if a.Field != "" {
		body["field"] = a.Field
	}
	if a.Missing != nil {
		body["missing"] = a.Missing
	}
	return sourceAgg("multivalue_doc_count", body, a.SubAggs, a.Meta, a.Script)
}
```

No `Format` field: server parser declares `(script=true, missing=true, format=false)`.

Accessor in `search_aggs.go` (result is `InternalValueCount`, identical to value_count):

```go
// MultiValueDocCount returns the multivalue_doc_count metric result named name.
func (a Aggregations) MultiValueDocCount(name string) (*AggregationValueMetric, bool) {
	// identical body to ValueCount(): unmarshal raw into AggregationValueMetric
}
```

Unit tests: Source() with field only / field+missing / script / sub-aggs+meta
(assert wrapped key `multivalue_doc_count`); accessor test unmarshaling `{"value":2}`.

Integration test `TestSearchService_MultiValueDocCount`: index
`test-search-mvdc` with keyword field `tags`; docs: `["a"]`, `["a","b"]`, `["a","b","c"]`;
search with agg `NewMultiValueDocCountAggregation().WithField("tags")`;
`res.Aggregations.MultiValueDocCount("mv")` → `Value == 2`.

### T3 — api: Indices.ModifyDataStream (G1, 3.8.0) — P1

Files: `api/param_types.go`, `api/options.go`, `api/indices_service.go`,
tests `api/indices_service_test.go`, `api/indices_service_integration_test.go`.

```go
// param_types.go
// DataStreamActionType identifies a modify action for POST /_data_stream/_modify.
type DataStreamActionType string

const (
	DataStreamActionAddBackingIndex    DataStreamActionType = "add_backing_index"
	DataStreamActionRemoveBackingIndex DataStreamActionType = "remove_backing_index"
)

// options.go
type ModifyDataStreamAction struct {
	Type       DataStreamActionType `validate:"required,oneof=add_backing_index remove_backing_index"`
	DataStream string               `validate:"required"`
	Index      string               `validate:"required"`
}

type ModifyDataStreamParams struct {
	ClusterManagerTimeout string
	Timeout               string
}

func (p *ModifyDataStreamParams) ToMap() map[string]string {
	if p == nil {
		return nil
	}
	m := make(map[string]string)
	if p.ClusterManagerTimeout != "" {
		m["cluster_manager_timeout"] = p.ClusterManagerTimeout
	}
	if p.Timeout != "" {
		m["timeout"] = p.Timeout
	}
	if len(m) == 0 {
		return nil
	}
	return m
}

type ModifyDataStreamRequest struct {
	Actions []*ModifyDataStreamAction `validate:"required,min=1,dive"`
	Params  *ModifyDataStreamParams
}

func (r *ModifyDataStreamRequest) Validate() error {
	return validationError(validate.Struct(r))
}
```

Interface + impl (`indices_service.go`), placed beside the other data-stream methods:

```go
// ModifyDataStream adds or removes backing indices of a data stream via
// metadata-only actions (POST /_data_stream/_modify, experimental, OpenSearch 3.8.0+).
ModifyDataStream(ctx context.Context, req *ModifyDataStreamRequest) (*types.AcknowledgedResponse, error)
```

Implementation: `req.Validate()`; build body
`map[string]any{"actions": []map[string]any{...}}` where each element is
`{string(a.Type): {"data_stream": a.DataStream, "index": a.Index}}`;
`POST /_data_stream/_modify` with `req.Params.ToMap()` query params; standard
three-helper error handling; unmarshal into `types.AcknowledgedResponse`.

Unit tests (`indices_service_test.go`): success (assert path, method, JSON body
shape incl. both action types, params); validation failures (nil request, empty
Actions, bad Type, empty DataStream/Index); HTTP 500 branch; network error branch
via `deadClient()`.

Integration test `TestIndicesService_ModifyDataStream`: put composable index
template matching `test-idx-svc-modify-*` with data_stream `{"timestamp_field":{"name":"@timestamp"}}`;
create data stream `test-idx-svc-modify-ds`; rollover twice (3 backing indices);
`ModifyDataStream` removeBackingIndex(gen-1 name `.ds-<ds>-000001`) → ack; verify
`GetDataStream` shows 2 backing indices and the detached index still `Exists`;
re-add it → ack; verify 3 backing indices in original order. `t.Cleanup`: delete
data stream + template.

### T4 — api: new Tiering service (G2, 3.7.0) — P1

New files: `api/tiering_service.go`, `api/tiering_model.go`; additions to
`api/param_types.go`, `api/options.go`, `client.go`, `generate.go`; tests
`api/tiering_service_test.go`, `api/tiering_service_integration_test.go`.

Endpoints (all verified against 3.8.0 sources; all gated server-side by the
`writable_warm_index` feature flag):

| Method | Route | Params | Response |
|---|---|---|---|
| GetStatus | `GET /{index}/_tier` | `detailed` (bool) | `TieringStatusResponse` |
| ListStatus | `GET /_tier/all` | `target` (`_hot`\|`_warm`), cat params; client forces `format=json` | `[]TieringListEntry` |
| HotToWarm | `POST /{index}/_tier/warm` | `timeout`, `cluster_manager_timeout` | `types.AcknowledgedResponse` |
| WarmToHot | `POST /{index}/_tier/hot` | same | `types.AcknowledgedResponse` |
| Cancel | `POST /_tier/_cancel/{index}` | same | `types.AcknowledgedResponse` |

```go
// param_types.go
// TierTarget selects the destination tier filter for TieringService.ListStatus.
type TierTarget string

const (
	TierTargetHot  TierTarget = "_hot"
	TierTargetWarm TierTarget = "_warm"
)

// options.go
type TierOperationParams struct {
	ClusterManagerTimeout string
	Timeout               string
}
func (p *TierOperationParams) ToMap() map[string]string // same pattern as T3

// tiering_model.go
type TieringStatusResponse struct {
	TieringStatus *TieringStatus `json:"tiering_status"`
}

type TieringStatus struct {
	Index            string                   `json:"index"`
	State            string                   `json:"state"`
	Source           string                   `json:"source"`
	Target           string                   `json:"target"`
	StartTime        int64                    `json:"start_time"`
	ShardLevelStatus *TieringShardLevelStatus `json:"shard_level_status"`
}

type TieringShardLevelStatus struct {
	Pending               int                   `json:"pending"`
	Running               int                   `json:"running"`
	Succeeded             int                   `json:"succeeded"`
	Total                 int                   `json:"total"`
	ShardRelocationStatus []TieringOngoingShard `json:"shard_relocation_status"`
}

type TieringOngoingShard struct {
	SourceShardId    int    `json:"source_shard_id"`
	RelocatingNodeId string `json:"relocating_node_id"`
}

// TieringListEntry is one row of GET /_tier/all (format=json).
type TieringListEntry struct {
	Index  string `json:"index"`
	State  string `json:"state"`
	Source string `json:"source"`
	Target string `json:"target"`
}

// tiering_service.go
type TieringService interface {
	GetStatus(ctx context.Context, index string, detailed bool) (*TieringStatusResponse, error)
	ListStatus(ctx context.Context, target TierTarget) ([]TieringListEntry, error)
	HotToWarm(ctx context.Context, index string, params ...*TierOperationParams) (*types.AcknowledgedResponse, error)
	WarmToHot(ctx context.Context, index string, params ...*TierOperationParams) (*types.AcknowledgedResponse, error)
	Cancel(ctx context.Context, index string, params ...*TierOperationParams) (*types.AcknowledgedResponse, error)
}
```

Implementation notes:
- Constructor `NewTieringService(client, logger)` scopes `logger.WithField("service", "tiering")`.
- `GetStatus`: `index == ""` → `fmt.Errorf("index is required")`; if detailed,
  `SetQueryParam("detailed", "true")`; GET `fmt.Sprintf("/%s/_tier", index)`.
- `ListStatus`: validate `target ∈ {"", TierTargetHot, TierTargetWarm}` else
  `fmt.Errorf`; always `format=json`; optional `target` param; GET `/_tier/all`;
  unmarshal into `[]TieringListEntry`.
- HotToWarm/WarmToHot/Cancel: index required check; variadic params (use first if
  len>0); POST to the routes above; `types.AcknowledgedResponse`.
- All methods use `wrapNetworkError` / `logAndReturnError` / `wrapUnmarshalError`.

Wiring:
- `client.go`: add `Tiering() api.TieringService` to `Client` interface; `tiering`
  field on `DefaultClient`; `tiering: api.NewTieringService(c, logger)` in `New`;
  accessor `func (c *DefaultClient) Tiering() api.TieringService { return c.tiering }`.
- `generate.go`: add
  `//go:generate mockgen -source=api/tiering_service.go -destination=mock/tiering_service.go -package=mock`.

Unit tests: per method — success (assert exact path/method/params and parsed model,
including a `shard_level_status` payload with relocation entries), empty-index
validation error, invalid target error, HTTP 500 branch, network error branch.

Integration test (`//go:build integration`, package `api_test`):
`TestTieringService_ListStatus` calls `ListStatus(ctx, "")`; on
`*types.OpenSearchError` (flag disabled → typically 400/404) → `t.Skip("tiering API
not enabled on this cluster")`; on success assert non-nil slice. Remaining tiering
operations are skipped unless ListStatus succeeded (single guard at top of each test).

### T5 — api: Cluster.PruneBlockCache (G3, 3.7.0) — P1

Files: `api/options.go`, `api/cluster_service.go`, `api/cluster_model.go`, tests
`api/cluster_service_test.go`, `api/cluster_service_integration_test.go`.

```go
// options.go
type PruneBlockCacheParams struct {
	Nodes   []string // restrict to specific warm node IDs
	Timeout string
}

func (p *PruneBlockCacheParams) ToMap() map[string]string {
	if p == nil {
		return nil
	}
	m := make(map[string]string)
	if len(p.Nodes) > 0 {
		m["nodes"] = strings.Join(p.Nodes, ",")
	}
	if p.Timeout != "" {
		m["timeout"] = p.Timeout
	}
	if len(m) == 0 {
		return nil
	}
	return m
}

// cluster_model.go
type PruneBlockCacheResponse struct {
	Acknowledged bool                            `json:"acknowledged"`
	Summary      *PruneBlockCacheSummary         `json:"summary"`
	Nodes        map[string]*NodePruneBlockCache `json:"nodes"`
	Failures     []PruneBlockCacheFailure        `json:"failures"`
}

type PruneBlockCacheSummary struct {
	TotalNodesTargeted int `json:"total_nodes_targeted"`
	SuccessfulNodes    int `json:"successful_nodes"`
	FailedNodes        int `json:"failed_nodes"`
}

type NodePruneBlockCache struct {
	Name    string `json:"name"`
	Cleared bool   `json:"cleared"`
}

type PruneBlockCacheFailure struct {
	NodeId string `json:"node_id"`
	Reason string `json:"reason"`
}

// cluster_service.go — new interface method + impl
PruneBlockCache(ctx context.Context, params ...*PruneBlockCacheParams) (*PruneBlockCacheResponse, error)
```

Impl: `POST /_blockcache/prune`; apply first variadic params via `ToMap()`;
standard error helpers. (Merged server handler accepts only `nodes` + `timeout`;
there is **no** `cache` query param in 3.7.0/3.8.0 releases.)

Unit tests: success (full response incl. failures array), params (nodes join),
HTTP 500, network error.

Integration test `TestClusterService_PruneBlockCache`: call with no params; assert
`Acknowledged == true` and `Summary` non-nil (on a cluster without warm nodes
`TotalNodesTargeted == 0`). Works on the standard Dagger two-node cluster.

### T6 — api: X-Request-Id on Search (G6, 3.5.0) — P1

Files: `api/options.go`, `api/search_service.go`, tests
`api/search_service_test.go`, `api/search_service_integration_test.go`.

- `SearchRequest`: add `RequestId string` (godoc: sent as the `X-Request-Id` HTTP
  header; OpenSearch ≥3.5.0 records it in slow logs/tasks; ≥3.6.0 accepts arbitrary
  values up to `search.request_id.max_length` — no client-side validation).
- `DefaultSearchService.Search`: after building `r`, add

```go
if req.RequestId != "" {
	r.SetHeader("X-Request-Id", req.RequestId)
}
```

Unit test: httptest handler asserts `r.Header.Get("X-Request-Id")` equals the value;
second subtest asserts header absent when RequestId empty.

Integration test `TestSearchService_SearchWithRequestId`: search existing index with
`RequestId: "4bf92f3577b34da6a3ce929d0e0e4736"`; assert no error and valid result.

### T7 — api: nodes stats `detailed` + new model fields (G7, 3.7.0) — P1

Files: `api/options.go`, `api/nodes_service.go`, `api/nodes_model.go`, tests
`api/nodes_service_test.go`, `api/nodes_service_integration_test.go`.

- `NodesStatsRequest`: add `Detailed bool` (godoc: requests detailed file-cache
  stats on warm nodes, `?detailed=true`, OpenSearch 3.7.0+).
- `Stats` impl: after path building, `if req.Detailed { r.SetQueryParam("detailed", "true") }`
  (convert `Get(path)` to a request builder variable).
- `NodesStatsNode` model additions:

```go
// FileCache holds warm-node file cache statistics (file_cache metric).
// Shape varies with the detailed flag and loaded block-cache plugins, so it is
// intentionally a loose map (OpenSearch 3.7.0+).
FileCache map[string]any `json:"file_cache"`

// NativeMemory reports native (off-heap) allocator statistics when a native
// bridge is loaded (OpenSearch 3.7.0+).
NativeMemory *NodesStatsNativeMemory `json:"native_memory"`
```

```go
type NodesStatsNativeMemory struct {
	TotalEstimatedBytes int64                           `json:"total_estimated_bytes"`
	AnalyticsBackend    *NodesStatsAnalyticsBackendMem  `json:"analytics_backend"`
}

type NodesStatsAnalyticsBackendMem struct {
	AllocatedBytes int64 `json:"allocated_bytes"`
	ResidentBytes  int64 `json:"resident_bytes"`
}
```

Unit tests: Stats with Detailed=true asserts query param present; Detailed=false
asserts absent; unmarshal test covering a payload with `native_memory` and
`file_cache` keys.

Integration test `TestNodesService_StatsDetailed`: `Stats(ctx, &api.NodesStatsRequest{Metrics: []string{"file_cache"}, Detailed: true})`
→ assert no error (non-warm clusters return empty/absent file_cache; only the call
contract is asserted).

### T8 — api: new Ingestion service (G8, GA in 3.6.0) — P2

New files: `api/ingestion_service.go`, `api/ingestion_model.go`; additions to
`api/param_types.go`, `api/options.go`, `client.go`, `generate.go`; tests
`api/ingestion_service_test.go`, `api/ingestion_service_integration_test.go`.

Endpoints (api-spec `ingestion.yaml`, x-version-added 3.1, experimental tag removed in 3.6.0):

| Method | Route | Params | Body | Response |
|---|---|---|---|---|
| Pause | `POST /{index}/ingestion/_pause` | `cluster_manager_timeout`, `timeout` | none | `IngestionStateResponse` |
| Resume | `POST /{index}/ingestion/_resume` | same | optional `{"reset_settings":[…]}` | `IngestionStateResponse` |
| GetState | `GET /{index}/ingestion/_state` | `timeout`, `size`, `next_token` | none | `GetIngestionStateResponse` |

```go
// param_types.go
// IngestionResetMode selects how a consumer position is reset on resume.
type IngestionResetMode string

const (
	IngestionResetModeOffset    IngestionResetMode = "OFFSET"
	IngestionResetModeTimestamp IngestionResetMode = "TIMESTAMP"
)

// options.go
type IngestionStateParams struct {
	ClusterManagerTimeout string
	Timeout               string
}
func (p *IngestionStateParams) ToMap() map[string]string

type IngestionGetStateParams struct {
	Timeout   string
	Size      *int
	NextToken string
}
func (p *IngestionGetStateParams) ToMap() map[string]string // keys: timeout, size, next_token

type IngestionResetSettings struct {
	Shard int                `json:"shard"`
	Mode  IngestionResetMode `json:"mode"`
	Value string             `json:"value"`
}

type IngestionResumeRequest struct {
	Index         string                    `validate:"required"`
	ResetSettings []*IngestionResetSettings // optional; becomes request body
	Params        *IngestionStateParams
}
func (r *IngestionResumeRequest) Validate() error

type IngestionGetStateRequest struct {
	Index  string `validate:"required"`
	Params *IngestionGetStateParams
}
func (r *IngestionGetStateRequest) Validate() error

// ingestion_model.go
type IngestionStateResponse struct {
	Acknowledged       bool                               `json:"acknowledged"`
	ShardsAcknowledged bool                               `json:"shards_acknowledged"`
	Failures           map[string][]IngestionShardFailure `json:"failures"`
	Error              string                             `json:"error"`
}

type IngestionShardFailure struct {
	Shard int    `json:"shard"`
	Error string `json:"error"`
}

type ShardIngestionState struct {
	Shard             int    `json:"shard"`
	PollerState       string `json:"poller_state"`
	ErrorPolicy       string `json:"error_policy"`
	PollerPaused      bool   `json:"poller_paused"`
	WriteBlockEnabled bool   `json:"write_block_enabled"`
	BatchStartPointer string `json:"batch_start_pointer"`
}

type GetIngestionStateResponse struct {
	IngestionState map[string][]ShardIngestionState `json:"ingestion_state"`
	NextPageToken  string                           `json:"next_page_token"`
	Shards         *types.ShardsInfo                `json:"_shards"`
}

// ingestion_service.go
type IngestionService interface {
	Pause(ctx context.Context, index string, params ...*IngestionStateParams) (*IngestionStateResponse, error)
	Resume(ctx context.Context, req *IngestionResumeRequest) (*IngestionStateResponse, error)
	GetState(ctx context.Context, req *IngestionGetStateRequest) (*GetIngestionStateResponse, error)
}
```

Impl notes: Pause positional (1–2 params rule); Resume builds body
`map[string]any{"reset_settings": …}` only when `len(req.ResetSettings) > 0`;
constructor scopes `service=ingestion`. Wiring identical to T4 (accessor
`Ingestion()`, generate.go mockgen line).

Unit tests: per method success (path/params/body assertions; full response models
including failures map and pagination token), validation errors, HTTP 500, network error.

Integration test (guarded): `TestIngestionService_PauseWithoutIngestion` — create
plain index `test-ingest-svc-plain`, call `Pause`; expect an
`*types.OpenSearchError` (index not configured for pull-based ingestion) and assert
it; if the cluster instead returns 404 (feature fully absent) → `t.Skip`. This
exercises the live endpoint without requiring a Kafka/ingestion-source cluster.

### T9 — api: ISM RefreshSearchAnalyzers (G9) — P2

Files: `api/ism_service.go`, `api/ism_model.go`, tests
`api/ism_service_test.go`, `api/ism_service_integration_test.go`.

```go
// ism_model.go
type RefreshSearchAnalyzersResponse struct {
	SuccessfulRefreshDetails []RefreshSearchAnalyzersDetail `json:"successful_refresh_details"`
}

type RefreshSearchAnalyzersDetail struct {
	Index              string   `json:"index"`
	RefreshedAnalyzers []string `json:"refreshed_analyzers"`
}

// ism_service.go — new method
// RefreshSearchAnalyzers reloads updateable search analyzers (e.g. synonym or
// hunspell dictionaries) for the given index via
// POST /_plugins/_refresh_search_analyzers/{index} (ISM-plugin endpoint;
// hunspell hot-reload supported since OpenSearch 3.7.0).
RefreshSearchAnalyzers(ctx context.Context, index string) (*RefreshSearchAnalyzersResponse, error)
```

Impl: `index == ""` → `fmt.Errorf`; POST; standard helpers.

Unit tests: success (payload from api-spec story:
`{"successful_refresh_details":[{"index":"books","refreshed_analyzers":["movie_titles"]}]}`),
empty-index error, HTTP 500, network error.

Integration test `TestIsmService_RefreshSearchAnalyzers`: call on a plain index;
expect success with zero/empty details; on `*types.OpenSearchError` (plugin absent)
→ `t.Skip`.

### T10 — behavior-change regression tests (G10, 3.8.0) — P1

No production code changes (verified): `DefaultRetryConditions` retries
`status >= 500 && status != 501`, so 504 (new timeout status, PR #22064) is retried
exactly like the old 500 was; `types.OpenSearchError.StatusCode()` exposes 504;
`IsNotFound`/`IsConflict` unaffected.

Add tests:
- Root `client_retry_test.go`: `TestRetryOn504GatewayTimeout` — httptest server
  returns 504 with an OpenSearch error body twice, then 200; client with
  `RetryCount: 3` + `DefaultRetryConditions()` succeeds; assert 3 attempts observed.
- `api/service_error_test.go`: parse-error-response test asserting a 504 payload
  yields `*types.OpenSearchError` with `StatusCode() == 504`.
- Godoc notes (one line each) on `Search().SearchTemplate` (Mustache partials
  disabled server-side in 3.8.0), `Search().Scroll` (stricter scroll_id validation),
  and `Indices().Rollover` (checkBlock scoped to write index) — documentation only.

### T11 — mocks, docs, CONTRIBUTING sync

- Run `go generate ./...` after T3–T9 (mockgen publishes mocks for consumers;
  interface changes in Indices/Cluster/Nodes/Search/Ism + new Tiering/Ingestion
  services).
- `CONTRIBUTING.md`: update the stale "16 service interfaces" wording in the
  Architecture Overview to the new count (26 services) — required by its own
  living-document rule.
- `api/doc.go`: add one-line mentions of Tiering and Ingestion services if the file
  enumerates service groups.
- No `docs/` folder, no new README sections (godoc is the documentation layer).

---

## 7. Edge Cases & Error Handling

1. **ModifyDataStream**: server rejects removing the write index ("because it is the
   write index") and adding an index without `@timestamp` mapped as date — both
   surface as `OpenSearchError` via `logAndReturnError`. Client-side: `Actions`
   must be non-empty; each action needs valid `Type` (enum-checked via
   `oneof`), `DataStream`, `Index`. Unknown body fields are rejected by the server
   ("only [actions] is supported") — client never sends extras.
2. **Tiering**: endpoints 404/400 when `writable_warm_index` flag is off → normal
   `OpenSearchError`; integration tests skip. Server requires exactly one index —
   client signatures take a single `index string`, preventing multi-index misuse.
   `ListStatus` target validated client-side to `""`/`_hot`/`_warm` (server rejects
   other values with 400). `shard_level_status` only present when `detailed=true`
   and relocations are ongoing → pointer field, omitempty-safe decoding.
3. **PruneBlockCache**: clusters without warm nodes return
   `summary.total_nodes_targeted: 0` (success, not an error). Targeting non-warm
   node IDs → server 400. `failures` array only present on partial failure → slice
   decodes to nil otherwise.
4. **X-Request-Id**: pre-3.6.0 servers expect 32-char hex; ≥3.6.0 accept any string
   up to `search.request_id.max_length`. Client performs no validation and documents
   both regimes. Header is per-request (resty `SetHeader` on the request object), so
   it never leaks to other calls.
5. **bitmap64 terms**: values must be base64 of Roaring portable serialization;
   client passes strings through untouched; malformed bitmaps yield server 400
   ("Failed to deserialize the 64-bit bitmap") as `OpenSearchError`. `value_type`
   emitted in both values and lookup branches; `store` only in lookup.
6. **multivalue_doc_count**: field-or-script requirement enforced server-side
   (consistent with existing `ValueCountAggregation`, which also does not
   client-side enforce it). Unmapped fields produce `{"value":0}` (server builds
   empty aggregation) — accessor handles it.
7. **Ingestion**: pause/resume on non-ingestion indices → server error (asserted in
   integration test). `failures` in responses is a per-index map of shard failures —
   modeled exactly. GetState pagination: callers loop with `NextPageToken` until
   empty; `Size` param pointer-typed so 0 is not sent accidentally.
8. **Additive response changes** (native_memory, file_cache, new node-stats
   sub-fields): goccy/go-json ignores unknown fields — older model structs never
   fail decoding against newer servers; verified no `DisallowUnknownFields` usage in
   the repo.
9. **504 timeouts**: retried under `DefaultRetryConditions` (5xx except 501). Users
   with `RetryCount > 0` will transparently retry server-side timeouts starting
   3.8.0 — documented in T10 godoc note on `Config.RetryConditions`.
10. **Nil-safety**: all new `*Params.ToMap()` methods handle nil receiver (existing
    convention); variadic params methods tolerate zero args.

---

## 8. Verification Plan

1. Static/build: `go fmt ./...`, `go vet ./...`, `go build ./...` (Go 1.26 toolchain).
2. Unit: `go test -race ./...` — must pass with zero data races.
3. Coverage: `go test -cover ./...` — `api`, `querydsl`, `types`, root each at 100%
   target (every new method has success + validation + HTTP-error + network-error
   tests; every new `Source()`/`ToMap()`/`Validate()` branch covered).
4. Mocks: `go generate ./...` succeeds; `mock/` output compiles.
5. Integration: `go test -tags=integration -cover ./api/...` against a real
   OpenSearch **3.8.0** cluster (or `dagger call --src . test`, which boots a
   two-node cluster and injects `OPENSEARCH_URL/USERNAME/PASSWORD`). Expected:
   - ModifyDataStream, PruneBlockCache, MultiValueDocCount, TermsBitmap64,
     RequestId, NodesStats detailed, RefreshSearchAnalyzers (skip-if-absent),
     Ingestion pause guard: pass.
   - Tiering tests: pass-or-skip depending on feature flag (Dagger cluster has it
     off → skip is the expected outcome; record this in the PR description).
6. Manual smoke (optional, local docker 3.8.0): `curl` each new endpoint and diff
   the JSON against the models in §6 (catches any doc/code drift — the reason
   integration tests exist per CONTRIBUTING.md).
7. Backward compatibility: existing test suite unchanged and green; new interface
   methods are additive — consumers compiling against `Client`/service interfaces
   only break if they maintain their own implementations of those interfaces
   (acceptable per module major-version conventions; note in changelog).

---

## 9. Investigated and Explicitly Excluded

| Item | Why excluded |
|---|---|
| `/_analytics/ppl`, `/_analytics/ppl/_explain` (3.7.0) | Handlers live in `sandbox/plugins/test-ppl-frontend` + `sandbox/plugins/analytics-engine`; sandbox code is not built into release distributions (requires `-Dsandbox.enabled=true`). Revisit when/if the analytics engine ships in a release. |
| WLM workload-group changes (`search_settings`, `search.max_buckets`, `override_request_values`, 3.6.0/3.7.0) | WLM APIs (`/_plugins/_workload_management/...`) predate 3.4.0 and the client has no WLM service — pre-existing gap outside this window's scope. |
| `_filecache/prune` (pre-3.4.0) | Pre-existing gap, unchanged in window. |
| gRPC/Arrow/HTTP-3 transport features | Not HTTP REST surfaces consumable by resty. |
| `hyperloglog` field mapper (3.5.0), `field_mapping`/`mapper_settings` ingestion mappers (3.6.0), `extra_fields` indexing (3.7.0) | Mapping/index-body JSON passed through as `any`; no typed surface to change. |
| New cluster/index settings (delayed timeout default, merge autoThrottle defaults, `search.query.max_query_string_length_monitor_only`, tiering concurrency settings) | Settings are managed via existing `Cluster().PutSettings` / `Indices().PutSettings` (body `any`). |
| Security/other plugin REST changes | No bundled-plugin REST changes affecting existing client methods were found in the five core release notes; plugin repos release in lockstep but their notes list no breaking REST changes for the endpoints this client implements. |

---

## 10. Risks

1. **Tiering response field names** were transcribed from `TieringStatus.java`
   constants at 3.8.0 (low risk). If integration against a flag-enabled cluster
   reveals drift, adjust `tiering_model.go` tags only.
2. **`_refresh_search_analyzers` ownership**: registered by the ISM plugin but not
   under `/_plugins/_ism/`; placed in `IsmService` per api-spec grouping. If the
   endpoint moves to core, only the path constant changes.
3. **Ingestion integration coverage** is guard-based (no Kafka-backed cluster in CI);
   unit tests carry the structural coverage.
4. Client targets "3.4.0+" — all additions are server-version-gated at runtime by
   normal HTTP errors; no client-side version sniffing is introduced.
