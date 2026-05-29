# How to Contribute

This is the Go client for [OpenSearch](https://opensearch.org/), module `github.com/disaster37/opensearch/v3`. It targets **OpenSearch 3.4.0+** and requires **Go 1.26**.

> Read this document end-to-end before writing code. A large portion of it applies to AI code assistants as well as human contributors.
>
> **This file is a living document.** When you add a new pattern, deprecate an old one, or significantly change an existing pattern in the codebase, update this file in the same PR. Contributors (human and AI) rely on it as the single source of truth for how this project is written.

---

## Architecture Overview

The project is split into **five packages** chosen to give each concern a clean boundary and a small per-file count:

```
github.com/disaster37/opensearch/v3/
├── client.go, common.go, errors.go, doc.go, generate.go    # package opensearch  (entry point)
├── types/                                                    # package types      (shared foundation, 0 internal imports)
├── api/                                                      # package api        (16 service interfaces + models)
│   ├── options.go                                            #   request param structs with validator tags
│   ├── {group}_service.go                                    #   service interfaces + impls
│   └── {group}_model.go                                      #   response/model structs
├── querydsl/                                                 # package querydsl   (query & aggregation builder DSL)
├── trace/opentelemetry/                                      # tracing middleware
└── ci/                                                       # Dagger CI pipeline
```

### Import DAG (no cycles allowed)

```
types          (imports nothing from this module)
  ↑
querydsl       (imports types)
  ↑
api            (imports types + querydsl)
  ↑
root           (assembles Client, re-exports type aliases)
```

The root package re-exports common types from `types/` as type aliases so users can write:

```go
import opensearch "github.com/disaster37/opensearch/v3"

os.IsNotFound(err)        // re-export from types
_, _ = client.Cluster().Health(ctx, nil)  // returns *api.ClusterHealthResponse
```

### Package Responsibilities

| Package | Contains | Never |
|---|---|---|
| `types` | Error types, shard/broadcast responses, `DocumentVersion`, `UnixMilliTime`, `ListResponse[T]` | HTTP code, resty calls, query builders |
| `querydsl` | `Query` and `Aggregation` interfaces, all query builders (Match, Bool, Term, Range, …), all aggregation builders, `SearchSource`, sort, highlight, script | HTTP code, service methods |
| `api` | `XxxService` interfaces + `DefaultXxxService` impls, all response/model structs (e.g. `IndexResponse`, `SearchResult` wrappers, plugin policy structs), `parseErrorResponse`, request param structs in `options.go` | The `Client` interface itself |
| root (`opensearch`) | `Client` interface, `Config`, `New()`, type aliases, `IsNotFound`/`IsConflict`, `go:generate` directives | Service method implementations, query builders |

### Dependencies (minimal)

Only five runtime dependencies are permitted:

| Dependency | Purpose |
|---|---|
| `github.com/go-resty/resty/v2` | HTTP client |
| `github.com/sirupsen/logrus` | Structured logging (passed as `*logrus.Entry`) |
| `go.opentelemetry.io/otel` | Tracing (via resty middleware in `trace/opentelemetry/`) |
| `github.com/goccy/go-json` | Fast JSON encoder/decoder (drop-in replacement for `encoding/json`) |
| `github.com/go-playground/validator/v10` | Struct field validation in `api/` service parameter structs |

A single test-only dependency is permitted:

| Test dependency | Purpose |
|---|---|
| `github.com/stretchr/testify` | Assertions (`assert`, `require`) and suites. Ginkgo/Gomega are not used. |

The project uses `goccy/go-json` instead of the standard `encoding/json` for significantly better performance. All files should import it as:

```go
import json "github.com/goccy/go-json"
```

The validator is used exclusively in `api/options.go` to validate service method parameter structs. Do **not** use it elsewhere in the codebase.

Do **not** add new runtime dependencies without justification. `encoding/json`, `emperror.dev/errors` and `easyjson` are **forbidden**.

---

## Branch Strategy

| Branch | Target |
|---|---|
| `release-branch.v3` | OpenSearch 3.x (active development) |
| `release-branch.v2` | OpenSearch 2.x (maintenance only) |

**Always branch from `release-branch.v3`:**

```bash
git checkout release-branch.v3 && git pull origin release-branch.v3
git checkout -b feature/my-feature
# or
git checkout -b fix/my-fix
```

---

## Coding Conventions

### 1. General rules

- Use `any` instead of `interface{}`.
- Use `*logrus.Entry` (not `*logrus.Logger`) for all logging in `api/`.
- All service methods accept `context.Context` as the first parameter.
- **Service methods with 3+ parameters (after `ctx`) must use a request struct** defined in `api/options.go`. The struct must have `validate` tags and a `Validate()` method. Call `req.Validate()` at the top of the method.
- **Service methods with 1–2 parameters** keep positional args. Validate required string/slice parameters at the top of the method with plain `fmt.Errorf` (not a typed error).
- Error returns use stdlib `error` only. Wrap with `fmt.Errorf("...: %w", err)` when helpful. Never use `emperror.dev/errors`.
- In service methods, use the three logging helpers in `api/errors.go`: `wrapNetworkError`, `logAndReturnError`, `wrapUnmarshalError`. They internally call `parseErrorResponse` and log context via `s.logger`. Do **not** call `parseErrorResponse` or `s.logger` directly in service methods. See §6.
- Prefer **value type receivers** on simple builder structs (`TermQuery struct { Field string }`). Use pointer receivers only where chaining requires mutation in place (e.g. `BoolQuery`, `SearchSource`, `FunctionScoreQuery`) and document the choice in the godoc.
- Avoid `*float64`/`*int` allocation in setters when callers will use literals. Struct-field assignment of value-type optional fields (e.g. `Boost *float64`) is fine, but do not add `SetX(v float64) *T { t.Boost = &v }` wrappers that heap-allocate.

### 2. `api/` — service implementations

Every API group is represented by **two files**:

- `{group}_service.go` — the `XxxService` interface, the `DefaultXxxService` impl, `NewXxxService`, service methods.
- `{group}_model.go` — request/response/model structs used by the service.
- `options.go` — shared request parameter structs with `validate` tags (when a method has 3+ params).

#### When to use a request struct vs positional parameters

| Params after `ctx` | Pattern | Example |
|---|---|---|
| 1 (e.g. `name`) | Positional string | `Delete(ctx, name string)` |
| 2 (e.g. `ctx, index, body`) | Positional | `Create(ctx, index string, body any)` |
| 3+ (e.g. `ctx, index, id, body, params`) | **Request struct** in `options.go` | `Index(ctx, *IndexRequest)` |

The struct pattern eliminates parameter confusion at call sites, groups semantically-related fields, and centralizes validation via `validate` tags instead of scattered `if x == ""` blocks.

#### Request struct pattern (in `options.go`)

```go
type IndexRequest struct {
    Index  string            `validate:"required"`
    Id     string
    Body   any
    Params map[string]string
}

func (r *IndexRequest) Validate() error {
    return validationError(validate.Struct(r))
}
```

Rules for request structs:
- Define them in `api/options.go`, not in `{group}_model.go`.
- Required fields get `validate:"required"`. Optional fields (slices, `any`, maps, pointers) get no tag.
- The struct must have a `Validate() error` method that calls the shared `validate` instance.
- Name the struct `{MethodName}Request` (e.g. `IndexRequest`, `SearchRequest`, `PutMappingRequest`).

#### Service method recipe: struct params

```go
func (s *DefaultDocumentService) Index(ctx context.Context, req *IndexRequest) (*IndexResponse, error) {
    if err := req.Validate(); err != nil {
        return nil, err
    }

    var path, method string
    if req.Id != "" {
        path = fmt.Sprintf("/%s/_doc/%s", req.Index, req.Id)
        method = "PUT"
    } else {
        path = fmt.Sprintf("/%s/_doc", req.Index)
        method = "POST"
    }

    r := s.client.R().SetContext(ctx).SetBody(req.Body)
    for k, v := range req.Params {
        r.SetQueryParam(k, v)
    }

    resp, err := r.Execute(method, path)
    if err != nil {
        return nil, wrapNetworkError(s.logger, err)
    }
    if resp.IsError() {
        return nil, logAndReturnError(s.logger, resp)
    }

    var result IndexResponse
    if err := json.Unmarshal(resp.Body(), &result); err != nil {
        return nil, wrapUnmarshalError(s.logger, resp, err)
    }
    return &result, nil
}
```

#### Service method recipe: positional params (1–2 params)

```go
func (s *DefaultSecurityService) GetRole(ctx context.Context, roleName string) (map[string]SecurityRole, error) {
    if roleName == "" {
        return nil, fmt.Errorf("role name is required")
    }
    resp, err := s.client.R().
        SetContext(ctx).
        Get(fmt.Sprintf("/_plugins/_security/api/roles/%s", roleName))
    if err != nil {
        return nil, wrapNetworkError(s.logger, err)
    }
    if resp.IsError() {
        return nil, logAndReturnError(s.logger, resp)
    }
    var result map[string]SecurityRole
    if err := json.Unmarshal(resp.Body(), &result); err != nil {
        return nil, wrapUnmarshalError(s.logger, resp, err)
    }
    return result, nil
}
```

**Checklist per method**:
- [ ] If 3+ params: request struct defined in `options.go` with `validate` tags + `Validate()` method
- [ ] If 1–2 params: required parameters validated with `fmt.Errorf` at top of method
- [ ] URL built with `fmt.Sprintf` (do **not** add uritemplates; they were removed)
- [ ] `SetContext(ctx)` called on every request
- [ ] Network error wrapped: `wrapNetworkError(s.logger, err)`
- [ ] `resp.IsError()` → `logAndReturnError(s.logger, resp)`
- [ ] Unmarshal error wrapped: `wrapUnmarshalError(s.logger, resp, err)` (never return a zero-value struct alongside an error)
- [ ] Model types live in `{group}_model.go`, request param structs in `options.go`

### 3. `querydsl/` — queries and aggregations

Query and aggregation builders are **struct-literal first**. This means users write:

```go
q := querydsl.Term{Field: "status", Value: "ok"}
```

rather than:

```go
q := querydsl.NewTermQuery("status", "ok")   // old pattern — do not reintroduce
```

For composable queries (Bool, FunctionScore, Interval) where clause accumulation is genuinely useful, keep a builder with pointer receiver. For all others, prefer value semantics.

**Query / aggregation file layout** (one type per file):

```go
package querydsl

// TermQuery matches documents where the field exactly equals value.
//
// JSON output shape:
//     {"term": {"status": "published"}}
//
// For boosted or case-insensitive variants, set the pointer fields:
//     Term{Field: "status", Value: "ok", Boost: ptr(1.5)}
type Term struct {
    Field           string
    Value           any
    Boost           *float64
    CaseInsensitive *bool
    QueryName       string
}

func NewTerm(field string, value any) Term {
    return Term{Field: field, Value: value}
}

func (q Term) Source() (any, error) {
    // build and return {"term": {...}}
}
```

**Serialization helpers** (use these instead of hand-rolling `map[string]any`):

```go
// In querydsl/common.go:
marshalStruct(v any) (any, error)              // json-tags-driven map
sourceAgg(aggType, body, subs, meta, script)   // wrap aggregation + sub-aggs + meta + script
sourcePipeline(aggType, body, meta, script)    // wrap pipeline aggregation + meta + script
pipelineBucketsPath([]string) any              // 0/1/N buckets path encoding
itoa(int) string                               // strconv.Itoa convenience
```

Example aggregation:

```go
type Avg struct {
    Field   string
    Script  *Script
    Format  string
    Missing any
    SubAggs map[string]Aggregation
    Meta    map[string]any
}

func (a Avg) Source() (any, error) {
    body := map[string]any{}
    if a.Field != "" { body["field"] = a.Field }
    if a.Format != "" { body["format"] = a.Format }
    if a.Missing != nil { body["missing"] = a.Missing }
    return sourceAgg("avg", body, a.SubAggs, a.Meta, a.Script)
}
```

**Checklist per query/agg file**:
- [ ] Main type with exported fields + JSON tags where simple fields map to JSON keys
- [ ] `Source() (any, error)` returning the OpenSearch JSON DSL shape
- [ ] `NewXxx()` constructor returning a value (or pointer when builder pattern is intentional)
- [ ] Uses `marshalStruct` / `sourceAgg` / `sourcePipeline` where appropriate — never hand-rolls the `sub-aggregations` map
- [ ] godoc comment on the type with JSON DSL example

### 4. `types/` — shared foundation

`types/` has **zero imports** from this module. If you find yourself needing to import `querydsl` or `api` from `types/`, you've put the type in the wrong package. Only add to `types/` when a symbol is used by both `api` and `querydsl` (or by the root re-exports).

### 5. Root package

The root package should stay tiny. It owns:

- `Client` interface + `DefaultClient` + `New(cfg, logger)`
- `Config` struct
- Type aliases that re-export from `types/` for user convenience
- `go:generate` directives for mockgen (see `generate.go`)
- `doc.go` with package-level overview

Do **not** add implementations here. A new API group goes in `api/`, not root.

---

### 6. Logging, Error Handling & Tracing

#### Logging — `logrus` structured JSON

Every service receives a `*logrus.Entry` scoped with `WithField("service", "...")` in its constructor. This entry **must be used** in every service method for error paths and debug traces. Never store a logger and ignore it.

**Three logging helpers** live in `api/errors.go` — use them consistently instead of calling `s.logger` directly:

| Helper | When to call | Level | Fields |
|---|---|---|---|
| `wrapNetworkError(s.logger, err)` | HTTP transport failure (DNS, TLS, timeout, connection refused) | `error` | the error (with full chain via `errors.Is`/`errors.As`) |
| `logAndReturnError(s.logger, resp)` | `resp.IsError()` — OpenSearch rejected the request | `error` | `http_method`, `http_path`, `http_status`, parsed `OpenSearchError` |
| `wrapUnmarshalError(s.logger, resp, err)` | `json.Unmarshal` fails on the response body | `error` | `http_method`, `http_path`, `http_status`, truncated `response_body`, error |

Each helper **returns** an error, so service methods stay one-liners:

```go
resp, err := s.client.R().SetContext(ctx).Get(path)
if err != nil {
    return nil, wrapNetworkError(s.logger, err)
}
if resp.IsError() {
    return nil, logAndReturnError(s.logger, resp)
}
var result FooResponse
if err := json.Unmarshal(resp.Body(), &result); err != nil {
    return nil, wrapUnmarshalError(s.logger, resp, err)
}
```

**Validation errors** (`req.Validate()` and manual `if param == ""` checks) are returned as-is — no logging. They are caller-facing bugs, not operational issues.

**Debug-level HTTP tracing** is installed globally in `client.go` via resty's `OnBeforeRequest` / `OnAfterResponse` hooks. Enable it at runtime with:

```go
logrus.SetLevel(logrus.DebugLevel)
```

It logs `http_method`, `http_path`, `http_status`, and `http_duration` for every request/response, with zero noise at the normal `info` level.

When adding a new service, copy the constructor pattern exactly:

```go
func NewFooService(client *resty.Client, logger *logrus.Entry) FooService {
    return &DefaultFooService{
        client: client,
        logger: logger.WithField("service", "foo"),
    }
}
```

#### Error handling — idiomatic Go wrapping

This library wraps errors with `fmt.Errorf("context: %w", err)` throughout. No external error library is permitted. `emperror.dev/errors` is **forbidden** as a dependency — it would force every consumer to include it.

The error chain is preserved so callers can use standard Go idioms:

```go
if opensearch.IsNotFound(err) { ... }     // helpers in types/errors.go
if opensearch.IsConflict(err) { ... }
if errors.Is(err, context.DeadlineExceeded) { ... }
```

Only `types/` and the logging helpers in `api/errors.go` may return errors. Service methods never invent new error types — they return `fmt.Errorf` or the helpers' return values.

#### Tracing — OpenTelemetry (opt-in)

OpenTelemetry tracing is already implemented in `trace/opentelemetry/`. It is **not** built into the library core — users opt in via resty middleware on the client they pass to the library, keeping the library free of mandatory OTel dependencies.

**How users enable it** (documented in `trace/opentelemetry/`):

```go
import "github.com/disaster37/opensearch/v3/trace/opentelemetry"

before, after := opentelemetry.Middleware("my-service")
client.RestyClient().
    OnBeforeRequest(before).
    OnAfterResponse(after)
```

Each request gets a span named `opensearch.request` with attributes `http.url`, `http.method`, `http.status_code`, and error status. Exporters (Jaeger, OTLP, Zipkin, etc.) are the caller's responsibility.

**If you add tracing to a new concern** (e.g. middleware for a new plugin API), follow the same pattern: expose it in `trace/` as a resty middleware, never as a mandatory import path.

---

## How to Add a New API

The recipe is the same for any new OpenSearch endpoint. Example: adding `indices.ResolveIndex` (a hypothetical new endpoint).

1. **Decide the owning service.** Most endpoints slot into one of the 16 existing groups. A brand-new plugin deserves a new pair: `{plugin}_service.go` + `{plugin}_model.go`, plus a new accessor on the `Client` interface and a new line in `generate.go`.

2. **Add request/response types** to the group's `{group}_model.go`:
   ```go
   // ResolveIndexResponse is the response from GET /_resolve/index/{name}.
   type ResolveIndexResponse struct {
       Indices    []ResolvedIndex    `json:"indices"`
       Aliases    []ResolvedAlias    `json:"aliases"`
       DataStreams []ResolvedDS      `json:"data_streams"`
   }
   ```

3. **Decide if a request struct is needed** (see §2 table):
   - If the method has **3+ params after `ctx`**, add a request struct in `api/options.go`:
     ```go
     type ResolveIndexRequest struct {
         Name    string `validate:"required"`
         Expand  bool
         Timeout string
     }

     func (r *ResolveIndexRequest) Validate() error {
         return validationError(validate.Struct(r))
     }
     ```
   - If the method has **1–2 params**, keep positional args.

4. **Add the interface method** to the group's `{group}_service.go`:
   ```go
   // With struct params:
   ResolveIndex(ctx context.Context, req *ResolveIndexRequest) (*ResolveIndexResponse, error)

   // With positional params:
   ResolveIndex(ctx context.Context, name string) (*ResolveIndexResponse, error)
   ```

5. **Implement it on `DefaultXxxService`** using the appropriate service method recipe from §2.

6. **Document it** with a Go doc comment that references the OpenSearch docs URL.

7. **Add tests** — see the [Testing](#testing) section for the full pattern:
    - **Unit tests** in `api/{group}_service_test.go`: test every `Validate()` method on new request structs, and every branch that doesn't need a live cluster (URL construction, error wrapping, request-parameter handling). These run with `go test ./...`.
    - **Integration tests** in `api/{group}_service_integration_test.go` (with `//go:build integration`): exercise the real OpenSearch endpoint via `newIntegrationClient()`. These are how we catch API drift when the docs lie. Register `t.Cleanup` to delete the index/resource you create.

8. **Regenerate mocks** if mock generation is still wired up for this interface:
    ```bash
    go generate ./...
    ```

## How to Add a New Query or Aggregation

1. Decide if it's a query (goes in `querydsl/search_queries_{name}.go`) or an aggregation (goes in `querydsl/search_aggs_{category}_{name}.go`, where `category ∈ {bucket, metric, pipeline}`).

2. Copy the struct-literal pattern from the closest existing file (e.g. `search_queries_term.go` for a leaf query, `search_aggs_metric_avg.go` for a metric agg).

3. Implement `Source() (any, error)`. Use `marshalStruct` for JSON-tag-driven serialization when possible; use `sourceAgg` / `sourcePipeline` for aggregations.

4. If the new builder references `querydsl.Query` or `querydsl.Aggregation` (composites), the type must be a pointer-receiver builder so it can accumulate clauses. Otherwise it should be value-typed.

5. Add a godoc comment with a JSON DSL example.

6. **Add tests** — see the [Testing](#testing) section for the full pattern:
    - A table-driven unit test in `querydsl/common_test.go` that calls every constructor/setter you added and verifies the JSON DSL shape returned by `Source()`.
    - A `Example{TypeName}` in `querydsl/example_test.go` (optional but preferred) so the godoc page on pkg.go.dev shows compiled-and-run output.

---

## Documentation Standards

Every exported type, function, and interface method must have a Go doc comment. The project uses a **three-layer** strategy — do not create a `docs/` markdown folder.

| Layer | File(s) | Audience |
|---|---|---|
| Package overview | `doc.go` in each package | Humans & AI deciding what a package does |
| README | `README.md` | Humans deciding whether to use the library |
| Godoc comments | on every exported symbol | IDE tooltips + pkg.go.dev + AI reading source |
| Testable examples | `example_test.go` per package | Both (compiled & validated by `go test`) |

### Writing good godoc

- First sentence must start with the type name and end with a period. It is what pkg.go.dev shows as the one-line summary.
- Include a JSON DSL example for query/agg types (inside a 4-space-indented block).
- Cross-link related types with `[XxxQuery]`.
- Reference the OpenSearch REST docs URL when it helps.

### Testable examples

Every `Example*` function in `example_test.go` is compiled and executed by `go test ./...`. Use `// Output:` comments to assert output:

```go
func ExampleBool() {
    q := Bool{Must: []Query{Match{Field: "a", Query: "b"}}}
    src, _ := q.Source()
    data, _ := json.MarshalIndent(src, "", "  ")
    fmt.Println(string(data))
    // Output:
    // {
    //   "bool": {
    //     "must": [
    //       {"match": {"a": {"query": "b"}}}
    //     ]
    //   }
    // }
}
```

Keep examples short and focused on one concept. They are the primary way AI assistants learn the API surface.

---

## Testing

**Target: 100 % code coverage.** Every exported function, method, builder, and error branch must be exercised by unit tests. Integration tests cover the live OpenSearch API surface and are not counted toward the unit-test coverage number (build tags exclude them from `go test ./...`).

### Test framework — `testify`

We use [**`github.com/stretchr/testify`**](https://github.com/stretchr/testify) for assertions:

| Import | When to use |
|---|---|
| `github.com/stretchr/testify/assert` | Soft assertions — test continues on failure |
| `github.com/stretchr/testify/require` | Hard assertions — abort the test on failure (use for critical setup like `require.NoError(t, err)`) |

Do **not** use Ginkgo, Gomega, or the standard-library `if got != want { t.Errorf(...) }` style — `testify` is the project standard.

### Two test categories, clearly separated

| Category | File pattern | Build tag | Where it lives | When it runs |
|---|---|---|---|---|
| **Unit tests** | `*_test.go` (no `_integration_` in the name) | *none* (runs by default) | Alongside the source file in the same package | `go test ./...` — no cluster needed |
| **Integration tests** | `*_integration_test.go` | `//go:build integration` (required, line 1) | In `api/` (or whatever package needs a live cluster) | `go test -tags=integration ./...` — requires real OpenSearch |

The build tag is **mandatory** on integration files. Without it, `go test ./...` tries to run the tests locally and either fails or silently hangs waiting for an OpenSearch cluster. A missing tag will be caught in review.

### Unit tests — the default

- Live next to the source file: `{name}_test.go` in the **same package** (e.g. `package querydsl` inside `querydsl/common_test.go`). Use internal-package tests (`package foo`), not `_test` packages, unless you specifically need a black-box view.
- Cover every public method, every branch in `Source()`, every validation error, and every edge case in marshal/unmarshal helpers.
- Use table-driven sub-tests (`t.Run("case name", func(t *testing.T) { ... })`) whenever there are ≥ 2 inputs.
- Do **not** depend on an OpenSearch cluster, a network, environment variables, or the file system. Anything a unit test needs should be in-process and deterministic. Use `httptest.NewServer` for HTTP mocking.

#### Test file naming conventions

Test file names must be **descriptive** — they should tell a reader what the tests cover without opening the file. Avoid names like `coverage_test.go`, `error_paths_test.go`, or numbered suffixes (`_test2.go`) — these describe the task that created them, not their content.

| Pattern | Example | Describes |
|---|---|---|
| `{source}_test.go` | `options_test.go`, `errors_test.go`, `time_test.go` | Tests for the corresponding source file |
| `{service}_service_test.go` | `document_service_test.go`, `security_service_test.go` | Unit tests for one API service (success + error branches) |
| `{topic}_test.go` | `search_queries_builder_test.go`, `search_aggs_builders_test.go` | Tests organized by topic/feature area |
| `{service}_service_integration_test.go` | `document_service_integration_test.go` | Integration tests requiring a live cluster |
| `example_test.go` | (one per package) | Compiled-and-run examples |
| `mocks_test.go` | (one per package when needed) | Shared mock type definitions |
| `test_helpers_test.go` | (one per package when needed) | Shared test infrastructure |

#### File-level doc comments

Every test file must have a package-level doc comment (placed before `package`) explaining **what** it tests and **how**:

```go
// document_service_test.go contains unit tests for DefaultDocumentService.
// Tests use httptest.NewServer to mock OpenSearch document CRUD endpoints.
// Each TestUnit* function covers one service method with success subtests,
// validation error subtests, and (where applicable) edge cases.
package api
```

For mock files:

```go
// mocks_test.go defines test-only mock implementations of the interfaces
// declared in this package. They exist to exercise error-propagation paths
// in composite query/aggregation builders (e.g. BoolQuery, FunctionScoreQuery,
// SearchSource) without requiring a real OpenSearch cluster.
//
// Naming convention: mock types follow the pattern "mock{Interface}{Behavior}"
// — e.g. mockQueryError (returns error from Source), mockSorterError
// (returns error from Source), mockContextQueryNonMap (returns non-map value).
package querydsl
```

For shared helpers:

```go
// test_helpers_test.go contains shared infrastructure for API service unit tests.
// All services under test use httptest.NewServer to simulate OpenSearch endpoints.
package api
```

#### `api/` test organization

Each of the 16 API services has its own test file (`{group}_service_test.go`) containing:
- **Success tests** — one `TestUnit{Service}{Method}` function per service method, using `httptest.NewServer` with route handlers that return valid JSON responses
- **Validation tests** — subtests that omit required fields and assert `require.Error`
- **HTTP error tests** — subtests where the mock server returns 500, exercising `logAndReturnError`
- **Network error tests** — a `TestUnit{Service}NetworkErrors` function using `deadClient()` pointed at `127.0.0.1:1`, exercising `wrapNetworkError`

Cross-service tests (error helpers, model utilities like `HasRole`/`IsMaster`) live in `service_error_test.go`.

#### `querydsl/` test organization

Tests in `querydsl/` are organized by feature area:

| File | Tests |
|---|---|
| `common_test.go` | Primary test file — TermQuery, BoolQuery, RangeQuery and other commonly-used builders |
| `search_queries_builder_test.go` | Source() tests for all remaining query types (HasChild, HasParent, Percolator, Interval, FSQ) |
| `search_source_and_queries_test.go` | SearchRequest, SearchSource, BoostingQuery, CommonTermsQuery, ConstantScoreQuery |
| `search_aggs_and_helpers_test.go` | Aggregation infrastructure (ScriptedMetric, Filter, SignificantTerms heuristics, Composite, InnerHit) |
| `search_aggs_builders_test.go` | All concrete aggregation builders (metric, bucket, pipeline) with full option sets |
| `error_propagation_test.go` | Error propagation tests for query/agg/sorter/SearchSource builders |
| `suggester_error_propagation_test.go` | Error propagation tests for suggester components (context, generator, smoothing) |

Example — `querydsl/common_test.go`:

```go
func TestTermQuery_Source(t *testing.T) {
    t.Run("simple", func(t *testing.T) {
        q := NewTermQuery("status", "published")
        src, err := q.Source()
        require.NoError(t, err)
        m := src.(map[string]any)
        inner := m["term"].(map[string]any)
        assert.Equal(t, "published", inner["status"])
    })

    t.Run("with boost", func(t *testing.T) {
        boost := 1.5
        q := TermQuery{Field: "status", Value: "ok", Boost: &boost}
        src, err := q.Source()
        require.NoError(t, err)
        m := src.(map[string]any)
        inner := m["term"].(map[string]any)
        statusMap := inner["status"].(map[string]any)
        assert.Equal(t, 1.5, statusMap["boost"])
    })
}
```

### Shared test helpers — `test_helpers_test.go`

When a package has three or more test helper functions, consolidate them into a single `test_helpers_test.go` file with a descriptive file-level doc comment. This prevents helper proliferation across test files and makes it easy for new contributors to discover available test infrastructure.

**`api/test_helpers_test.go`** provides:

| Helper | Purpose |
|---|---|
| `testLogger()` | Returns `*logrus.Entry` at DebugLevel for use with service constructors |
| `restyClient(ts)` | Wraps `*httptest.Server` in a `*resty.Client` pointed at its URL |
| `deadClient()` | Returns `*resty.Client` at `127.0.0.1:1` with 100ms timeout for network-error branches |
| `jsonResponse(w, status, body)` | Convenience: sets Content-Type + status code + writes body in one call |

**`querydsl/mocks_test.go`** provides documented mock types for all package interfaces (see [Mock types](#mock-types-for-error-propagation) below).

Do **not** define per-file helpers like `newMyServiceTestServer()`. Instead, define the server inline in the test using `httptest.NewServer` + `restyClient`. Only extract a helper when it appears in ≥ 3 test files.

### Mock types for error propagation — `mocks_test.go`

Several builder types in `querydsl/` compose child components that implement interfaces like `Query`, `Aggregation`, `Sorter`, `Suggester`, or `ScoreFunction`. To test that parent builders correctly propagate (not swallow) errors from children, we define **mock types that always return errors**.

All mock types live in `querydsl/mocks_test.go` and follow a strict naming convention:

| Pattern | Behavior | Example |
|---|---|---|
| `mock{Interface}Error` | `Source()` returns `(nil, error)` | `mockQueryError`, `mockAggregationError`, `mockSorterError` |
| `mock{Interface}NonMap` | `Source()` returns a non-map value | `mockContextQueryNonMap` (for type-assertion branches) |
| `mock{Interface}NonSerializable` | Returns value that `json.Marshal` can't encode | `mockNonSerializableQuery` (channel in map) |

Each mock type has a godoc explaining:
1. Which interface it implements
2. What it does differently from a real implementation
3. Which error-propagation path it exercises

**When adding a new mock:** do not scatter it across test files. Add it to `mocks_test.go` with a godoc comment, then reference it from wherever it's needed. Mocks are test-only (`_test.go`) and invisible to library consumers.

Usage example:

```go
// Verify that BoolQuery.Must propagates errors from child queries
func TestBoolQuery_MustError(t *testing.T) {
    q := NewBoolQuery().Must(mockQueryError{})
    _, err := q.Source()
    assert.Error(t, err)
}

// Verify that CompletionSuggester detects non-map context queries
func TestCompletionSuggester_NonMapContext(t *testing.T) {
    s := NewCompletionSuggester("cs").
        ContextQueries(mockContextQueryNonMap{}, mockContextQueryNonMap{})
    _, err := s.Source(false)
    assert.Error(t, err)
}
```

### Integration tests — against a real cluster

Integration tests exist because **the official OpenSearch documentation is sometimes wrong**. We do not mock the HTTP layer — we exercise the real API against a cluster that Dagger spins up fresh for every CI run.

- **File name:** `{group}_service_integration_test.go` in `api/` (e.g. `document_service_integration_test.go`).
- **First line:** `//go:build integration` — without exception.
- **Package:** `api_test` (black-box). Imports `github.com/disaster37/opensearch/v3` and `github.com/disaster37/opensearch/v3/api`.
- **Shared client:** use the `newIntegrationClient()` helper from `api/integration_helper_test.go`. It panics if `OPENSEARCH_URL`, `OPENSEARCH_USERNAME`, or `OPENSEARCH_PASSWORD` are unset — do **not** re-introduce hardcoded credential fallbacks.
- **Index names:** use a predictable prefix `test-<service>-` (e.g. `test-doc-svc`, `test-idx-svc`) and register cleanup with `t.Cleanup(func() { client.Indices().Delete(ctx, []string{name}) })`. Do **not** leave test indices behind.
- **Ordering:** when a test needs an existing resource (document, index, user, pipeline), create it inside the test body, then register cleanup. Don't rely on a `TestMain` that sets up global state — each integration test function should be self-contained.

Example — `api/search_service_integration_test.go`:

```go
//go:build integration

package api_test

import (
    "context"
    "testing"

    "github.com/disaster37/opensearch/v3/api"
    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/require"
)

func TestSearchService_Count(t *testing.T) {
    client := newIntegrationClient()
    ctx := context.Background()
    const index = "test-search-count"

    // Setup: create index and a couple of docs
    _, err := client.Indices().Create(ctx, index, map[string]any{
        "settings": map[string]any{"number_of_shards": 1, "number_of_replicas": 0},
    })
    require.NoError(t, err)
    t.Cleanup(func() { _, _ = client.Indices().Delete(ctx, []string{index}) })

    for _, id := range []string{"1", "2", "3"} {
        _, err := client.Document().Index(ctx, &api.IndexRequest{
            Index: index, Id: id,
            Body:   map[string]any{"title": "hello"},
            Params: map[string]string{"refresh": "true"},
        })
        require.NoError(t, err)
    }

    count, err := client.Search().Count(ctx, []string{index}, nil)
    require.NoError(t, err)
    assert.Equal(t, int64(3), count)
}
```

### Testable examples

- Live in `{pkg}/example_test.go` with `package {pkg}_test` (black-box).
- Run with `go test -run=Example ./{pkg}`.
- Must match exact `// Output:` comments; otherwise `go test` fails.
- These count toward coverage for the package and are the main way users (and AI) learn the API.

### Running tests

```bash
# Unit tests + examples only (no OpenSearch cluster needed)
go test ./...

# Unit tests + examples + integration tests (requires a real cluster)
go test -tags=integration ./...

# Unit tests with the race detector (must pass CI)
go test -race ./...

# Unit + integration tests with coverage, via Dagger (CI target)
dagger call --src . test

# Full CI pipeline: build → lint → format → test → codecov → git
dagger call --src . ci
```

The Dagger pipeline boots a two-node OpenSearch cluster and injects the required env vars (`OPENSEARCH_URL`, `OPENSEARCH_USERNAME`, `OPENSEARCH_PASSWORD`) into the test container before running `go test -tags=integration`.

### Coverage enforcement

CI uploads `coverage.out` to Codecov via `dagger call --src . code-cov`. Coverage targets per package:

| Package | Target | What it covers |
|---|---|---|
| `types`       | 100 % | Error types, `UnixMilliTime` marshal/unmarshal, `DocumentVersion` |
| `querydsl`    | 100 % | Every query/agg builder's `Source()`, every `SearchSource` setter, every sort/highlight/script variant |
| `api`         | 100 % | Every `Validate()` method on request structs, error helpers, model utilities (HasRole, IsMaster), service methods (success + error + network branches via `httptest` unit tests) |
| `trace/opentelemetry` | 100 % | Middleware `before`/`after`, URL redaction, span lifecycle (including the GC-finalizer path) |
| root          | 100 % | `New()`, `IsNotFound`/`IsConflict`, service accessor methods |

Run `go test -cover ./...` (or `-tags=integration` for the API package) to check locally before pushing.

---

## Running CI (Dagger)

All CI is managed via [Dagger](https://dagger.io/). No Makefile.

```bash
dagger call --src . ci        # full pipeline: build → lint → format → test → codecov → git
dagger call --src . lint      # lint only
dagger call --src . test      # test only (spins up two-node OpenSearch cluster via gotestsum)
```

### Debug a specific test

```bash
dagger call --src . debug-test --run TestMyFeature up
```

Starts Delve on port 4000. Connect via VS Code:

```json
{
  "name": "Connect to Dagger debugger",
  "type": "go",
  "request": "attach",
  "mode": "remote",
  "remotePath": "/src",
  "port": 4000,
  "host": "127.0.0.1"
}
```

---

## Pull Request Checklist

- [ ] Branch is based on `release-branch.v3`
- [ ] One feature or fix per PR
- [ ] New code is in the correct package (see Architecture Overview)
- [ ] Code is formatted: `go fmt ./...`
- [ ] All exported symbols have godoc comments
- [ ] Service methods use the three logging helpers (`wrapNetworkError`, `logAndReturnError`, `wrapUnmarshalError`) on every error path
- [ ] New services scope their logger with `WithField("service", "name")` in the constructor
- [ ] **Unit tests written** using `testify` (see [Testing](#testing) for conventions); **integration tests written** for every new API endpoint
- [ ] Integration test files have `//go:build integration` as line 1 — without exception
- [ ] `go build ./...` passes
- [ ] `go vet ./...` passes
- [ ] `go test -race ./...` passes (the race detector must be clean)
- [ ] `go test -cover ./...` shows coverage is at or above the package targets (see [Coverage enforcement](#coverage-enforcement))
- [ ] For `api/` changes: `go test -tags=integration -cover ./api/...` also shows coverage at target against a local cluster (or via `dagger call --src . test`)
- [ ] Mocks regenerated (`go generate ./...`) if any `api/` interface signature changed — mocks are published for library consumers, not used by our own test suite
- [ ] **This `CONTRIBUTING.md` updated** if the PR introduces, changes, or deprecates a pattern documented here
- [ ] Commit message explains the "why"
- [ ] PR description links to the relevant issue / OpenSearch doc URL

---

## AI Agent Instructions

This section is addressed to LLM-based coding assistants (Kilo, Cursor, Copilot, etc.). When contributing to this project:

### Before writing code

1. **Read this file first** — the conventions below override any pattern you'd apply by default.
2. **Determine the target package** using the table in the Architecture Overview. Do not default to the root package.
3. **Read the closest existing file** as a reference implementation. The codebase is now well-organised; find the sibling file to the thing you're adding.

### While writing code

- **Do NOT** reintroduce hand-rolled `map[string]any` serializers in `querydsl/` — use `marshalStruct`, `sourceAgg`, `sourcePipeline`.
- **Do NOT** add builder-pattern setters to leaf query structs — use struct-literal construction with exported fields.
- **Do NOT** add new top-level dependencies. Runtime deps are fixed to `resty`, `logrus`, `otel`, `goccy/go-json`, and `validator/v10`. The only permitted test-only dependency is `github.com/stretchr/testify` — do not add Ginkgo, Gomega, httptest servers in non-test code, or any other test framework.
- **Do NOT** import `encoding/json` — always use `json "github.com/goccy/go-json"`.
- **Do NOT** use `emperror.dev/errors` or `easyjson` — both are forbidden.
- **Do NOT** create a `docs/` markdown folder — documentation lives in godoc + README + `example_test.go`.
- **Do NOT** put service methods in the root package — they belong in `api/`.
- **Do NOT** use uritemplates — they were deleted. URLs are built with `fmt.Sprintf`.
- **Do NOT** use positional params for service methods with 3+ args after `ctx` — use a request struct in `api/options.go`.
- **Do NOT** use `validator/v10` outside of `api/options.go`.
- **Do NOT** store a `*logrus.Entry` in a service and never call it — every service method must log on error paths.
- **Do NOT** call `s.logger.Error(...)` directly in service methods — use the three helpers in `api/errors.go`.
- **Do NOT** call `parseErrorResponse(resp)` directly — use `logAndReturnError(s.logger, resp)` which calls it and logs context.
- **Always** propagate `context.Context` into resty requests via `SetContext(ctx)`.
- **Always** wrap network errors: `return nil, wrapNetworkError(s.logger, err)`.
- **Always** log OpenSearch error responses: `return nil, logAndReturnError(s.logger, resp)`.
- **Always** wrap unmarshal errors: `return nil, wrapUnmarshalError(s.logger, resp, err)` — never `return &result, err`.
- **Always** scope a new service logger with `logger.WithField("service", "name")` in its constructor.
- **Always** add a godoc comment on every exported symbol. The first sentence must start with the type name.
- **Always** add a test using `testify` (see [Testing](#testing)):
    - New `api/` service method → unit test in `{group}_service_test.go` for success, validation, HTTP error, and network error branches, **and** an integration test in `{group}_service_integration_test.go` that calls the real OpenSearch endpoint. Use `restyClient(srv)` and `testLogger()` from `test_helpers_test.go`.
    - New `querydsl/` builder → table-driven unit test in the appropriate test file (see [querydsl/ test organization](#querydsl-test-organization)). Use mock types from `mocks_test.go` for error-propagation tests — do not define new mocks in test files.
    - New `types/` type → unit test in `types/` covering every field and every branch in marshal/unmarshal helpers.
    - New error branch → unit test asserting the exact error string or status code.
    - Integration test files **must** start with `//go:build integration` on line 1.
    - Test files must have a file-level doc comment describing what they test.
    - Test file names must be descriptive (e.g. `search_queries_builder_test.go`, not `coverage_test.go`).
- **Always** update this `CONTRIBUTING.md` when you introduce a new pattern, deprecate an old one, or change how an existing pattern works — the doc is the single source of truth for contributors (human and AI).

### After writing code

```bash
go fmt ./...
go vet ./...
go build ./...
go test -race ./...                # race detector must be clean
go test -cover ./...               # unit coverage must hit per-package targets
go generate ./...                  # only if you changed an api/ interface signature
```

For `api/` changes, also run against a real cluster:

```bash
go test -tags=integration -cover ./api/...
# or the Dagger pipeline which handles the cluster for you:
dagger call --src . test
```

All of the above must pass before the PR is ready. Coverage must be at or above the targets in the [Coverage enforcement](#coverage-enforcement) section (100 % is the goal).

If your change introduces, changes, or deprecates a pattern documented in this file (logging helpers, error wrapping, tracing integration, service structure, etc.), update this `CONTRIBUTING.md` in the same PR. This file is the single source of truth — if the code and the doc disagree, the next contributor (human or AI) will get confused.

### Common pitfalls

- `client.Cluster().Health(ctx, nil)` — **no** third `waitForStatus` parameter; pass that via query string params if needed.
- `client.Document().Update(ctx, &api.UpdateRequest{...})` — the method uses a struct param; optimistic concurrency is done via the `Params` map (`if_seq_no` / `if_primary_term`), not a top-level `DocumentVersion` argument.
- `client.Document().Index(ctx, &api.IndexRequest{Index: "...", Id: "...", Body: ...})` — all document CRUD operations with 3+ params use request structs from `api/options.go`.
- `client.Document().Bulk(ctx, index, body)` — body is **NDJSON as a string**, not `[]byte` or structured data. Bulk has only 2 params after `ctx` so it stays positional.
- Response types live in `querydsl/` (e.g. `SearchResult`, `MultiSearchResult`, `CountResponse`), not `api/`. The `api/` package imports them.
- Type aliases in `common.go` make `opensearch.AcknowledgedResponse` resolve to `types.AcknowledgedResponse`. Don't redefine them in root.
- Request parameter structs live in `api/options.go`, not in `{group}_model.go`. Model/response structs stay in `{group}_model.go`.

---

## Additional Resources

- [OpenSearch documentation](https://opensearch.org/docs/latest/)
- [OpenSearch REST API reference](https://opensearch.org/docs/latest/api-reference/)
- [Go project layout](https://go.dev/doc/modules/layout)
- [Effective Go](https://go.dev/doc/effective_go)
- [Dagger documentation](https://docs.dagger.io/)
- [mockgen](https://github.com/uber-go/mock)
