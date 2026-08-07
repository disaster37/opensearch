# Auto-serialize search body in `DefaultSearchService.Search()`

## 1. Summary

Today, callers must manually serialize a `querydsl.SearchRequest` before passing it to the
client: call `.Body()`, handle the error, and put the resulting string into
`api.SearchRequest.Body`. If they instead put the `*querydsl.SearchRequest` object directly into
`Body any`, resty JSON-marshals the struct (which has only unexported fields) and sends `{}`.

This change makes `DefaultSearchService.Search()` normalize `req.Body` before handing it to resty:

1. Bodies that are already serialized (`string`, `[]byte`, `json.RawMessage`, `*json.RawMessage`)
   are used as-is (wrapped as `json.RawMessage` so resty transmits the exact bytes).
2. Bodies implementing a `Body() (string, error)` method (e.g. `*querydsl.SearchRequest`) are
   serialized automatically; errors propagate to the caller.
3. Anything else (maps, structs, nil) is passed to resty unchanged — current behavior.

The logic lives in a single reusable, unexported helper `normalizeBody` in the `api` package so it
can later be applied to `MultiSearch`, `SearchTemplate`, `Count`, `Validate`, etc. Only `Search()`
is wired up in this change.

After this change, the user's example works without the manual `.Body()` call:

```go
searchReq := querydsl.NewSearchRequest().
    Query(boolQuery).
    Sort("@timestamp", false).
    Size(int(params.MaxLines)).
    DocValueFields("event.original").
    FetchSource(false).
    TrackTotalHits(true)

searchRes, err := t.client.Search().Search(ctx, &opensearchv4api.SearchRequest{
    Indices: []string{t.index},
    Body:    searchReq, // no manual .Body() needed
})
```

No public API changes. No breaking changes: every existing call path keeps its behavior.

## 2. Files to modify

| File | Change |
|---|---|
| `/projects/opensearch/api/options.go` | Add `reflect` import; add `bodyProvider` interface, `normalizeBody`, `isNilValue` next to `rawBody` (lines 14-18). |
| `/projects/opensearch/api/search_service.go` | In `DefaultSearchService.Search()` (lines 46-72), normalize `req.Body` before `SetBody` (line 54). |
| `/projects/opensearch/api/options_test.go` | Add `TestNormalizeBody` unit tests (mock type with failing `Body()` already exists: `mockQueryError`, lines 13-17). |
| `/projects/opensearch/api/search_service_test.go` | Add `TestUnitSearchServiceSearchAutoSerialize` wire-level test that captures the request body; add `io` and `querydsl` imports. |

No changes to `querydsl/`, `client.go`, or any other service.

## 3. New data structures

All additions are unexported, in package `api`, in `options.go` directly after `rawBody`
(after line 18, before `var validate` at line 20). Follows the existing camelCase convention for
unexported helpers (`rawBody`, `validationError`, `wrapNetworkError`).

```go
// bodyProvider is implemented by request types that can serialize themselves
// into a JSON search body. *querydsl.SearchRequest satisfies this interface
// via its Body() (string, error) method.
type bodyProvider interface {
	Body() (string, error)
}
```

Notes:
- The method set matches `(*querydsl.SearchRequest).Body()` exactly
  (`querydsl/search_request.go:603`). Pointer receiver: `*querydsl.SearchRequest` satisfies the
  interface; a bare `querydsl.SearchRequest` value does not (see edge cases).
- A repo-wide grep confirms `*querydsl.SearchRequest` is currently the only implementer, but the
  interface is structural, so future querydsl types opt in automatically.

No new struct types are introduced. `SearchRequest.Body` stays `any`.

## 4. Function signatures

```go
// normalizeBody prepares req.Body for resty. See step 2 for full doc comment.
func normalizeBody(body any) (any, error)

// isNilValue reports whether v holds a nil pointer or nil interface ("typed nil").
// Must only be called with a non-nil interface value.
func isNilValue(v any) bool
```

Existing helper reused unchanged: `func rawBody(s string) json.RawMessage` (options.go:16).
`json` is the existing alias for `github.com/goccy/go-json` used throughout the package.

## 5. Implementation steps

### Step 1 — `api/options.go`: add `reflect` import

Current import block (lines 3-12):

```go
import (
	"fmt"
	"strconv"

	json "github.com/goccy/go-json"

	"github.com/disaster37/opensearch/v4/querydsl"
	"github.com/disaster37/opensearch/v4/types"
	"github.com/go-playground/validator/v10"
)
```

Add `"reflect"` to the stdlib group:

```go
import (
	"fmt"
	"reflect"
	"strconv"

	json "github.com/goccy/go-json"
	...
)
```

(`reflect` is not used anywhere in the repo today; it is required for typed-nil detection, see
edge cases. Standard library only — no new module dependency.)

### Step 2 — `api/options.go`: add the helper block after `rawBody` (after line 18)

Insert exactly this block between `rawBody` (ends line 18) and `var validate` (line 20):

```go
// bodyProvider is implemented by request types that can serialize themselves
// into a JSON search body. *querydsl.SearchRequest satisfies this interface
// via its Body() (string, error) method.
type bodyProvider interface {
	Body() (string, error)
}

// normalizeBody prepares a request body for transmission via resty.
//
//   - string, []byte, json.RawMessage and *json.RawMessage are treated as
//     already-serialized JSON. They are wrapped as json.RawMessage so that
//     resty transmits the exact bytes without re-marshaling (see rawBody).
//   - A value implementing bodyProvider (e.g. *querydsl.SearchRequest) has its
//     Body() method called and the result wrapped as json.RawMessage. An error
//     from Body() is returned wrapped.
//   - nil and typed-nil values yield a nil body.
//   - Any other value (map, struct, ...) is returned unchanged so resty
//     marshals it exactly as before.
func normalizeBody(body any) (any, error) {
	if body == nil {
		return nil, nil
	}

	switch b := body.(type) {
	case string:
		return rawBody(b), nil
	case []byte:
		return rawBody(string(b)), nil
	case json.RawMessage:
		return b, nil
	case *json.RawMessage:
		if b == nil {
			return nil, nil
		}
		return *b, nil
	}

	if bp, ok := body.(bodyProvider); ok {
		// Guard against a typed nil (e.g. (*querydsl.SearchRequest)(nil))
		// whose Body() method would panic on a nil receiver.
		if isNilValue(body) {
			return nil, nil
		}
		s, err := bp.Body()
		if err != nil {
			return nil, fmt.Errorf("serialize request body: %w", err)
		}
		return rawBody(s), nil
	}

	return body, nil
}

// isNilValue reports whether v holds a nil pointer or nil interface value
// (a "typed nil"). It must only be called with a non-nil interface value.
func isNilValue(v any) bool {
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Ptr, reflect.Interface:
		return rv.IsNil()
	}
	return false
}
```

Design rationale (important for the implementer):

- **Why wrap serialized forms in `json.RawMessage` instead of passing the raw `string`/`[]byte`
  through?** "Use as-is" means "use these already-serialized bytes without re-serialization".
  `json.RawMessage.MarshalJSON` returns the bytes unchanged, so wrapping guarantees resty emits
  exactly those bytes regardless of how resty treats raw strings/byte slices. This is precisely the
  existing, tested pattern: `rawBody` (options.go:14-18) exists for this purpose, the bridge
  `NewSearchRequest` (options.go:650) uses it, and `client.go:230-232` documents that resty sends
  raw `string`/`[]byte` bodies with a non-JSON default content type. Wrapping is never worse and
  removes any double-encoding / content-type risk.
- **`[]byte` via `rawBody(string(b))`** (rather than `json.RawMessage(b)`) so a nil `[]byte`
  becomes an empty, non-nil `json.RawMessage` (empty body) instead of a nil `json.RawMessage`
  (whose marshaling is implementation-defined).
- **Check order follows the spec**: serialized forms first, then `Body()` detection, then
  pass-through. A named type like `type myBytes []byte` does not match `case []byte`, so if it
  implements `Body()` it is still detected — correct behavior.
- **`normalizeBody` wraps `Body()` errors itself** so every future caller (MultiSearch,
  SearchTemplate, ...) gets consistent error context for free.

### Step 3 — `api/search_service.go`: wire `normalizeBody` into `Search()`

Current code (lines 46-57):

```go
func (s *DefaultSearchService) Search(ctx context.Context, req *SearchRequest) (*querydsl.SearchResult, error) {
	var path string
	if len(req.Indices) > 0 {
		path = fmt.Sprintf("/%s/_search", strings.Join(req.Indices, ","))
	} else {
		path = "/_search"
	}

	r := s.client.R().SetContext(ctx).SetBody(req.Body)
	for k, v := range req.Params.ToMap() {
		r.SetQueryParam(k, v)
	}
```

Replace line 54 (`r := s.client.R().SetContext(ctx).SetBody(req.Body)`) with:

```go
	body, err := normalizeBody(req.Body)
	if err != nil {
		return nil, err
	}

	r := s.client.R().SetContext(ctx).SetBody(body)
```

Notes:
- No import changes in this file (`normalizeBody` is in the same package).
- No compile issue with the later `resp, err := r.Post(path)` (line 59): `:=` is valid because
  `resp` is a new variable while `err` is reused.
- The body error is returned without logging, matching the bridge `NewSearchRequest`
  (options.go:642-653), which treats `Body()` failures as caller-side errors, not
  network/server errors (`wrapNetworkError`/`logAndReturnError` are reserved for HTTP failures).
- Do NOT modify `MultiSearch`, `SearchTemplate`, `MultiSearchTemplate`, `RankEval`, `Count`,
  `Validate` in this change (see section 11).

### Step 4 — `api/options_test.go`: add `TestNormalizeBody`

Append at the end of the file (package `api`; `mockQueryError` from lines 13-17 and the
`querydsl` import already available). Local test types needed:

```go
// mockBodyProvider implements bodyProvider with a fixed result.
type mockBodyProvider struct {
	body string
	err  error
}

func (m *mockBodyProvider) Body() (string, error) { return m.body, m.err }
```

Test function (table-style with subtests, matching file conventions — `assert`, no server needed):

```go
func TestNormalizeBody(t *testing.T) {
	t.Run("nil body", func(t *testing.T) {
		got, err := normalizeBody(nil)
		assert.NoError(t, err)
		assert.Nil(t, got)
	})

	t.Run("string is wrapped as raw message", func(t *testing.T) {
		got, err := normalizeBody(`{"query":{"match_all":{}}}`)
		assert.NoError(t, err)
		assert.Equal(t, json.RawMessage(`{"query":{"match_all":{}}}`), got)
	})

	t.Run("empty string", func(t *testing.T) {
		got, err := normalizeBody("")
		assert.NoError(t, err)
		assert.Equal(t, json.RawMessage(""), got)
	})

	t.Run("byte slice is wrapped as raw message", func(t *testing.T) {
		got, err := normalizeBody([]byte(`{"a":1}`))
		assert.NoError(t, err)
		assert.Equal(t, json.RawMessage(`{"a":1}`), got)
	})

	t.Run("nil byte slice becomes empty raw message", func(t *testing.T) {
		var b []byte
		got, err := normalizeBody(b)
		assert.NoError(t, err)
		assert.Equal(t, json.RawMessage(""), got)
	})

	t.Run("json.RawMessage unchanged", func(t *testing.T) {
		in := json.RawMessage(`{"a":1}`)
		got, err := normalizeBody(in)
		assert.NoError(t, err)
		assert.Equal(t, in, got)
	})

	t.Run("pointer to json.RawMessage dereferenced", func(t *testing.T) {
		in := json.RawMessage(`{"a":1}`)
		got, err := normalizeBody(&in)
		assert.NoError(t, err)
		assert.Equal(t, json.RawMessage(`{"a":1}`), got)
	})

	t.Run("nil pointer to json.RawMessage", func(t *testing.T) {
		var in *json.RawMessage
		got, err := normalizeBody(in)
		assert.NoError(t, err)
		assert.Nil(t, got)
	})

	t.Run("querydsl request serialized via Body()", func(t *testing.T) {
		qr := querydsl.NewSearchRequest().Query(querydsl.NewMatchAllQuery()).Size(5)
		expected, err := qr.Body()
		assert.NoError(t, err)

		got, err := normalizeBody(qr)
		assert.NoError(t, err)
		assert.Equal(t, json.RawMessage(expected), got)
	})

	t.Run("Body() error is wrapped and propagated", func(t *testing.T) {
		qr := querydsl.NewSearchRequest().Query(mockQueryError{})
		got, err := normalizeBody(qr)
		assert.Error(t, err)
		assert.Nil(t, got)
		assert.Contains(t, err.Error(), "serialize request body")
		assert.Contains(t, err.Error(), "mock query error")
	})

	t.Run("typed nil querydsl request does not panic", func(t *testing.T) {
		var qr *querydsl.SearchRequest
		got, err := normalizeBody(qr) // non-nil interface holding nil pointer
		assert.NoError(t, err)
		assert.Nil(t, got)
	})

	t.Run("typed nil bodyProvider does not panic", func(t *testing.T) {
		var bp *mockBodyProvider
		got, err := normalizeBody(bp)
		assert.NoError(t, err)
		assert.Nil(t, got)
	})

	t.Run("custom bodyProvider is called", func(t *testing.T) {
		got, err := normalizeBody(&mockBodyProvider{body: `{"ok":true}`})
		assert.NoError(t, err)
		assert.Equal(t, json.RawMessage(`{"ok":true}`), got)
	})

	t.Run("custom bodyProvider error propagates", func(t *testing.T) {
		_, err := normalizeBody(&mockBodyProvider{err: fmt.Errorf("boom")})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "serialize request body")
	})

	t.Run("map passes through unchanged", func(t *testing.T) {
		in := map[string]any{"query": map[string]any{"match_all": map[string]any{}}}
		got, err := normalizeBody(in)
		assert.NoError(t, err)
		assert.Same(t, any(in), got) // same value, not re-serialized
	})

	t.Run("bridge output (json.RawMessage) passes through unchanged", func(t *testing.T) {
		req, err := NewSearchRequest(querydsl.NewSearchRequest().Query(querydsl.NewMatchAllQuery()))
		assert.NoError(t, err)
		got, err := normalizeBody(req.Body)
		assert.NoError(t, err)
		assert.Equal(t, req.Body, got)
	})
}
```

`options_test.go` must add the import `json "github.com/goccy/go-json"` (not currently imported
there; it imports fmt, testing, querydsl, types, testify/assert).

### Step 5 — `api/search_service_test.go`: wire-level test through `Search()`

Add imports: `"io"` and `"github.com/disaster37/opensearch/v4/querydsl"` (keep existing imports).
Append this test; it captures the bytes actually sent over the wire to prove there is no
double-encoding and that auto-serialization happens:

```go
func TestUnitSearchServiceSearchAutoSerialize(t *testing.T) {
	respJSON := `{"took":5,"timed_out":false,"_shards":{"total":1,"successful":1,"skipped":0,"failed":0},"hits":{"total":{"value":0,"relation":"eq"},"max_score":null,"hits":[]}}`

	var captured []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		captured, _ = io.ReadAll(r.Body)
		w.WriteHeader(200)
		_, _ = fmt.Fprint(w, respJSON)
	}))
	defer srv.Close()

	svc := NewSearchService(restyClient(srv), testLogger())
	ctx := context.Background()

	t.Run("querydsl request auto-serialized", func(t *testing.T) {
		qr := querydsl.NewSearchRequest().Query(querydsl.NewMatchAllQuery()).Size(5)
		expected, err := qr.Body()
		require.NoError(t, err)

		_, err = svc.Search(ctx, &SearchRequest{Indices: []string{"idx1"}, Body: qr})
		require.NoError(t, err)
		assert.JSONEq(t, expected, string(captured))
	})

	t.Run("string body sent as-is without double encoding", func(t *testing.T) {
		_, err := svc.Search(ctx, &SearchRequest{Indices: []string{"idx1"}, Body: `{"query":{"match_all":{}}}`})
		require.NoError(t, err)
		assert.JSONEq(t, `{"query":{"match_all":{}}}`, string(captured))
	})

	t.Run("byte slice body sent as-is", func(t *testing.T) {
		_, err := svc.Search(ctx, &SearchRequest{Indices: []string{"idx1"}, Body: []byte(`{"query":{"match_all":{}}}`)})
		require.NoError(t, err)
		assert.JSONEq(t, `{"query":{"match_all":{}}}`, string(captured))
	})

	t.Run("map body still marshaled by resty", func(t *testing.T) {
		_, err := svc.Search(ctx, &SearchRequest{
			Indices: []string{"idx1"},
			Body:    map[string]any{"query": map[string]any{"match_all": map[string]any{}}},
		})
		require.NoError(t, err)
		assert.JSONEq(t, `{"query":{"match_all":{}}}`, string(captured))
	})

	t.Run("bridge-built request still works", func(t *testing.T) {
		req, err := NewSearchRequest(querydsl.NewSearchRequest().Query(querydsl.NewMatchAllQuery()))
		require.NoError(t, err)
		req.Indices = []string{"idx1"}

		_, err = svc.Search(ctx, req)
		require.NoError(t, err)
		assert.JSONEq(t, `{"query":{"match_all":{}}}`, string(captured))
	})

	t.Run("Body() error propagates and no request is sent", func(t *testing.T) {
		captured = nil
		qr := querydsl.NewSearchRequest().Query(mockQueryError{})
		_, err := svc.Search(ctx, &SearchRequest{Indices: []string{"idx1"}, Body: qr})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "serialize request body")
		assert.Empty(t, captured) // failed before any HTTP call
	})

	t.Run("typed nil querydsl request sends no body and does not panic", func(t *testing.T) {
		captured = nil
		var qr *querydsl.SearchRequest
		_, err := svc.Search(ctx, &SearchRequest{Indices: []string{"idx1"}, Body: qr})
		require.NoError(t, err)
	})
}
```

Notes:
- `restyClient(srv)` (test_helpers_test.go:19) sets `Content-Type: application/json`, mirroring
  the production client (client.go:232), so the wire behavior is faithful.
- `mockQueryError` is defined in options_test.go (same package) and is reusable here.
- `assert.JSONEq` avoids flakiness from JSON key ordering.

## 6. Edge cases (all handled + tested)

| Case | Behavior |
|---|---|
| `Body: nil` (untyped) | `normalizeBody` returns `nil, nil`; `SetBody(nil)` — identical to today's behavior for nil bodies. |
| Empty string `""` | Becomes `json.RawMessage("")` → empty request body. Not an error ("as-is" semantics). |
| `Body()` returns error | Wrapped as `fmt.Errorf("serialize request body: %w", err)` and returned by `Search()` before any HTTP call. `%w` preserves `errors.Is/As`. |
| Pointer vs value receivers | `Body()` on `querydsl.SearchRequest` is pointer-receiver, so `*querydsl.SearchRequest` (what `querydsl.NewSearchRequest()` returns) auto-serializes. A bare value does not satisfy `bodyProvider` and falls through to resty marshaling (produces `{}` since fields are unexported) — accepted; the builder API always yields pointers. Documented in the `bodyProvider` doc comment. |
| `*json.RawMessage` | Dereferenced; the underlying `json.RawMessage` is used. |
| nil `*json.RawMessage` | Returns nil body (handled explicitly in the type switch). |
| Typed nil interface values | e.g. `var qr *querydsl.SearchRequest = nil` stored in `Body any`: `body != nil` is true but the pointer is nil; calling `Body()` would panic. `isNilValue` (reflect, Ptr/Interface kinds) detects this before the call and yields a nil body. Deliberate choice: treat as "no body", consistent with untyped nil (alternative: return an error — easy to swap if preferred during review). |
| nil `[]byte` | Becomes empty `json.RawMessage` (empty body), not a nil `json.RawMessage`. |
| Named types (e.g. `type myBytes []byte`) | Don't match the `[]byte` case; if they implement `Body()` they are detected, otherwise pass through to resty — same as today. |
| Body already built by the bridge `api.NewSearchRequest` | It is a `json.RawMessage`; returned unchanged. No double processing, no double wrap. |

## 7. Error handling

- Errors from `Body()` are wrapped once, inside `normalizeBody`:
  `fmt.Errorf("serialize request body: %w", err)` — same `%w` style as the bridge
  (`"search request body: %w"`, options.go:645) and `validationError` (`"validation: %w"`).
- `Search()` returns the error immediately, before building/sending the HTTP request, and without
  logging — consistent with `NewSearchRequest`, which treats serialization failures as caller-side
  errors. `wrapNetworkError` / `logAndReturnError` / `wrapUnmarshalError` (errors.go) remain
  reserved for transport- and response-level failures.
- No new error types; `*types.OpenSearchError` handling untouched.

## 8. Validation strategy

1. New unit tests: `TestNormalizeBody` (options_test.go) and
   `TestUnitSearchServiceSearchAutoSerialize` (search_service_test.go) — see steps 4-5.
2. Regression: `cd /projects/opensearch && go build ./... && go vet ./api/...`
3. Full unit suite (integration tests are behind the `integration` build tag and are skipped):
   `go test ./api/... ./querydsl/...` — all existing tests must pass unchanged
   (in particular `TestUnitSearchServiceSearch`, `TestNewSearchRequest`).
4. Optional end-to-end check if an OpenSearch instance is available:
   `go test -tags integration ./api/ -run TestSearchService_Search`.
5. Manual sanity check of the wire format is covered by the captured-body assertions
   (`assert.JSONEq` proves no double-encoding: a double-encoded body would be a JSON *string*,
   which fails `JSONEq` against an object).

## 9. Project conventions followed

- Unexported camelCase helpers (`normalizeBody`, `isNilValue`, `bodyProvider`) next to the
  existing `rawBody` in `options.go`; package-private scope since all services live in `api`.
- JSON via the package-wide alias `json "github.com/goccy/go-json"` (its `RawMessage` implements
  `encoding/json.Marshaler`, so resty's default encoder emits the raw bytes — same mechanism the
  bridge already relies on).
- Error wrapping with `fmt.Errorf("...: %w", err)`, message style matching existing wrappers.
- Tests: `TestUnit*` naming for unit tests, `httptest` + `restyClient`/`testLogger` helpers,
  testify `require` for preconditions and `assert` for checks, subtests via `t.Run`.
- resty usage unchanged: single `SetBody` call on the request chain; query params via
  `req.Params.ToMap()`; no header changes (client-level `Content-Type: application/json` already
  covers `json.RawMessage` bodies).
- Go version 1.26 (go.mod) — `any` is fine; no generics needed.

## 10. No breaking changes — checklist

- `SearchRequest` struct, `SearchService` interface, and `NewSearchRequest` bridge: untouched.
- `string` / `[]byte` / `json.RawMessage` bodies: transmitted bytes identical (raw bytes in,
  raw bytes out); content type remains `application/json` via client.go:232.
- map / struct bodies: returned unchanged from `normalizeBody`, so resty marshals them exactly as
  before (verified by existing `TestUnitSearchServiceSearch` + new map wire test).
- nil body: `SetBody(nil)` exactly as today.
- Bridge path (`api.NewSearchRequest` → `Body` is `json.RawMessage`): passes through unchanged.
- Only new behavior: objects implementing `Body() (string, error)` are now serialized instead of
  being marshaled to `{}` — which is the point of the feature and cannot break callers that
  previously worked around the limitation by calling `.Body()` themselves (their string /
  `json.RawMessage` falls into the "already serialized" branch).

## 11. Out of scope / future work

- Applying `normalizeBody` to `MultiSearch`, `SearchTemplate`, `MultiSearchTemplate`, `RankEval`,
  `Count`, `Validate`, and other `Body any` endpoints. The helper is deliberately generic so each
  is a future one-line change (`body, err := normalizeBody(...)` before `SetBody`). Note:
  `MultiSearch`/`MultiSearchTemplate` use NDJSON — any future wiring there must be reviewed
  separately since `Body()` produces single-line JSON only.
- Exporting the helper or the `bodyProvider` interface (not needed; all callers are in-package).
- Changing `querydsl` itself.
