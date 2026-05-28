# How to Contribute

This is the Go client for [OpenSearch](https://opensearch.org/), module `github.com/disaster37/opensearch/v3`. It targets **OpenSearch 3.4.0+**.

---

## Branch Strategy

| Branch | Target |
|--------|--------|
| `release-branch.v3` | OpenSearch 3.x (active development) |
| `release-branch.v2` | OpenSearch 2.x (maintenance only) |

**Always create your feature or fix branch from `release-branch.v3`:**

```bash
git checkout release-branch.v3
git pull origin release-branch.v3
git checkout -b feature/my-feature
# or
git checkout -b fix/my-fix
```

---

## Project Structure

This is a **flat Go package** — all source files live at the repository root. There are no `cmd/`, `pkg/`, or `internal/` subdirectories.

```
/
├── *.go                    # All API implementations and types
├── ci/dagger/main.go       # Dagger CI pipeline definition
├── config/                 # Configuration helpers
├── recipes/                # Usage examples
├── trace/                  # OpenTelemetry tracing support
├── uritemplates/           # URI template utilities
├── dagger.json             # Dagger module configuration
├── go.mod                  # Go module (github.com/disaster37/opensearch/v3)
└── go.sum
```

---

## File Naming Conventions

Follow these patterns strictly when adding new files:

| Pattern | Purpose | Example |
|---------|---------|---------|
| `{resource}_{action}.go` | API implementation | `indices_create.go`, `snapshot_delete.go` |
| `{resource}_{action}_test.go` | Unit tests | `indices_create_test.go` |
| `{resource}_{action}_integration_test.go` | Integration tests | `indices_create_integration_test.go` |
| `search_queries_{type}.go` | Query DSL types | `search_queries_bool.go` |
| `search_aggs_{category}_{type}.go` | Aggregations | `search_aggs_bucket_terms.go` |
| `security_{action}_{resource}.go` | Security plugin APIs | `security_put_role.go` |
| `{plugin}_{action}_policy.go` | Plugin policy management | `ism_put_policy.go`, `sm_get_policy.go` |
| `ccr_{action}.go` | Cross-cluster replication | `ccr_start.go`, `ccr_pause.go` |
| `transform_{action}_job.go` | Transform jobs | `transform_put_job.go` |
| `*_easyjson.go` | Generated (do not edit) | `bulk_index_request_easyjson.go` |

**Key rules:**
- Use lowercase and underscores only
- Group by resource prefix (e.g., all `indices_*` files together)
- Keep one API action per file
- Tests go in the same package (`package opensearch`, not `package opensearch_test`)

---

## Writing Code

### API Implementation Pattern

Each API action file should contain:
1. A **service struct** with configuration fields
2. A **constructor** function (e.g., `NewIndicesCreateService`)
3. **Chainable setter methods** for options
4. A **Do(ctx)** method that executes the request
5. A **response struct** for the JSON response

All files are in `package opensearch`.

### Writing Tests

- Unit tests: `{resource}_{action}_test.go`
- Integration tests: `{resource}_{action}_integration_test.go`
- Use the test helper `setupTestClientAndCreateIndex(t)` from `setup_test.go`
- Use `context.TODO()` for API calls in tests
- Use standard `testing` package with `t.Fatal`/`t.Errorf`
- Integration tests require a running OpenSearch cluster (handled by Dagger)

Example:
```go
package opensearch

import (
    "context"
    "testing"
)

func TestMyFeature(t *testing.T) {
    client := setupTestClientAndCreateIndex(t)
    
    res, err := client.MyFeature().Do(context.TODO())
    if err != nil {
        t.Fatal(err)
    }
    if res == nil {
        t.Errorf("expected response, got nil")
    }
}
```

---

## Running CI (Dagger)

All CI is managed via [Dagger](https://dagger.io/). No Makefile exists. The pipeline is defined in `ci/dagger/main.go`.

### Run the full CI pipeline

```bash
dagger call --src . ci
```

This runs: **build** -> **lint** -> **format** -> **test** -> codecov upload -> git commit/push.

### Lint only

```bash
dagger call --src . lint
```

### Test only

```bash
dagger call --src . test
```

The test command spins up a two-node OpenSearch cluster (leader + follower) with security enabled, then runs all tests with coverage via `gotestsum`.

### Debug a specific test

```bash
dagger call --src . debug-test --run TestMyFeature up
```

This starts a Delve debugger on port 4000. Connect with your IDE:

**VS Code** (`.vscode/launch.json`):
```json
{
    "version": "0.2.0",
    "configurations": [
        {
            "name": "Connect to Dagger debugger",
            "type": "go",
            "request": "attach",
            "mode": "remote",
            "remotePath": "/src",
            "port": 4000,
            "host": "127.0.0.1"
        }
    ]
}
```

---

## Pull Request Checklist

Before submitting your PR:

- [ ] Branch is based on `release-branch.v3`
- [ ] One feature or fix per PR (do not mix concerns)
- [ ] File naming follows the conventions above
- [ ] Code is formatted: `go fmt ./...`
- [ ] Unit tests are written (`*_test.go`)
- [ ] Integration tests are written (`*_integration_test.go`) — these run against a real OpenSearch cluster via Dagger
- [ ] All tests pass: `dagger call --src . test`
- [ ] Lint passes: `dagger call --src . lint`
- [ ] Commit message is meaningful and explains the "why"
- [ ] PR description links to the relevant issue and/or OpenSearch documentation

---

## AI Agent Instructions

When contributing as an AI agent:

1. **Always branch from `release-branch.v3`** — never commit directly to it
2. **Respect naming conventions exactly** — the flat structure means file names are the only organizational mechanism
3. **Always create both implementation and test files** — no code without tests (unit tests in `*_test.go` and integration tests in `*_integration_test.go`)
4. **Always create integration tests** — integration tests run against a real OpenSearch cluster started by Dagger; they validate the feature works end-to-end
5. **Run `dagger call --src . test` and `dagger call --src . lint`** to validate before submitting
5. **Look at existing similar files** as reference implementations (e.g., if adding a new `indices_*` action, look at `indices_create.go` and `indices_create_test.go`)
6. **Do not edit `*_easyjson.go` files** — they are generated
7. **Keep all files in the root package** (`package opensearch`) — do not create subdirectories for new features
8. **Use `emperror.dev/errors`** for error wrapping (not `fmt.Errorf` with `%w`)

---

## Additional Resources

- [OpenSearch documentation](https://opensearch.org/docs/latest/)
- [OpenSearch REST API reference](https://opensearch.org/docs/latest/api-reference/)
- [GitHub pull request documentation](https://docs.github.com/en/pull-requests)
- [Dagger documentation](https://docs.dagger.io/)
