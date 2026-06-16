# Plan: Complete `_open` Index API parameters

## Context / Findings

The request mentions exposing `wait_for_active_shards` on the Open service and
checking whether any other `_open` parameters are missing.

Findings from `indices_open.go`:

- `WaitForActiveShards` is **already exposed** (`indices_open.go:124-130`) and
  wired into `buildURL` (lines 171-173). No action required.
- Comparing the current service against the OpenSearch Open Index API query
  parameters:

  | Parameter | Exposed? |
  |---|---|
  | `allow_no_indices` | yes |
  | `expand_wildcards` | yes |
  | `ignore_unavailable` | yes |
  | `wait_for_active_shards` | yes |
  | `master_timeout` (deprecated) | yes |
  | `timeout` | yes |
  | `cluster_manager_timeout` | **NO (missing)** |

The only missing parameter is `cluster_manager_timeout`, the OpenSearch-preferred
replacement for the deprecated `master_timeout`. The repo already has an
established pattern for it in `cluster_get_setting.go:120` and
`cluster_put_setting.go:114`.

## Changes

### 1. `indices_open.go`

- Add struct field next to `masterTimeout`:
  ```go
  clusterManagerTimeout string
  ```
- Add a setter method (mirroring the `MasterTimeout` doc/style):
  ```go
  // ClusterManagerTimeout specifies the timeout for connection to the
  // cluster manager node. Preferred over the deprecated MasterTimeout.
  func (s *IndicesOpenService) ClusterManagerTimeout(clusterManagerTimeout string) *IndicesOpenService {
      s.clusterManagerTimeout = clusterManagerTimeout
      return s
  }
  ```
- In `buildURL`, after the `master_timeout` block (line 159-161), add:
  ```go
  if s.clusterManagerTimeout != "" {
      params.Set("cluster_manager_timeout", s.clusterManagerTimeout)
  }
  ```

### 2. `indices_open_test.go` (optional but recommended)

- Add a `buildURL` test asserting the query string contains
  `cluster_manager_timeout` (and `wait_for_active_shards`) when set, following
  the pattern used in other `*_test.go` buildURL tests in the repo.

## Validation

- `go build ./...`
- `go vet ./...`
- `go test -run TestIndicesOpen ./...`

## Notes

- Keep `MasterTimeout` for backward compatibility; `cluster_manager_timeout` is
  additive.
- No response-struct changes needed.
