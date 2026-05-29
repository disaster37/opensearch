# opensearch-go

[![Go Reference](https://pkg.go.dev/badge/github.com/disaster37/opensearch/v3.svg)](https://pkg.go.dev/github.com/disaster37/opensearch/v3)
[![Go Report Card](https://goreportcard.com/badge/github.com/disaster37/opensearch/v3)](https://goreportcard.com/report/github.com/disaster37/opensearch/v3)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

Go client for [OpenSearch](https://opensearch.org/) and OpenSearch-compatible clusters.

- Type-safe interfaces for all 16 OpenSearch API groups
- Fluent query-builder DSL (`querydsl/`) — no raw JSON required
- Struct-literal configuration (value semantics, no pointer surprise)
- Built on [resty](https://github.com/go-resty/resty) — middleware, retries, timeouts
- Structured logging via [logrus](https://github.com/sirupsen/logrus)
- OpenTelemetry tracing middleware (`trace/opentelemetry/`)

## Install

```bash
go get github.com/disaster37/opensearch/v3
```

Requires Go 1.18+ (uses generics in `api/` and `querydsl/`).

## Quick Start

```go
package main

import (
    "context"
    "log"

    os "github.com/disaster37/opensearch/v3"
    "github.com/sirupsen/logrus"
)

func main() {
    logger := logrus.NewEntry(logrus.StandardLogger())

    client, err := os.New(&os.Config{
        URL:      "https://localhost:9200",
        Username: "admin",
        Password: "admin",
    }, logger)
    if err != nil {
        log.Fatal(err)
    }

    ctx := context.Background()

    // Index a document
    doc := map[string]any{
        "name":  "OpenSearch",
        "stars": 10000,
    }
    resp, err := client.Document().Index(ctx, "repos", nil, doc, nil)
    if err != nil {
        log.Fatal(err)
    }
    log.Printf("Indexed %s/%s\n", resp.Index, resp.ID)
}
```

## Using the Query DSL

Build type-safe queries without raw JSON maps:

```go
import "github.com/disaster37/opensearch/v3/querydsl"

// Bool query with must/should/filter clauses
query := querydsl.Bool{
    Must: []querydsl.Query{
        querydsl.Match{"status": "published"},
    },
    Should: []querydsl.Query{
        querydsl.Term{"Field": "featured", "Value": true, "Boost": ptr(1.5)},
    },
    Filter: []querydsl.Query{
        querydsl.Range{
            Field:     "date",
            Gte:       "2024-01-01",
            Format:    "yyyy-MM-dd",
        },
    },
    MinimumShouldMatch: "1",
}
```

### Aggregations

```go
// Terms aggregation with sub-aggregation
agg := querydsl.TermsAggregation("genre").
    Size(20).
    SubAggregation("avg_rating", querydsl.AvgAggregation("rating"))

// Date histogram for time-series
histogram := querydsl.DateHistogramAggregation("timestamp").
    CalendarInterval("1d").
    MinDocCount(0).
    SubAggregation("total_sales", querydsl.SumAggregation("amount"))
```

## Available Services

All services are accessed through the `Client` interface:

| Service | Accessor | Description |
|---------|----------|-------------|
| Document | `client.Document()` | Index, Get, Update, Delete, Bulk, Reindex |
| Search | `client.Search()` | Search, Count, Scroll, MultiSearch, PIT |
| Indices | `client.Indices()` | Create, Delete, Settings, Mappings, Templates |
| Cluster | `client.Cluster()` | Health, State, Stats, Settings |
| Nodes | `client.Nodes()` | Info, Stats |
| Cat | `client.Cat()` | Human-readable cluster info |
| Ingest | `client.Ingest()` | Pipeline management |
| Snapshot | `client.Snapshot()` | Snapshots and repositories |
| Tasks | `client.Tasks()` | Task management |
| Script | `client.Script()` | Stored scripts |
| Security | `client.Security()` | Users, roles, tenants (Security plugin) |
| ISM | `client.ISM()` | Index State Management |
| SM | `client.SM()` | Snapshot Management |
| Alerting | `client.Alerting()` | Monitors and alerts |
| Transform | `client.Transform()` | Transform jobs |
| CCR | `client.CCR()` | Cross-Cluster Replication |

## Error Handling

All API errors are returned as `*types.OpenSearchError`:

```go
result, err := client.Document().Get(ctx, "my-index", "doc-id", nil)
if err != nil {
    if os.IsNotFound(err) {
        log.Println("Document not found")
    } else if os.IsConflict(err) {
        log.Println("Version conflict — retry or refresh")
    } else {
        var osErr *types.OpenSearchError
        if errors.As(err, &osErr) {
            log.Printf("OpenSearch error %d: %s\n",
                osErr.Status, osErr.Details.Reason)
        } else {
            log.Fatal(err)
        }
    }
}
```

## Optimistic Concurrency

Use `DocumentVersion` to prevent overwrites:

```go
// First fetch
doc, err := client.Document().Get(ctx, "my-index", "doc-id", nil)

// Update with version
_, err = client.Document().Update(ctx, "my-index", "doc-id", newDoc,
    &os.DocumentVersion{
        SeqNo:       doc.SeqNo,
        PrimaryTerm: doc.PrimaryTerm,
    },
    nil)
```

## Configuration

```go
import (
    "time"

    os "github.com/disaster37/opensearch/v3"
)

client, err := os.New(&os.Config{
    URL:           "https://localhost:9200",
    Username:      "admin",
    Password:      "admin",
    TLSSkipVerify: false, // production: always false
    CACert:        caCertPEM,
    Timeout:       30 * time.Second,
}, logger)
```

## OpenTelemetry Tracing

Wrap the client with the OTel trace middleware:

```go
import (
    otel "github.com/disaster37/opensearch/v3/trace/opentelemetry"
)

client, _ := os.New(cfg, logger)
otel.WrapClient(client.RestyClient(), otel.WithServiceName("my-service"))
```

## Testing

```bash
go test ./...
```

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

MIT — see [LICENSE](LICENSE).

## Project Structure

```
opensearch-go/
├── client.go          # Client interface + New()
├── common.go          # Re-exported type aliases
├── errors.go          # Error helpers
├── types/             # Common types (ShardsInfo, OpenSearchError, etc.)
├── api/               # 16 service interfaces + models
│   ├── document_service.go
│   ├── search_service.go
│   ├── indices_service.go
│   └── ...
├── querydsl/          # Query and aggregation builder DSL
│   ├── search_queries_*.go
│   ├── search_aggs_*.go
│   └── search_source.go
└── trace/opentelemetry/
```
