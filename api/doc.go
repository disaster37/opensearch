// Package api provides service interfaces for interacting with OpenSearch
// REST API endpoints.
//
// The api package contains 16 service interfaces, each corresponding to a
// major OpenSearch feature area:
//
//   - DocumentService: Document CRUD operations
//   - SearchService: Search, count, scroll, and validate operations
//   - IndicesService: Index lifecycle and settings management
//   - ClusterService: Cluster health, state, and settings
//   - NodesService: Node information and statistics
//   - CatService: Human-readable cluster information (_cat APIs)
//   - IngestService: Ingest pipeline management
//   - SnapshotService: Snapshot and repository management
//   - TasksService: Task management and monitoring
//   - ScriptService: Script management
//   - SecurityService: Security plugin operations (roles, users, tenants)
//   - IsmService: Index State Management operations
//   - SmService: Snapshot Management operations
//   - AlertingService: Alerting monitor operations
//   - TransformService: Transform job operations
//   - CcrService: Cross-Cluster Replication operations
//
// # Accessing Services
//
// Services are accessed through the root Client interface:
//
//	client, err := opensearch.New(&opensearch.Config{
//	    URL:      "https://localhost:9200",
//	    Username: "admin",
//	    Password: "admin",
//	})
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	// Index a document
//	doc := map[string]any{
//	    "title": "My Document",
//	    "tags":  []string{"example", "test"},
//	}
//	resp, err := client.Document().Index(ctx, "my-index", "doc-1", doc, nil)
//
//	// Search for documents
//	query := map[string]any{
//	    "query": map[string]any{
//	        "match": map[string]any{
//	            "title": "Document",
//	        },
//	    },
//	}
//	result, err := client.Search().Search(ctx, []string{"my-index"}, query, nil)
//
// # Request Parameters
//
// Many methods accept a params parameter (map[string]string) for query string
// arguments. Common parameters include:
//
//	params := map[string]string{
//	    "routing":               "custom-routing",
//	    "wait_for_active_shards": "2",
//	    "refresh":               "true",
//	    "timeout":               "5s",
//	}
//
// Refer to the OpenSearch documentation for all available parameters for each
// API endpoint.
package api
