// Package api provides service interfaces for interacting with OpenSearch
// REST API endpoints.
//
// The api package contains 26 service interfaces, each corresponding to a
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
//   - RollupService: Rollup job operations
//   - MlService: Machine Learning operations
//   - SqlService: SQL operations
//   - AdService: Anomaly Detection operations
//   - AsyncSearchService: Async search operations
//   - KnnService: k-NN operations
//   - NeuralService: Neural search operations
//   - TieringService: Tiering operations (hot/warm, OpenSearch 3.7.0+)
//   - IngestionService: Pull-based ingestion control (pause/resume/state, GA in 3.6.0)
//   - InfoService: Cluster information
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
//	resp, err := client.Document().Index(ctx, &api.IndexRequest{
//	    Index: "my-index", Id: "doc-1", Body: doc,
//	    Params: &api.IndexParams{Refresh: api.RefreshTrue},
//	})
//
//	// Search for documents
//	query := map[string]any{
//	    "query": map[string]any{
//	        "match": map[string]any{
//	            "title": "Document",
//	        },
//	    },
//	}
//	result, err := client.Search().Search(ctx, &api.SearchRequest{
//	    Indices: []string{"my-index"}, Body: query,
//	})
//
// # Request Parameters
//
// Methods with three or more parameters accept typed request structs with
// query parameter sub-structs. All param structs implement a ToMap() method
// that converts them to URL query parameters:
//
//	params := &api.IndexParams{
//	    Routing:             "custom-routing",
//	    WaitForActiveShards: "2",
//	    Refresh:             api.RefreshTrue,
//	    Timeout:             "5s",
//	}
//
// Refer to the OpenSearch documentation for all available parameters for each
// API endpoint.
//
// # OpenSearch 3.8.0 Features
//
// Modify data stream backing indices:
//
//	resp, err := client.Indices().ModifyDataStream(ctx, &api.ModifyDataStreamRequest{
//	    Actions: []*api.ModifyDataStreamAction{
//	        {
//	            Type:       api.DataStreamActionRemoveBackingIndex,
//	            DataStream: "logs-ds",
//	            Index:      ".ds-logs-ds-000001",
//	        },
//	    },
//	})
//
// Multivalue doc count aggregation (counts docs with ≥2 values per field):
//
//	aggs := querydsl.NewSearchSource().
//	    Aggregation("mv", querydsl.NewMultiValueDocCountAggregation().Field("tags"))
//	src, _ := aggs.Source()
//	result, _ := client.Search().Search(ctx, &api.SearchRequest{
//	    Indices: []string{"my-index"}, Body: src,
//	})
//	mv, _ := result.Aggregations.MultiValueDocCount("mv")
//	fmt.Println(mv.Value)
//
// Tiering (hot/warm, OpenSearch 3.7.0+):
//
//	status, err := client.Tiering().GetStatus(ctx, "my-index", true)
//	list, err := client.Tiering().ListStatus(ctx, api.TierTargetWarm)
//
// Pull-based ingestion control (GA in 3.6.0):
//
//	state, err := client.Ingestion().GetState(ctx, &api.IngestionGetStateRequest{
//	    Index: "my-index",
//	})
//
// X-Request-Id header on search (OpenSearch 3.5.0+):
//
//	result, _ := client.Search().Search(ctx, &api.SearchRequest{
//	    Indices:   []string{"my-index"},
//	    Body:      body,
//	    RequestId: "4bf92f3577b34da6a3ce929d0e0e4736",
//	})
//
// Block cache pruning (OpenSearch 3.7.0+):
//
//	resp, err := client.Cluster().PruneBlockCache(ctx, &api.PruneBlockCacheParams{
//	    Nodes: []string{"warm-node-1"},
//	})
//
// Nodes stats with detailed file cache (OpenSearch 3.7.0+):
//
//	resp, err := client.Nodes().Stats(ctx, &api.NodesStatsRequest{
//	    Metrics:  []string{"file_cache"},
//	    Detailed: true,
//	})
//
// Refresh search analyzers (ISM plugin, OpenSearch 3.7.0+):
//
//	resp, err := client.ISM().RefreshSearchAnalyzers(ctx, "my-index")
//
// Bitmap64 terms query (OpenSearch 3.6.0+):
//
//	q := querydsl.NewTermsQuery("employee_id", base64Bitmap).
//	    WithValueType("bitmap")
package api
