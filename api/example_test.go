package api_test

import (
	"context"
	"fmt"
	"log"

	"github.com/disaster37/opensearch/v4/api"
	"github.com/disaster37/opensearch/v4/querydsl"
	"github.com/sirupsen/logrus"
)

func ExampleDocumentService_Index() {
	ctx := context.Background()
	logger := logrus.NewEntry(logrus.New())

	_ = logger

	doc := map[string]any{
		"title": "Example Document",
		"tags":  []string{"example", "test"},
		"count": 42,
	}

	var docSvc api.DocumentService
	if docSvc == nil {
		fmt.Println("DocumentService.Index(ctx, *IndexRequest)")
		return
	}

	resp, err := docSvc.Index(ctx, &api.IndexRequest{
		Index: "my-index",
		Id:    "doc-1",
		Body:  doc,
		Params: map[string]string{
			"refresh": "true",
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("indexed %s into %s (version %d)\n", resp.Id, resp.Index, resp.Version)
	// Output:
	// DocumentService.Index(ctx, *IndexRequest)
}

func ExampleDocumentService_Bulk() {
	ctx := context.Background()

	bulkBody := `{"index":{"_id":"1"}}
{"title":"First","count":1}
{"index":{"_id":"2"}}
{"title":"Second","count":2}
{"delete":{"_id":"3"}}
`

	var docSvc api.DocumentService
	if docSvc == nil {
		fmt.Println("DocumentService.Bulk(ctx, index, body)")
		return
	}

	resp, err := docSvc.Bulk(ctx, "my-index", bulkBody)
	if err != nil {
		log.Fatal(err)
	}

	if resp.Errors {
		for _, item := range resp.Items {
			for op, result := range item {
				if result.Error != nil {
					log.Printf("bulk %s failed for id=%s: %s", op, result.Id, result.Error.Reason)
				}
			}
		}
	}

	fmt.Printf("bulk took %d ms, errors=%v\n", resp.Took, resp.Errors)
	// Output:
	// DocumentService.Bulk(ctx, index, body)
}

func ExampleSearchService_Search() {
	ctx := context.Background()

	searchSource := querydsl.NewSearchSource().
		Query(querydsl.NewMatchQuery("title", "example")).
		Size(10).
		From(0)

	body, err := searchSource.Source()
	if err != nil {
		log.Fatal(err)
	}

	var searchSvc api.SearchService
	if searchSvc == nil {
		fmt.Println("SearchService.Search(ctx, *SearchRequest)")
		return
	}

	result, err := searchSvc.Search(ctx, &api.SearchRequest{
		Indices: []string{"my-index"},
		Body:    body,
	})
	if err != nil {
		log.Fatal(err)
	}

	if result.Hits != nil {
		fmt.Printf("total hits: %d\n", result.Hits.TotalHits.Value)
		for _, hit := range result.Hits.Hits {
			fmt.Printf("  hit: id=%s, score=%f\n", hit.Id, *hit.Score)
		}
	}
	// Output:
	// SearchService.Search(ctx, *SearchRequest)
}

func ExampleSearchService_Count() {
	ctx := context.Background()

	var searchSvc api.SearchService
	if searchSvc == nil {
		fmt.Println("SearchService.Count(ctx, indices, body)")
		return
	}

	count, err := searchSvc.Count(ctx, []string{"my-index"}, nil)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("document count: %d\n", count)
	// Output:
	// SearchService.Count(ctx, indices, body)
}

func ExampleIndicesService_Create() {
	ctx := context.Background()

	body := map[string]any{
		"settings": map[string]any{
			"number_of_shards":   3,
			"number_of_replicas": 1,
		},
		"mappings": map[string]any{
			"properties": map[string]any{
				"title": map[string]any{"type": "text"},
				"count": map[string]any{"type": "integer"},
				"tags":  map[string]any{"type": "keyword"},
			},
		},
	}

	var idxSvc api.IndicesService
	if idxSvc == nil {
		fmt.Println("IndicesService.Create(ctx, index, body)")
		return
	}

	resp, err := idxSvc.Create(ctx, "my-index", body)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("acknowledged: %v\n", resp.Acknowledged)
	// Output:
	// IndicesService.Create(ctx, index, body)
}

func ExampleIndicesService_Exists() {
	ctx := context.Background()

	var idxSvc api.IndicesService
	if idxSvc == nil {
		fmt.Println("IndicesService.Exists(ctx, indices) -> bool")
		return
	}

	exists, err := idxSvc.Exists(ctx, []string{"my-index"})
	if err != nil {
		log.Fatal(err)
	}

	if exists {
		fmt.Println("index exists")
	} else {
		fmt.Println("index does not exist")
	}
	// Output:
	// IndicesService.Exists(ctx, indices) -> bool
}

func ExampleClusterService_Health() {
	ctx := context.Background()

	var clusterSvc api.ClusterService
	if clusterSvc == nil {
		fmt.Println("ClusterService.Health(ctx, indices)")
		return
	}

	health, err := clusterSvc.Health(ctx, nil)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("cluster %s status: %s (nodes: %d)\n",
		health.ClusterName, health.Status, health.NumberOfNodes)
	// Output:
	// ClusterService.Health(ctx, indices)
}

func ExampleIsmService_GetPolicy() {
	ctx := context.Background()

	var ismSvc api.IsmService
	if ismSvc == nil {
		fmt.Println("IsmService.GetPolicy(ctx, policyName)")
		return
	}

	resp, err := ismSvc.GetPolicy(ctx, "my-ism-policy")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("policy %s (version %d, default_state: %s)\n",
		resp.Id, resp.Version, strVal(resp.Policy.DefaultState))

	defaultState := "hot"
	policyBase := &api.IsmPolicyBase{
		Description:  strPtr("My lifecycle policy"),
		DefaultState: &defaultState,
		States: []api.IsmPolicyState{
			{
				Name: "hot",
				Actions: []map[string]any{
					{"rollover": map[string]any{"min_doc_count": 5}},
				},
				Transitions: []api.IsmPolicyStateTransition{
					{StateName: "warm", Conditions: map[string]any{"min_index_age": "30d"}},
				},
			},
			{Name: "warm"},
		},
	}

	_, _ = ismSvc.PutPolicy(ctx, &api.IsmPutPolicyRequest{
		PolicyName: "my-ism-policy",
		Body:       policyBase,
	})
	// Output:
	// IsmService.GetPolicy(ctx, policyName)
}

func ExampleSecurityService_PutRole() {
	ctx := context.Background()

	var secSvc api.SecurityService
	if secSvc == nil {
		fmt.Println("SecurityService.PutRole(ctx, roleName, body)")
		return
	}

	role := &api.SecurityPutRole{
		Description: strPtr("Read-only role for logs indices"),
		ClusterPermissions: []string{
			"cluster_composite_ops_ro",
		},
		IndexPermissions: []api.SecurityIndexPermissions{
			{
				IndexPatterns:  []string{"logs-*"},
				AllowedActions: []string{"indices:data/read/search", "indices:data/read/get"},
			},
		},
		TenantPermissions: []api.SecurityTenantPermissions{
			{
				TenantPatterns: []string{"global_tenant"},
				AllowedActions: []string{"kibana_all_read"},
			},
		},
	}

	resp, err := secSvc.PutRole(ctx, "logs-reader", role)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("status: %s, message: %s\n", resp.Status, resp.Message)
	// Output:
	// SecurityService.PutRole(ctx, roleName, body)
}

func ExampleSnapshotService_Create() {
	ctx := context.Background()

	var snapSvc api.SnapshotService
	if snapSvc == nil {
		fmt.Println("SnapshotService.Create(ctx, *SnapshotCreateRequest)")
		return
	}

	body := map[string]any{
		"indices":              "my-index,logs-*",
		"ignore_unavailable":   true,
		"include_global_state": false,
	}

	resp, err := snapSvc.Create(ctx, &api.SnapshotCreateRequest{
		Repository: "my-repo",
		Snapshot:   "snap-2024.01",
		Body:       body,
	})
	if err != nil {
		log.Fatal(err)
	}

	if resp.Snapshot != nil {
		fmt.Printf("snapshot %s state: %s\n", resp.Snapshot.Snapshot, resp.Snapshot.State)
	}
	// Output:
	// SnapshotService.Create(ctx, *SnapshotCreateRequest)
}

func strPtr(s string) *string { return &s }

func strVal(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func ExampleDocumentService_Create() {
	ctx := context.Background()

	doc := map[string]any{
		"title": "Strictly unique document",
	}

	var docSvc api.DocumentService
	if docSvc == nil {
		fmt.Println("DocumentService.Create(ctx, *CreateRequest)")
		return
	}

	resp, err := docSvc.Create(ctx, &api.CreateRequest{
		Index:  "my-index",
		Id:     "strictly-unique-id",
		Body:   doc,
		Params: map[string]string{"refresh": "true"},
	})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("created %s in %s (result: %s)\n", resp.Id, resp.Index, resp.Result)
	// Output:
	// DocumentService.Create(ctx, *CreateRequest)
}

func ExampleSearchService_CreatePIT() {
	ctx := context.Background()

	var searchSvc api.SearchService
	if searchSvc == nil {
		fmt.Println("SearchService.CreatePIT(ctx, *CreatePITRequest)")
		return
	}

	resp, err := searchSvc.CreatePIT(ctx, &api.CreatePITRequest{
		Indices:   []string{"my-index"},
		KeepAlive: "5m",
	})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("PIT id=%s keep_alive=%s\n", resp.PitId, resp.KeepAlive)
	// Output:
	// SearchService.CreatePIT(ctx, *CreatePITRequest)
}

func ExampleSearchService_SearchTemplate() {
	ctx := context.Background()

	var searchSvc api.SearchService
	if searchSvc == nil {
		fmt.Println("SearchService.SearchTemplate(ctx, *SearchTemplateRequest)")
		return
	}

	resp, err := searchSvc.SearchTemplate(ctx, &api.SearchTemplateRequest{
		Indices: []string{"my-index"},
		Body: map[string]any{
			"id": "my-search-template",
			"params": map[string]any{
				"query_string": "opensearch",
			},
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("template returned %d hits\n", len(resp.Hits.Hits))
	// Output:
	// SearchService.SearchTemplate(ctx, *SearchTemplateRequest)
}

func ExampleIndicesService_ResolveIndex() {
	ctx := context.Background()

	var idxSvc api.IndicesService
	if idxSvc == nil {
		fmt.Println("IndicesService.ResolveIndex(ctx, name)")
		return
	}

	resp, err := idxSvc.ResolveIndex(ctx, "my-index*")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("resolved %d indices, %d aliases, %d data streams\n",
		len(resp.Indices), len(resp.Aliases), len(resp.DataStreams))
	// Output:
	// IndicesService.ResolveIndex(ctx, name)
}

func ExampleIndicesService_DataStreamsStats() {
	ctx := context.Background()

	var idxSvc api.IndicesService
	if idxSvc == nil {
		fmt.Println("IndicesService.DataStreamsStats(ctx, names)")
		return
	}

	resp, err := idxSvc.DataStreamsStats(ctx, []string{"logs-*"})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("%d data streams, %d backing indices\n",
		resp.DataStreamCount, resp.BackingIndices)
	// Output:
	// IndicesService.DataStreamsStats(ctx, names)
}

func ExampleClusterService_AllocationExplain() {
	ctx := context.Background()

	var clusterSvc api.ClusterService
	if clusterSvc == nil {
		fmt.Println("ClusterService.AllocationExplain(ctx, body)")
		return
	}

	resp, err := clusterSvc.AllocationExplain(ctx, map[string]any{
		"index":   "my-index",
		"shard":   0,
		"primary": true,
	})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("shard state: %s\n", resp.CurrentState)
	// Output:
	// ClusterService.AllocationExplain(ctx, body)
}

func ExampleSecurityService_Health() {
	ctx := context.Background()

	var secSvc api.SecurityService
	if secSvc == nil {
		fmt.Println("SecurityService.Health(ctx)")
		return
	}

	resp, err := secSvc.Health(ctx)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("security status: %s\n", resp.Status)
	// Output:
	// SecurityService.Health(ctx)
}

func ExampleSecurityService_WhoAmI() {
	ctx := context.Background()

	var secSvc api.SecurityService
	if secSvc == nil {
		fmt.Println("SecurityService.WhoAmI(ctx)")
		return
	}

	resp, err := secSvc.WhoAmI(ctx)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("authenticated as DN=%s is_admin=%t\n", resp.Dn, resp.IsAdmin)
	// Output:
	// SecurityService.WhoAmI(ctx)
}

func ExampleAlertingService_ExecuteMonitor() {
	ctx := context.Background()

	var alertSvc api.AlertingService
	if alertSvc == nil {
		fmt.Println("AlertingService.ExecuteMonitor(ctx, monitorId, body)")
		return
	}

	resp, err := alertSvc.ExecuteMonitor(ctx, "my-monitor-id", nil)
	if err != nil {
		log.Fatal(err)
	}

	if resp.Error != nil {
		fmt.Printf("monitor run failed: %s\n", *resp.Error)
	}
	fmt.Printf("monitor %s executed\n", resp.MonitorName)
	// Output:
	// AlertingService.ExecuteMonitor(ctx, monitorId, body)
}

func ExampleIsmService_AddPolicy() {
	ctx := context.Background()

	var ismSvc api.IsmService
	if ismSvc == nil {
		fmt.Println("IsmService.AddPolicy(ctx, index, body)")
		return
	}

	resp, err := ismSvc.AddPolicy(ctx, "my-index-*", map[string]string{
		"policy_id": "hot-warm-cold",
	})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("updated %d indices\n", resp.UpdatedIndices)
	// Output:
	// IsmService.AddPolicy(ctx, index, body)
}

func ExampleRollupService_PutRollup() {
	ctx := context.Background()

	var rollupSvc api.RollupService
	if rollupSvc == nil {
		fmt.Println("RollupService.PutRollup(ctx, rollupId, body)")
		return
	}

	resp, err := rollupSvc.PutRollup(ctx, "hourly-rollup", map[string]any{
		"rollup_id":    "hourly-rollup",
		"source_index": "logs-2024.01",
		"target_index": "rollup-logs",
		"schedule": map[string]any{
			"interval": map[string]any{"period": 1, "unit": "Hours"},
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("rollup %s created (version %d)\n", resp.Rollup.RollupId, resp.Version)
	// Output:
	// RollupService.PutRollup(ctx, rollupId, body)
}

func ExampleInfoService_Info() {
	ctx := context.Background()

	var infoSvc api.InfoService
	if infoSvc == nil {
		fmt.Println("InfoService.Info(ctx)")
		return
	}

	resp, err := infoSvc.Info(ctx)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("cluster %s running %s on %s\n",
		resp.ClusterName, resp.Version.Number, resp.Version.Distribution)
	// Output:
	// InfoService.Info(ctx)
}

func ExampleAdService_IndexDetector() {
	ctx := context.Background()

	var adSvc api.AdService
	if adSvc == nil {
		fmt.Println("AdService.IndexDetector(ctx, *AdIndexDetectorRequest)")
		return
	}

	resp, err := adSvc.IndexDetector(ctx, &api.AdIndexDetectorRequest{
		Body: map[string]any{
			"name":       "cpu-anomaly-detector",
			"time_field": "@timestamp",
			"indices":    []string{"metrics-*"},
			"feature_attributes": []map[string]any{
				{"feature_name": "cpu_avg", "field": "cpu_usage", "aggregation_query": map[string]any{"avg_value": map[string]any{"avg": map[string]any{"field": "cpu_usage"}}}},
			},
			"detection_interval": map[string]any{"period": map[string]any{"interval": 10, "unit": "Minutes"}},
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("detector %s version %d\n", resp.Id, resp.Version)
	// Output:
	// AdService.IndexDetector(ctx, *AdIndexDetectorRequest)
}

func ExampleSqlService_PPLQuery() {
	ctx := context.Background()

	var sqlSvc api.SqlService
	if sqlSvc == nil {
		fmt.Println("SqlService.PPLQuery(ctx, body)")
		return
	}

	resp, err := sqlSvc.PPLQuery(ctx, map[string]any{
		"query": "source=logs-2024.01 | where status = 500 | fields host, status",
	})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("PPL returned %d rows\n", resp.Total)
	// Output:
	// SqlService.PPLQuery(ctx, body)
}

func ExampleMlService_Predict() {
	ctx := context.Background()

	var mlSvc api.MlService
	if mlSvc == nil {
		fmt.Println("MlService.Predict(ctx, modelId, body)")
		return
	}

	resp, err := mlSvc.Predict(ctx, "my-text-embedding-model", map[string]any{
		"parameters": map[string]any{"input_texts": []string{"hello world"}},
	})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("got %d inference results\n", len(resp.InferenceResults))
	// Output:
	// MlService.Predict(ctx, modelId, body)
}

func ExampleAsyncSearchService_Submit() {
	ctx := context.Background()

	var asyncSvc api.AsyncSearchService
	if asyncSvc == nil {
		fmt.Println("AsyncSearchService.Submit(ctx, body, params)")
		return
	}

	resp, err := asyncSvc.Submit(ctx, map[string]any{
		"query": map[string]any{"match_all": map[string]any{}},
	}, map[string]string{
		"wait_for_completion_timeout": "500ms",
		"keep_alive":                  "5m",
		"keep_on_completion":          "true",
	})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("async search %s state=%s\n", resp.Id, resp.State)
	// Output:
	// AsyncSearchService.Submit(ctx, body, params)
}

func ExampleKnnService_KnnStats() {
	ctx := context.Background()

	var knnSvc api.KnnService
	if knnSvc == nil {
		fmt.Println("KnnService.KnnStats(ctx, stat)")
		return
	}

	resp, err := knnSvc.KnnStats(ctx, "graph_memory_usage")
	if err != nil {
		log.Fatal(err)
	}

	if usage, ok := (*resp)["graph_memory_usage"]; ok {
		fmt.Printf("graph memory usage: %v\n", usage)
	}
	// Output:
	// KnnService.KnnStats(ctx, stat)
}

func ExampleNeuralService_NeuralWarmup() {
	ctx := context.Background()

	var neuralSvc api.NeuralService
	if neuralSvc == nil {
		fmt.Println("NeuralService.NeuralWarmup(ctx, index)")
		return
	}

	resp, err := neuralSvc.NeuralWarmup(ctx, "semantic-search-index")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("warmed up %d shards\n", resp.Shards.Successful)
	// Output:
	// NeuralService.NeuralWarmup(ctx, index)
}

