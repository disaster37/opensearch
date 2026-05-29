package api_test

import (
	"context"
	"fmt"
	"log"

	"github.com/disaster37/opensearch/v3/api"
	"github.com/disaster37/opensearch/v3/querydsl"
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
