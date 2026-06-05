//go:build integration

package api_test

import (
	"context"
	"testing"

	"github.com/disaster37/opensearch/v4/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testIndexName = "test-idx-svc"

func indexSettings() map[string]any {
	return map[string]any{
		"settings": map[string]any{
			"number_of_shards":   1,
			"number_of_replicas": 0,
		},
	}
}

func createTestIndex(t *testing.T, svc api.IndicesService, name string) {
	t.Helper()
	_, err := svc.Create(context.Background(), name, indexSettings())
	require.NoError(t, err)
}

func TestIndicesService_Create(t *testing.T) {
	client := newIntegrationClient()
	ctx := context.Background()
	svc := client.Indices()

	t.Cleanup(func() { svc.Delete(ctx, []string{testIndexName}) })

	resp, err := svc.Create(ctx, testIndexName, indexSettings())
	require.NoError(t, err)
	assert.True(t, resp.Acknowledged)

	exists, err := svc.Exists(ctx, []string{testIndexName})
	require.NoError(t, err)
	assert.True(t, exists)
}

func TestIndicesService_Delete(t *testing.T) {
	client := newIntegrationClient()
	ctx := context.Background()
	svc := client.Indices()

	createTestIndex(t, svc, testIndexName)

	resp, err := svc.Delete(ctx, []string{testIndexName})
	require.NoError(t, err)
	assert.True(t, resp.Acknowledged)

	exists, err := svc.Exists(ctx, []string{testIndexName})
	require.NoError(t, err)
	assert.False(t, exists)
}

func TestIndicesService_Get(t *testing.T) {
	client := newIntegrationClient()
	ctx := context.Background()
	svc := client.Indices()

	t.Cleanup(func() { svc.Delete(ctx, []string{testIndexName}) })
	createTestIndex(t, svc, testIndexName)

	resp, err := svc.Get(ctx, []string{testIndexName})
	require.NoError(t, err)
	assert.Contains(t, resp, testIndexName)
	assert.NotNil(t, resp[testIndexName].Settings)
}

func TestIndicesService_Exists(t *testing.T) {
	client := newIntegrationClient()
	ctx := context.Background()
	svc := client.Indices()

	t.Cleanup(func() { svc.Delete(ctx, []string{testIndexName}) })

	exists, err := svc.Exists(ctx, []string{testIndexName})
	require.NoError(t, err)
	assert.False(t, exists)

	createTestIndex(t, svc, testIndexName)

	exists, err = svc.Exists(ctx, []string{testIndexName})
	require.NoError(t, err)
	assert.True(t, exists)
}

func TestIndicesService_OpenClose(t *testing.T) {
	client := newIntegrationClient()
	ctx := context.Background()
	svc := client.Indices()

	t.Cleanup(func() { svc.Delete(ctx, []string{testIndexName}) })
	createTestIndex(t, svc, testIndexName)

	closeResp, err := svc.Close(ctx, testIndexName)
	require.NoError(t, err)
	assert.True(t, closeResp.Acknowledged)

	openResp, err := svc.Open(ctx, testIndexName)
	require.NoError(t, err)
	assert.True(t, openResp.Acknowledged)
}

func TestIndicesService_Rollover(t *testing.T) {
	client := newIntegrationClient()
	ctx := context.Background()
	svc := client.Indices()

	idx := "test-idx-svc-000001"
	aliasName := "test-idx-svc-alias"

	t.Cleanup(func() { svc.Delete(ctx, []string{idx, "test-idx-svc-000002"}) })

	body := map[string]any{
		"settings": map[string]any{
			"number_of_shards":   1,
			"number_of_replicas": 0,
		},
		"aliases": map[string]any{
			aliasName: map[string]any{
				"is_write_index": true,
			},
		},
	}
	_, err := svc.Create(ctx, idx, body)
	require.NoError(t, err)

	_, err = client.Document().Index(ctx, &api.IndexRequest{
		Index:  idx,
		Id:     "roll-doc-1",
		Body:   map[string]any{"data": "rollover"},
		Params: &api.IndexParams{Refresh: "true"},
	})
	require.NoError(t, err)

	resp, err := svc.Rollover(ctx, aliasName, map[string]any{
		"conditions": map[string]any{
			"max_docs": 1,
		},
	})
	require.NoError(t, err)
	assert.NotNil(t, resp)
	assert.True(t, resp.Acknowledged)
}

func TestIndicesService_Flush(t *testing.T) {
	client := newIntegrationClient()
	ctx := context.Background()
	svc := client.Indices()

	t.Cleanup(func() { svc.Delete(ctx, []string{testIndexName}) })
	createTestIndex(t, svc, testIndexName)

	resp, err := svc.Flush(ctx, []string{testIndexName})
	require.NoError(t, err)
	assert.NotNil(t, resp)
	assert.NotNil(t, resp.Shards)
}

func TestIndicesService_Refresh(t *testing.T) {
	client := newIntegrationClient()
	ctx := context.Background()
	svc := client.Indices()

	t.Cleanup(func() { svc.Delete(ctx, []string{testIndexName}) })
	createTestIndex(t, svc, testIndexName)

	resp, err := svc.Refresh(ctx, []string{testIndexName})
	require.NoError(t, err)
	assert.NotNil(t, resp)
}

func TestIndicesService_Forcemerge(t *testing.T) {
	client := newIntegrationClient()
	ctx := context.Background()
	svc := client.Indices()

	t.Cleanup(func() { svc.Delete(ctx, []string{testIndexName}) })
	createTestIndex(t, svc, testIndexName)

	resp, err := svc.Forcemerge(ctx, []string{testIndexName})
	require.NoError(t, err)
	assert.NotNil(t, resp)
	assert.NotNil(t, resp.Shards)
}

func TestIndicesService_ClearCache(t *testing.T) {
	client := newIntegrationClient()
	ctx := context.Background()
	svc := client.Indices()

	t.Cleanup(func() { svc.Delete(ctx, []string{testIndexName}) })
	createTestIndex(t, svc, testIndexName)

	resp, err := svc.ClearCache(ctx, []string{testIndexName})
	require.NoError(t, err)
	assert.NotNil(t, resp)
}

func TestIndicesService_Stats(t *testing.T) {
	client := newIntegrationClient()
	ctx := context.Background()
	svc := client.Indices()

	t.Cleanup(func() { svc.Delete(ctx, []string{testIndexName}) })
	createTestIndex(t, svc, testIndexName)

	resp, err := svc.Stats(ctx, &api.IndicesStatsRequest{
		Indices: []string{testIndexName},
		Metrics: []string{"docs", "store"},
	})
	require.NoError(t, err)
	assert.NotNil(t, resp)
}

func TestIndicesService_Segments(t *testing.T) {
	client := newIntegrationClient()
	ctx := context.Background()
	svc := client.Indices()

	t.Cleanup(func() { svc.Delete(ctx, []string{testIndexName}) })
	createTestIndex(t, svc, testIndexName)

	resp, err := svc.Segments(ctx, []string{testIndexName})
	require.NoError(t, err)
	assert.NotNil(t, resp)
}

func TestIndicesService_Analyze(t *testing.T) {
	client := newIntegrationClient()
	ctx := context.Background()
	svc := client.Indices()

	t.Cleanup(func() { svc.Delete(ctx, []string{testIndexName}) })
	createTestIndex(t, svc, testIndexName)

	resp, err := svc.Analyze(ctx, testIndexName, map[string]any{
		"text":     "this is a test",
		"analyzer": "standard",
	})
	require.NoError(t, err)
	assert.NotNil(t, resp)
}

func TestIndicesService_PutAlias(t *testing.T) {
	client := newIntegrationClient()
	ctx := context.Background()
	svc := client.Indices()

	aliasName := "test-idx-svc-alias"

	t.Cleanup(func() { svc.Delete(ctx, []string{testIndexName}) })
	createTestIndex(t, svc, testIndexName)

	resp, err := svc.PutAlias(ctx, &api.PutAliasRequest{
		Index: testIndexName,
		Alias: aliasName,
	})
	require.NoError(t, err)
	assert.True(t, resp.Acknowledged)

	aliases, err := svc.GetAliases(ctx, []string{testIndexName})
	require.NoError(t, err)
	assert.Contains(t, aliases, testIndexName)
}

func TestIndicesService_GetAliases(t *testing.T) {
	client := newIntegrationClient()
	ctx := context.Background()
	svc := client.Indices()

	t.Cleanup(func() { svc.Delete(ctx, []string{testIndexName}) })
	createTestIndex(t, svc, testIndexName)

	resp, err := svc.GetAliases(ctx, []string{testIndexName})
	require.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Contains(t, resp, testIndexName)
}

func TestIndicesService_GetSettings(t *testing.T) {
	client := newIntegrationClient()
	ctx := context.Background()
	svc := client.Indices()

	t.Cleanup(func() { svc.Delete(ctx, []string{testIndexName}) })
	createTestIndex(t, svc, testIndexName)

	resp, err := svc.GetSettings(ctx, []string{testIndexName})
	require.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Contains(t, resp, testIndexName)
}

func TestIndicesService_PutSettings(t *testing.T) {
	client := newIntegrationClient()
	ctx := context.Background()
	svc := client.Indices()

	t.Cleanup(func() { svc.Delete(ctx, []string{testIndexName}) })
	createTestIndex(t, svc, testIndexName)

	resp, err := svc.PutSettings(ctx, []string{testIndexName}, map[string]any{
		"index": map[string]any{
			"refresh_interval": "5s",
		},
	})
	require.NoError(t, err)
	assert.True(t, resp.Acknowledged)
}

func TestIndicesService_GetMapping(t *testing.T) {
	client := newIntegrationClient()
	ctx := context.Background()
	svc := client.Indices()

	t.Cleanup(func() { svc.Delete(ctx, []string{testIndexName}) })
	createTestIndex(t, svc, testIndexName)

	resp, err := svc.GetMapping(ctx, []string{testIndexName})
	require.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Contains(t, resp, testIndexName)
}

func TestIndicesService_PutMapping(t *testing.T) {
	client := newIntegrationClient()
	ctx := context.Background()
	svc := client.Indices()

	t.Cleanup(func() { svc.Delete(ctx, []string{testIndexName}) })
	createTestIndex(t, svc, testIndexName)

	resp, err := svc.PutMapping(ctx, &api.PutMappingRequest{
		Indices: []string{testIndexName},
		Body: map[string]any{
			"properties": map[string]any{
				"title": map[string]any{
					"type": "text",
				},
			},
		},
	})
	require.NoError(t, err)
	assert.True(t, resp.Acknowledged)

	mapping, err := svc.GetMapping(ctx, []string{testIndexName})
	require.NoError(t, err)
	assert.NotNil(t, mapping[testIndexName])
}

func TestIndicesService_GetFieldMapping(t *testing.T) {
	client := newIntegrationClient()
	ctx := context.Background()
	svc := client.Indices()

	t.Cleanup(func() { svc.Delete(ctx, []string{testIndexName}) })
	createTestIndex(t, svc, testIndexName)

	_, err := svc.PutMapping(ctx, &api.PutMappingRequest{
		Indices: []string{testIndexName},
		Body: map[string]any{
			"properties": map[string]any{
				"title": map[string]any{
					"type": "text",
				},
			},
		},
	})
	require.NoError(t, err)

	resp, err := svc.GetFieldMapping(ctx, &api.GetFieldMappingRequest{
		Indices: []string{testIndexName},
		Fields:  []string{"title"},
	})
	require.NoError(t, err)
	assert.NotNil(t, resp)
}

func TestIndicesService_PutTemplate(t *testing.T) {
	client := newIntegrationClient()
	ctx := context.Background()
	svc := client.Indices()

	name := "test-tpl"
	t.Cleanup(func() { svc.DeleteTemplate(ctx, name) })

	resp, err := svc.PutTemplate(ctx, &api.PutTemplateRequest{
		Name: name,
		Body: map[string]any{
			"index_patterns": []string{"test-tpl-*"},
			"settings": map[string]any{
				"number_of_shards": 1,
			},
		},
	})
	require.NoError(t, err)
	assert.True(t, resp.Acknowledged)

	exists, err := svc.ExistsTemplate(ctx, name)
	require.NoError(t, err)
	assert.True(t, exists)
}

func TestIndicesService_GetTemplate(t *testing.T) {
	client := newIntegrationClient()
	ctx := context.Background()
	svc := client.Indices()

	name := "test-tpl"
	t.Cleanup(func() { svc.DeleteTemplate(ctx, name) })

	_, err := svc.PutTemplate(ctx, &api.PutTemplateRequest{
		Name: name,
		Body: map[string]any{
			"index_patterns": []string{"test-tpl-*"},
			"settings": map[string]any{
				"number_of_shards": 1,
			},
		},
	})
	require.NoError(t, err)

	resp, err := svc.GetTemplate(ctx, []string{name})
	require.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Contains(t, resp, name)
}

func TestIndicesService_ExistsTemplate(t *testing.T) {
	client := newIntegrationClient()
	ctx := context.Background()
	svc := client.Indices()

	name := "test-tpl"
	t.Cleanup(func() { svc.DeleteTemplate(ctx, name) })

	exists, err := svc.ExistsTemplate(ctx, name)
	require.NoError(t, err)
	assert.False(t, exists)

	_, err = svc.PutTemplate(ctx, &api.PutTemplateRequest{
		Name: name,
		Body: map[string]any{
			"index_patterns": []string{"test-tpl-*"},
			"settings": map[string]any{
				"number_of_shards": 1,
			},
		},
	})
	require.NoError(t, err)

	exists, err = svc.ExistsTemplate(ctx, name)
	require.NoError(t, err)
	assert.True(t, exists)
}

func TestIndicesService_DeleteTemplate(t *testing.T) {
	client := newIntegrationClient()
	ctx := context.Background()
	svc := client.Indices()

	name := "test-tpl"

	_, err := svc.PutTemplate(ctx, &api.PutTemplateRequest{
		Name: name,
		Body: map[string]any{
			"index_patterns": []string{"test-tpl-*"},
			"settings": map[string]any{
				"number_of_shards": 1,
			},
		},
	})
	require.NoError(t, err)

	resp, err := svc.DeleteTemplate(ctx, name)
	require.NoError(t, err)
	assert.True(t, resp.Acknowledged)

	exists, err := svc.ExistsTemplate(ctx, name)
	require.NoError(t, err)
	assert.False(t, exists)
}

func TestIndicesService_PutIndexTemplate(t *testing.T) {
	client := newIntegrationClient()
	ctx := context.Background()
	svc := client.Indices()

	name := "test-itpl"
	t.Cleanup(func() { svc.DeleteIndexTemplate(ctx, name) })

	resp, err := svc.PutIndexTemplate(ctx, &api.PutIndexTemplateRequest{
		Name: name,
		Body: map[string]any{
			"index_patterns": []string{"test-itpl-*"},
			"template": map[string]any{
				"settings": map[string]any{
					"number_of_shards": 1,
				},
			},
		},
	})
	require.NoError(t, err)
	assert.True(t, resp.Acknowledged)

	getResp, err := svc.GetIndexTemplate(ctx, []string{name})
	require.NoError(t, err)
	assert.NotNil(t, getResp)
}

func TestIndicesService_GetIndexTemplate(t *testing.T) {
	client := newIntegrationClient()
	ctx := context.Background()
	svc := client.Indices()

	name := "test-itpl"
	t.Cleanup(func() { svc.DeleteIndexTemplate(ctx, name) })

	_, err := svc.PutIndexTemplate(ctx, &api.PutIndexTemplateRequest{
		Name: name,
		Body: map[string]any{
			"index_patterns": []string{"test-itpl-*"},
			"template": map[string]any{
				"settings": map[string]any{
					"number_of_shards": 1,
				},
			},
		},
	})
	require.NoError(t, err)

	resp, err := svc.GetIndexTemplate(ctx, []string{name})
	require.NoError(t, err)
	assert.NotNil(t, resp)
}

func TestIndicesService_DeleteIndexTemplate(t *testing.T) {
	client := newIntegrationClient()
	ctx := context.Background()
	svc := client.Indices()

	name := "test-itpl"

	_, err := svc.PutIndexTemplate(ctx, &api.PutIndexTemplateRequest{
		Name: name,
		Body: map[string]any{
			"index_patterns": []string{"test-itpl-*"},
			"template": map[string]any{
				"settings": map[string]any{
					"number_of_shards": 1,
				},
			},
		},
	})
	require.NoError(t, err)

	resp, err := svc.DeleteIndexTemplate(ctx, name)
	require.NoError(t, err)
	assert.True(t, resp.Acknowledged)
}

func TestIndicesService_PutComponentTemplate(t *testing.T) {
	client := newIntegrationClient()
	ctx := context.Background()
	svc := client.Indices()

	name := "test-ctpl"
	t.Cleanup(func() { svc.DeleteComponentTemplate(ctx, name) })

	resp, err := svc.PutComponentTemplate(ctx, &api.PutComponentTemplateRequest{
		Name: name,
		Body: map[string]any{
			"template": map[string]any{
				"settings": map[string]any{
					"number_of_shards": 1,
				},
			},
		},
	})
	require.NoError(t, err)
	assert.True(t, resp.Acknowledged)

	getResp, err := svc.GetComponentTemplate(ctx, []string{name})
	require.NoError(t, err)
	assert.NotNil(t, getResp)
}

func TestIndicesService_GetComponentTemplate(t *testing.T) {
	client := newIntegrationClient()
	ctx := context.Background()
	svc := client.Indices()

	name := "test-ctpl"
	t.Cleanup(func() { svc.DeleteComponentTemplate(ctx, name) })

	_, err := svc.PutComponentTemplate(ctx, &api.PutComponentTemplateRequest{
		Name: name,
		Body: map[string]any{
			"template": map[string]any{
				"settings": map[string]any{
					"number_of_shards": 1,
				},
			},
		},
	})
	require.NoError(t, err)

	resp, err := svc.GetComponentTemplate(ctx, []string{name})
	require.NoError(t, err)
	assert.NotNil(t, resp)
}

func TestIndicesService_DeleteComponentTemplate(t *testing.T) {
	client := newIntegrationClient()
	ctx := context.Background()
	svc := client.Indices()

	name := "test-ctpl"

	_, err := svc.PutComponentTemplate(ctx, &api.PutComponentTemplateRequest{
		Name: name,
		Body: map[string]any{
			"template": map[string]any{
				"settings": map[string]any{
					"number_of_shards": 1,
				},
			},
		},
	})
	require.NoError(t, err)

	resp, err := svc.DeleteComponentTemplate(ctx, name)
	require.NoError(t, err)
	assert.True(t, resp.Acknowledged)
}

func TestIndicesService_CreateDataStream(t *testing.T) {
	client := newIntegrationClient()
	ctx := context.Background()
	svc := client.Indices()

	dsName := "test-ds-svc"

	_, err := svc.PutIndexTemplate(ctx, &api.PutIndexTemplateRequest{
		Name: "test-ds-svc-tpl",
		Body: map[string]any{
			"index_patterns": []string{"test-ds-svc"},
			"data_stream":    map[string]any{},
			"priority":       501,
			"template": map[string]any{
				"settings": map[string]any{
					"number_of_shards": 1,
				},
			},
		},
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		svc.DeleteDataStream(ctx, []string{dsName})
		svc.DeleteIndexTemplate(ctx, "test-ds-svc-tpl")
	})

	resp, err := svc.CreateDataStream(ctx, dsName)
	require.NoError(t, err)
	assert.True(t, resp.Acknowledged)

	getResp, err := svc.GetDataStream(ctx, []string{dsName})
	require.NoError(t, err)
	assert.NotNil(t, getResp)
}

func TestIndicesService_GetDataStream(t *testing.T) {
	client := newIntegrationClient()
	ctx := context.Background()
	svc := client.Indices()

	dsName := "test-ds-svc"

	_, err := svc.PutIndexTemplate(ctx, &api.PutIndexTemplateRequest{
		Name: "test-ds-svc-tpl",
		Body: map[string]any{
			"index_patterns": []string{"test-ds-svc"},
			"data_stream":    map[string]any{},
			"priority":       501,
			"template": map[string]any{
				"settings": map[string]any{
					"number_of_shards": 1,
				},
			},
		},
	})
	require.NoError(t, err)

	_, err = svc.CreateDataStream(ctx, dsName)
	require.NoError(t, err)

	t.Cleanup(func() {
		svc.DeleteDataStream(ctx, []string{dsName})
		svc.DeleteIndexTemplate(ctx, "test-ds-svc-tpl")
	})

	resp, err := svc.GetDataStream(ctx, []string{dsName})
	require.NoError(t, err)
	assert.NotNil(t, resp)
}

func TestIndicesService_DeleteDataStream(t *testing.T) {
	client := newIntegrationClient()
	ctx := context.Background()
	svc := client.Indices()

	dsName := "test-ds-svc"

	_, err := svc.PutIndexTemplate(ctx, &api.PutIndexTemplateRequest{
		Name: "test-ds-svc-tpl",
		Body: map[string]any{
			"index_patterns": []string{"test-ds-svc"},
			"data_stream":    map[string]any{},
			"priority":       501,
			"template": map[string]any{
				"settings": map[string]any{
					"number_of_shards": 1,
				},
			},
		},
	})
	require.NoError(t, err)

	_, err = svc.CreateDataStream(ctx, dsName)
	require.NoError(t, err)

	t.Cleanup(func() { svc.DeleteIndexTemplate(ctx, "test-ds-svc-tpl") })

	resp, err := svc.DeleteDataStream(ctx, []string{dsName})
	require.NoError(t, err)
	assert.True(t, resp.Acknowledged)
}
