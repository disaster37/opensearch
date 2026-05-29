//go:build integration

package api_test

import (
	"context"
	"testing"

	"github.com/disaster37/opensearch/v3/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSearchService_Search(t *testing.T) {
	client := newIntegrationClient()
	ctx := context.Background()
	indexName := "test-search-svc"

	_, err := client.Indices().Create(ctx, indexName, map[string]any{
		"settings": map[string]any{
			"number_of_shards":   1,
			"number_of_replicas": 0,
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		client.Indices().Delete(ctx, []string{indexName})
	})

	docs := []struct {
		id   string
		body map[string]any
	}{
		{"1", map[string]any{"title": "opensearch basics", "category": "search"}},
		{"2", map[string]any{"title": "advanced queries", "category": "search"}},
		{"3", map[string]any{"title": "cluster management", "category": "admin"}},
	}
	for _, d := range docs {
		_, err := client.Document().Index(ctx, &api.IndexRequest{
			Index: indexName,
			Id:    d.id,
			Body:  d.body,
		})
		require.NoError(t, err)
	}

	_, err = client.Indices().Refresh(ctx, []string{indexName})
	require.NoError(t, err)

	result, err := client.Search().Search(ctx, &api.SearchRequest{
		Indices: []string{indexName},
		Body: map[string]any{
			"query": map[string]any{
				"match_all": map[string]any{},
			},
		},
	})
	require.NoError(t, err)
	assert.NotNil(t, result)
	assert.NotNil(t, result.Hits)
	assert.NotNil(t, result.Hits.TotalHits)
	assert.Equal(t, int64(3), result.Hits.TotalHits.Value)
}

func TestSearchService_Count(t *testing.T) {
	client := newIntegrationClient()
	ctx := context.Background()
	indexName := "test-search-svc-count"

	_, err := client.Indices().Create(ctx, indexName, map[string]any{
		"settings": map[string]any{
			"number_of_shards":   1,
			"number_of_replicas": 0,
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		client.Indices().Delete(ctx, []string{indexName})
	})

	_, err = client.Document().Index(ctx, &api.IndexRequest{
		Index: indexName,
		Id:    "1",
		Body:  map[string]any{"msg": "hello"},
	})
	require.NoError(t, err)

	_, err = client.Indices().Refresh(ctx, []string{indexName})
	require.NoError(t, err)

	count, err := client.Search().Count(ctx, []string{indexName}, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(1), count)
}

func TestSearchService_ScrollAndClearScroll(t *testing.T) {
	client := newIntegrationClient()
	ctx := context.Background()
	indexName := "test-search-svc-scroll"

	_, err := client.Indices().Create(ctx, indexName, map[string]any{
		"settings": map[string]any{
			"number_of_shards":   1,
			"number_of_replicas": 0,
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		client.Indices().Delete(ctx, []string{indexName})
	})

	for i := 0; i < 5; i++ {
		_, err := client.Document().Index(ctx, &api.IndexRequest{
			Index: indexName,
			Body:  map[string]any{"value": i},
		})
		require.NoError(t, err)
	}

	_, err = client.Indices().Refresh(ctx, []string{indexName})
	require.NoError(t, err)

	initialResult, err := client.Search().Search(ctx, &api.SearchRequest{
		Indices: []string{indexName},
		Body: map[string]any{
			"size":  2,
			"query": map[string]any{"match_all": map[string]any{}},
		},
		Params: map[string]string{"scroll": "1m"},
	})
	require.NoError(t, err)
	assert.NotNil(t, initialResult)
	assert.NotEmpty(t, initialResult.ScrollId)

	scrollResult, err := client.Search().Scroll(ctx, &api.ScrollRequest{
		ScrollId:  initialResult.ScrollId,
		KeepAlive: "1m",
	})
	require.NoError(t, err)
	assert.NotNil(t, scrollResult)

	clearResult, err := client.Search().ClearScroll(ctx, []string{scrollResult.ScrollId})
	require.NoError(t, err)
	assert.NotNil(t, clearResult)
}

func TestSearchService_FieldCaps(t *testing.T) {
	client := newIntegrationClient()
	ctx := context.Background()
	indexName := "test-search-svc-fieldcaps"

	_, err := client.Indices().Create(ctx, indexName, map[string]any{
		"settings": map[string]any{
			"number_of_shards":   1,
			"number_of_replicas": 0,
		},
		"mappings": map[string]any{
			"properties": map[string]any{
				"title":   map[string]any{"type": "text"},
				"counter": map[string]any{"type": "integer"},
			},
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		client.Indices().Delete(ctx, []string{indexName})
	})

	_, err = client.Document().Index(ctx, &api.IndexRequest{
		Index: indexName,
		Id:    "1",
		Body:  map[string]any{"title": "test", "counter": 42},
	})
	require.NoError(t, err)
	_, err = client.Indices().Refresh(ctx, []string{indexName})
	require.NoError(t, err)

	result, err := client.Search().FieldCaps(ctx, &api.FieldCapsRequest{
		Indices: []string{indexName},
		Fields:  []string{"title", "counter"},
	})
	require.NoError(t, err)
	assert.NotNil(t, result)
}

func TestSearchService_Validate(t *testing.T) {
	client := newIntegrationClient()
	ctx := context.Background()
	indexName := "test-search-svc-validate"

	_, err := client.Indices().Create(ctx, indexName, map[string]any{
		"settings": map[string]any{
			"number_of_shards":   1,
			"number_of_replicas": 0,
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		client.Indices().Delete(ctx, []string{indexName})
	})

	result, err := client.Search().Validate(ctx, []string{indexName}, map[string]any{
		"query": map[string]any{
			"match_all": map[string]any{},
		},
	})
	require.NoError(t, err)
	assert.NotNil(t, result)
	assert.True(t, result.Valid)
}

func TestSearchService_SearchShards(t *testing.T) {
	client := newIntegrationClient()
	ctx := context.Background()
	indexName := "test-search-svc-shards"

	_, err := client.Indices().Create(ctx, indexName, map[string]any{
		"settings": map[string]any{
			"number_of_shards":   1,
			"number_of_replicas": 0,
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		client.Indices().Delete(ctx, []string{indexName})
	})

	result, err := client.Search().SearchShards(ctx, []string{indexName})
	require.NoError(t, err)
	assert.NotNil(t, result)
	assert.NotEmpty(t, result.Shards)
}
