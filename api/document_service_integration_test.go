//go:build integration

package api_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/disaster37/opensearch/v3/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testDocIndex = "test-doc-svc"

func cleanupTestDocIndex(t *testing.T, client interface{ Indices() api.IndicesService }) {
	t.Helper()
	_, _ = client.Indices().Delete(context.Background(), []string{testDocIndex})
}

func indexTestDoc(t *testing.T, client interface{ Document() api.DocumentService }, id string, body any) {
	t.Helper()
	_, err := client.Document().Index(context.Background(), &api.IndexRequest{
		Index:  testDocIndex,
		Id:     id,
		Body:   body,
		Params: map[string]string{"refresh": "true"},
	})
	require.NoError(t, err)
}

func TestDocumentService_Index(t *testing.T) {
	client := newIntegrationClient()
	defer cleanupTestDocIndex(t, client)
	cleanupTestDocIndex(t, client)

	ctx := context.Background()
	doc := client.Document()

	resp, err := doc.Index(ctx, &api.IndexRequest{
		Index:  testDocIndex,
		Id:     "doc-1",
		Body:   map[string]any{"title": "test document"},
		Params: map[string]string{"refresh": "true"},
	})
	require.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Equal(t, testDocIndex, resp.Index)
	assert.Equal(t, "doc-1", resp.Id)
	assert.Equal(t, "created", resp.Result)
	assert.Equal(t, 201, resp.Status)
	assert.Greater(t, resp.Version, int64(0))

	resp2, err := doc.Index(ctx, &api.IndexRequest{
		Index:  testDocIndex,
		Id:     "doc-1",
		Body:   map[string]any{"title": "updated document"},
		Params: map[string]string{"refresh": "true"},
	})
	require.NoError(t, err)
	assert.Equal(t, "updated", resp2.Result)
	assert.Equal(t, int64(2), resp2.Version)

	resp3, err := doc.Index(ctx, &api.IndexRequest{
		Index: testDocIndex,
		Body:  map[string]any{"title": "auto id"},
	})
	require.NoError(t, err)
	assert.NotEmpty(t, resp3.Id)
	assert.Equal(t, "created", resp3.Result)
}

func TestDocumentService_Get(t *testing.T) {
	client := newIntegrationClient()
	defer cleanupTestDocIndex(t, client)
	cleanupTestDocIndex(t, client)

	ctx := context.Background()
	doc := client.Document()

	indexTestDoc(t, client, "get-1", map[string]any{"title": "hello world", "count": 42})

	result, err := doc.Get(ctx, &api.GetRequest{
		Index: testDocIndex,
		Id:    "get-1",
	})
	require.NoError(t, err)
	assert.NotNil(t, result)
	assert.True(t, result.Found)
	assert.Equal(t, testDocIndex, result.Index)
	assert.Equal(t, "get-1", result.Id)
	assert.NotNil(t, result.Source)
	assert.NotNil(t, result.Version)
	assert.Greater(t, *result.Version, int64(0))

	notFound, err := doc.Get(ctx, &api.GetRequest{
		Index: testDocIndex,
		Id:    "nonexistent",
	})
	require.NoError(t, err)
	assert.NotNil(t, notFound)
	assert.False(t, notFound.Found)
}

func TestDocumentService_MultiGet(t *testing.T) {
	client := newIntegrationClient()
	defer cleanupTestDocIndex(t, client)
	cleanupTestDocIndex(t, client)

	ctx := context.Background()
	doc := client.Document()

	indexTestDoc(t, client, "mget-1", map[string]any{"title": "first"})
	indexTestDoc(t, client, "mget-2", map[string]any{"title": "second"})

	resp, err := doc.MultiGet(ctx, []*api.MultiGetItem{
		{Index: testDocIndex, Id: "mget-1"},
		{Index: testDocIndex, Id: "mget-2"},
		{Index: testDocIndex, Id: "mget-nonexistent"},
	})
	require.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Len(t, resp.Docs, 3)
	assert.True(t, resp.Docs[0].Found)
	assert.Equal(t, "mget-1", resp.Docs[0].Id)
	assert.True(t, resp.Docs[1].Found)
	assert.Equal(t, "mget-2", resp.Docs[1].Id)
	assert.False(t, resp.Docs[2].Found)
}

func TestDocumentService_Delete(t *testing.T) {
	client := newIntegrationClient()
	defer cleanupTestDocIndex(t, client)
	cleanupTestDocIndex(t, client)

	ctx := context.Background()
	doc := client.Document()

	indexTestDoc(t, client, "del-1", map[string]any{"title": "to delete"})

	resp, err := doc.Delete(ctx, &api.DeleteRequest{
		Index:  testDocIndex,
		Id:     "del-1",
		Params: map[string]string{"refresh": "true"},
	})
	require.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Equal(t, "del-1", resp.Id)
	assert.Equal(t, "deleted", resp.Result)

	_, err = doc.Delete(ctx, &api.DeleteRequest{
		Index: testDocIndex,
		Id:    "del-nonexistent",
	})
	assert.Error(t, err)
}

func TestDocumentService_DeleteByQuery(t *testing.T) {
	client := newIntegrationClient()
	defer cleanupTestDocIndex(t, client)
	cleanupTestDocIndex(t, client)

	ctx := context.Background()
	doc := client.Document()

	indexTestDoc(t, client, "dbq-1", map[string]any{"title": "a"})
	indexTestDoc(t, client, "dbq-2", map[string]any{"title": "b"})
	indexTestDoc(t, client, "dbq-3", map[string]any{"title": "c"})

	resp, err := doc.DeleteByQuery(ctx, []string{testDocIndex}, map[string]any{
		"query": map[string]any{"match_all": map[string]any{}},
	})
	require.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Equal(t, int64(3), resp.Deleted)
	assert.False(t, resp.TimedOut)
	assert.Empty(t, resp.Failures)
}

func TestDocumentService_Update(t *testing.T) {
	client := newIntegrationClient()
	defer cleanupTestDocIndex(t, client)
	cleanupTestDocIndex(t, client)

	ctx := context.Background()
	doc := client.Document()

	indexTestDoc(t, client, "upd-1", map[string]any{"title": "original", "count": 1})

	resp, err := doc.Update(ctx, &api.UpdateRequest{
		Index: testDocIndex,
		Id:    "upd-1",
		Body: map[string]any{
			"doc": map[string]any{"title": "updated", "count": 2},
		},
		Params: map[string]string{"refresh": "true"},
	})
	require.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Equal(t, "upd-1", resp.Id)
	assert.Equal(t, "updated", resp.Result)
	assert.Greater(t, resp.Version, int64(1))

	result, err := doc.Get(ctx, &api.GetRequest{Index: testDocIndex, Id: "upd-1"})
	require.NoError(t, err)
	assert.True(t, result.Found)
	assert.NotNil(t, result.Source)
}

func TestDocumentService_UpdateByQuery(t *testing.T) {
	client := newIntegrationClient()
	defer cleanupTestDocIndex(t, client)
	cleanupTestDocIndex(t, client)

	ctx := context.Background()
	doc := client.Document()

	indexTestDoc(t, client, "ubq-1", map[string]any{"title": "alpha", "count": 1})
	indexTestDoc(t, client, "ubq-2", map[string]any{"title": "beta", "count": 2})

	resp, err := doc.UpdateByQuery(ctx, []string{testDocIndex}, map[string]any{
		"query": map[string]any{"match_all": map[string]any{}},
		"script": map[string]any{
			"source": "ctx._source.count = ctx._source.count + 10",
		},
	})
	require.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Equal(t, int64(2), resp.Updated)
	assert.False(t, resp.TimedOut)
	assert.Empty(t, resp.Failures)
}

func TestDocumentService_Bulk(t *testing.T) {
	client := newIntegrationClient()
	defer cleanupTestDocIndex(t, client)
	cleanupTestDocIndex(t, client)

	ctx := context.Background()
	doc := client.Document()

	body := `{"index":{"_id":"bulk-1"}}
{"title":"bulk doc 1"}
{"index":{"_id":"bulk-2"}}
{"title":"bulk doc 2"}
{"index":{"_id":"bulk-3"}}
{"title":"bulk doc 3"}
`

	resp, err := doc.Bulk(ctx, testDocIndex, body)
	require.NoError(t, err)
	assert.NotNil(t, resp)
	assert.False(t, resp.Errors)
	assert.Len(t, resp.Items, 3)

	for i, item := range resp.Items {
		indexItem, ok := item["index"]
		require.True(t, ok, fmt.Sprintf("item %d missing 'index' key", i))
		assert.Equal(t, 201, indexItem.Status)
		assert.Equal(t, "created", indexItem.Result)
		assert.Equal(t, testDocIndex, indexItem.Index)
	}

	result1, err := doc.Get(ctx, &api.GetRequest{Index: testDocIndex, Id: "bulk-1"})
	require.NoError(t, err)
	assert.True(t, result1.Found)

	result2, err := doc.Get(ctx, &api.GetRequest{Index: testDocIndex, Id: "bulk-2"})
	require.NoError(t, err)
	assert.True(t, result2.Found)

	result3, err := doc.Get(ctx, &api.GetRequest{Index: testDocIndex, Id: "bulk-3"})
	require.NoError(t, err)
	assert.True(t, result3.Found)
}

func TestDocumentService_Exists(t *testing.T) {
	client := newIntegrationClient()
	defer cleanupTestDocIndex(t, client)
	cleanupTestDocIndex(t, client)

	ctx := context.Background()
	doc := client.Document()

	indexTestDoc(t, client, "exists-1", map[string]any{"title": "i exist"})

	exists, err := doc.Exists(ctx, testDocIndex, "exists-1")
	require.NoError(t, err)
	assert.True(t, exists)

	notExists, err := doc.Exists(ctx, testDocIndex, "exists-nonexistent")
	require.NoError(t, err)
	assert.False(t, notExists)

	_, err = doc.Delete(ctx, &api.DeleteRequest{
		Index:  testDocIndex,
		Id:     "exists-1",
		Params: map[string]string{"refresh": "true"},
	})
	require.NoError(t, err)

	afterDelete, err := doc.Exists(ctx, testDocIndex, "exists-1")
	require.NoError(t, err)
	assert.False(t, afterDelete)
}

func TestDocumentService_Explain(t *testing.T) {
	client := newIntegrationClient()
	defer cleanupTestDocIndex(t, client)
	cleanupTestDocIndex(t, client)

	ctx := context.Background()
	doc := client.Document()

	indexTestDoc(t, client, "explain-1", map[string]any{"title": "explain test document"})

	resp, err := doc.Explain(ctx, testDocIndex, "explain-1", map[string]any{
		"query": map[string]any{
			"match": map[string]any{
				"title": "explain",
			},
		},
	})
	require.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Equal(t, testDocIndex, resp.Index)
	assert.Equal(t, "explain-1", resp.Id)
	assert.True(t, resp.Matched)
	assert.NotNil(t, resp.Explanation)

	resp2, err := doc.Explain(ctx, testDocIndex, "explain-1", map[string]any{
		"query": map[string]any{
			"match": map[string]any{
				"title": "nonexistent_term_xyz",
			},
		},
	})
	require.NoError(t, err)
	assert.NotNil(t, resp2)
	assert.False(t, resp2.Matched)
}

func TestDocumentService_TermVectors(t *testing.T) {
	client := newIntegrationClient()
	defer cleanupTestDocIndex(t, client)
	cleanupTestDocIndex(t, client)

	ctx := context.Background()
	doc := client.Document()

	indexTestDoc(t, client, "tv-1", map[string]any{"title": "the quick brown fox"})

	resp, err := doc.TermVectors(ctx, testDocIndex, "tv-1", map[string]any{
		"fields": []string{"title"},
	})
	require.NoError(t, err)
	assert.NotNil(t, resp)
	assert.True(t, resp.Found)
	assert.Equal(t, "tv-1", resp.Id)
	assert.Equal(t, testDocIndex, resp.Index)
	assert.NotEmpty(t, resp.TermVectors)
	_, hasTitle := resp.TermVectors["title"]
	assert.True(t, hasTitle, "expected title field in term vectors")
}

func TestDocumentService_MultiTermVectors(t *testing.T) {
	client := newIntegrationClient()
	defer cleanupTestDocIndex(t, client)
	cleanupTestDocIndex(t, client)

	ctx := context.Background()
	doc := client.Document()

	indexTestDoc(t, client, "mtv-1", map[string]any{"title": "hello world"})
	indexTestDoc(t, client, "mtv-2", map[string]any{"title": "foo bar"})

	resp, err := doc.MultiTermVectors(ctx, testDocIndex, map[string]any{
		"ids":    []string{"mtv-1", "mtv-2"},
		"fields": []string{"title"},
	})
	require.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Len(t, resp.Docs, 2)
	assert.Equal(t, "mtv-1", resp.Docs[0].Id)
	assert.True(t, resp.Docs[0].Found)
	assert.Equal(t, "mtv-2", resp.Docs[1].Id)
	assert.True(t, resp.Docs[1].Found)
}

func TestDocumentService_Reindex(t *testing.T) {
	client := newIntegrationClient()
	defer cleanupTestDocIndex(t, client)
	defer func() {
		_, _ = client.Indices().Delete(context.Background(), []string{"test-doc-svc-reindex"})
	}()
	cleanupTestDocIndex(t, client)
	_, _ = client.Indices().Delete(context.Background(), []string{"test-doc-svc-reindex"})

	ctx := context.Background()
	doc := client.Document()

	indexTestDoc(t, client, "reix-1", map[string]any{"title": "reindex me"})
	indexTestDoc(t, client, "reix-2", map[string]any{"title": "reindex me too"})

	resp, err := doc.Reindex(ctx, map[string]any{
		"source": map[string]any{
			"index": testDocIndex,
		},
		"dest": map[string]any{
			"index": "test-doc-svc-reindex",
		},
	})
	require.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Equal(t, int64(2), resp.Created)
	assert.Equal(t, int64(2), resp.Total)
	assert.False(t, resp.TimedOut)
	assert.Empty(t, resp.Failures)

	result, err := doc.Get(ctx, &api.GetRequest{Index: "test-doc-svc-reindex", Id: "reix-1"})
	require.NoError(t, err)
	assert.True(t, result.Found)

	result2, err := doc.Get(ctx, &api.GetRequest{Index: "test-doc-svc-reindex", Id: "reix-2"})
	require.NoError(t, err)
	assert.True(t, result2.Found)
}
