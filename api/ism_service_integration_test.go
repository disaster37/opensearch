//go:build integration

package api_test

import (
	"context"
	"testing"

	"github.com/disaster37/opensearch/v4/api"
	"github.com/disaster37/opensearch/v4/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIsmService_PutGetDeletePolicy(t *testing.T) {
	client := newIntegrationClient()
	ctx := context.Background()
	policyName := "test-ism"

	t.Cleanup(func() {
		client.ISM().DeletePolicy(ctx, policyName)
	})

	putResult, err := client.ISM().PutPolicy(ctx, &api.IsmPutPolicyRequest{
		PolicyName: policyName,
		Body: &api.IsmPolicyBase{
			Description:  strPtr("test ISM policy"),
			DefaultState: strPtr("hot"),
			States: []api.IsmPolicyState{
				{Name: "hot"},
			},
		},
	})
	require.NoError(t, err)
	assert.NotNil(t, putResult)
	assert.NotEmpty(t, putResult.Id)
	assert.Equal(t, policyName, putResult.Id)

	getResult, err := client.ISM().GetPolicy(ctx, policyName)
	require.NoError(t, err)
	assert.NotNil(t, getResult)
	assert.Equal(t, policyName, getResult.Id)
	assert.Equal(t, "hot", *getResult.Policy.DefaultState)

	deleteResult, err := client.ISM().DeletePolicy(ctx, policyName)
	require.NoError(t, err)
	assert.NotNil(t, deleteResult)
}

// TestIsmService_RefreshSearchAnalyzers exercises
// POST /_plugins/_refresh_search_analyzers/{index}. On clusters without
// the ISM/index-management plugin the call returns an *types.OpenSearchError
// and the test is skipped.
func TestIsmService_RefreshSearchAnalyzers(t *testing.T) {
	client := newIntegrationClient()
	ctx := context.Background()
	indexName := "test-ism-refresh-analyzers"

	_, err := client.Indices().Create(ctx, indexName, map[string]any{
		"settings": map[string]any{
			"number_of_shards":   1,
			"number_of_replicas": 0,
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = client.Indices().Delete(ctx, []string{indexName})
	})

	resp, err := client.ISM().RefreshSearchAnalyzers(ctx, indexName)
	if osErr, ok := err.(*types.OpenSearchError); ok {
		t.Skipf("refresh_search_analyzers plugin not available on this cluster (status %d)", osErr.StatusCode())
		return
	}
	require.NoError(t, err)
	require.NotNil(t, resp)
}
