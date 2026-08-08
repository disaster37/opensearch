//go:build integration

package api_test

import (
	"context"
	"testing"

	"github.com/disaster37/opensearch/v4/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClusterService_Health(t *testing.T) {
	client := newIntegrationClient()
	ctx := context.Background()

	result, err := client.Cluster().Health(ctx, nil)
	require.NoError(t, err)
	assert.NotNil(t, result)
	assert.NotEmpty(t, result.ClusterName)
	assert.NotEmpty(t, result.Status)
}

func TestClusterService_State(t *testing.T) {
	client := newIntegrationClient()
	ctx := context.Background()

	result, err := client.Cluster().State(ctx, &api.ClusterStateRequest{
		Metrics: []string{"metadata"},
	})
	require.NoError(t, err)
	assert.NotNil(t, result)
	assert.NotEmpty(t, result.ClusterName)
	assert.NotNil(t, result.Metadata)
}

func TestClusterService_Stats(t *testing.T) {
	client := newIntegrationClient()
	ctx := context.Background()

	result, err := client.Cluster().Stats(ctx, nil)
	require.NoError(t, err)
	assert.NotNil(t, result)
	assert.NotEmpty(t, result.ClusterName)
}

func TestClusterService_GetSettings(t *testing.T) {
	client := newIntegrationClient()
	ctx := context.Background()

	result, err := client.Cluster().GetSettings(ctx)
	require.NoError(t, err)
	assert.NotNil(t, result)
}

// TestClusterService_PruneBlockCache exercises POST /_blockcache/prune
// (OpenSearch 3.7.0+). On a cluster without warm nodes the call still
// succeeds with TotalNodesTargeted == 0.
func TestClusterService_PruneBlockCache(t *testing.T) {
	client := newIntegrationClient()
	ctx := context.Background()

	resp, err := client.Cluster().PruneBlockCache(ctx)
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.True(t, resp.Acknowledged)
	require.NotNil(t, resp.Summary)
}
