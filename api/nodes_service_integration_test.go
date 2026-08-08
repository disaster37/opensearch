//go:build integration

package api_test

import (
	"context"
	"testing"

	"github.com/disaster37/opensearch/v4/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNodesService_Info(t *testing.T) {
	client := newIntegrationClient()
	ctx := context.Background()

	result, err := client.Nodes().Info(ctx, &api.NodesInfoRequest{})
	require.NoError(t, err)
	assert.NotNil(t, result)
	assert.NotEmpty(t, result.ClusterName)
	assert.NotEmpty(t, result.Nodes)
}

func TestNodesService_Stats(t *testing.T) {
	client := newIntegrationClient()
	ctx := context.Background()

	result, err := client.Nodes().Stats(ctx, &api.NodesStatsRequest{})
	require.NoError(t, err)
	assert.NotNil(t, result)
	assert.NotEmpty(t, result.ClusterName)
	assert.NotEmpty(t, result.Nodes)
}

// TestNodesService_StatsDetailed exercises the detailed file_cache stats
// query parameter added in OpenSearch 3.7.0. Non-warm clusters return
// empty/absent file_cache; only the call contract is asserted.
func TestNodesService_StatsDetailed(t *testing.T) {
	client := newIntegrationClient()
	ctx := context.Background()

	result, err := client.Nodes().Stats(ctx, &api.NodesStatsRequest{
		Metrics:  []string{"file_cache"},
		Detailed: true,
	})
	require.NoError(t, err)
	assert.NotNil(t, result)
	assert.NotEmpty(t, result.Nodes)
}
