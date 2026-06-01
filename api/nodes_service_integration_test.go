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
