//go:build integration

package api_test

import (
	"context"
	"testing"

	"github.com/disaster37/opensearch/v3/api"
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
