//go:build integration

// nodes_service_integration_test.go contains integration tests for NodesService
// against a live OpenSearch cluster. It exercises the nodes info, stats, and
// usage endpoints, including the cgroup-aware resource_usage_stats metric that
// needs a live resource tracker window before it reports values.
package api_test

import (
	"context"
	"testing"
	"time"

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

// TestNodesService_StatsResourceUsage exercises GET /_nodes/stats/resource_usage_stats.
// OpenSearch only publishes the metric once every resource tracker has filled
// its moving-average window: CPU/JVM/native-memory default to 30s windows and
// I/O defaults to a 120s window polled every 5s. A freshly booted node can
// therefore take around three minutes to report values, so poll with a deadline
// that covers those defaults instead of assuming a short warmup.
func TestNodesService_StatsResourceUsage(t *testing.T) {
	client := newIntegrationClient()
	ctx := context.Background()
	request := &api.NodesStatsRequest{
		Metrics: []string{api.NodesStatsMetricResourceUsageStats},
	}

	var (
		nodeID string
		node   *api.NodesStatsNode
		usage  *api.NodesStatsResourceUsage
	)

	deadline := time.Now().Add(4 * time.Minute)
	for {
		result, err := client.Nodes().Stats(ctx, request)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.NotEmpty(t, result.Nodes)

		for id, n := range result.Nodes {
			if u := n.ResourceUsage(id); u != nil {
				nodeID, node, usage = id, n, u
				break
			}
		}
		if usage != nil || !time.Now().Before(deadline) {
			break
		}
		time.Sleep(2 * time.Second)
	}

	require.NotNil(t, usage, "resource_usage_stats was not populated after retries")
	require.NotNil(t, node)

	require.NotNil(t, usage.Timestamp)
	assert.Positive(t, *usage.Timestamp)

	cpu := node.ResourceCPUUtilization(nodeID)
	require.NotNil(t, cpu)
	assert.GreaterOrEqual(t, *cpu, 0.0)

	mem := usage.MemoryUtilization()
	require.NotNil(t, mem)
	assert.GreaterOrEqual(t, *mem, 0.0)

	io := usage.MaxIOUtilization()
	require.NotNil(t, io)
	assert.GreaterOrEqual(t, *io, 0.0)

	if usage.NativeMemoryUtilizationPercent != nil {
		native := usage.NativeMemoryUtilization()
		require.NotNil(t, native)
		assert.GreaterOrEqual(t, *native, 0.0)
	}
}
