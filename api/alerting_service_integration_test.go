//go:build integration

package api_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAlertingService_SearchMonitor(t *testing.T) {
	client := newIntegrationClient()
	ctx := context.Background()

	result, err := client.Alerting().SearchMonitor(ctx, map[string]any{
		"query": map[string]any{
			"match_all": map[string]any{},
		},
	})
	require.NoError(t, err)
	assert.NotNil(t, result)
}

func TestAlertingService_PostGetDeleteMonitor(t *testing.T) {
	client := newIntegrationClient()
	ctx := context.Background()

	enabled := false
	postResult, err := client.Alerting().PostMonitor(ctx, map[string]any{
		"type":         "monitor",
		"monitor_type": "query_level_monitor",
		"name":         "test-monitor",
		"enabled":      enabled,
		"schedule": map[string]any{
			"period": map[string]any{
				"interval": 1,
				"unit":     "MINUTES",
			},
		},
		"inputs": []map[string]any{
			{
				"search": map[string]any{
					"indices": []string{".opendistro-alerting-config"},
					"query": map[string]any{
						"match_all": map[string]any{},
					},
				},
			},
		},
		"triggers": []map[string]any{},
	})
	require.NoError(t, err)
	assert.NotNil(t, postResult)
	assert.NotEmpty(t, postResult.Id)

	monitorId := postResult.Id
	t.Cleanup(func() {
		client.Alerting().DeleteMonitor(ctx, monitorId)
	})

	getResult, err := client.Alerting().GetMonitor(ctx, monitorId)
	require.NoError(t, err)
	assert.NotNil(t, getResult)
	assert.Equal(t, monitorId, getResult.Id)
	assert.Equal(t, "test-monitor", getResult.Monitor.Name)

	deleteResult, err := client.Alerting().DeleteMonitor(ctx, monitorId)
	require.NoError(t, err)
	assert.NotNil(t, deleteResult)
}
