//go:build integration

package api_test

import (
	"context"
	"testing"

	"github.com/disaster37/opensearch/v3/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSmService_PutGetDeletePolicy(t *testing.T) {
	client := newIntegrationClient()
	ctx := context.Background()
	policyName := "test-sm"
	enabled := false
	desc := "test snapshot management policy"

	t.Cleanup(func() {
		client.SM().DeletePolicy(ctx, policyName)
	})

	putResult, err := client.SM().PutPolicy(ctx, &api.SmPutPolicyRequest{
		PolicyName: policyName,
		Body: &api.SmPutPolicy{
			Description: &desc,
			Enabled:     &enabled,
			SnapshotConfig: api.SmPolicySnapshotConfig{
				Repository: "test-repo",
				Indices:    strPtr("*"),
			},
			Creation: api.SmPolicyCreation{
				Schedule: map[string]any{
					"cron": map[string]any{
						"expression": "0 0 * * *",
						"timezone":   "UTC",
					},
				},
			},
		},
	})
	require.NoError(t, err)
	assert.NotNil(t, putResult)
	assert.NotEmpty(t, putResult.Id)

	getResult, err := client.SM().GetPolicy(ctx, policyName)
	require.NoError(t, err)
	assert.NotNil(t, getResult)
	assert.Equal(t, policyName, getResult.Id)

	deleteResult, err := client.SM().DeletePolicy(ctx, policyName)
	require.NoError(t, err)
	assert.NotNil(t, deleteResult)
}
