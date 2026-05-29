//go:build integration

package api_test

import (
	"context"
	"testing"

	"github.com/disaster37/opensearch/v3/api"
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
