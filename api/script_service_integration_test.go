//go:build integration

package api_test

import (
	"context"
	"testing"

	"github.com/disaster37/opensearch/v4/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestScriptService_PutGetDelete(t *testing.T) {
	client := newIntegrationClient()
	ctx := context.Background()
	scriptId := "test-script"

	t.Cleanup(func() {
		client.Script().Delete(ctx, scriptId)
	})

	putResult, err := client.Script().Put(ctx, &api.ScriptPutRequest{
		Id: scriptId,
		Body: map[string]any{
			"script": map[string]any{
				"lang":   "painless",
				"source": "Math.log(_score * 2) + doc['my-int'].value",
			},
		},
	})
	require.NoError(t, err)
	assert.NotNil(t, putResult)
	assert.True(t, putResult.Acknowledged)

	getResult, err := client.Script().Get(ctx, scriptId)
	require.NoError(t, err)
	assert.NotNil(t, getResult)
	assert.True(t, getResult.Found)
	assert.Equal(t, scriptId, getResult.Id)

	deleteResult, err := client.Script().Delete(ctx, scriptId)
	require.NoError(t, err)
	assert.NotNil(t, deleteResult)
	assert.True(t, deleteResult.Acknowledged)
}
