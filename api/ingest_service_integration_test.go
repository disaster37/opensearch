//go:build integration

package api_test

import (
	"context"
	"testing"

	"github.com/disaster37/opensearch/v3/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIngestService_PutGetDeletePipeline(t *testing.T) {
	client := newIntegrationClient()
	ctx := context.Background()
	pipelineId := "test-pipeline"

	t.Cleanup(func() {
		client.Ingest().DeletePipeline(ctx, pipelineId)
	})

	putResult, err := client.Ingest().PutPipeline(ctx, &api.IngestPutPipelineRequest{
		Id: pipelineId,
		Body: map[string]any{
			"description": "test",
			"processors": []map[string]any{
				{
					"set": map[string]any{
						"field": "test_field",
						"value": "hello",
					},
				},
			},
		},
	})
	require.NoError(t, err)
	assert.NotNil(t, putResult)
	assert.True(t, putResult.Acknowledged)

	getResult, err := client.Ingest().GetPipeline(ctx, []string{pipelineId})
	require.NoError(t, err)
	assert.NotNil(t, getResult)
	assert.Contains(t, getResult, pipelineId)

	deleteResult, err := client.Ingest().DeletePipeline(ctx, pipelineId)
	require.NoError(t, err)
	assert.NotNil(t, deleteResult)
	assert.True(t, deleteResult.Acknowledged)
}
