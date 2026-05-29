//go:build integration

package api_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSnapshotService_CreateGetDeleteRepository(t *testing.T) {
	client := newIntegrationClient()
	ctx := context.Background()
	repoName := "test-repo"

	t.Cleanup(func() {
		client.Snapshot().DeleteRepository(ctx, repoName)
	})

	createResult, err := client.Snapshot().CreateRepository(ctx, repoName, map[string]any{
		"type": "fs",
		"settings": map[string]any{
			"location": "/usr/share/opensearch/backup/test-repo",
		},
	})
	require.NoError(t, err)
	assert.NotNil(t, createResult)
	assert.True(t, createResult.Acknowledged)

	getResult, err := client.Snapshot().GetRepository(ctx, []string{repoName})
	require.NoError(t, err)
	assert.NotNil(t, getResult)
	assert.Contains(t, getResult, repoName)
	repo := getResult[repoName]
	assert.Equal(t, "fs", repo.Type)

	deleteResult, err := client.Snapshot().DeleteRepository(ctx, repoName)
	require.NoError(t, err)
	assert.NotNil(t, deleteResult)
	assert.True(t, deleteResult.Acknowledged)
}
