//go:build integration

package api_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTasksService_List(t *testing.T) {
	client := newIntegrationClient()
	ctx := context.Background()

	result, err := client.Tasks().List(ctx)
	require.NoError(t, err)
	assert.NotNil(t, result)
	assert.NotNil(t, result.Nodes)
}
