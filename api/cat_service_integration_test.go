//go:build integration

package api_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCatService_Indices(t *testing.T) {
	client := newIntegrationClient()
	ctx := context.Background()

	result, err := client.Cat().Indices(ctx, nil)
	require.NoError(t, err)
	assert.NotNil(t, result)
}

func TestCatService_Health(t *testing.T) {
	client := newIntegrationClient()
	ctx := context.Background()

	result, err := client.Cat().Health(ctx)
	require.NoError(t, err)
	assert.NotNil(t, result)
	assert.NotEmpty(t, result)
}

func TestCatService_Count(t *testing.T) {
	client := newIntegrationClient()
	ctx := context.Background()

	result, err := client.Cat().Count(ctx, nil)
	require.NoError(t, err)
	assert.NotNil(t, result)
	assert.NotEmpty(t, result)
}

func TestCatService_Master(t *testing.T) {
	client := newIntegrationClient()
	ctx := context.Background()

	result, err := client.Cat().Master(ctx)
	require.NoError(t, err)
	assert.NotNil(t, result)
	assert.NotEmpty(t, result)
}
