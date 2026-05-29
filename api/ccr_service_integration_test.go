//go:build integration

package api_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCcrService_AutoFollowStatus(t *testing.T) {
	client := newIntegrationClient()
	ctx := context.Background()

	result, err := client.CCR().AutoFollowStatus(ctx)
	require.NoError(t, err)
	assert.NotNil(t, result)
}

func TestCcrService_FollowerStats(t *testing.T) {
	client := newIntegrationClient()
	ctx := context.Background()

	result, err := client.CCR().FollowerStats(ctx)
	require.NoError(t, err)
	assert.NotNil(t, result)
}

func TestCcrService_LeaderStats(t *testing.T) {
	client := newIntegrationClient()
	ctx := context.Background()

	result, err := client.CCR().LeaderStats(ctx)
	require.NoError(t, err)
	assert.NotNil(t, result)
}
