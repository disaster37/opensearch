//go:build integration

package api_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTransformService_SearchJob(t *testing.T) {
	client := newIntegrationClient()
	ctx := context.Background()

	result, err := client.Transform().SearchJob(ctx, nil)
	require.NoError(t, err)
	assert.NotNil(t, result)
}
