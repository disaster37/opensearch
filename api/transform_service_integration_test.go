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

	result, err := client.Transform().SearchJob(ctx, map[string]any{
		"query": map[string]any{
			"match_all": map[string]any{},
		},
	})
	require.NoError(t, err)
	assert.NotNil(t, result)
}
