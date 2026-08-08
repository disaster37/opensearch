//go:build integration

package api_test

import (
	"context"
	"testing"

	"github.com/disaster37/opensearch/v4/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTieringService_ListStatus calls GET /_tier/all. On clusters where the
// writable_warm_index feature flag is disabled (the default for the Dagger
// two-node cluster), the endpoint returns an OpenSearchError and the test
// is skipped.
func TestTieringService_ListStatus(t *testing.T) {
	client := newIntegrationClient()
	ctx := context.Background()

	entries, err := client.Tiering().ListStatus(ctx, "")
	if osErr, ok := err.(*types.OpenSearchError); ok {
		t.Skipf("tiering API not enabled on this cluster (status %d)", osErr.StatusCode())
		return
	}
	require.NoError(t, err)
	assert.NotNil(t, entries)
}
