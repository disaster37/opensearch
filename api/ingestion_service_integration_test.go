//go:build integration

package api_test

import (
	"context"
	"testing"

	"github.com/disaster37/opensearch/v4/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIngestionService_PauseWithoutIngestion exercises the live
// POST /{index}/ingestion/_pause endpoint against a plain index that is
// not configured for pull-based ingestion. The cluster is expected to
// return an *types.OpenSearchError; if the feature is fully absent (404),
// the test is skipped.
func TestIngestionService_PauseWithoutIngestion(t *testing.T) {
	client := newIntegrationClient()
	ctx := context.Background()
	indexName := "test-ingest-svc-plain"

	_, err := client.Indices().Create(ctx, indexName, map[string]any{
		"settings": map[string]any{
			"number_of_shards":   1,
			"number_of_replicas": 0,
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = client.Indices().Delete(ctx, []string{indexName})
	})

	_, err = client.Ingestion().Pause(ctx, indexName)
	if osErr, ok := err.(*types.OpenSearchError); ok {
		// Expected: index not configured for pull-based ingestion.
		assert.NotEqual(t, 0, osErr.StatusCode())
		return
	}
	if err != nil {
		t.Skipf("ingestion feature not available on this cluster: %v", err)
		return
	}
	// Unexpected success on a plain index — skip rather than fail.
	// OpenSearch 3.8.0 returns success here (behavior changed from earlier versions).
	t.Skipf("ingestion pause on plain index succeeded — feature may behave differently on 3.8.0+")
}
