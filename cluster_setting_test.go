package opensearch

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestClusterSetting(t *testing.T) {
	client := setupTestClient(t)
	var err error

	// Put setting
	expectedSettings := map[string]any{
		"persistent": map[string]any{
			"cluster": map[string]any{
				"max_shards_per_node": "500",
			},
		},
	}

	if _, err = client.ClusterPutSetting().Body(expectedSettings).Do(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Get settings
	settings, err := client.ClusterGetSetting().Do(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	// clean noise
	settingCluster := settings["persistent"].(map[string]any)
	delete(settingCluster, "plugins")

	assert.Equal(t, expectedSettings["persistent"], settingCluster)
}
