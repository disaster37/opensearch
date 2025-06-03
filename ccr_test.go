package opensearch

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCcr(t *testing.T) {
	client := setupTestClientAndCreateIndex(t)
	var err error

	// CCR rule
	rule := &CcrRule{
		LeaderAlias: "test",
		LeaderIndex: "leader-01",
		UseRoles: CcrRuleUseRoles{
			LeaderClusterRole:   "all_access",
			FollowerClusterRole: "all_access",
		},
	}

	// Start CCR rule
	resStart, err := client.CcrStartRule("follower-index").Body(rule).Do(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	assert.True(t, resStart.Acknowledged)

	// Wait finish to restore
	time.Sleep(60 * time.Second)

	// Pause CCR
	resPause, err := client.CcrPauseRule("follower-index").Do(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	assert.True(t, resPause.Acknowledged)

	// Resume CCR
	resResume, err := client.CcrResumeRule("follower-index").Do(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	assert.True(t, resResume.Acknowledged)

	// Get CCR status
	resStatus, err := client.CcrStatusRule("follower-index").Do(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	assert.NotEmpty(t, resStatus.Status)

	// Get CCR follower stats
	resFollowerStats, err := client.CcrFollowerStats().Do(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	assert.NotNil(t, resFollowerStats)

	// Get CCR leader stats
	resLeaderStats, err := client.CcrLeaderStats().Do(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	assert.NotNil(t, resLeaderStats)

	// Delete CCR rule
	resDelete, err := client.CcrStopRule("follower-index").Do(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	assert.True(t, resDelete.Acknowledged)

	// Auto follow rule
	autoFollowRule := &CcrAutoFollowRule{
		LeaderAlias: "test",
		Name:        "test",
		Pattern:     "test*",
		UseRoles: CcrRuleUseRoles{
			LeaderClusterRole:   "all_access",
			FollowerClusterRole: "all_access",
		},
	}

	// Start auto follow rule
	resAutoFollow, err := client.CcrPostAutoFollow().Body(autoFollowRule).Do(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	assert.True(t, resAutoFollow.Acknowledged)

	// Get stats
	resAutoFollowStats, err := client.CcrAutoFollowStatus().Do(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	assert.NotNil(t, resAutoFollowStats)

	// Delete auto follow rule
	resDeleteAutoFollow, err := client.CcrDeleteAutoFollow("test", "test").Do(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	assert.True(t, resDeleteAutoFollow.Acknowledged)
}
