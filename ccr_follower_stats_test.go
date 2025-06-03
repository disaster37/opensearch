package opensearch

import (
	"testing"
)

func TestCcrFollowerStatsBuildURL(t *testing.T) {
	client := setupTestClient(t)

	tests := []struct {
		ExpectedPath string
		ExpectErr    bool
	}{
		{
			"/_plugins/_replication/follower_stats",
			false,
		},
	}

	for i, test := range tests {
		builder := client.CcrFollowerStats()
		err := builder.Validate()
		if err != nil {
			if !test.ExpectErr {
				t.Errorf("case #%d: %v", i+1, err)
				continue
			}
		} else {
			// err == nil
			if test.ExpectErr {
				t.Errorf("case #%d: expected error", i+1)
				continue
			}
			path, _, _ := builder.buildURL()
			if path != test.ExpectedPath {
				t.Errorf("case #%d: expected %q; got: %q", i+1, test.ExpectedPath, path)
			}
		}
	}
}
