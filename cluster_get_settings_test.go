package opensearch

import (
	"testing"
)

func TestClusterGetSettingBuildURL(t *testing.T) {

	client := setupTestClient(t)

	tests := []struct {
		ExpectedPath string
		ExpectErr    bool
	}{
		{
			"/_cluster/settings",
			false,
		},
	}

	for i, test := range tests {
		builder := client.ClusterGetSetting()
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
