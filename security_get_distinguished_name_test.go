package opensearch

import (
	"testing"
)

func TestSecurityGetDistinguishedNameBuildURL(t *testing.T) {
	client := setupTestClient(t)

	tests := []struct {
		Name         string
		ExpectedPath string
		ExpectErr    bool
	}{
		{
			"",
			"",
			true,
		},
		{
			"my-cluster",
			"/_plugins/_security/api/nodesdn/my-cluster",
			false,
		},
	}

	for i, test := range tests {
		builder := client.SecurityGetDistinguishedName(test.Name)
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
