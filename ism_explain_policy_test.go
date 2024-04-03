package opensearch

import (
	"testing"
)

func TestIsmExplainPolicyBuildURL(t *testing.T) {

	client := setupTestClient(t)

	tests := []struct {
		Name         string
		ExpectedPath string
		ExpectErr    bool
	}{
		{
			"",
			"/_plugins/_ism/explain/",
			false,
		},
		{
			"my-index",
			"/_plugins/_ism/explain/my-index",
			false,
		},
	}

	for i, test := range tests {
		builder := client.IsmExplainPolicy(test.Name)
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
