package opensearch

import (
	"testing"
)

func TestCcrResumeBuildURL(t *testing.T) {
	client := setupTestClient(t)

	tests := []struct {
		Name         string
		ExpectedPath string
		ExpectErr    bool
	}{
		{
			"test1",
			"/_plugins/_replication/test1/_resume",
			false,
		},
		{
			"",
			"/_plugins/_replication/test1/_resume",
			true,
		},
	}

	for i, test := range tests {
		builder := client.CcrResumeRule(test.Name)
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
