package opensearch

import (
	"testing"
)

func TestTransformStartJobBuildURL(t *testing.T) {
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
			"my-job",
			"/_plugins/_transform/my-job/_start",
			false,
		},
	}

	for i, test := range tests {
		builder := client.TransformStartJob(test.Name)
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
