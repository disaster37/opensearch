package opensearch

import (
	"testing"
)

func TestCcrStartBuildURL(t *testing.T) {
	client := setupTestClient(t)

	tests := []struct {
		Name         string
		Body         any
		ExpectedPath string
		ExpectErr    bool
	}{
		{
			"test1",
			`{}`,
			"/_plugins/_replication/test1/_start",
			false,
		},
		{
			"",
			`{}`,
			"/_plugins/_replication/test1/_start",
			true,
		},
		{
			"test2",
			nil,
			"/_plugins/_replication/test1/_start",
			true,
		},
	}

	for i, test := range tests {
		builder := client.CcrStartRule(test.Name).Body(test.Body)
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
