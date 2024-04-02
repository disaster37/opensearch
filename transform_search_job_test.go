package opensearch

import (
	"testing"

	"k8s.io/utils/ptr"
)

func TestTransformSearchJobBuildURL(t *testing.T) {

	client := setupTestClient(t)

	tests := []struct {
		Search        *string
		From          *int64
		Size          *int64
		SortField     *string
		SortDirection *string
		ExpectedPath  string
		ExpectErr     bool
	}{
		{
			nil,
			nil,
			nil,
			nil,
			nil,
			"/_plugins/_transform",
			false,
		},
		{
			ptr.To[string]("job-*"),
			ptr.To[int64](0),
			ptr.To[int64](1000),
			ptr.To[string]("transform_id"),
			ptr.To[string]("ASC"),
			"/_plugins/_transform",
			false,
		},
	}

	for i, test := range tests {
		builder := client.TransformSearchJob()
		if test.Search != nil {
			builder.Search(*test.Search)
		}
		if test.From != nil {
			builder.From(*test.From)
		}
		if test.Size != nil {
			builder.Size(*test.Size)
		}
		if test.SortField != nil {
			builder.SortField(*test.SortField)
		}
		if test.SortDirection != nil {
			builder.SortDirection(*test.SortDirection)
		}
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
