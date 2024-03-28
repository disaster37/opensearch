package opensearch

import (
	"testing"

	"k8s.io/utils/ptr"
)

func TestTransformPutJobBuildURL(t *testing.T) {

	client := setupTestClient(t)

	tests := []struct {
		Name         string
		Body         any
		ExpectedPath string
		ExpectErr    bool
		SeqNum       *int64
		PrimaryTerm  *int64
	}{
		{
			"",
			nil,
			"",
			true,
			nil,
			nil,
		},
		{
			"my-job",
			nil,
			"",
			true,
			nil,
			nil,
		},
		{
			"",
			`{}`,
			"",
			true,
			nil,
			nil,
		},
		{
			"my-job",
			`{}`,
			"/_plugins/_transform/my-job",
			false,
			nil,
			nil,
		},
		{
			"my-job",
			`{}`,
			"/_plugins/_transform/my-job?if_seq_no=10&if_primary_term=1",
			false,
			ptr.To[int64](10),
			ptr.To[int64](1),
		},
	}

	for i, test := range tests {
		var builder *TransformPutJobService
		if test.PrimaryTerm != nil && test.SeqNum != nil {
			builder = client.TransformPutJob(test.Name).Body(test.Body).SequenceNumber(*test.SeqNum).PrimaryTerm(*test.PrimaryTerm)
		} else {
			builder = client.TransformPutJob(test.Name).Body(test.Body)
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
