package api

import (
	"fmt"
	"testing"

	"github.com/disaster37/opensearch/v4/querydsl"
	"github.com/disaster37/opensearch/v4/types"
	"github.com/stretchr/testify/assert"
)

// mockQueryError implements querydsl.Query and always returns an error from Source().
type mockQueryError struct{}

func (m mockQueryError) Source() (any, error) {
	return nil, fmt.Errorf("mock query error")
}

func TestIndexRequest_Validate(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		req := &IndexRequest{Index: "my-index"}
		assert.NoError(t, req.Validate())
	})

	t.Run("missing index", func(t *testing.T) {
		req := &IndexRequest{}
		assert.Error(t, req.Validate())
	})
}

func TestGetRequest_Validate(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		req := &GetRequest{Index: "my-index", Id: "doc-1"}
		assert.NoError(t, req.Validate())
	})

	t.Run("missing index", func(t *testing.T) {
		req := &GetRequest{Id: "doc-1"}
		assert.Error(t, req.Validate())
	})

	t.Run("missing id", func(t *testing.T) {
		req := &GetRequest{Index: "my-index"}
		assert.Error(t, req.Validate())
	})
}

func TestDeleteRequest_Validate(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		req := &DeleteRequest{Index: "my-index", Id: "doc-1"}
		assert.NoError(t, req.Validate())
	})

	t.Run("missing index", func(t *testing.T) {
		req := &DeleteRequest{Id: "doc-1"}
		assert.Error(t, req.Validate())
	})

	t.Run("missing id", func(t *testing.T) {
		req := &DeleteRequest{Index: "my-index"}
		assert.Error(t, req.Validate())
	})
}

func TestUpdateRequest_Validate(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		req := &UpdateRequest{Index: "my-index", Id: "doc-1", Body: map[string]any{}}
		assert.NoError(t, req.Validate())
	})

	t.Run("missing index", func(t *testing.T) {
		req := &UpdateRequest{Id: "doc-1"}
		assert.Error(t, req.Validate())
	})

	t.Run("missing id", func(t *testing.T) {
		req := &UpdateRequest{Index: "my-index"}
		assert.Error(t, req.Validate())
	})
}

func TestScrollRequest_Validate(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		req := &ScrollRequest{ScrollId: "scroll-123"}
		assert.NoError(t, req.Validate())
	})

	t.Run("missing scroll_id", func(t *testing.T) {
		req := &ScrollRequest{}
		assert.Error(t, req.Validate())
	})
}

func TestShrinkRequest_Validate(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		req := &ShrinkRequest{Source: "source-idx", Target: "target-idx"}
		assert.NoError(t, req.Validate())
	})

	t.Run("missing source", func(t *testing.T) {
		req := &ShrinkRequest{Target: "target-idx"}
		assert.Error(t, req.Validate())
	})

	t.Run("missing target", func(t *testing.T) {
		req := &ShrinkRequest{Source: "source-idx"}
		assert.Error(t, req.Validate())
	})
}

func TestPutAliasRequest_Validate(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		req := &PutAliasRequest{Index: "my-index", Alias: "my-alias"}
		assert.NoError(t, req.Validate())
	})

	t.Run("missing index", func(t *testing.T) {
		req := &PutAliasRequest{Alias: "my-alias"}
		assert.Error(t, req.Validate())
	})

	t.Run("missing alias", func(t *testing.T) {
		req := &PutAliasRequest{Index: "my-index"}
		assert.Error(t, req.Validate())
	})
}

func TestGetFieldMappingRequest_Validate(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		req := &GetFieldMappingRequest{Fields: []string{"title"}}
		assert.NoError(t, req.Validate())
	})

	t.Run("missing fields", func(t *testing.T) {
		req := &GetFieldMappingRequest{}
		assert.Error(t, req.Validate())
	})

	t.Run("empty fields", func(t *testing.T) {
		req := &GetFieldMappingRequest{Fields: []string{}}
		assert.Error(t, req.Validate())
	})
}

func TestPutMappingRequest_Validate(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		req := &PutMappingRequest{Indices: []string{"my-index"}, Body: map[string]any{}}
		assert.NoError(t, req.Validate())
	})

	t.Run("missing indices", func(t *testing.T) {
		req := &PutMappingRequest{Body: map[string]any{}}
		assert.Error(t, req.Validate())
	})

	t.Run("empty indices", func(t *testing.T) {
		req := &PutMappingRequest{Indices: []string{}, Body: map[string]any{}}
		assert.Error(t, req.Validate())
	})

	t.Run("missing body", func(t *testing.T) {
		req := &PutMappingRequest{Indices: []string{"my-index"}}
		assert.Error(t, req.Validate())
	})
}

func TestPutTemplateRequest_Validate(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		req := &PutTemplateRequest{Name: "my-template", Body: map[string]any{}}
		assert.NoError(t, req.Validate())
	})

	t.Run("missing name", func(t *testing.T) {
		req := &PutTemplateRequest{Body: map[string]any{}}
		assert.Error(t, req.Validate())
	})

	t.Run("missing body", func(t *testing.T) {
		req := &PutTemplateRequest{Name: "my-template"}
		assert.Error(t, req.Validate())
	})
}

func TestPutIndexTemplateRequest_Validate(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		req := &PutIndexTemplateRequest{Name: "my-template", Body: map[string]any{}}
		assert.NoError(t, req.Validate())
	})

	t.Run("missing name", func(t *testing.T) {
		req := &PutIndexTemplateRequest{Body: map[string]any{}}
		assert.Error(t, req.Validate())
	})

	t.Run("missing body", func(t *testing.T) {
		req := &PutIndexTemplateRequest{Name: "my-template"}
		assert.Error(t, req.Validate())
	})
}

func TestPutComponentTemplateRequest_Validate(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		req := &PutComponentTemplateRequest{Name: "my-template", Body: map[string]any{}}
		assert.NoError(t, req.Validate())
	})

	t.Run("missing name", func(t *testing.T) {
		req := &PutComponentTemplateRequest{Body: map[string]any{}}
		assert.Error(t, req.Validate())
	})

	t.Run("missing body", func(t *testing.T) {
		req := &PutComponentTemplateRequest{Name: "my-template"}
		assert.Error(t, req.Validate())
	})
}

func TestSnapshotCreateRequest_Validate(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		req := &SnapshotCreateRequest{Repository: "my-repo", Snapshot: "snap-1"}
		assert.NoError(t, req.Validate())
	})

	t.Run("missing repository", func(t *testing.T) {
		req := &SnapshotCreateRequest{Snapshot: "snap-1"}
		assert.Error(t, req.Validate())
	})

	t.Run("missing snapshot", func(t *testing.T) {
		req := &SnapshotCreateRequest{Repository: "my-repo"}
		assert.Error(t, req.Validate())
	})
}

func TestSnapshotGetRequest_Validate(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		req := &SnapshotGetRequest{Repository: "my-repo"}
		assert.NoError(t, req.Validate())
	})

	t.Run("missing repository", func(t *testing.T) {
		req := &SnapshotGetRequest{}
		assert.Error(t, req.Validate())
	})
}

func TestSnapshotDeleteRequest_Validate(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		req := &SnapshotDeleteRequest{Repository: "my-repo", Snapshot: "snap-1"}
		assert.NoError(t, req.Validate())
	})

	t.Run("missing repository", func(t *testing.T) {
		req := &SnapshotDeleteRequest{Snapshot: "snap-1"}
		assert.Error(t, req.Validate())
	})

	t.Run("missing snapshot", func(t *testing.T) {
		req := &SnapshotDeleteRequest{Repository: "my-repo"}
		assert.Error(t, req.Validate())
	})
}

func TestSnapshotStatusRequest_Validate(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		req := &SnapshotStatusRequest{Repository: "my-repo"}
		assert.NoError(t, req.Validate())
	})

	t.Run("missing repository", func(t *testing.T) {
		req := &SnapshotStatusRequest{}
		assert.Error(t, req.Validate())
	})
}

func TestSnapshotRestoreRequest_Validate(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		req := &SnapshotRestoreRequest{Repository: "my-repo", Snapshot: "snap-1"}
		assert.NoError(t, req.Validate())
	})

	t.Run("missing repository", func(t *testing.T) {
		req := &SnapshotRestoreRequest{Snapshot: "snap-1"}
		assert.Error(t, req.Validate())
	})

	t.Run("missing snapshot", func(t *testing.T) {
		req := &SnapshotRestoreRequest{Repository: "my-repo"}
		assert.Error(t, req.Validate())
	})
}

func TestSmPutPolicyRequest_Validate(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		req := &SmPutPolicyRequest{PolicyName: "test", Body: &SmPutPolicy{}}
		assert.NoError(t, req.Validate())
	})

	t.Run("missing policy name", func(t *testing.T) {
		req := &SmPutPolicyRequest{Body: &SmPutPolicy{}}
		assert.Error(t, req.Validate())
	})

	t.Run("missing body", func(t *testing.T) {
		req := &SmPutPolicyRequest{PolicyName: "test"}
		assert.Error(t, req.Validate())
	})
}

func TestIsmPutPolicyRequest_Validate(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		req := &IsmPutPolicyRequest{PolicyName: "test", Body: &IsmPolicyBase{}}
		assert.NoError(t, req.Validate())
	})

	t.Run("missing policy name", func(t *testing.T) {
		req := &IsmPutPolicyRequest{Body: &IsmPolicyBase{}}
		assert.Error(t, req.Validate())
	})

	t.Run("missing body", func(t *testing.T) {
		req := &IsmPutPolicyRequest{PolicyName: "test"}
		assert.Error(t, req.Validate())
	})
}

func TestAlertingPutMonitorRequest_Validate(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		req := &AlertingPutMonitorRequest{MonitorId: "mon-1", Body: map[string]any{}}
		assert.NoError(t, req.Validate())
	})

	t.Run("missing monitor id", func(t *testing.T) {
		req := &AlertingPutMonitorRequest{Body: map[string]any{}}
		assert.Error(t, req.Validate())
	})

	t.Run("missing body", func(t *testing.T) {
		req := &AlertingPutMonitorRequest{MonitorId: "mon-1"}
		assert.Error(t, req.Validate())
	})
}

func TestTransformPutJobRequest_Validate(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		req := &TransformPutJobRequest{JobName: "job-1", Body: &TransformJobBase{}}
		assert.NoError(t, req.Validate())
	})

	t.Run("missing job name", func(t *testing.T) {
		req := &TransformPutJobRequest{Body: &TransformJobBase{}}
		assert.Error(t, req.Validate())
	})

	t.Run("missing body", func(t *testing.T) {
		req := &TransformPutJobRequest{JobName: "job-1"}
		assert.Error(t, req.Validate())
	})
}

func TestCcrDeleteAutoFollowOptions_Validate(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		req := &CcrDeleteAutoFollowOptions{LeaderAlias: "leader", Name: "rule-1"}
		assert.NoError(t, req.Validate())
	})

	t.Run("missing leader alias", func(t *testing.T) {
		req := &CcrDeleteAutoFollowOptions{Name: "rule-1"}
		assert.Error(t, req.Validate())
	})

	t.Run("missing name", func(t *testing.T) {
		req := &CcrDeleteAutoFollowOptions{LeaderAlias: "leader"}
		assert.Error(t, req.Validate())
	})
}

func TestCcrStartRuleRequest_Validate(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		req := &CcrStartRuleRequest{Name: "rule-1"}
		assert.NoError(t, req.Validate())
	})

	t.Run("missing name", func(t *testing.T) {
		req := &CcrStartRuleRequest{}
		assert.Error(t, req.Validate())
	})
}

func TestIngestPutPipelineRequest_Validate(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		req := &IngestPutPipelineRequest{Id: "pipe-1", Body: map[string]any{}}
		assert.NoError(t, req.Validate())
	})

	t.Run("missing id", func(t *testing.T) {
		req := &IngestPutPipelineRequest{Body: map[string]any{}}
		assert.Error(t, req.Validate())
	})

	t.Run("missing body", func(t *testing.T) {
		req := &IngestPutPipelineRequest{Id: "pipe-1"}
		assert.Error(t, req.Validate())
	})
}

func TestIngestSimulatePipelineRequest_Validate(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		req := &IngestSimulatePipelineRequest{Body: map[string]any{}}
		assert.NoError(t, req.Validate())
	})

	t.Run("missing body", func(t *testing.T) {
		req := &IngestSimulatePipelineRequest{}
		assert.Error(t, req.Validate())
	})
}

func TestScriptPutRequest_Validate(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		req := &ScriptPutRequest{Id: "script-1", Body: map[string]any{}}
		assert.NoError(t, req.Validate())
	})

	t.Run("missing id", func(t *testing.T) {
		req := &ScriptPutRequest{Body: map[string]any{}}
		assert.Error(t, req.Validate())
	})

	t.Run("missing body", func(t *testing.T) {
		req := &ScriptPutRequest{Id: "script-1"}
		assert.Error(t, req.Validate())
	})
}

func TestValidationError(t *testing.T) {
	req := &GetRequest{}
	err := req.Validate()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "validation:")
}

func TestIsmPutPolicyRequest_Validate_WithVersion(t *testing.T) {
	seqNo := int64(1)
	primaryTerm := int64(1)
	req := &IsmPutPolicyRequest{
		PolicyName: "test",
		Body:       &IsmPolicyBase{},
		Version:    &types.DocumentVersion{SeqNo: &seqNo, PrimaryTerm: &primaryTerm},
	}
	assert.NoError(t, req.Validate())
}

func TestNewSearchRequest(t *testing.T) {
	t.Run("simple query", func(t *testing.T) {
		qr := querydsl.NewSearchRequest().
			Index("my-index").
			Query(querydsl.NewMatchAllQuery()).
			Size(10)

		req, err := NewSearchRequest(qr)
		assert.NoError(t, err)
		assert.Equal(t, []string{"my-index"}, req.Indices)
		assert.NotNil(t, req.Body)
		assert.Nil(t, req.Params)
	})

	t.Run("multiple indices", func(t *testing.T) {
		qr := querydsl.NewSearchRequest().
			Index("idx1", "idx2").
			Query(querydsl.NewMatchAllQuery())

		req, err := NewSearchRequest(qr)
		assert.NoError(t, err)
		assert.Equal(t, []string{"idx1", "idx2"}, req.Indices)
	})

	t.Run("with url params", func(t *testing.T) {
		qr := querydsl.NewSearchRequest().
			Index("my-index").
			SearchType("dfs_query_then_fetch").
			Routing("r1").
			Scroll("5m").
			Query(querydsl.NewMatchAllQuery())

		req, err := NewSearchRequest(qr)
		assert.NoError(t, err)
		assert.Equal(t, SearchTypeDfsQueryThenFetch, req.Params.SearchType)
		assert.Equal(t, "r1", req.Params.Routing)
		assert.Equal(t, "5m", req.Params.Scroll)
	})

	t.Run("no indices", func(t *testing.T) {
		qr := querydsl.NewSearchRequest().Query(querydsl.NewMatchAllQuery())

		req, err := NewSearchRequest(qr)
		assert.NoError(t, err)
		assert.Empty(t, req.Indices)
	})

	t.Run("error from body", func(t *testing.T) {
		qr := querydsl.NewSearchRequest().
			Query(mockQueryError{})

		_, err := NewSearchRequest(qr)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "search request body")
	})
}
