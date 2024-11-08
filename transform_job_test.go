package opensearch

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/utils/ptr"
)

func TestTransformJob(t *testing.T) {
	client := setupTestClientAndCreateIndexAndAddDocs(t)
	var err error

	expecedTransformJob := &TransformPutJob{
		Transform: TransformJobBase{
			Enabled:    ptr.To[bool](true),
			Continuous: ptr.To[bool](true),
			Schedule: map[string]any{
				"interval": map[string]any{
					"period":     1,
					"unit":       "Minutes",
					"start_time": 1602100553,
				},
			},
			Description:        ptr.To[string]("Sample transform job"),
			SourceIndex:        "opensearch-test",
			TargetIndex:        "sample_target",
			DataSelectionQuery: NewMatchAllQuery(),
			PageSize:           1,
			Groups: []any{
				map[string]any{
					"terms": map[string]any{
						"source_field": "user",
						"target_field": "user",
					},
				},
				map[string]any{
					"terms": map[string]any{
						"source_field": "tags",
						"target_field": "tags",
					},
				},
			},
			Aggregations: map[string]any{
				"quantity": map[string]any{
					"sum": map[string]any{
						"field": "total_quantity",
					},
				},
			},
		},
	}

	// Create transform job
	resPut, err := client.TransformPutJob("test").Body(expecedTransformJob).Do(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	assert.NotNil(t, resPut)

	// Get transform job
	resGet, err := client.TransformGetJob("test").Do(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	assert.NotNil(t, resGet)
	assert.NotNil(t, resGet.Transform)

	// Search transform jon
	resSearch, err := client.TransformSearchJob().Search("tes*").Size(1000).Do(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	assert.NotNil(t, resGet)
	assert.Equal(t, int64(1), resSearch.TotalTransforms)
	assert.NotEmpty(t, resSearch.Transforms)

	// Update transform job
	expecedTransformJob.Transform.Description = ptr.To[string]("test")
	_, err = client.TransformPutJob("test").Body(expecedTransformJob).SequenceNumber(resGet.SequenceNumber).PrimaryTerm(resGet.PrimaryTerm).Do(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	resGet, err = client.TransformGetJob("test").Do(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, "test", *resGet.Transform.Description)

	// Start job
	resStart, err := client.TransformStartJob("test").Do(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	assert.True(t, resStart.Acknowledged)

	// Stop job
	resStop, err := client.TransformStopJob("test").Do(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	assert.True(t, resStop.Acknowledged)

	// Explain job
	resExplain, err := client.TransformExplainJob("test").Do(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	assert.NotNil(t, resExplain)

	// Delete job
	resDelete, err := client.TransformDeleteJob("test").Do(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	assert.NotNil(t, resDelete)
	_, err = client.TransformGetJob("test").Do(context.Background())
	assert.True(t, IsNotFound(err))

	// Preview job
	resPreview, err := client.TransformPreviewJobResults(expecedTransformJob).Do(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	assert.NotNil(t, resPreview)
}
