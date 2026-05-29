package api

import "github.com/disaster37/opensearch/v3/types"

// KnnStatsResponse represents the response from the KNN stats endpoint.
// The map contains cluster-level statistics about the k-NN plugin.
type KnnStatsResponse map[string]any

// KnnWarmupResponse represents the response from the KNN warmup endpoint.
// It contains shard-level information about the warmup operation.
type KnnWarmupResponse struct {
	Shards *types.ShardsInfo `json:"_shards,omitempty"`
}

// KnnTrainModelResponse represents the response from training a k-NN model.
// The map contains the trained model identifier and training status.
type KnnTrainModelResponse map[string]any

// KnnGetModelResponse represents the response from retrieving a k-NN model
// by its identifier. The map contains the model definition and metadata.
type KnnGetModelResponse map[string]any

// KnnSearchModelsResponse represents the response from searching k-NN models.
// The map contains search hits and pagination information.
type KnnSearchModelsResponse map[string]any
