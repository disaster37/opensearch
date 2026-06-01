package api

import "github.com/disaster37/opensearch/v4/types"

// NeuralStatsResponse represents the response from the neural plugin
// stats endpoint. The map contains cluster-level statistics about the
// neural search plugin.
type NeuralStatsResponse map[string]any

// NeuralWarmupResponse represents the response from the neural plugin
// warmup endpoint. It contains shard-level information about the warmup.
type NeuralWarmupResponse struct {
	Shards *types.ShardsInfo `json:"_shards,omitempty"`
}
