package api

import "github.com/disaster37/opensearch/v4/types"

// IngestionStateResponse is the response from POST /{index}/ingestion/_pause
// and POST /{index}/ingestion/_resume (pull-based ingestion, GA in
// OpenSearch 3.6.0).
type IngestionStateResponse struct {
	Acknowledged       bool                               `json:"acknowledged"`
	ShardsAcknowledged bool                               `json:"shards_acknowledged"`
	Failures           map[string][]IngestionShardFailure `json:"failures"`
	Error              string                             `json:"error"`
}

// IngestionShardFailure describes a single shard failure within an
// ingestion pause/resume response.
type IngestionShardFailure struct {
	Shard int    `json:"shard"`
	Error string `json:"error"`
}

// ShardIngestionState describes the state of a single shard's pull-based
// ingestion poller as returned by GET /{index}/ingestion/_state.
type ShardIngestionState struct {
	Shard             int    `json:"shard"`
	PollerState       string `json:"poller_state"`
	ErrorPolicy       string `json:"error_policy"`
	PollerPaused      bool   `json:"poller_paused"`
	WriteBlockEnabled bool   `json:"write_block_enabled"`
	BatchStartPointer string `json:"batch_start_pointer"`
}

// GetIngestionStateResponse is the response from
// GET /{index}/ingestion/_state.
type GetIngestionStateResponse struct {
	IngestionState map[string][]ShardIngestionState `json:"ingestion_state"`
	NextPageToken  string                           `json:"next_page_token"`
	Shards         *types.ShardsInfo                `json:"_shards"`
}
