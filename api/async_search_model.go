package api

// AsyncSearchSubmitResponse represents the response from submitting an
// asynchronous search. It contains the search identifier, state, timing
// information, expiration time, and the search response if available.
type AsyncSearchSubmitResponse struct {
	Id             string         `json:"id,omitempty"`
	State          string         `json:"state,omitempty"`
	StartInMillis  int64          `json:"start_time_in_millis,omitempty"`
	ExpirationTime string         `json:"expiration_time,omitempty"`
	Response       map[string]any `json:"response,omitempty"`
}

// AsyncSearchGetResponse represents the response from retrieving an
// asynchronous search by its identifier. It embeds the submit response
// fields to provide full search status and results.
type AsyncSearchGetResponse struct {
	AsyncSearchSubmitResponse
}

// AsyncSearchStatsResponse represents the response from the asynchronous
// search stats endpoint. The map contains cluster-level statistics about
// asynchronous search execution.
type AsyncSearchStatsResponse map[string]any
