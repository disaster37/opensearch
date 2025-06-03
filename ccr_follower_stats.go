package opensearch

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
)

// CcrFollowerStatsService get CCR follower stats.
// See https://docs.opensearch.org/docs/latest/tuning-your-cluster/replication-plugin/api/
type CcrFollowerStatsService struct {
	client *Client

	pretty     *bool       // pretty format the returned JSON response
	human      *bool       // return human readable values for statistics
	errorTrace *bool       // include the stack trace of returned errors
	filterPath []string    // list of filters used to reduce the response
	headers    http.Header // custom request-level HTTP headers
}

// NewCcrFollowerStatsService creates a new CcrFollowerStatsService.
func NewCcrFollowerStatsService(client *Client) *CcrFollowerStatsService {
	return &CcrFollowerStatsService{
		client: client,
	}
}

// Pretty tells Opensearch whether to return a formatted JSON response.
func (s *CcrFollowerStatsService) Pretty(pretty bool) *CcrFollowerStatsService {
	s.pretty = &pretty
	return s
}

// Human specifies whether human readable values should be returned in
// the JSON response, e.g. "7.5mb".
func (s *CcrFollowerStatsService) Human(human bool) *CcrFollowerStatsService {
	s.human = &human
	return s
}

// ErrorTrace specifies whether to include the stack trace of returned errors.
func (s *CcrFollowerStatsService) ErrorTrace(errorTrace bool) *CcrFollowerStatsService {
	s.errorTrace = &errorTrace
	return s
}

// FilterPath specifies a list of filters used to reduce the response.
func (s *CcrFollowerStatsService) FilterPath(filterPath ...string) *CcrFollowerStatsService {
	s.filterPath = filterPath
	return s
}

// Header adds a header to the request.
func (s *CcrFollowerStatsService) Header(name string, value string) *CcrFollowerStatsService {
	if s.headers == nil {
		s.headers = http.Header{}
	}
	s.headers.Add(name, value)
	return s
}

// Headers specifies the headers of the request.
func (s *CcrFollowerStatsService) Headers(headers http.Header) *CcrFollowerStatsService {
	s.headers = headers
	return s
}

// buildURL builds the URL for the operation.
func (s *CcrFollowerStatsService) buildURL() (string, url.Values, error) {
	// Build URL
	path := "/_plugins/_replication/follower_stats"

	// Add query string parameters
	params := url.Values{}
	if v := s.pretty; v != nil {
		params.Set("pretty", fmt.Sprint(*v))
	}
	if v := s.human; v != nil {
		params.Set("human", fmt.Sprint(*v))
	}
	if v := s.errorTrace; v != nil {
		params.Set("error_trace", fmt.Sprint(*v))
	}
	if len(s.filterPath) > 0 {
		params.Set("filter_path", strings.Join(s.filterPath, ","))
	}
	return path, params, nil
}

// Validate checks if the operation is valid.
func (s *CcrFollowerStatsService) Validate() error {
	return nil
}

// Do executes the operation.
func (s *CcrFollowerStatsService) Do(ctx context.Context) (*CcrFollowerStatsResponse, error) {
	// Check pre-conditions
	if err := s.Validate(); err != nil {
		return nil, err
	}

	// Get URL for request
	path, params, err := s.buildURL()
	if err != nil {
		return nil, err
	}

	// Get HTTP response
	res, err := s.client.PerformRequest(ctx, PerformRequestOptions{
		Method:  "GET",
		Path:    path,
		Params:  params,
		Headers: s.headers,
	})
	if err != nil {
		return nil, err
	}

	// Return operation response
	ret := new(CcrFollowerStatsResponse)
	if err := json.Unmarshal(res.Body, ret); err != nil {
		return nil, err
	}
	return ret, nil
}

// CcrFollowerStatsResponse get status of follower
// https://opensearch.org/docs/latest/im-plugin/ism/api/#get-policy
type CcrFollowerStatsResponse struct {
	CcrStatusFollowerState
	NumSyncingIndices       int64                             `json:"num_syncing_indices"`
	NumBootstrappingIndices int64                             `json:"num_bootstrapping_indices"`
	NumPausedIndices        int64                             `json:"num_paused_indices"`
	NumFailedIndices        int64                             `json:"num_failed_indices"`
	NumShardTasks           int64                             `json:"num_shard_tasks"`
	NumIndexTasks           int64                             `json:"num_index_tasks"`
	IndexStats              map[string]CcrStatusFollowerState `json:"index_stats"`
}

// CcrStatusFollowerState is the state of the followers
type CcrStatusFollowerState struct {
	OperationsWritten      int64 `json:"operations_written"`
	OperationsRead         int64 `json:"operations_read"`
	FailedReadRequests     int64 `json:"failed_read_requests"`
	ThrottledReadRequests  int64 `json:"throttled_read_requests"`
	FailedWriteRequests    int64 `json:"failed_write_requests"`
	ThrottledWriteRequests int64 `json:"throttled_write_requests"`
	FollowerCheckpoint     int64 `json:"follower_checkpoint"`
	LeaderCheckpoint       int64 `json:"leader_checkpoint"`
	TotalWriteTimeMillis   int64 `json:"total_write_time_millis"`
}
