package opensearch

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
)

// CcrLeaderStatusRuleService get CCR leader stats.
// See https://docs.opensearch.org/docs/latest/tuning-your-cluster/replication-plugin/api/
type CcrLeaderStatusRuleService struct {
	client *Client

	pretty     *bool       // pretty format the returned JSON response
	human      *bool       // return human readable values for statistics
	errorTrace *bool       // include the stack trace of returned errors
	filterPath []string    // list of filters used to reduce the response
	headers    http.Header // custom request-level HTTP headers
}

// NewCcrLeaderStatusRuleService creates a new CcrLeaderStatusRuleService.
func NewCcrLeaderStatusRuleService(client *Client) *CcrLeaderStatusRuleService {
	return &CcrLeaderStatusRuleService{
		client: client,
	}
}

// Pretty tells Opensearch whether to return a formatted JSON response.
func (s *CcrLeaderStatusRuleService) Pretty(pretty bool) *CcrLeaderStatusRuleService {
	s.pretty = &pretty
	return s
}

// Human specifies whether human readable values should be returned in
// the JSON response, e.g. "7.5mb".
func (s *CcrLeaderStatusRuleService) Human(human bool) *CcrLeaderStatusRuleService {
	s.human = &human
	return s
}

// ErrorTrace specifies whether to include the stack trace of returned errors.
func (s *CcrLeaderStatusRuleService) ErrorTrace(errorTrace bool) *CcrLeaderStatusRuleService {
	s.errorTrace = &errorTrace
	return s
}

// FilterPath specifies a list of filters used to reduce the response.
func (s *CcrLeaderStatusRuleService) FilterPath(filterPath ...string) *CcrLeaderStatusRuleService {
	s.filterPath = filterPath
	return s
}

// Header adds a header to the request.
func (s *CcrLeaderStatusRuleService) Header(name string, value string) *CcrLeaderStatusRuleService {
	if s.headers == nil {
		s.headers = http.Header{}
	}
	s.headers.Add(name, value)
	return s
}

// Headers specifies the headers of the request.
func (s *CcrLeaderStatusRuleService) Headers(headers http.Header) *CcrLeaderStatusRuleService {
	s.headers = headers
	return s
}

// buildURL builds the URL for the operation.
func (s *CcrLeaderStatusRuleService) buildURL() (string, url.Values, error) {
	// Build URL
	path := "/_plugins/_replication/leader_stats"

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
func (s *CcrLeaderStatusRuleService) Validate() error {

	return nil
}

// Do executes the operation.
func (s *CcrLeaderStatusRuleService) Do(ctx context.Context) (*CcrLeaderStatusRuleResponse, error) {
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
	ret := new(CcrLeaderStatusRuleResponse)
	if err := json.Unmarshal(res.Body, ret); err != nil {
		return nil, err
	}
	return ret, nil
}

// CcrLeaderStatusRuleResponse get status of leader
// https://opensearch.org/docs/latest/im-plugin/ism/api/#get-policy
type CcrLeaderStatusRuleResponse struct {
	CcrStatusLeaderState
	NumReplicatedIndices int64               `json:"num_replicated_indices"`
	IndexStats           map[string]CcrStatusLeaderState `json:"index_stats"`
}

type CcrStatusLeaderState struct {
	OperationsRead              int64 `json:"operations_read"`
	TranslogSizeBytes           int64 `json:"translog_size_bytes"`
	OperationsReadLucene        int64 `json:"operations_read_lucene"`
	OperationsReadTranslog      int64 `json:"operations_read_translog"`
	TotalReadTimeLuceneMillis   int64 `json:"total_read_time_lucene_millis"`
	TotalReadTimeTranslogMillis int64 `json:"total_read_time_translog_millis"`
	BytesRead                   int64 `json:"bytes_read"`
}
