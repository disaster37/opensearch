package opensearch

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
)

// CcrAutoFollowStatusService get a CCR rule by its name.
// See https://docs.opensearch.org/docs/latest/tuning-your-cluster/replication-plugin/api/
type CcrAutoFollowStatusService struct {
	client *Client

	pretty     *bool       // pretty format the returned JSON response
	human      *bool       // return human readable values for statistics
	errorTrace *bool       // include the stack trace of returned errors
	filterPath []string    // list of filters used to reduce the response
	headers    http.Header // custom request-level HTTP headers
}

// NewCcrAutoFollowStatusService creates a new CcrAutoFollowStatusService.
func NewCcrAutoFollowStatusService(client *Client) *CcrAutoFollowStatusService {
	return &CcrAutoFollowStatusService{
		client: client,
	}
}

// Pretty tells Opensearch whether to return a formatted JSON response.
func (s *CcrAutoFollowStatusService) Pretty(pretty bool) *CcrAutoFollowStatusService {
	s.pretty = &pretty
	return s
}

// Human specifies whether human readable values should be returned in
// the JSON response, e.g. "7.5mb".
func (s *CcrAutoFollowStatusService) Human(human bool) *CcrAutoFollowStatusService {
	s.human = &human
	return s
}

// ErrorTrace specifies whether to include the stack trace of returned errors.
func (s *CcrAutoFollowStatusService) ErrorTrace(errorTrace bool) *CcrAutoFollowStatusService {
	s.errorTrace = &errorTrace
	return s
}

// FilterPath specifies a list of filters used to reduce the response.
func (s *CcrAutoFollowStatusService) FilterPath(filterPath ...string) *CcrAutoFollowStatusService {
	s.filterPath = filterPath
	return s
}

// Header adds a header to the request.
func (s *CcrAutoFollowStatusService) Header(name string, value string) *CcrAutoFollowStatusService {
	if s.headers == nil {
		s.headers = http.Header{}
	}
	s.headers.Add(name, value)
	return s
}

// Headers specifies the headers of the request.
func (s *CcrAutoFollowStatusService) Headers(headers http.Header) *CcrAutoFollowStatusService {
	s.headers = headers
	return s
}

// buildURL builds the URL for the operation.
func (s *CcrAutoFollowStatusService) buildURL() (string, url.Values, error) {
	// Build URL
	path := "/_plugins/_replication/autofollow_stats"

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
func (s *CcrAutoFollowStatusService) Validate() error {

	return nil
}

// Do executes the operation.
func (s *CcrAutoFollowStatusService) Do(ctx context.Context) (*CcrAutoFollowStatusResponse, error) {
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
	ret := new(CcrAutoFollowStatusResponse)
	if err := json.Unmarshal(res.Body, ret); err != nil {
		return nil, err
	}
	return ret, nil
}

// CcrAutoFollowStatusResponse is the get index state management response object
// https://opensearch.org/docs/latest/im-plugin/ism/api/#get-policy
type CcrAutoFollowStatusResponse struct {
	NumSuccessStartReplications int64                  `json:"num_success_start_replications"`
	NumFailedStartReplications  int64                  `json:"num_failed_start_replications"`
	NumFailedLeaderCalls        int64                  `json:"num_failed_leader_calls"`
	FailedIndices               []string               `json:"failed_indices"`
	AutofollowStats             []CcrFollowStatusState `json:"autofollow_stats"`
}

type CcrFollowStatusState struct {
	Name                        string        `json:"name"`
	Pattern                     string        `json:"pattern"`
	NumSuccessStartReplications int64         `json:"num_success_start_replications"`
	NumFailedStartReplications  int64         `json:"num_failed_start_replications"`
	NumFailedLeaderCalls        int64         `json:"num_failed_leader_calls"`
	FailedIndices               []string      `json:"failed_indices"`
	LastExecutionTime           UnixMilliTime `json:"last_execution_time"`
}
