package opensearch

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
)

// CcrFollowStatusRuleService get a CCR rule by its name.
// See https://docs.opensearch.org/docs/latest/tuning-your-cluster/replication-plugin/api/
type CcrFollowStatusRuleService struct {
	client *Client

	pretty     *bool       // pretty format the returned JSON response
	human      *bool       // return human readable values for statistics
	errorTrace *bool       // include the stack trace of returned errors
	filterPath []string    // list of filters used to reduce the response
	headers    http.Header // custom request-level HTTP headers
}

// NewCcrFollowStatusRuleService creates a new CcrFollowStatusRuleService.
func NewCcrFollowStatusRuleService(client *Client) *CcrFollowStatusRuleService {
	return &CcrFollowStatusRuleService{
		client: client,
	}
}

// Pretty tells Opensearch whether to return a formatted JSON response.
func (s *CcrFollowStatusRuleService) Pretty(pretty bool) *CcrFollowStatusRuleService {
	s.pretty = &pretty
	return s
}

// Human specifies whether human readable values should be returned in
// the JSON response, e.g. "7.5mb".
func (s *CcrFollowStatusRuleService) Human(human bool) *CcrFollowStatusRuleService {
	s.human = &human
	return s
}

// ErrorTrace specifies whether to include the stack trace of returned errors.
func (s *CcrFollowStatusRuleService) ErrorTrace(errorTrace bool) *CcrFollowStatusRuleService {
	s.errorTrace = &errorTrace
	return s
}

// FilterPath specifies a list of filters used to reduce the response.
func (s *CcrFollowStatusRuleService) FilterPath(filterPath ...string) *CcrFollowStatusRuleService {
	s.filterPath = filterPath
	return s
}

// Header adds a header to the request.
func (s *CcrFollowStatusRuleService) Header(name string, value string) *CcrFollowStatusRuleService {
	if s.headers == nil {
		s.headers = http.Header{}
	}
	s.headers.Add(name, value)
	return s
}

// Headers specifies the headers of the request.
func (s *CcrFollowStatusRuleService) Headers(headers http.Header) *CcrFollowStatusRuleService {
	s.headers = headers
	return s
}

// buildURL builds the URL for the operation.
func (s *CcrFollowStatusRuleService) buildURL() (string, url.Values, error) {
	// Build URL
	path := "_plugins/_replication/autofollow_stats"

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
func (s *CcrFollowStatusRuleService) Validate() error {

	return nil
}

// Do executes the operation.
func (s *CcrFollowStatusRuleService) Do(ctx context.Context) (*CcrFollowStatusRuleResponse, error) {
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
	ret := new(CcrFollowStatusRuleResponse)
	if err := json.Unmarshal(res.Body, ret); err != nil {
		return nil, err
	}
	return ret, nil
}

// CcrFollowStatusRuleResponse is the get index state management response object
// https://opensearch.org/docs/latest/im-plugin/ism/api/#get-policy
type CcrFollowStatusRuleResponse struct {
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
