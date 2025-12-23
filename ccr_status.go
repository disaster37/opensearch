package opensearch

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/disaster37/opensearch/v3/uritemplates"
)

// CcrStatusRuleService get a CCR rule by its name.
// See https://docs.opensearch.org/docs/latest/tuning-your-cluster/replication-plugin/api/
type CcrStatusRuleService struct {
	client *Client

	pretty     *bool       // pretty format the returned JSON response
	human      *bool       // return human readable values for statistics
	errorTrace *bool       // include the stack trace of returned errors
	filterPath []string    // list of filters used to reduce the response
	headers    http.Header // custom request-level HTTP headers

	name string
}

// NewCcrStatusRuleService creates a new CcrStatusRuleService.
func NewCcrStatusRuleService(client *Client) *CcrStatusRuleService {
	return &CcrStatusRuleService{
		client: client,
	}
}

// Pretty tells Opensearch whether to return a formatted JSON response.
func (s *CcrStatusRuleService) Pretty(pretty bool) *CcrStatusRuleService {
	s.pretty = &pretty
	return s
}

// Human specifies whether human readable values should be returned in
// the JSON response, e.g. "7.5mb".
func (s *CcrStatusRuleService) Human(human bool) *CcrStatusRuleService {
	s.human = &human
	return s
}

// ErrorTrace specifies whether to include the stack trace of returned errors.
func (s *CcrStatusRuleService) ErrorTrace(errorTrace bool) *CcrStatusRuleService {
	s.errorTrace = &errorTrace
	return s
}

// FilterPath specifies a list of filters used to reduce the response.
func (s *CcrStatusRuleService) FilterPath(filterPath ...string) *CcrStatusRuleService {
	s.filterPath = filterPath
	return s
}

// Header adds a header to the request.
func (s *CcrStatusRuleService) Header(name string, value string) *CcrStatusRuleService {
	if s.headers == nil {
		s.headers = http.Header{}
	}
	s.headers.Add(name, value)
	return s
}

// Headers specifies the headers of the request.
func (s *CcrStatusRuleService) Headers(headers http.Header) *CcrStatusRuleService {
	s.headers = headers
	return s
}

// Name is name of the rule to get.
func (s *CcrStatusRuleService) Name(name string) *CcrStatusRuleService {
	s.name = name
	return s
}

// buildURL builds the URL for the operation.
func (s *CcrStatusRuleService) buildURL() (string, url.Values, error) {
	// Build URL
	path, err := uritemplates.Expand("/_plugins/_replication/{name}/_status", map[string]string{
		"name": s.name,
	})
	if err != nil {
		return "", url.Values{}, err
	}

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
func (s *CcrStatusRuleService) Validate() error {
	var invalid []string
	if s.name == "" {
		invalid = append(invalid, "Name")
	}
	if len(invalid) > 0 {
		return fmt.Errorf("missing required fields: %v", invalid)
	}
	return nil
}

// Do executes the operation.
func (s *CcrStatusRuleService) Do(ctx context.Context) (*CcrStatusRuleResponse, error) {
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
	ret := new(CcrStatusRuleResponse)
	if err := json.Unmarshal(res.Body, ret); err != nil {
		return nil, err
	}
	return ret, nil
}

// CcrStatusRuleResponse is the get index state management response object
// https://opensearch.org/docs/latest/im-plugin/ism/api/#get-policy
type CcrStatusRuleResponse struct {
	Status         string                `json:"status"`
	Reason         string                `json:"reason"`
	LeaderAlias    string                `json:"leader_alias"`
	LeaderIndex    string                `json:"leader_index"`
	FollowerIndex  string                `json:"follower_index"`
	SyncingDetails CcrRuleSyncingDetails `json:"syncing_details"`
}

type CcrRuleSyncingDetails struct {
	LeaderCheckpoint   int64 `json:"leader_checkpoint"`
	FollowerCheckpoint int64 `json:"follower_checkpoint"`
	SeqNumber          int64 `json:"seq_no"`
}

// CcrStatus is the status of the CCR rule
type CcrStatus string

const (
	CcrStatusSyncing                 CcrStatus = "SYNCING"
	CcrStatusBootstraping            CcrStatus = "BOOTSTRAPING"
	CcrStatusPaused                  CcrStatus = "PAUSED"
	CcrStausReplicationNotInProgress CcrStatus = "REPLICATION NOT IN PROGRESS"
)
