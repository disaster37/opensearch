package opensearch

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
)

// CcrPostAutoFollowService start a CCR rule by its name.
// See https://docs.opensearch.org/docs/latest/tuning-your-cluster/replication-plugin/api/
type CcrPostAutoFollowService struct {
	client *Client

	pretty     *bool       // pretty format the returned JSON response
	human      *bool       // return human readable values for statistics
	errorTrace *bool       // include the stack trace of returned errors
	filterPath []string    // list of filters used to reduce the response
	headers    http.Header // custom request-level HTTP headers

	body any
}

// NewCcrPostAutoFollowService creates a new CcrPostAutoFollowService.
func NewCcrPostAutoFollowService(client *Client) *CcrPostAutoFollowService {
	return &CcrPostAutoFollowService{
		client: client,
	}
}

// Pretty tells Opensearch whether to return a formatted JSON response.
func (s *CcrPostAutoFollowService) Pretty(pretty bool) *CcrPostAutoFollowService {
	s.pretty = &pretty
	return s
}

// Human specifies whether human readable values should be returned in
// the JSON response, e.g. "7.5mb".
func (s *CcrPostAutoFollowService) Human(human bool) *CcrPostAutoFollowService {
	s.human = &human
	return s
}

// ErrorTrace specifies whether to include the stack trace of returned errors.
func (s *CcrPostAutoFollowService) ErrorTrace(errorTrace bool) *CcrPostAutoFollowService {
	s.errorTrace = &errorTrace
	return s
}

// FilterPath specifies a list of filters used to reduce the response.
func (s *CcrPostAutoFollowService) FilterPath(filterPath ...string) *CcrPostAutoFollowService {
	s.filterPath = filterPath
	return s
}

// Header adds a header to the request.
func (s *CcrPostAutoFollowService) Header(name string, value string) *CcrPostAutoFollowService {
	if s.headers == nil {
		s.headers = http.Header{}
	}
	s.headers.Add(name, value)
	return s
}

// Headers specifies the headers of the request.
func (s *CcrPostAutoFollowService) Headers(headers http.Header) *CcrPostAutoFollowService {
	s.headers = headers
	return s
}

// Body specifies the policy. Use a string or a type that will get serialized as JSON.
func (s *CcrPostAutoFollowService) Body(body interface{}) *CcrPostAutoFollowService {
	s.body = body
	return s
}

// buildURL builds the URL for the operation.
func (s *CcrPostAutoFollowService) buildURL() (string, url.Values, error) {
	// Build URL
	path := "/_plugins/_replication/_autofollow"

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
func (s *CcrPostAutoFollowService) Validate() error {
	var invalid []string
	if s.body == nil {
		invalid = append(invalid, "Body")
	}
	if len(invalid) > 0 {
		return fmt.Errorf("missing required fields: %v", invalid)
	}
	return nil
}

// Do executes the operation.
func (s *CcrPostAutoFollowService) Do(ctx context.Context) (*CcrPostAutoFollowResponse, error) {
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
		Method:  "POST",
		Path:    path,
		Params:  params,
		Body:    s.body,
		Headers: s.headers,
	})
	if err != nil {
		return nil, err
	}

	// Return operation response
	ret := new(CcrPostAutoFollowResponse)
	if err := json.Unmarshal(res.Body, ret); err != nil {
		return nil, err
	}
	return ret, nil
}

// CcrPostAutoFollowResponse is the response when start CCR
type CcrPostAutoFollowResponse struct {
	Acknowledged bool `json:"acknowledged"`
}

// CcrPostAutoFollow is a follow CCR rule
type CcrAutoFollowRule struct {
	LeaderAlias string         `json:"leader_alias"`
	Name        string         `json:"name"`
	Pattern     string         `json:"pattern"`
	UseRoles     CcrRuleUseRoles `json:"use_roles"`
}
