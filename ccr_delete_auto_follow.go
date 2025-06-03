package opensearch

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
)

// CcrDeleteAutoFollowService start a CCR rule by its name.
// See https://docs.opensearch.org/docs/latest/tuning-your-cluster/replication-plugin/api/
type CcrDeleteAutoFollowService struct {
	client *Client

	pretty     *bool       // pretty format the returned JSON response
	human      *bool       // return human readable values for statistics
	errorTrace *bool       // include the stack trace of returned errors
	filterPath []string    // list of filters used to reduce the response
	headers    http.Header // custom request-level HTTP headers

	leaderAlias string
	name        string
}

// NewCcrDeleteAutoFollowService creates a new CcrDeleteAutoFollowService.
func NewCcrDeleteAutoFollowService(client *Client) *CcrDeleteAutoFollowService {
	return &CcrDeleteAutoFollowService{
		client: client,
	}
}

// Pretty tells Opensearch whether to return a formatted JSON response.
func (s *CcrDeleteAutoFollowService) Pretty(pretty bool) *CcrDeleteAutoFollowService {
	s.pretty = &pretty
	return s
}

// Human specifies whether human readable values should be returned in
// the JSON response, e.g. "7.5mb".
func (s *CcrDeleteAutoFollowService) Human(human bool) *CcrDeleteAutoFollowService {
	s.human = &human
	return s
}

// ErrorTrace specifies whether to include the stack trace of returned errors.
func (s *CcrDeleteAutoFollowService) ErrorTrace(errorTrace bool) *CcrDeleteAutoFollowService {
	s.errorTrace = &errorTrace
	return s
}

// FilterPath specifies a list of filters used to reduce the response.
func (s *CcrDeleteAutoFollowService) FilterPath(filterPath ...string) *CcrDeleteAutoFollowService {
	s.filterPath = filterPath
	return s
}

// Header adds a header to the request.
func (s *CcrDeleteAutoFollowService) Header(name string, value string) *CcrDeleteAutoFollowService {
	if s.headers == nil {
		s.headers = http.Header{}
	}
	s.headers.Add(name, value)
	return s
}

// Headers specifies the headers of the request.
func (s *CcrDeleteAutoFollowService) Headers(headers http.Header) *CcrDeleteAutoFollowService {
	s.headers = headers
	return s
}

// Name is name of the rule to get.
func (s *CcrDeleteAutoFollowService) Name(name string) *CcrDeleteAutoFollowService {
	s.name = name
	return s
}

// Body specifies the policy. Use a string or a type that will get serialized as JSON.
func (s *CcrDeleteAutoFollowService) LeaderAlias(name string) *CcrDeleteAutoFollowService {
	s.leaderAlias = name
	return s
}

// buildURL builds the URL for the operation.
func (s *CcrDeleteAutoFollowService) buildURL() (string, url.Values, error) {
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
func (s *CcrDeleteAutoFollowService) Validate() error {
	var invalid []string
	if s.name == "" {
		invalid = append(invalid, "Name")
	}
	if s.leaderAlias == "" {
		invalid = append(invalid, "LeaderAlias")
	}
	if len(invalid) > 0 {
		return fmt.Errorf("missing required fields: %v", invalid)
	}
	return nil
}

// Do executes the operation.
func (s *CcrDeleteAutoFollowService) Do(ctx context.Context) (*CcrDeleteAutoFollowResponse, error) {
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
		Method: "DELETE",
		Path:   path,
		Params: params,
		Body: &CcrDeleteAutoFollowRequest{
			LeaderAlias: s.leaderAlias,
			Name:        s.name,
		},
		Headers: s.headers,
	})
	if err != nil {
		return nil, err
	}

	// Return operation response
	ret := new(CcrDeleteAutoFollowResponse)
	if err := json.Unmarshal(res.Body, ret); err != nil {
		return nil, err
	}
	return ret, nil
}

type CcrDeleteAutoFollowRequest struct {
	LeaderAlias string `json:"leader_alias"`
	Name        string `json:"name"`
}

// CcrDeleteAutoFollowResponse is the response when start CCR
type CcrDeleteAutoFollowResponse struct {
	Acknowledged bool `json:"acknowledged"`
}
