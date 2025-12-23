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

// CcrStartRuleService start a CCR rule by its name.
// See https://docs.opensearch.org/docs/latest/tuning-your-cluster/replication-plugin/api/
type CcrStartRuleService struct {
	client *Client

	pretty     *bool       // pretty format the returned JSON response
	human      *bool       // return human readable values for statistics
	errorTrace *bool       // include the stack trace of returned errors
	filterPath []string    // list of filters used to reduce the response
	headers    http.Header // custom request-level HTTP headers

	name string
	body any
}

// NewCcrStartRuleService creates a new CcrStartRuleService.
func NewCcrStartRuleService(client *Client) *CcrStartRuleService {
	return &CcrStartRuleService{
		client: client,
	}
}

// Pretty tells Opensearch whether to return a formatted JSON response.
func (s *CcrStartRuleService) Pretty(pretty bool) *CcrStartRuleService {
	s.pretty = &pretty
	return s
}

// Human specifies whether human readable values should be returned in
// the JSON response, e.g. "7.5mb".
func (s *CcrStartRuleService) Human(human bool) *CcrStartRuleService {
	s.human = &human
	return s
}

// ErrorTrace specifies whether to include the stack trace of returned errors.
func (s *CcrStartRuleService) ErrorTrace(errorTrace bool) *CcrStartRuleService {
	s.errorTrace = &errorTrace
	return s
}

// FilterPath specifies a list of filters used to reduce the response.
func (s *CcrStartRuleService) FilterPath(filterPath ...string) *CcrStartRuleService {
	s.filterPath = filterPath
	return s
}

// Header adds a header to the request.
func (s *CcrStartRuleService) Header(name string, value string) *CcrStartRuleService {
	if s.headers == nil {
		s.headers = http.Header{}
	}
	s.headers.Add(name, value)
	return s
}

// Headers specifies the headers of the request.
func (s *CcrStartRuleService) Headers(headers http.Header) *CcrStartRuleService {
	s.headers = headers
	return s
}

// Name is name of the rule to get.
func (s *CcrStartRuleService) Name(name string) *CcrStartRuleService {
	s.name = name
	return s
}

// Body specifies the policy. Use a string or a type that will get serialized as JSON.
func (s *CcrStartRuleService) Body(body interface{}) *CcrStartRuleService {
	s.body = body
	return s
}

// buildURL builds the URL for the operation.
func (s *CcrStartRuleService) buildURL() (string, url.Values, error) {
	// Build URL
	path, err := uritemplates.Expand("/_plugins/_replication/{name}/_start", map[string]string{
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
func (s *CcrStartRuleService) Validate() error {
	var invalid []string
	if s.name == "" {
		invalid = append(invalid, "Name")
	}
	if s.body == nil {
		invalid = append(invalid, "Body")
	}
	if len(invalid) > 0 {
		return fmt.Errorf("missing required fields: %v", invalid)
	}
	return nil
}

// Do executes the operation.
func (s *CcrStartRuleService) Do(ctx context.Context) (*CcrStartRuleResponse, error) {
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
		Method:  "PUT",
		Path:    path,
		Params:  params,
		Body:    s.body,
		Headers: s.headers,
	})
	if err != nil {
		return nil, err
	}

	// Return operation response
	ret := new(CcrStartRuleResponse)
	if err := json.Unmarshal(res.Body, ret); err != nil {
		return nil, err
	}
	return ret, nil
}

// CcrStartRuleResponse is the response when start CCR
type CcrStartRuleResponse struct {
	Acknowledged bool `json:"acknowledged"`
}

// CcrRule is a CCR rule
type CcrRule struct {
	LeaderAlias string          `json:"leader_alias"`
	LeaderIndex string          `json:"leader_index"`
	UseRoles    CcrRuleUseRoles `json:"use_roles"`
}

// CcrRuleUseRole is a user role for rule
type CcrRuleUseRoles struct {
	LeaderClusterRole   string `json:"leader_cluster_role"`
	FollowerClusterRole string `json:"follower_cluster_role"`
}
