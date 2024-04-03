package opensearch

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/disaster37/opensearch/v2/uritemplates"
)

// SmExplainPolicyService explain the SM policies.
// See https://opensearch.org/docs/latest/tuning-your-cluster/availability-and-recovery/snapshots/sm-api/#explain
type SmExplainPolicyService struct {
	client *Client

	pretty     *bool       // pretty format the returned JSON response
	human      *bool       // return human readable values for statistics
	errorTrace *bool       // include the stack trace of returned errors
	filterPath []string    // list of filters used to reduce the response
	headers    http.Header // custom request-level HTTP headers

	names []string
}

// NewSmExplainPolicyService creates a new SmExplainPolicyService.
func NewSmExplainPolicyService(client *Client) *SmExplainPolicyService {
	return &SmExplainPolicyService{
		client: client,
	}
}

// Pretty tells Opensearch whether to return a formatted JSON response.
func (s *SmExplainPolicyService) Pretty(pretty bool) *SmExplainPolicyService {
	s.pretty = &pretty
	return s
}

// Human specifies whether human readable values should be returned in
// the JSON response, e.g. "7.5mb".
func (s *SmExplainPolicyService) Human(human bool) *SmExplainPolicyService {
	s.human = &human
	return s
}

// ErrorTrace specifies whether to include the stack trace of returned errors.
func (s *SmExplainPolicyService) ErrorTrace(errorTrace bool) *SmExplainPolicyService {
	s.errorTrace = &errorTrace
	return s
}

// FilterPath specifies a list of filters used to reduce the response.
func (s *SmExplainPolicyService) FilterPath(filterPath ...string) *SmExplainPolicyService {
	s.filterPath = filterPath
	return s
}

// Header adds a header to the request.
func (s *SmExplainPolicyService) Header(name string, value string) *SmExplainPolicyService {
	if s.headers == nil {
		s.headers = http.Header{}
	}
	s.headers.Add(name, value)
	return s
}

// Headers specifies the headers of the request.
func (s *SmExplainPolicyService) Headers(headers http.Header) *SmExplainPolicyService {
	s.headers = headers
	return s
}

// Name is name of the policy to get.
func (s *SmExplainPolicyService) Names(name ...string) *SmExplainPolicyService {
	s.names = name
	return s
}

// buildURL builds the URL for the operation.
func (s *SmExplainPolicyService) buildURL() (string, url.Values, error) {
	// Build URL
	var (
		path string
		err  error
	)

	name := strings.Join(s.names, ",")
	if name != "" {
		path, err = uritemplates.Expand("/_plugins/_sm/policies/{name}/_explain", map[string]string{
			"name": name,
		})
	} else {
		path = "/_plugins/_sm/policies/_explain"
	}

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
func (s *SmExplainPolicyService) Validate() error {
	var invalid []string
	if len(invalid) > 0 {
		return fmt.Errorf("missing required fields: %v", invalid)
	}
	return nil
}

// Do executes the operation.
func (s *SmExplainPolicyService) Do(ctx context.Context) (*SmExplainPolicyResponse, error) {
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
	ret := new(SmExplainPolicyResponse)
	if err := json.Unmarshal(res.Body, ret); err != nil {
		return nil, err
	}
	return ret, nil
}

// SmExplainPolicyResponse is the get policy response object
// https://opensearch.org/docs/latest/im-plugin/ism/api/#get-policy
type SmExplainPolicyResponse struct {
	Policies []SmExplainPolicy `json:"policies"`
}

// SmExplainPolicy is the snapshot policy explain
type SmExplainPolicy struct {
	Name           string                `json:"policy_id,omitempty"`
	SequenceNumber int64                 `json:"policy_seq_no,omitempty"`
	PrimaryTerm    int64                 `json:"policy_primary_term,omitempty"`
	Enabled        bool                  `json:"enabled,omitempty"`
	Creation       *SmExplainPolicyState `json:"creation,omitempty"`
	Deletion       *SmExplainPolicyState `json:"deletion,omitempty"`
}

type SmExplainPolicyState struct {
	CurrentState    string                         `json:"current_state,omitempty"`
	Trigger         SmExplainPolicyTrigger         `json:"trigger,omitempty"`
	LatestExecution SmExplainPolicyLatestExecution `json:"latest_execution,omitempty"`
	Retry           SmExplainPolicyRetry           `json:"retry,omitempty"`
}

type SmExplainPolicyTrigger struct {
	Time int64 `json:"time,omitempty"`
}

type SmExplainPolicyLatestExecution struct {
	StartTime int64               `json:"start_time,omitempty"`
	EndTime   int64               `json:"end_time,omitempty"`
	Info      SmExplainPolicyInfo `json:"info,omitempty"`
}

type SmExplainPolicyRetry struct {
	Count int64 `json:"count,omitempty"`
}

type SmExplainPolicyInfo struct {
	Message string `json:"message,omitempty"`
	Cause   int64  `json:"cause,omitempty"`
}
