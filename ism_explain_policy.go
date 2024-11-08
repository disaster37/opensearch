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

// IsmExplainPolicyService explain policy for given index.
// See https://opensearch.org/docs/latest/im-plugin/ism/api/#explain-index
type IsmExplainPolicyService struct {
	client *Client

	pretty     *bool       // pretty format the returned JSON response
	human      *bool       // return human readable values for statistics
	errorTrace *bool       // include the stack trace of returned errors
	filterPath []string    // list of filters used to reduce the response
	headers    http.Header // custom request-level HTTP headers

	indexName  string
	showPolicy *bool
}

// NewIsmExplainPolicyService creates a new IsmExplainPolicyService.
func NewIsmExplainPolicyService(client *Client) *IsmExplainPolicyService {
	return &IsmExplainPolicyService{
		client: client,
	}
}

// Pretty tells Opensearch whether to return a formatted JSON response.
func (s *IsmExplainPolicyService) Pretty(pretty bool) *IsmExplainPolicyService {
	s.pretty = &pretty
	return s
}

// Human specifies whether human readable values should be returned in
// the JSON response, e.g. "7.5mb".
func (s *IsmExplainPolicyService) Human(human bool) *IsmExplainPolicyService {
	s.human = &human
	return s
}

// ErrorTrace specifies whether to include the stack trace of returned errors.
func (s *IsmExplainPolicyService) ErrorTrace(errorTrace bool) *IsmExplainPolicyService {
	s.errorTrace = &errorTrace
	return s
}

// FilterPath specifies a list of filters used to reduce the response.
func (s *IsmExplainPolicyService) FilterPath(filterPath ...string) *IsmExplainPolicyService {
	s.filterPath = filterPath
	return s
}

// Header adds a header to the request.
func (s *IsmExplainPolicyService) Header(name string, value string) *IsmExplainPolicyService {
	if s.headers == nil {
		s.headers = http.Header{}
	}
	s.headers.Add(name, value)
	return s
}

// Headers specifies the headers of the request.
func (s *IsmExplainPolicyService) Headers(headers http.Header) *IsmExplainPolicyService {
	s.headers = headers
	return s
}

// IndexName is the index name where we should to explain ISM policy
func (s *IsmExplainPolicyService) IndexName(name string) *IsmExplainPolicyService {
	s.indexName = name
	return s
}

// buildURL builds the URL for the operation.
func (s *IsmExplainPolicyService) buildURL() (string, url.Values, error) {
	// Build URL
	path, err := uritemplates.Expand("/_plugins/_ism/explain/{name}", map[string]string{
		"name": s.indexName,
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
	if v := s.showPolicy; v != nil {
		params.Set("show_policy", fmt.Sprint(*v))
	}
	return path, params, nil
}

// Validate checks if the operation is valid.
func (s *IsmExplainPolicyService) Validate() error {
	var invalid []string
	if len(invalid) > 0 {
		return fmt.Errorf("missing required fields: %v", invalid)
	}
	return nil
}

// Do executes the operation.
func (s *IsmExplainPolicyService) Do(ctx context.Context) (*IsmExplainPolicyResponse, error) {
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
	ret := new(IsmExplainPolicyResponse)
	if err := json.Unmarshal(res.Body, ret); err != nil {
		return nil, err
	}
	return ret, nil
}

func (h *IsmExplainPolicyResponse) UnmarshalJSON(data []byte) error {
	v := map[string]json.RawMessage{}
	if err := json.Unmarshal(data, &v); err != nil {
		return err
	}

	h.Indexes = map[string]IsmExplainPolicy{}

	for key, rawB := range v {
		if key == "total_managed_indices" {
			if err := json.Unmarshal(rawB, &h.TotalManagedIndices); err != nil {
				return err
			}
		} else {
			explainPolicy := new(IsmExplainPolicy)
			if err := json.Unmarshal(rawB, explainPolicy); err != nil {
				return err
			}
			h.Indexes[key] = *explainPolicy
		}
	}

	return nil
}

type IsmExplainPolicyResponse struct {
	Indexes             map[string]IsmExplainPolicy `json:",inline"`
	TotalManagedIndices int64                       `json:"total_managed_indices"`
}

// IsmExplainPolicy is the explain response object
type IsmExplainPolicy struct {
	PolicyId             string                    `json:"policy_id,omitempty"`
	PolicySequenceNumber int64                     `json:"policy_seq_no,omitempty"`
	PolicyPrimaryTerm    int64                     `json:"policy_primary_term,omitempty"`
	Index                string                    `json:"index,omitempty"`
	IndexId              string                    `json:"index_uuid,omitempty"`
	IndexCreationDate    int64                     `json:"index_creation_date,omitempty"`
	Enabled              bool                      `json:"enabled,omitempty"`
	Policy               *IsmGetPolicy             `json:"policy,omitempty"`
	State                IsmExplainPolicyState     `json:"state"`
	Action               IsmExplainPolicyAction    `json:"action"`
	Step                 IsmExplainPolicyStep      `json:"step"`
	RetryInfo            IsmExplainPolicyRetryInfo `json:"retry_info"`
	Info                 IsmExplainPolicyInfo      `json:"info"`
}

// IsmExplainPolicyState is the current state
type IsmExplainPolicyState struct {
	Name      string `json:"name"`
	StartTime int64  `json:"start_time"`
}

// IsmExplainPolicyAction is the current action
type IsmExplainPolicyAction struct {
	Name            string `json:"name"`
	StartTime       int64  `json:"start_time"`
	Index           int64  `json:"index"`
	Failed          bool   `json:"failed"`
	ConsumedRetries int64  `json:"consumed_retries"`
	LastRetryTime   int64  `json:"last_retry_time"`
}

// IsmExplainPolicyStep is the current step
type IsmExplainPolicyStep struct {
	Name       string `json:"name"`
	StartTime  int64  `json:"start_time"`
	StepStatus string `json:"step_status"`
}

type IsmExplainPolicyRetryInfo struct {
	Failed          bool  `json:"failed"`
	ConsumedRetries int64 `json:"consumed_retries"`
}

// IsmExplainPolicyInfo is the current info
type IsmExplainPolicyInfo struct {
	Message string `json:"message"`
	Cause   string `json:"cause"`
}
