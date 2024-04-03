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

// TransformStopJobService stop a transform job by its name.
// See https://opensearch.org/docs/latest/im-plugin/index-transforms/transforms-apis/#stop-a-transform-job
type TransformStopJobService struct {
	client *Client

	pretty     *bool       // pretty format the returned JSON response
	human      *bool       // return human readable values for statistics
	errorTrace *bool       // include the stack trace of returned errors
	filterPath []string    // list of filters used to reduce the response
	headers    http.Header // custom request-level HTTP headers

	name string
}

// NewTransformStopJobService creates a new TransformStopJobService.
func NewTransformStopJobService(client *Client) *TransformStopJobService {
	return &TransformStopJobService{
		client: client,
	}
}

// Pretty tells Opensearch whether to return a formatted JSON response.
func (s *TransformStopJobService) Pretty(pretty bool) *TransformStopJobService {
	s.pretty = &pretty
	return s
}

// Human specifies whether human readable values should be returned in
// the JSON response, e.g. "7.5mb".
func (s *TransformStopJobService) Human(human bool) *TransformStopJobService {
	s.human = &human
	return s
}

// ErrorTrace specifies whether to include the stack trace of returned errors.
func (s *TransformStopJobService) ErrorTrace(errorTrace bool) *TransformStopJobService {
	s.errorTrace = &errorTrace
	return s
}

// FilterPath specifies a list of filters used to reduce the response.
func (s *TransformStopJobService) FilterPath(filterPath ...string) *TransformStopJobService {
	s.filterPath = filterPath
	return s
}

// Header adds a header to the request.
func (s *TransformStopJobService) Header(name string, value string) *TransformStopJobService {
	if s.headers == nil {
		s.headers = http.Header{}
	}
	s.headers.Add(name, value)
	return s
}

// Headers specifies the headers of the request.
func (s *TransformStopJobService) Headers(headers http.Header) *TransformStopJobService {
	s.headers = headers
	return s
}

// Name is name of the transform job to stop.
func (s *TransformStopJobService) Name(name string) *TransformStopJobService {
	s.name = name
	return s
}

// buildURL builds the URL for the operation.
func (s *TransformStopJobService) buildURL() (string, url.Values, error) {
	// Build URL
	path, err := uritemplates.Expand("/_plugins/_transform/{name}/_stop", map[string]string{
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
func (s *TransformStopJobService) Validate() error {
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
func (s *TransformStopJobService) Do(ctx context.Context) (*TransformStopJobResponse, error) {
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
		Headers: s.headers,
	})
	if err != nil {
		return nil, err
	}

	// Return operation response
	ret := new(TransformStopJobResponse)
	if err := json.Unmarshal(res.Body, ret); err != nil {
		return nil, err
	}
	return ret, nil
}

type TransformStopJobResponse struct {
	Acknowledged bool `json:"acknowledged,omitempty"`
}