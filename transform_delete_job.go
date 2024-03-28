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

// TransformDeleteJobService delete a transform job by its name.
// See https://opensearch.org/docs/latest/im-plugin/index-transforms/transforms-apis/#delete-a-transform-job
type TransformDeleteJobService struct {
	client *Client

	pretty     *bool       // pretty format the returned JSON response
	human      *bool       // return human readable values for statistics
	errorTrace *bool       // include the stack trace of returned errors
	filterPath []string    // list of filters used to reduce the response
	headers    http.Header // custom request-level HTTP headers

	name string
}

// NewTransformDeleteJobService creates a new TransformDeleteJobService.
func NewTransformDeleteJobService(client *Client) *TransformDeleteJobService {
	return &TransformDeleteJobService{
		client: client,
	}
}

// Pretty tells Opensearch whether to return a formatted JSON response.
func (s *TransformDeleteJobService) Pretty(pretty bool) *TransformDeleteJobService {
	s.pretty = &pretty
	return s
}

// Human specifies whether human readable values should be returned in
// the JSON response, e.g. "7.5mb".
func (s *TransformDeleteJobService) Human(human bool) *TransformDeleteJobService {
	s.human = &human
	return s
}

// ErrorTrace specifies whether to include the stack trace of returned errors.
func (s *TransformDeleteJobService) ErrorTrace(errorTrace bool) *TransformDeleteJobService {
	s.errorTrace = &errorTrace
	return s
}

// FilterPath specifies a list of filters used to reduce the response.
func (s *TransformDeleteJobService) FilterPath(filterPath ...string) *TransformDeleteJobService {
	s.filterPath = filterPath
	return s
}

// Header adds a header to the request.
func (s *TransformDeleteJobService) Header(name string, value string) *TransformDeleteJobService {
	if s.headers == nil {
		s.headers = http.Header{}
	}
	s.headers.Add(name, value)
	return s
}

// Headers specifies the headers of the request.
func (s *TransformDeleteJobService) Headers(headers http.Header) *TransformDeleteJobService {
	s.headers = headers
	return s
}

// Name is name of the transform job to delete.
func (s *TransformDeleteJobService) Name(name string) *TransformDeleteJobService {
	s.name = name
	return s
}

// buildURL builds the URL for the operation.
func (s *TransformDeleteJobService) buildURL() (string, url.Values, error) {
	// Build URL
	path, err := uritemplates.Expand("/_plugins/_transform/{name}", map[string]string{
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
func (s *TransformDeleteJobService) Validate() error {
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
func (s *TransformDeleteJobService) Do(ctx context.Context) (*TransformDeleteJobResponse, error) {
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
		Method:  "DELETE",
		Path:    path,
		Params:  params,
		Headers: s.headers,
	})
	if err != nil {
		return nil, err
	}

	// Return operation response
	ret := new(TransformDeleteJobResponse)
	if err := json.Unmarshal(res.Body, ret); err != nil {
		return nil, err
	}
	return ret, nil
}

type TransformDeleteJobResponse struct {
	Took   int64            `json:"took,omitempty"`
	Errors bool             `json:"errors,omitempty"`
	Items  []map[string]any `json:"items,omitempty"`
}
