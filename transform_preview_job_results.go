package opensearch

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
)

// TransformPreviewJobResultsService permit to preview job result
// See https://opensearch.org/docs/latest/im-plugin/index-transforms/transforms-apis/#preview-a-transform-jobs-results
type TransformPreviewJobResultsService struct {
	client *Client

	pretty     *bool       // pretty format the returned JSON response
	human      *bool       // return human readable values for statistics
	errorTrace *bool       // include the stack trace of returned errors
	filterPath []string    // list of filters used to reduce the response
	headers    http.Header // custom request-level HTTP headers

	body interface{}
}

// NewTransformPreviewJobResultsService creates a new TransformPreviewJobResultsService.
func NewTransformPreviewJobResultsService(client *Client) *TransformPreviewJobResultsService {
	return &TransformPreviewJobResultsService{
		client: client,
	}
}

// Pretty tells Opensearch whether to return a formatted JSON response.
func (s *TransformPreviewJobResultsService) Pretty(pretty bool) *TransformPreviewJobResultsService {
	s.pretty = &pretty
	return s
}

// Human specifies whether human readable values should be returned in
// the JSON response, e.g. "7.5mb".
func (s *TransformPreviewJobResultsService) Human(human bool) *TransformPreviewJobResultsService {
	s.human = &human
	return s
}

// ErrorTrace specifies whether to include the stack trace of returned errors.
func (s *TransformPreviewJobResultsService) ErrorTrace(errorTrace bool) *TransformPreviewJobResultsService {
	s.errorTrace = &errorTrace
	return s
}

// FilterPath specifies a list of filters used to reduce the response.
func (s *TransformPreviewJobResultsService) FilterPath(filterPath ...string) *TransformPreviewJobResultsService {
	s.filterPath = filterPath
	return s
}

// Header adds a header to the request.
func (s *TransformPreviewJobResultsService) Header(name string, value string) *TransformPreviewJobResultsService {
	if s.headers == nil {
		s.headers = http.Header{}
	}
	s.headers.Add(name, value)
	return s
}

// Headers specifies the headers of the request.
func (s *TransformPreviewJobResultsService) Headers(headers http.Header) *TransformPreviewJobResultsService {
	s.headers = headers
	return s
}

// Body specifies the transform. Use a string or a type that will get serialized as JSON.
func (s *TransformPreviewJobResultsService) Body(body interface{}) *TransformPreviewJobResultsService {
	s.body = body
	return s
}

// buildURL builds the URL for the operation.
func (s *TransformPreviewJobResultsService) buildURL() (string, url.Values, error) {
	path := "/_plugins/_transform/_preview"

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
func (s *TransformPreviewJobResultsService) Validate() error {
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
func (s *TransformPreviewJobResultsService) Do(ctx context.Context) (*TransformPreviewJobResponse, error) {
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
	ret := new(TransformPreviewJobResponse)
	if err := json.Unmarshal(res.Body, ret); err != nil {
		return nil, err
	}
	return ret, nil
}

// TransformPreviewJobResponse is the response of job preview
type TransformPreviewJobResponse struct {
	Documents []map[string]any `json:"documents"`
}
