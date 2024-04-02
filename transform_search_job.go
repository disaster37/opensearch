package opensearch

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
)

// TransformSearchJobService get a transform job by query.
// See https://opensearch.org/docs/latest/im-plugin/index-transforms/transforms-apis/#query-parameters-1
type TransformSearchJobService struct {
	client *Client

	pretty     *bool       // pretty format the returned JSON response
	human      *bool       // return human readable values for statistics
	errorTrace *bool       // include the stack trace of returned errors
	filterPath []string    // list of filters used to reduce the response
	headers    http.Header // custom request-level HTTP headers

	search        *string
	from          *int64
	size          *int64
	sortField     *string
	sortDirection *string
}

// NewTransformSearchJobService creates a new TransformSearchJobService.
func NewTransformSearchJobService(client *Client) *TransformSearchJobService {
	return &TransformSearchJobService{
		client: client,
	}
}

// Pretty tells Opensearch whether to return a formatted JSON response.
func (s *TransformSearchJobService) Pretty(pretty bool) *TransformSearchJobService {
	s.pretty = &pretty
	return s
}

// Human specifies whether human readable values should be returned in
// the JSON response, e.g. "7.5mb".
func (s *TransformSearchJobService) Human(human bool) *TransformSearchJobService {
	s.human = &human
	return s
}

// ErrorTrace specifies whether to include the stack trace of returned errors.
func (s *TransformSearchJobService) ErrorTrace(errorTrace bool) *TransformSearchJobService {
	s.errorTrace = &errorTrace
	return s
}

// FilterPath specifies a list of filters used to reduce the response.
func (s *TransformSearchJobService) FilterPath(filterPath ...string) *TransformSearchJobService {
	s.filterPath = filterPath
	return s
}

// Header adds a header to the request.
func (s *TransformSearchJobService) Header(name string, value string) *TransformSearchJobService {
	if s.headers == nil {
		s.headers = http.Header{}
	}
	s.headers.Add(name, value)
	return s
}

// Headers specifies the headers of the request.
func (s *TransformSearchJobService) Headers(headers http.Header) *TransformSearchJobService {
	s.headers = headers
	return s
}

// Search is the query to search transforms.
func (s *TransformSearchJobService) Search(search string) *TransformSearchJobService {
	s.search = &search
	return s
}

// From is the starting transform to return. Default is 0.
func (s *TransformSearchJobService) From(from int64) *TransformSearchJobService {
	s.from = &from
	return s
}

// Size specifies the number of transforms to return. Default is 10.
func (s *TransformSearchJobService) Size(size int64) *TransformSearchJobService {
	s.size = &size
	return s
}

// SortField is the field to sort results with.
func (s *TransformSearchJobService) SortField(sortField string) *TransformSearchJobService {
	s.sortField = &sortField
	return s
}

// SortField specifies the direction to sort results in. Can be ASC or DESC. Default is ASC
func (s *TransformSearchJobService) SortDirection(sortDirection string) *TransformSearchJobService {
	s.sortDirection = &sortDirection
	return s
}

// buildURL builds the URL for the operation.
func (s *TransformSearchJobService) buildURL() (string, url.Values, error) {
	// Build URL
	path := "/_plugins/_transform"

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
	if v := s.search; v != nil {
		params.Set("search", fmt.Sprint(*v))
	}
	if v := s.from; v != nil {
		params.Set("from", fmt.Sprint(*v))
	}
	if v := s.size; v != nil {
		params.Set("size", fmt.Sprint(*v))
	}
	if v := s.sortField; v != nil {
		params.Set("sortField", fmt.Sprint(*v))
	}
	if v := s.sortDirection; v != nil {
		params.Set("sortDirection", fmt.Sprint(*v))
	}
	return path, params, nil
}

// Validate checks if the operation is valid.
func (s *TransformSearchJobService) Validate() error {
	var invalid []string
	if len(invalid) > 0 {
		return fmt.Errorf("missing required fields: %v", invalid)
	}
	return nil
}

// Do executes the operation.
func (s *TransformSearchJobService) Do(ctx context.Context) (*TransformSearchJobResponse, error) {
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
	ret := new(TransformSearchJobResponse)
	if err := json.Unmarshal(res.Body, ret); err != nil {
		return nil, err
	}
	return ret, nil
}

// TransformSearchJobResponse is the get transform job response object
type TransformSearchJobResponse struct {
	TotalTransforms int64                     `json:"total_transforms"`
	Transforms      []TransformGetJobResponse `json:"transforms"`
}
