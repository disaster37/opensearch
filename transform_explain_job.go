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

// TransformExplainJobService explain a transform job by its name.
// See https://opensearch.org/docs/latest/im-plugin/index-transforms/transforms-apis/#get-the-status-of-a-transform-job
type TransformExplainJobService struct {
	client *Client

	pretty     *bool       // pretty format the returned JSON response
	human      *bool       // return human readable values for statistics
	errorTrace *bool       // include the stack trace of returned errors
	filterPath []string    // list of filters used to reduce the response
	headers    http.Header // custom request-level HTTP headers

	name string
}

// NewTransformExplainJobService creates a new TransformExplainJobService.
func NewTransformExplainJobService(client *Client) *TransformExplainJobService {
	return &TransformExplainJobService{
		client: client,
	}
}

// Pretty tells Opensearch whether to return a formatted JSON response.
func (s *TransformExplainJobService) Pretty(pretty bool) *TransformExplainJobService {
	s.pretty = &pretty
	return s
}

// Human specifies whether human readable values should be returned in
// the JSON response, e.g. "7.5mb".
func (s *TransformExplainJobService) Human(human bool) *TransformExplainJobService {
	s.human = &human
	return s
}

// ErrorTrace specifies whether to include the stack trace of returned errors.
func (s *TransformExplainJobService) ErrorTrace(errorTrace bool) *TransformExplainJobService {
	s.errorTrace = &errorTrace
	return s
}

// FilterPath specifies a list of filters used to reduce the response.
func (s *TransformExplainJobService) FilterPath(filterPath ...string) *TransformExplainJobService {
	s.filterPath = filterPath
	return s
}

// Header adds a header to the request.
func (s *TransformExplainJobService) Header(name string, value string) *TransformExplainJobService {
	if s.headers == nil {
		s.headers = http.Header{}
	}
	s.headers.Add(name, value)
	return s
}

// Headers specifies the headers of the request.
func (s *TransformExplainJobService) Headers(headers http.Header) *TransformExplainJobService {
	s.headers = headers
	return s
}

// Name is name of the transform job to explain.
func (s *TransformExplainJobService) Name(name string) *TransformExplainJobService {
	s.name = name
	return s
}

// buildURL builds the URL for the operation.
func (s *TransformExplainJobService) buildURL() (string, url.Values, error) {
	// Build URL
	path, err := uritemplates.Expand("/_plugins/_transform/{name}/_explain", map[string]string{
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
func (s *TransformExplainJobService) Validate() error {
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
func (s *TransformExplainJobService) Do(ctx context.Context) (map[string]TransformExplainJob, error) {
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
	ret := map[string]TransformExplainJob{}
	if err := json.Unmarshal(res.Body, &ret); err != nil {
		return nil, err
	}
	return ret, nil
}

// TransformExplainJob is the explain object
type TransformExplainJob struct {
	MetadataId        string                  `json:"metadata_id"`
	TransformMetadata map[string]any          `json:"transform_metadata"`
	TransformId       string                  `json:"transform_id"`
	LastUpdatedAt     int64                   `json:"last_updated_at"`
	Status            string                  `json:"status"`
	FailureReason     string                  `json:"failure_reason"`
	Stats             TransformExplainJobStat `json:"stats"`
}

// TransformExplainJobStat is the stat of stansform job
type TransformExplainJobStat struct {
	PagesProcessed     int64 `json:"pages_processed"`
	DocumentsProcessed int64 `json:"documents_processed"`
	DocumentsIndexed   int64 `json:"documents_indexed"`
	IndexTimeInMillis  int64 `json:"index_time_in_millis"`
	SearchTimeInMillis int64 `json:"search_time_in_millis"`
}
