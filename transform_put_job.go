package opensearch

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/disaster37/opensearch/v2/uritemplates"
	"k8s.io/utils/ptr"
)

// TransformPutJobService update or create transform jobs by id
// See https://opensearch.org/docs/latest/im-plugin/index-transforms/transforms-apis/#create-a-transform-job
type TransformPutJobService struct {
	client *Client

	pretty     *bool       // pretty format the returned JSON response
	human      *bool       // return human readable values for statistics
	errorTrace *bool       // include the stack trace of returned errors
	filterPath []string    // list of filters used to reduce the response
	headers    http.Header // custom request-level HTTP headers

	name           string
	body           interface{}
	sequenceNumber *int64
	primaryTerm    *int64
}

// NewTransformPutJobService creates a new TransformPutJobService.
func NewTransformPutJobService(client *Client) *TransformPutJobService {
	return &TransformPutJobService{
		client: client,
	}
}

// Pretty tells Opensearch whether to return a formatted JSON response.
func (s *TransformPutJobService) Pretty(pretty bool) *TransformPutJobService {
	s.pretty = &pretty
	return s
}

// Human specifies whether human readable values should be returned in
// the JSON response, e.g. "7.5mb".
func (s *TransformPutJobService) Human(human bool) *TransformPutJobService {
	s.human = &human
	return s
}

// ErrorTrace specifies whether to include the stack trace of returned errors.
func (s *TransformPutJobService) ErrorTrace(errorTrace bool) *TransformPutJobService {
	s.errorTrace = &errorTrace
	return s
}

// FilterPath specifies a list of filters used to reduce the response.
func (s *TransformPutJobService) FilterPath(filterPath ...string) *TransformPutJobService {
	s.filterPath = filterPath
	return s
}

// Header adds a header to the request.
func (s *TransformPutJobService) Header(name string, value string) *TransformPutJobService {
	if s.headers == nil {
		s.headers = http.Header{}
	}
	s.headers.Add(name, value)
	return s
}

// Headers specifies the headers of the request.
func (s *TransformPutJobService) Headers(headers http.Header) *TransformPutJobService {
	s.headers = headers
	return s
}

// Name is name of the transform to create or update.
func (s *TransformPutJobService) Name(name string) *TransformPutJobService {
	s.name = name
	return s
}

// Body specifies the transform. Use a string or a type that will get serialized as JSON.
func (s *TransformPutJobService) Body(body interface{}) *TransformPutJobService {
	s.body = body
	return s
}

// SequenceNumber specifies the sequence number to update.
func (s *TransformPutJobService) SequenceNumber(seqNum int64) *TransformPutJobService {
	s.sequenceNumber = ptr.To[int64](seqNum)
	return s
}

// PrimaryTerm specifies the primary term to update.
func (s *TransformPutJobService) PrimaryTerm(primaryTerm int64) *TransformPutJobService {
	s.primaryTerm = ptr.To[int64](primaryTerm)
	return s
}

// buildURL builds the URL for the operation.
func (s *TransformPutJobService) buildURL() (string, url.Values, error) {
	var (
		path string
		err  error
	)

	// Build URL
	if s.primaryTerm != nil && s.sequenceNumber != nil {
		path, err = uritemplates.Expand("/_plugins/_transform/{name}?if_seq_no={seqNum}&if_primary_term={priTerm}", map[string]string{
			"name":    s.name,
			"seqNum":  strconv.FormatInt(*s.sequenceNumber, 10),
			"priTerm": strconv.FormatInt(*s.primaryTerm, 10),
		})
	} else {
		path, err = uritemplates.Expand("/_plugins/_transform/{name}", map[string]string{
			"name": s.name,
		})
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
func (s *TransformPutJobService) Validate() error {
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
func (s *TransformPutJobService) Do(ctx context.Context) (*TransformGetJobResponse, error) {
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
	ret := new(TransformGetJobResponse)
	if err := json.Unmarshal(res.Body, ret); err != nil {
		return nil, err
	}
	return ret, nil
}

// TransformPutJob is the transform job object to create or update
type TransformPutJob struct {
	Transform TransformJobBase `json:"transform"`
}
