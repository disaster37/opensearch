// Copyright 2012-present Oliver Eilhard. All rights reserved.
// Use of this source code is governed by a MIT-license.
// See http://olivere.mit-license.org/license.txt for details.

package opensearch

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/disaster37/opensearch/v3/uritemplates"
)

// IndicesDataStreamCreateService creates a new index.
//
// See https://docs.opensearch.org/latest/im-plugin/data-streams/
// for details.
type IndicesDataStreamCreateService struct {
	client *Client

	pretty     *bool       // pretty format the returned JSON response
	human      *bool       // return human readable values for statistics
	errorTrace *bool       // include the stack trace of returned errors
	filterPath []string    // list of filters used to reduce the response
	headers    http.Header // custom request-level HTTP headers

	datastream      string
	timeout         string
	masterTimeout   string
	includeTypeName *bool
	bodyJson        interface{}
	bodyString      string
}

// NewIndicesDataStreamCreateService returns a new IndicesDataStreamCreateService.
func NewIndicesDataStreamCreateService(client *Client) *IndicesDataStreamCreateService {
	return &IndicesDataStreamCreateService{client: client}
}

// Pretty tells Opensearch whether to return a formatted JSON response.
func (s *IndicesDataStreamCreateService) Pretty(pretty bool) *IndicesDataStreamCreateService {
	s.pretty = &pretty
	return s
}

// Human specifies whether human readable values should be returned in
// the JSON response, e.g. "7.5mb".
func (s *IndicesDataStreamCreateService) Human(human bool) *IndicesDataStreamCreateService {
	s.human = &human
	return s
}

// ErrorTrace specifies whether to include the stack trace of returned errors.
func (s *IndicesDataStreamCreateService) ErrorTrace(errorTrace bool) *IndicesDataStreamCreateService {
	s.errorTrace = &errorTrace
	return s
}

// FilterPath specifies a list of filters used to reduce the response.
func (s *IndicesDataStreamCreateService) FilterPath(filterPath ...string) *IndicesDataStreamCreateService {
	s.filterPath = filterPath
	return s
}

// Header adds a header to the request.
func (s *IndicesDataStreamCreateService) Header(name string, value string) *IndicesDataStreamCreateService {
	if s.headers == nil {
		s.headers = http.Header{}
	}
	s.headers.Add(name, value)
	return s
}

// Headers specifies the headers of the request.
func (s *IndicesDataStreamCreateService) Headers(headers http.Header) *IndicesDataStreamCreateService {
	s.headers = headers
	return s
}

// DataStream is the name of the index data stream to create.
func (s *IndicesDataStreamCreateService) DataStream(datastream string) *IndicesDataStreamCreateService {
	s.datastream = datastream
	return s
}

// Timeout the explicit operation timeout, e.g. "5s".
func (s *IndicesDataStreamCreateService) Timeout(timeout string) *IndicesDataStreamCreateService {
	s.timeout = timeout
	return s
}

// MasterTimeout specifies the timeout for connection to master.
func (s *IndicesDataStreamCreateService) MasterTimeout(masterTimeout string) *IndicesDataStreamCreateService {
	s.masterTimeout = masterTimeout
	return s
}

// IncludeTypeName indicates whether a type should be expected in the body of the mappings.
func (s *IndicesDataStreamCreateService) IncludeTypeName(includeTypeName bool) *IndicesDataStreamCreateService {
	s.includeTypeName = &includeTypeName
	return s
}

// Body specifies the configuration of the index as a string.
// It is an alias for BodyString.
func (s *IndicesDataStreamCreateService) Body(body string) *IndicesDataStreamCreateService {
	s.bodyString = body
	return s
}

// BodyString specifies the configuration of the index as a string.
func (s *IndicesDataStreamCreateService) BodyString(body string) *IndicesDataStreamCreateService {
	s.bodyString = body
	return s
}

// BodyJson specifies the configuration of the index. The interface{} will
// be serializes as a JSON document, so use a map[string]interface{}.
func (s *IndicesDataStreamCreateService) BodyJson(body interface{}) *IndicesDataStreamCreateService {
	s.bodyJson = body
	return s
}

// Do executes the operation.
func (s *IndicesDataStreamCreateService) Do(ctx context.Context) (*IndicesDataStreamCreateResult, error) {
	if s.datastream == "" {
		return nil, errors.New("missing index name")
	}

	// Build url
	path, err := uritemplates.Expand("/_data_stream/{datastream}", map[string]string{
		"datastream": s.datastream,
	})
	if err != nil {
		return nil, err
	}

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
	if s.masterTimeout != "" {
		params.Set("master_timeout", s.masterTimeout)
	}
	if s.timeout != "" {
		params.Set("timeout", s.timeout)
	}
	if v := s.includeTypeName; v != nil {
		params.Set("include_type_name", fmt.Sprint(*v))
	}

	// Setup HTTP request body
	var body interface{}
	if s.bodyJson != nil {
		body = s.bodyJson
	} else {
		body = s.bodyString
	}

	// Get response
	res, err := s.client.PerformRequest(ctx, PerformRequestOptions{
		Method:  "PUT",
		Path:    path,
		Params:  params,
		Body:    body,
		Headers: s.headers,
	})
	if err != nil {
		return nil, err
	}

	ret := new(IndicesDataStreamCreateResult)
	if err := s.client.decoder.Decode(res.Body, ret); err != nil {
		return nil, err
	}
	return ret, nil
}

// -- Result of a create index datastream request.

// IndicesDataStreamCreateResult is the outcome of creating a new index data stream.
type IndicesDataStreamCreateResult struct {
	Acknowledged bool `json:"acknowledged"`
}
