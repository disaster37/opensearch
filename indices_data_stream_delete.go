// Copyright 2012-present Oliver Eilhard. All rights reserved.
// Use of this source code is governed by a MIT-license.
// See http://olivere.mit-license.org/license.txt for details.

package opensearch

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/disaster37/opensearch/v3/uritemplates"
)

// IndicesDataStreamDeleteService allows to delete existing data stream indices.
//
// See https://docs.opensearch.org/latest/im-plugin/data-streams/
// for details.
type IndicesDataStreamDeleteService struct {
	client *Client

	pretty     *bool       // pretty format the returned JSON response
	human      *bool       // return human readable values for statistics
	errorTrace *bool       // include the stack trace of returned errors
	filterPath []string    // list of filters used to reduce the response
	headers    http.Header // custom request-level HTTP headers

	datastream        []string
	timeout           string
	masterTimeout     string
	ignoreUnavailable *bool
	allowNoIndices    *bool
	expandWildcards   string
}

// NewIndicesDataStreamDeleteService creates and initializes a new IndicesDataStreamDeleteService.
func NewIndicesDataStreamDeleteService(client *Client) *IndicesDataStreamDeleteService {
	return &IndicesDataStreamDeleteService{
		client:     client,
		datastream: make([]string, 0),
	}
}

// Pretty tells Opensearch whether to return a formatted JSON response.
func (s *IndicesDataStreamDeleteService) Pretty(pretty bool) *IndicesDataStreamDeleteService {
	s.pretty = &pretty
	return s
}

// Human specifies whether human readable values should be returned in
// the JSON response, e.g. "7.5mb".
func (s *IndicesDataStreamDeleteService) Human(human bool) *IndicesDataStreamDeleteService {
	s.human = &human
	return s
}

// ErrorTrace specifies whether to include the stack trace of returned errors.
func (s *IndicesDataStreamDeleteService) ErrorTrace(errorTrace bool) *IndicesDataStreamDeleteService {
	s.errorTrace = &errorTrace
	return s
}

// FilterPath specifies a list of filters used to reduce the response.
func (s *IndicesDataStreamDeleteService) FilterPath(filterPath ...string) *IndicesDataStreamDeleteService {
	s.filterPath = filterPath
	return s
}

// Header adds a header to the request.
func (s *IndicesDataStreamDeleteService) Header(name string, value string) *IndicesDataStreamDeleteService {
	if s.headers == nil {
		s.headers = http.Header{}
	}
	s.headers.Add(name, value)
	return s
}

// Headers specifies the headers of the request.
func (s *IndicesDataStreamDeleteService) Headers(headers http.Header) *IndicesDataStreamDeleteService {
	s.headers = headers
	return s
}

// Datastream adds the list of data streams to delete.
// Use `_all` or `*` string to delete all data streams.
func (s *IndicesDataStreamDeleteService) Datastream(datastream ...string) *IndicesDataStreamDeleteService {
	s.datastream = datastream
	return s
}

// Timeout is an explicit operation timeout.
func (s *IndicesDataStreamDeleteService) Timeout(timeout string) *IndicesDataStreamDeleteService {
	s.timeout = timeout
	return s
}

// MasterTimeout specifies the timeout for connection to master.
func (s *IndicesDataStreamDeleteService) MasterTimeout(masterTimeout string) *IndicesDataStreamDeleteService {
	s.masterTimeout = masterTimeout
	return s
}

// IgnoreUnavailable indicates whether to ignore unavailable indexes (default: false).
func (s *IndicesDataStreamDeleteService) IgnoreUnavailable(ignoreUnavailable bool) *IndicesDataStreamDeleteService {
	s.ignoreUnavailable = &ignoreUnavailable
	return s
}

// AllowNoIndices indicates whether to ignore if a wildcard expression
// resolves to no concrete indices (default: false).
func (s *IndicesDataStreamDeleteService) AllowNoIndices(allowNoIndices bool) *IndicesDataStreamDeleteService {
	s.allowNoIndices = &allowNoIndices
	return s
}

// ExpandWildcards indicates whether wildcard expressions should get
// expanded to open or closed indices (default: open).
func (s *IndicesDataStreamDeleteService) ExpandWildcards(expandWildcards string) *IndicesDataStreamDeleteService {
	s.expandWildcards = expandWildcards
	return s
}

// buildURL builds the URL for the operation.
func (s *IndicesDataStreamDeleteService) buildURL() (string, url.Values, error) {
	// Build URL
	path, err := uritemplates.Expand("/_data_stream/{datastream}", map[string]string{
		"datastream": strings.Join(s.datastream, ","),
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
	if s.timeout != "" {
		params.Set("timeout", s.timeout)
	}
	if s.masterTimeout != "" {
		params.Set("master_timeout", s.masterTimeout)
	}
	if s.ignoreUnavailable != nil {
		params.Set("ignore_unavailable", fmt.Sprintf("%v", *s.ignoreUnavailable))
	}
	if s.allowNoIndices != nil {
		params.Set("allow_no_indices", fmt.Sprintf("%v", *s.allowNoIndices))
	}
	if s.expandWildcards != "" {
		params.Set("expand_wildcards", s.expandWildcards)
	}
	return path, params, nil
}

// Validate checks if the operation is valid.
func (s *IndicesDataStreamDeleteService) Validate() error {
	var invalid []string
	if len(s.datastream) == 0 {
		invalid = append(invalid, "Datastream")
	}
	if len(invalid) > 0 {
		return fmt.Errorf("missing required fields: %v", invalid)
	}
	return nil
}

// Do executes the operation.
func (s *IndicesDataStreamDeleteService) Do(ctx context.Context) (*IndicesDatastreamDeleteResponse, error) {
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
	ret := new(IndicesDatastreamDeleteResponse)
	if err := s.client.decoder.Decode(res.Body, ret); err != nil {
		return nil, err
	}
	return ret, nil
}

// -- Result of a delete data stream request.

// IndicesDatastreamDeleteResponse is the response of IndicesDataStreamDeleteService.Do.
type IndicesDatastreamDeleteResponse struct {
	Acknowledged bool `json:"acknowledged"`
}
