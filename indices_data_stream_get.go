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

// IndicesDataStreamGetService retrieves information about one or more data stream indices.
//
// See https://docs.opensearch.org/latest/im-plugin/data-streams/
// for more details.
type IndicesDataStreamGetService struct {
	client *Client

	pretty     *bool       // pretty format the returned JSON response
	human      *bool       // return human readable values for statistics
	errorTrace *bool       // include the stack trace of returned errors
	filterPath []string    // list of filters used to reduce the response
	headers    http.Header // custom request-level HTTP headers

	datastream        []string
	feature           []string
	local             *bool
	ignoreUnavailable *bool
	allowNoIndices    *bool
	expandWildcards   string
	flatSettings      *bool
}

// NewIndicesDataStreamGetService creates a new IndicesDataStreamGetService.
func NewIndicesDataStreamGetService(client *Client) *IndicesDataStreamGetService {
	return &IndicesDataStreamGetService{
		client:     client,
		datastream: make([]string, 0),
		feature:    make([]string, 0),
	}
}

// Pretty tells Opensearch whether to return a formatted JSON response.
func (s *IndicesDataStreamGetService) Pretty(pretty bool) *IndicesDataStreamGetService {
	s.pretty = &pretty
	return s
}

// Human specifies whether human readable values should be returned in
// the JSON response, e.g. "7.5mb".
func (s *IndicesDataStreamGetService) Human(human bool) *IndicesDataStreamGetService {
	s.human = &human
	return s
}

// ErrorTrace specifies whether to include the stack trace of returned errors.
func (s *IndicesDataStreamGetService) ErrorTrace(errorTrace bool) *IndicesDataStreamGetService {
	s.errorTrace = &errorTrace
	return s
}

// FilterPath specifies a list of filters used to reduce the response.
func (s *IndicesDataStreamGetService) FilterPath(filterPath ...string) *IndicesDataStreamGetService {
	s.filterPath = filterPath
	return s
}

// Header adds a header to the request.
func (s *IndicesDataStreamGetService) Header(name string, value string) *IndicesDataStreamGetService {
	if s.headers == nil {
		s.headers = http.Header{}
	}
	s.headers.Add(name, value)
	return s
}

// Headers specifies the headers of the request.
func (s *IndicesDataStreamGetService) Headers(headers http.Header) *IndicesDataStreamGetService {
	s.headers = headers
	return s
}

// Datastream is a list of  datastream index names.
func (s *IndicesDataStreamGetService) Datastream(datastreams ...string) *IndicesDataStreamGetService {
	s.datastream = append(s.datastream, datastreams...)
	return s
}

// Feature is a list of features.
func (s *IndicesDataStreamGetService) Feature(features ...string) *IndicesDataStreamGetService {
	s.feature = append(s.feature, features...)
	return s
}

// Local indicates whether to return local information, i.e. do not retrieve
// the state from master node (default: false).
func (s *IndicesDataStreamGetService) Local(local bool) *IndicesDataStreamGetService {
	s.local = &local
	return s
}

// IgnoreUnavailable indicates whether to ignore unavailable indexes (default: false).
func (s *IndicesDataStreamGetService) IgnoreUnavailable(ignoreUnavailable bool) *IndicesDataStreamGetService {
	s.ignoreUnavailable = &ignoreUnavailable
	return s
}

// AllowNoIndices indicates whether to ignore if a wildcard expression
// resolves to no concrete indices (default: false).
func (s *IndicesDataStreamGetService) AllowNoIndices(allowNoIndices bool) *IndicesDataStreamGetService {
	s.allowNoIndices = &allowNoIndices
	return s
}

// ExpandWildcards indicates whether wildcard expressions should get
// expanded to open or closed indices (default: open).
func (s *IndicesDataStreamGetService) ExpandWildcards(expandWildcards string) *IndicesDataStreamGetService {
	s.expandWildcards = expandWildcards
	return s
}

// buildURL builds the URL for the operation.
func (s *IndicesDataStreamGetService) buildURL() (string, url.Values, error) {
	var err error
	var path string
	var datastream []string

	if len(s.datastream) > 0 {
		datastream = s.datastream
	} else {
		datastream = []string{"_all"}
	}

	if len(s.feature) > 0 {
		// Build URL
		path, err = uritemplates.Expand("/_data_stream/{datastream}/{feature}", map[string]string{
			"datastream": strings.Join(datastream, ","),
			"feature":    strings.Join(s.feature, ","),
		})
	} else {
		// Build URL
		path, err = uritemplates.Expand("/_data_stream/{datastream}", map[string]string{
			"datastream": strings.Join(datastream, ","),
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
	if s.expandWildcards != "" {
		params.Set("expand_wildcards", s.expandWildcards)
	}
	if s.flatSettings != nil {
		params.Set("flat_settings", fmt.Sprintf("%v", *s.flatSettings))
	}
	if s.local != nil {
		params.Set("local", fmt.Sprintf("%v", *s.local))
	}
	if s.ignoreUnavailable != nil {
		params.Set("ignore_unavailable", fmt.Sprintf("%v", *s.ignoreUnavailable))
	}
	if s.allowNoIndices != nil {
		params.Set("allow_no_indices", fmt.Sprintf("%v", *s.allowNoIndices))
	}
	return path, params, nil
}

// Validate checks if the operation is valid.
func (s *IndicesDataStreamGetService) Validate() error {
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
func (s *IndicesDataStreamGetService) Do(ctx context.Context) (*IndicesDataStreamGetResponse, error) {
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
	var ret IndicesDataStreamGetResponse
	if err := s.client.decoder.Decode(res.Body, &ret); err != nil {
		return nil, err
	}
	return &ret, nil
}

// IndicesDataStreamGetResponse is part of the response of IndicesDataStreamGetService.Do.
type IndicesDataStreamGetResponse struct {
	Datastreams []*IndicesDataStreamGetDataStream `json:"data_streams"`
}

// IndicesDataStreamGetDataStream is part of the response of IndicesDataStreamGetService.Do.
type IndicesDataStreamGetDataStream struct {
	Name           string `json:"name"`
	TimestampField struct {
		Name string `json:"name"`
	} `json:"timestamp_field"`
	Indices []*IndicesDataStreamGetDataStreamIndice `json:"indices"`
	Generation int64 `json:"generation"`
	Status     string `json:"status"`
	Template   string `json:"template"`
}

// IndicesDataStreamGetDataStreamIndice is part of the response of IndicesDataStreamGetService.Do.
type IndicesDataStreamGetDataStreamIndice struct {
	IndexName string `json:"index_name"`
	IndexUUID string `json:"index_uuid"`
}
