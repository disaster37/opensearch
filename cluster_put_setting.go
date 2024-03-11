package opensearch

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
)

// ClusterPutSettingService allows to get a very simple status on the health of the cluster.
//
// See http://www.opensearch.co/guide/en/opensearchsearch/reference/7.0/cluster-health.html
// for details.
type ClusterPutSettingService struct {
	client *Client

	pretty     *bool       // pretty format the returned JSON response
	human      *bool       // return human readable values for statistics
	errorTrace *bool       // include the stack trace of returned errors
	filterPath []string    // list of filters used to reduce the response
	headers    http.Header // custom request-level HTTP headers

	clusterManagerTimeout string //The amount of time to wait for a response from the cluster manager node. Default is 30 seconds.
	timeout               string //The amount of time to wait for a response from the cluster. Default is 30 seconds.
	body                  any    // Settings to put
}

// NewClusterPutSettingService creates a new ClusterPutSettingService.
func NewClusterPutSettingService(client *Client) *ClusterPutSettingService {
	return &ClusterPutSettingService{
		client: client,
	}
}

// Pretty tells Opensearch whether to return a formatted JSON response.
func (s *ClusterPutSettingService) Pretty(pretty bool) *ClusterPutSettingService {
	s.pretty = &pretty
	return s
}

// Human specifies whether human readable values should be returned in
// the JSON response, e.g. "7.5mb".
func (s *ClusterPutSettingService) Human(human bool) *ClusterPutSettingService {
	s.human = &human
	return s
}

// ErrorTrace specifies whether to include the stack trace of returned errors.
func (s *ClusterPutSettingService) ErrorTrace(errorTrace bool) *ClusterPutSettingService {
	s.errorTrace = &errorTrace
	return s
}

// FilterPath specifies a list of filters used to reduce the response.
func (s *ClusterPutSettingService) FilterPath(filterPath ...string) *ClusterPutSettingService {
	s.filterPath = filterPath
	return s
}

// Header adds a header to the request.
func (s *ClusterPutSettingService) Header(name string, value string) *ClusterPutSettingService {
	if s.headers == nil {
		s.headers = http.Header{}
	}
	s.headers.Add(name, value)
	return s
}

// Headers specifies the headers of the request.
func (s *ClusterPutSettingService) Headers(headers http.Header) *ClusterPutSettingService {
	s.headers = headers
	return s
}

// MasterTimeout specifies an explicit operation timeout for connection to master node.
func (s *ClusterPutSettingService) CLusterManagerTimeout(masterTimeout string) *ClusterPutSettingService {
	s.clusterManagerTimeout = masterTimeout
	return s
}

// Timeout specifies an explicit operation timeout.
func (s *ClusterPutSettingService) Timeout(timeout string) *ClusterPutSettingService {
	s.timeout = timeout
	return s
}

// Body is the cluster settings to update
func (s *ClusterPutSettingService) Body(body any) *ClusterPutSettingService {
	s.body = body
	return s
}

// buildURL builds the URL for the operation.
func (s *ClusterPutSettingService) buildURL() (string, url.Values, error) {
	// Build URL
	path := "/_cluster/settings"

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
	if s.clusterManagerTimeout != "" {
		params.Set("cluster_manager_timeout", s.clusterManagerTimeout)
	}
	if s.timeout != "" {
		params.Set("timeout", s.timeout)
	}

	return path, params, nil
}

// Validate checks if the operation is valid.
func (s *ClusterPutSettingService) Validate() error {
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
func (s *ClusterPutSettingService) Do(ctx context.Context) (map[string]any, error) {
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
	ret := map[string]any{}
	if err := s.client.decoder.Decode(res.Body, &ret); err != nil {
		return nil, err
	}
	return ret, nil
}
