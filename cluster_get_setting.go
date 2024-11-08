package opensearch

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
)

// ClusterGetSettingService allows to get a very simple status on the health of the cluster.
//
// See http://www.opensearch.co/guide/en/opensearchsearch/reference/7.0/cluster-health.html
// for details.
type ClusterGetSettingService struct {
	client *Client

	pretty     *bool       // pretty format the returned JSON response
	human      *bool       // return human readable values for statistics
	errorTrace *bool       // include the stack trace of returned errors
	filterPath []string    // list of filters used to reduce the response
	headers    http.Header // custom request-level HTTP headers

	flatSettings          *bool  // Whether to return settings in the flat form, which can improve readability, especially for heavily nested settings. For example, the flat form of "cluster": { "max_shards_per_node": 500 } is "cluster.max_shards_per_node": "500".
	includeDefaults       *bool  // 	Whether to include default settings as part of the response. This parameter is useful for identifying the names and current values of settings you want to update.
	clusterManagerTimeout string // The amount of time to wait for a response from the cluster manager node. Default is 30 seconds.
	timeout               string // The amount of time to wait for a response from the cluster. Default is 30 seconds.
}

// NewClusterGetSettingService creates a new ClusterGetSettingService.
func NewClusterGetSettingService(client *Client) *ClusterGetSettingService {
	return &ClusterGetSettingService{
		client: client,
	}
}

// Pretty tells Opensearch whether to return a formatted JSON response.
func (s *ClusterGetSettingService) Pretty(pretty bool) *ClusterGetSettingService {
	s.pretty = &pretty
	return s
}

// Human specifies whether human readable values should be returned in
// the JSON response, e.g. "7.5mb".
func (s *ClusterGetSettingService) Human(human bool) *ClusterGetSettingService {
	s.human = &human
	return s
}

// ErrorTrace specifies whether to include the stack trace of returned errors.
func (s *ClusterGetSettingService) ErrorTrace(errorTrace bool) *ClusterGetSettingService {
	s.errorTrace = &errorTrace
	return s
}

// FilterPath specifies a list of filters used to reduce the response.
func (s *ClusterGetSettingService) FilterPath(filterPath ...string) *ClusterGetSettingService {
	s.filterPath = filterPath
	return s
}

// Header adds a header to the request.
func (s *ClusterGetSettingService) Header(name string, value string) *ClusterGetSettingService {
	if s.headers == nil {
		s.headers = http.Header{}
	}
	s.headers.Add(name, value)
	return s
}

// Headers specifies the headers of the request.
func (s *ClusterGetSettingService) Headers(headers http.Header) *ClusterGetSettingService {
	s.headers = headers
	return s
}

// MasterTimeout specifies an explicit operation timeout for connection to master node.
func (s *ClusterGetSettingService) CLusterManagerTimeout(masterTimeout string) *ClusterGetSettingService {
	s.clusterManagerTimeout = masterTimeout
	return s
}

// Timeout specifies an explicit operation timeout.
func (s *ClusterGetSettingService) Timeout(timeout string) *ClusterGetSettingService {
	s.timeout = timeout
	return s
}

// FlatSettings specified an explicit flat settings
func (s *ClusterGetSettingService) FlatSettings(flatSettings bool) *ClusterGetSettingService {
	s.flatSettings = &flatSettings
	return s
}

// IncludeDefaults specified an explicit include defaults
func (s *ClusterGetSettingService) IncludeDefaults(includeDefault bool) *ClusterGetSettingService {
	s.includeDefaults = &includeDefault
	return s
}

// buildURL builds the URL for the operation.
func (s *ClusterGetSettingService) buildURL() (string, url.Values, error) {
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
	if s.flatSettings != nil {
		params.Set("flat_settings", fmt.Sprint(*s.flatSettings))
	}
	if s.includeDefaults != nil {
		params.Set("include_defaults", fmt.Sprint(*s.includeDefaults))
	}

	return path, params, nil
}

// Validate checks if the operation is valid.
func (s *ClusterGetSettingService) Validate() error {
	return nil
}

// Do executes the operation.
func (s *ClusterGetSettingService) Do(ctx context.Context) (map[string]any, error) {
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
	ret := map[string]any{}
	if err := s.client.decoder.Decode(res.Body, &ret); err != nil {
		return nil, err
	}
	return ret, nil
}
