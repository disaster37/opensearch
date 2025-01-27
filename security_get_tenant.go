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

// SecurityGetTenantService retrieves a tenant by its name.
// See https://opensearch.org/docs/latest/security/access-control/api/#get-tenant
type SecurityGetTenantService struct {
	client *Client

	pretty     *bool       // pretty format the returned JSON response
	human      *bool       // return human readable values for statistics
	errorTrace *bool       // include the stack trace of returned errors
	filterPath []string    // list of filters used to reduce the response
	headers    http.Header // custom request-level HTTP headers

	name string
}

// NewSecurityGetTenantService creates a new SecurityGetTenantService.
func NewSecurityGetTenantService(client *Client) *SecurityGetTenantService {
	return &SecurityGetTenantService{
		client: client,
	}
}

// Pretty tells Opensearch whether to return a formatted JSON response.
func (s *SecurityGetTenantService) Pretty(pretty bool) *SecurityGetTenantService {
	s.pretty = &pretty
	return s
}

// Human specifies whether human readable values should be returned in
// the JSON response, e.g. "7.5mb".
func (s *SecurityGetTenantService) Human(human bool) *SecurityGetTenantService {
	s.human = &human
	return s
}

// ErrorTrace specifies whether to include the stack trace of returned errors.
func (s *SecurityGetTenantService) ErrorTrace(errorTrace bool) *SecurityGetTenantService {
	s.errorTrace = &errorTrace
	return s
}

// FilterPath specifies a list of filters used to reduce the response.
func (s *SecurityGetTenantService) FilterPath(filterPath ...string) *SecurityGetTenantService {
	s.filterPath = filterPath
	return s
}

// Header adds a header to the request.
func (s *SecurityGetTenantService) Header(name string, value string) *SecurityGetTenantService {
	if s.headers == nil {
		s.headers = http.Header{}
	}
	s.headers.Add(name, value)
	return s
}

// Headers specifies the headers of the request.
func (s *SecurityGetTenantService) Headers(headers http.Header) *SecurityGetTenantService {
	s.headers = headers
	return s
}

// Name is name of the tenant to retrieve.
func (s *SecurityGetTenantService) Name(name string) *SecurityGetTenantService {
	s.name = name
	return s
}

// buildURL builds the URL for the operation.
func (s *SecurityGetTenantService) buildURL() (string, url.Values, error) {
	// Build URL
	path, err := uritemplates.Expand("/_plugins/_security/api/tenants/{name}", map[string]string{
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
func (s *SecurityGetTenantService) Validate() error {
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
func (s *SecurityGetTenantService) Do(ctx context.Context) (*SecurityGetTenantResponse, error) {
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
	ret := new(SecurityGetTenantResponse)
	if err := json.Unmarshal(res.Body, ret); err != nil {
		return nil, err
	}
	return ret, nil
}

// SecurityGetTenantResponse is the response of SecurityGetTenantService.Do.
type SecurityGetTenantResponse map[string]SecurityTenant

// SecurityTenant is the tenant object.
// Source code: https://github.com/opensearch-project/security/blob/main/src/main/java/org/opensearch/security/securityconf/impl/v7/TenantV7.java
type SecurityTenant struct {
	SecurityPutTenant `json:",inline"`
	Reserved          *bool `json:"reserved,omitempty"`
	Hidden            *bool `json:"hidden,omitempty"`
	Static            *bool `json:"static,omitempty"`
}
