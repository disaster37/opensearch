package opensearch

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/disaster37/opensearch/v3/uritemplates"
)

// SecurityAuthInfoService retrieves authentication information for the currently authenticated user.
// See https://docs.opensearch.org/latest/api-reference/security/authentication/auth-info/
type SecurityAuthInfoService struct {
	client *Client

	pretty     *bool       // pretty format the returned JSON response
	human      *bool       // return human readable values for statistics
	errorTrace *bool       // include the stack trace of returned errors
	filterPath []string    // list of filters used to reduce the response
	headers    http.Header // custom request-level HTTP headers

	authType *string // the type of the current authentication request
	verbose  *bool   // whether to return a verbose response
}

// NewSecurityAuthInfoService creates a new SecurityAuthInfoService.
func NewSecurityAuthInfoService(client *Client) *SecurityAuthInfoService {
	return &SecurityAuthInfoService{
		client: client,
	}
}

// Pretty tells Opensearch whether to return a formatted JSON response.
func (s *SecurityAuthInfoService) Pretty(pretty bool) *SecurityAuthInfoService {
	s.pretty = &pretty
	return s
}

// Human specifies whether human readable values should be returned in
// the JSON response, e.g. "7.5mb".
func (s *SecurityAuthInfoService) Human(human bool) *SecurityAuthInfoService {
	s.human = &human
	return s
}

// ErrorTrace specifies whether to include the stack trace of returned errors.
func (s *SecurityAuthInfoService) ErrorTrace(errorTrace bool) *SecurityAuthInfoService {
	s.errorTrace = &errorTrace
	return s
}

// FilterPath specifies a list of filters used to reduce the response.
func (s *SecurityAuthInfoService) FilterPath(filterPath ...string) *SecurityAuthInfoService {
	s.filterPath = filterPath
	return s
}

// Header adds a header to the request.
func (s *SecurityAuthInfoService) Header(name string, value string) *SecurityAuthInfoService {
	if s.headers == nil {
		s.headers = http.Header{}
	}
	s.headers.Add(name, value)
	return s
}

// Headers specifies the headers of the request.
func (s *SecurityAuthInfoService) Headers(headers http.Header) *SecurityAuthInfoService {
	s.headers = headers
	return s
}

// AuthType sets the type of the current authentication request.
func (s *SecurityAuthInfoService) AuthType(authType string) *SecurityAuthInfoService {
	s.authType = &authType
	return s
}

// Verbose specifies whether to return a verbose response.
func (s *SecurityAuthInfoService) Verbose(verbose bool) *SecurityAuthInfoService {
	s.verbose = &verbose
	return s
}

// buildURL builds the URL for the operation.
func (s *SecurityAuthInfoService) buildURL() (string, url.Values, error) {
	// Build URL
	path, err := uritemplates.Expand("/_plugins/_security/authinfo", nil)
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
	if v := s.authType; v != nil {
		params.Set("auth_type", *v)
	}
	if v := s.verbose; v != nil {
		params.Set("verbose", fmt.Sprint(*v))
	}
	return path, params, nil
}

// Validate checks if the operation is valid.
func (s *SecurityAuthInfoService) Validate() error {
	var invalid []string
	if len(invalid) > 0 {
		return fmt.Errorf("missing required fields: %v", invalid)
	}
	return nil
}

// Do executes the operation.
func (s *SecurityAuthInfoService) Do(ctx context.Context) (*SecurityAuthInfoResponse, error) {
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
	ret := new(SecurityAuthInfoResponse)
	if err := json.Unmarshal(res.Body, ret); err != nil {
		return nil, err
	}
	return ret, nil
}

// SecurityAuthInfoResponse is the response of SecurityAuthInfoService.Do.
type SecurityAuthInfoResponse struct {
	User                    string          `json:"user"`
	UserName                string          `json:"user_name"`
	BackendRoles            []string        `json:"backend_roles"`
	CustomAttributeNames    []string        `json:"custom_attribute_names,omitempty"`
	Roles                   []string        `json:"roles"`
	Tenants                 map[string]bool `json:"tenants"`
	Principal               *string         `json:"principal"`
	PeerCertificates        string          `json:"peer_certificates"`
	SSOLogoutURL            *string         `json:"sso_logout_url"`
	RemoteAddress           string          `json:"remote_address"`
	SizeOfUser              string          `json:"size_of_user,omitempty"`
	SizeOfBackendRoles      string          `json:"size_of_backendroles,omitempty"`
	SizeOfCustomAttributes  string          `json:"size_of_custom_attributes,omitempty"`
	UserRequestedTenant     *string         `json:"user_requested_tenant,omitempty"`
}
