package api

import (
	"context"
	"fmt"

	json "github.com/goccy/go-json"

	"github.com/go-resty/resty/v2"
	"github.com/sirupsen/logrus"
)

// SecurityService defines the interface for interacting with the OpenSearch Security plugin API.
// It provides methods to manage roles, role mappings, users, action groups, tenants,
// distinguished names, cache, authentication info, security configuration, and audit logging.
// See https://opensearch.org/docs/latest/security/access-control/api/ for the OpenSearch Security plugin documentation.
type SecurityService interface {
	// GetRole retrieves a security role by name from the Security plugin.
	// The roleName parameter is required and specifies the name of the role to retrieve.
	// Returns a map keyed by role name containing the SecurityRole definition, or an error.
	GetRole(ctx context.Context, roleName string) (map[string]SecurityRole, error)

	// PutRole creates or updates a security role in the Security plugin.
	// The roleName parameter is required and specifies the name of the role.
	// The body parameter should contain the role definition including cluster permissions,
	// index permissions, and tenant permissions.
	// Returns a SecurityResponse confirming the operation, or an error.
	PutRole(ctx context.Context, roleName string, body *SecurityPutRole) (*SecurityResponse, error)

	// DeleteRole deletes a security role from the Security plugin.
	// The roleName parameter is required and specifies the name of the role to delete.
	// Returns a SecurityResponse confirming the deletion, or an error.
	DeleteRole(ctx context.Context, roleName string) (*SecurityResponse, error)

	// GetRoleMapping retrieves the role mapping for a given role from the Security plugin.
	// The roleName parameter is required and specifies which role's mapping to retrieve.
	// Returns a map keyed by role name containing the SecurityRoleMapping, or an error.
	GetRoleMapping(ctx context.Context, roleName string) (map[string]SecurityRoleMapping, error)

	// PutRoleMapping creates or updates a role mapping that assigns backend roles, hosts,
	// and users to a security role.
	// The roleName parameter is required. The body should contain the mapping definition
	// with backend_roles, hosts, and users.
	// Returns a SecurityResponse confirming the operation, or an error.
	PutRoleMapping(ctx context.Context, roleName string, body *SecurityPutRoleMapping) (*SecurityResponse, error)

	// DeleteRoleMapping deletes the role mapping for a given security role.
	// The roleName parameter is required.
	// Returns a SecurityResponse confirming the deletion, or an error.
	DeleteRoleMapping(ctx context.Context, roleName string) (*SecurityResponse, error)

	// GetUser retrieves an internal user from the Security plugin by username.
	// The username parameter is required.
	// Returns a map keyed by username containing the SecurityUser definition, or an error.
	GetUser(ctx context.Context, username string) (map[string]SecurityUser, error)

	// PutUser creates or updates an internal user in the Security plugin.
	// The username parameter is required. The body should contain the user definition
	// including password, backend roles, attributes, and other user properties.
	// Returns a SecurityResponse confirming the operation, or an error.
	PutUser(ctx context.Context, username string, body *SecurityPutUser) (*SecurityResponse, error)

	// DeleteUser deletes an internal user from the Security plugin.
	// The username parameter is required.
	// Returns a SecurityResponse confirming the deletion, or an error.
	DeleteUser(ctx context.Context, username string) (*SecurityResponse, error)

	// GetActionGroup retrieves a security action group by name.
	// The name parameter is required and specifies the action group to retrieve.
	// Returns a map keyed by action group name containing the SecurityActionGroup, or an error.
	GetActionGroup(ctx context.Context, name string) (map[string]SecurityActionGroup, error)

	// PutActionGroup creates or updates a security action group.
	// The name parameter is required. The body should contain the action group definition
	// including allowed_actions, type, and description.
	// Returns a SecurityResponse confirming the operation, or an error.
	PutActionGroup(ctx context.Context, name string, body *SecurityPutActionGroup) (*SecurityResponse, error)

	// DeleteActionGroup deletes a security action group by name.
	// The name parameter is required.
	// Returns a SecurityResponse confirming the deletion, or an error.
	DeleteActionGroup(ctx context.Context, name string) (*SecurityResponse, error)

	// GetTenant retrieves a security tenant by name.
	// The name parameter is required and specifies the tenant to retrieve.
	// Returns a map keyed by tenant name containing the SecurityTenant definition, or an error.
	GetTenant(ctx context.Context, name string) (map[string]SecurityTenant, error)

	// PutTenant creates or updates a security tenant.
	// The name parameter is required. The body should contain the tenant definition
	// including description.
	// Returns a SecurityResponse confirming the operation, or an error.
	PutTenant(ctx context.Context, name string, body *SecurityPutTenant) (*SecurityResponse, error)

	// DeleteTenant deletes a security tenant by name.
	// The name parameter is required.
	// Returns a SecurityResponse confirming the deletion, or an error.
	DeleteTenant(ctx context.Context, name string) (*SecurityResponse, error)

	// GetDistinguishedName retrieves a node distinguished name (DN) entry by name
	// from the Security plugin's nodes DN configuration.
	// The name parameter is required.
	// Returns a map keyed by name containing the SecurityDistinguishedName, or an error.
	GetDistinguishedName(ctx context.Context, name string) (map[string]SecurityDistinguishedName, error)

	// PutDistinguishedName creates or updates a node distinguished name entry
	// in the Security plugin's nodes DN configuration.
	// The name parameter is required. The body should contain the nodes_dn list.
	// Returns a SecurityResponse confirming the operation, or an error.
	PutDistinguishedName(ctx context.Context, name string, body *SecurityDistinguishedName) (*SecurityResponse, error)

	// DeleteDistinguishedName deletes a node distinguished name entry by name
	// from the Security plugin's nodes DN configuration.
	// The name parameter is required.
	// Returns a SecurityResponse confirming the deletion, or an error.
	DeleteDistinguishedName(ctx context.Context, name string) (*SecurityResponse, error)

	// FlushCache flushes the Security plugin's internal caches.
	// Returns a SecurityResponse confirming the cache flush, or an error.
	FlushCache(ctx context.Context) (*SecurityResponse, error)

	// AuthInfo retrieves the authentication information for the currently authenticated user.
	// Returns a SecurityAuthInfoResponse containing user details, roles, tenants, and other auth metadata, or an error.
	AuthInfo(ctx context.Context) (*SecurityAuthInfoResponse, error)

	// GetConfig retrieves the current Security plugin dynamic configuration.
	// Returns a SecurityGetConfigResponse containing the full security configuration, or an error.
	GetConfig(ctx context.Context) (*SecurityGetConfigResponse, error)

	// PutConfig updates the Security plugin's dynamic configuration.
	// The body parameter should contain the security configuration to apply.
	// Returns a SecurityResponse confirming the update, or an error.
	PutConfig(ctx context.Context, body any) (*SecurityResponse, error)

	// GetAudit retrieves the current Security plugin audit logging configuration.
	// Returns a SecurityAudit containing the audit configuration, or an error.
	GetAudit(ctx context.Context) (*SecurityAudit, error)

	// PutAudit updates the Security plugin's audit logging configuration.
	// The body parameter should contain the audit configuration to apply.
	// Returns a SecurityResponse confirming the update, or an error.
	PutAudit(ctx context.Context, body any) (*SecurityResponse, error)

	// Health retrieves the health status of the Security plugin.
	// Returns a SecurityHealthResponse containing the plugin health status, or an error.
	Health(ctx context.Context) (*SecurityHealthResponse, error)

	// WhoAmI retrieves the identity information of the currently authenticated user.
	// Returns a SecurityWhoAmIResponse containing the user's DN and admin status, or an error.
	WhoAmI(ctx context.Context) (*SecurityWhoAmIResponse, error)

	// TenantInfo retrieves the available tenant information from the Security plugin.
	// Returns a map of tenant data, or an error.
	TenantInfo(ctx context.Context) (map[string]any, error)

	// DashboardsInfo retrieves the Dashboards integration information from the Security plugin.
	// Returns a map of Dashboards info data, or an error.
	DashboardsInfo(ctx context.Context) (map[string]any, error)

	// ConfigUpdate triggers a security configuration update across cluster nodes.
	// The configTypes parameter specifies which configuration types to update.
	// Returns a SecurityConfigUpdateResponse with node update results, or an error.
	ConfigUpdate(ctx context.Context, configTypes []string) (*SecurityConfigUpdateResponse, error)

	// SSLInfo retrieves SSL certificate information from the Security plugin.
	// Returns a SecuritySSLInfoResponse containing HTTP and transport SSL info, or an error.
	SSLInfo(ctx context.Context) (*SecuritySSLInfoResponse, error)

	// PermissionsInfo retrieves the current user's permission information from the Security plugin.
	// Returns a SecurityPermissionsInfoResponse with access and endpoint details, or an error.
	PermissionsInfo(ctx context.Context) (*SecurityPermissionsInfoResponse, error)

	// GetAccount retrieves the current user's account information from the Security plugin.
	// Returns a SecurityAccountResponse with user account details, or an error.
	GetAccount(ctx context.Context) (*SecurityAccountResponse, error)

	// PutAccount updates the current user's account in the Security plugin.
	// The body parameter should contain the account fields to update.
	// Returns a SecurityResponse confirming the update, or an error.
	PutAccount(ctx context.Context, body any) (*SecurityResponse, error)

	// AuthToken requests an authentication token from the Security plugin.
	// Returns a SecurityAuthTokenResponse containing the issued token, or an error.
	AuthToken(ctx context.Context) (*SecurityAuthTokenResponse, error)

	// GetAllowlist retrieves the Security plugin REST API allowlist configuration.
	// Returns a SecurityAllowlist with the current allowlist settings, or an error.
	GetAllowlist(ctx context.Context) (*SecurityAllowlist, error)

	// PutAllowlist updates the Security plugin REST API allowlist configuration.
	// The body parameter should contain the allowlist configuration to apply.
	// Returns a SecurityResponse confirming the update, or an error.
	PutAllowlist(ctx context.Context, body any) (*SecurityResponse, error)

	// GetCertificates retrieves the SSL certificates known to the Security plugin.
	// Returns a SecurityCertificatesResponse containing the certificate list, or an error.
	GetCertificates(ctx context.Context) (*SecurityCertificatesResponse, error)

	// GetMultiTenancyConfig retrieves the multi-tenancy configuration from the Security plugin.
	// Returns a SecurityMultiTenancyConfigResponse with tenancy settings, or an error.
	GetMultiTenancyConfig(ctx context.Context) (*SecurityMultiTenancyConfigResponse, error)

	// PutMultiTenancyConfig updates the multi-tenancy configuration in the Security plugin.
	// The body parameter should contain the tenancy configuration to apply.
	// Returns a SecurityResponse confirming the update, or an error.
	PutMultiTenancyConfig(ctx context.Context, body any) (*SecurityResponse, error)

	// GetRateLimiters retrieves the authentication failure rate limiter configurations.
	// Returns a map of rate limiter name to SecurityRateLimiter, or an error.
	GetRateLimiters(ctx context.Context) (map[string]SecurityRateLimiter, error)

	// PutRateLimiter creates or updates an authentication failure rate limiter.
	// The name parameter is required. The body should contain the rate limiter definition.
	// Returns a SecurityResponse confirming the operation, or an error.
	PutRateLimiter(ctx context.Context, name string, body any) (*SecurityResponse, error)

	// DeleteRateLimiter deletes an authentication failure rate limiter by name.
	// The name parameter is required.
	// Returns a SecurityResponse confirming the deletion, or an error.
	DeleteRateLimiter(ctx context.Context, name string) (*SecurityResponse, error)

	// ListRoles retrieves all security roles from the Security plugin.
	// Returns a map keyed by role name containing each SecurityRole definition, or an error.
	ListRoles(ctx context.Context) (map[string]SecurityRole, error)

	// ListUsers retrieves all internal users from the Security plugin.
	// Returns a map keyed by username containing each SecurityUser definition, or an error.
	ListUsers(ctx context.Context) (map[string]SecurityUser, error)

	// ListRoleMappings retrieves all role mappings from the Security plugin.
	// Returns a map keyed by role name containing each SecurityRoleMapping, or an error.
	ListRoleMappings(ctx context.Context) (map[string]SecurityRoleMapping, error)

	// ListActionGroups retrieves all security action groups from the Security plugin.
	// Returns a map keyed by action group name containing each SecurityActionGroup, or an error.
	ListActionGroups(ctx context.Context) (map[string]SecurityActionGroup, error)

	// ListTenants retrieves all security tenants from the Security plugin.
	// Returns a map keyed by tenant name containing each SecurityTenant definition, or an error.
	ListTenants(ctx context.Context) (map[string]SecurityTenant, error)

	// ListNodesDN retrieves all node distinguished name entries from the Security plugin.
	// Returns a map keyed by name containing each SecurityDistinguishedName, or an error.
	ListNodesDN(ctx context.Context) (map[string]SecurityDistinguishedName, error)
}

// DefaultSecurityService is the default implementation of the SecurityService interface.
// It communicates with the OpenSearch Security plugin REST API.
type DefaultSecurityService struct {
	client *resty.Client
	logger *logrus.Entry
}

// NewSecurityService creates a new DefaultSecurityService with the given HTTP client and logger.
func NewSecurityService(client *resty.Client, logger *logrus.Entry) SecurityService {
	return &DefaultSecurityService{
		client: client,
		logger: logger.WithField("service", "security"),
	}
}

// GetRole retrieves a security role by name from the Security plugin.
func (s *DefaultSecurityService) GetRole(ctx context.Context, roleName string) (map[string]SecurityRole, error) {
	if roleName == "" {
		return nil, fmt.Errorf("role name is required")
	}
	resp, err := s.client.R().SetContext(ctx).Get(fmt.Sprintf("/_plugins/_security/api/roles/%s", roleName))
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}
	var result map[string]SecurityRole
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return result, nil
}

// PutRole creates or updates a security role in the Security plugin.
func (s *DefaultSecurityService) PutRole(ctx context.Context, roleName string, body *SecurityPutRole) (*SecurityResponse, error) {
	if roleName == "" {
		return nil, fmt.Errorf("role name is required")
	}
	resp, err := s.client.R().SetContext(ctx).SetBody(body).Put(fmt.Sprintf("/_plugins/_security/api/roles/%s", roleName))
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}
	var result SecurityResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

// DeleteRole deletes a security role from the Security plugin.
func (s *DefaultSecurityService) DeleteRole(ctx context.Context, roleName string) (*SecurityResponse, error) {
	if roleName == "" {
		return nil, fmt.Errorf("role name is required")
	}
	resp, err := s.client.R().SetContext(ctx).Delete(fmt.Sprintf("/_plugins/_security/api/roles/%s", roleName))
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}
	var result SecurityResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

// GetRoleMapping retrieves the role mapping for a given role from the Security plugin.
func (s *DefaultSecurityService) GetRoleMapping(ctx context.Context, roleName string) (map[string]SecurityRoleMapping, error) {
	if roleName == "" {
		return nil, fmt.Errorf("role name is required")
	}
	resp, err := s.client.R().SetContext(ctx).Get(fmt.Sprintf("/_plugins/_security/api/rolesmapping/%s", roleName))
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}
	var result map[string]SecurityRoleMapping
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return result, nil
}

// PutRoleMapping creates or updates a role mapping in the Security plugin.
func (s *DefaultSecurityService) PutRoleMapping(ctx context.Context, roleName string, body *SecurityPutRoleMapping) (*SecurityResponse, error) {
	if roleName == "" {
		return nil, fmt.Errorf("role name is required")
	}
	resp, err := s.client.R().SetContext(ctx).SetBody(body).Put(fmt.Sprintf("/_plugins/_security/api/rolesmapping/%s", roleName))
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}
	var result SecurityResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

// DeleteRoleMapping deletes the role mapping for a given security role.
func (s *DefaultSecurityService) DeleteRoleMapping(ctx context.Context, roleName string) (*SecurityResponse, error) {
	if roleName == "" {
		return nil, fmt.Errorf("role name is required")
	}
	resp, err := s.client.R().SetContext(ctx).Delete(fmt.Sprintf("/_plugins/_security/api/rolesmapping/%s", roleName))
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}
	var result SecurityResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

// GetUser retrieves an internal user from the Security plugin by username.
func (s *DefaultSecurityService) GetUser(ctx context.Context, username string) (map[string]SecurityUser, error) {
	if username == "" {
		return nil, fmt.Errorf("username is required")
	}
	resp, err := s.client.R().SetContext(ctx).Get(fmt.Sprintf("/_plugins/_security/api/internalusers/%s", username))
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}
	var result map[string]SecurityUser
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return result, nil
}

// PutUser creates or updates an internal user in the Security plugin.
func (s *DefaultSecurityService) PutUser(ctx context.Context, username string, body *SecurityPutUser) (*SecurityResponse, error) {
	if username == "" {
		return nil, fmt.Errorf("username is required")
	}
	resp, err := s.client.R().SetContext(ctx).SetBody(body).Put(fmt.Sprintf("/_plugins/_security/api/internalusers/%s", username))
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}
	var result SecurityResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

// DeleteUser deletes an internal user from the Security plugin.
func (s *DefaultSecurityService) DeleteUser(ctx context.Context, username string) (*SecurityResponse, error) {
	if username == "" {
		return nil, fmt.Errorf("username is required")
	}
	resp, err := s.client.R().SetContext(ctx).Delete(fmt.Sprintf("/_plugins/_security/api/internalusers/%s", username))
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}
	var result SecurityResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

// GetActionGroup retrieves a security action group by name.
func (s *DefaultSecurityService) GetActionGroup(ctx context.Context, name string) (map[string]SecurityActionGroup, error) {
	if name == "" {
		return nil, fmt.Errorf("action group name is required")
	}
	resp, err := s.client.R().SetContext(ctx).Get(fmt.Sprintf("/_plugins/_security/api/actiongroups/%s", name))
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}
	var result map[string]SecurityActionGroup
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return result, nil
}

// PutActionGroup creates or updates a security action group.
func (s *DefaultSecurityService) PutActionGroup(ctx context.Context, name string, body *SecurityPutActionGroup) (*SecurityResponse, error) {
	if name == "" {
		return nil, fmt.Errorf("action group name is required")
	}
	resp, err := s.client.R().SetContext(ctx).SetBody(body).Put(fmt.Sprintf("/_plugins/_security/api/actiongroups/%s", name))
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}
	var result SecurityResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

// DeleteActionGroup deletes a security action group by name.
func (s *DefaultSecurityService) DeleteActionGroup(ctx context.Context, name string) (*SecurityResponse, error) {
	if name == "" {
		return nil, fmt.Errorf("action group name is required")
	}
	resp, err := s.client.R().SetContext(ctx).Delete(fmt.Sprintf("/_plugins/_security/api/actiongroups/%s", name))
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}
	var result SecurityResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

// GetTenant retrieves a security tenant by name.
func (s *DefaultSecurityService) GetTenant(ctx context.Context, name string) (map[string]SecurityTenant, error) {
	if name == "" {
		return nil, fmt.Errorf("tenant name is required")
	}
	resp, err := s.client.R().SetContext(ctx).Get(fmt.Sprintf("/_plugins/_security/api/tenants/%s", name))
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}
	var result map[string]SecurityTenant
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return result, nil
}

// PutTenant creates or updates a security tenant.
func (s *DefaultSecurityService) PutTenant(ctx context.Context, name string, body *SecurityPutTenant) (*SecurityResponse, error) {
	if name == "" {
		return nil, fmt.Errorf("tenant name is required")
	}
	resp, err := s.client.R().SetContext(ctx).SetBody(body).Put(fmt.Sprintf("/_plugins/_security/api/tenants/%s", name))
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}
	var result SecurityResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

// DeleteTenant deletes a security tenant by name.
func (s *DefaultSecurityService) DeleteTenant(ctx context.Context, name string) (*SecurityResponse, error) {
	if name == "" {
		return nil, fmt.Errorf("tenant name is required")
	}
	resp, err := s.client.R().SetContext(ctx).Delete(fmt.Sprintf("/_plugins/_security/api/tenants/%s", name))
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}
	var result SecurityResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

// GetDistinguishedName retrieves a node distinguished name entry by name.
func (s *DefaultSecurityService) GetDistinguishedName(ctx context.Context, name string) (map[string]SecurityDistinguishedName, error) {
	if name == "" {
		return nil, fmt.Errorf("distinguished name is required")
	}
	resp, err := s.client.R().SetContext(ctx).Get(fmt.Sprintf("/_plugins/_security/api/nodesdn/%s", name))
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}
	var result map[string]SecurityDistinguishedName
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return result, nil
}

// PutDistinguishedName creates or updates a node distinguished name entry.
func (s *DefaultSecurityService) PutDistinguishedName(ctx context.Context, name string, body *SecurityDistinguishedName) (*SecurityResponse, error) {
	if name == "" {
		return nil, fmt.Errorf("distinguished name is required")
	}
	resp, err := s.client.R().SetContext(ctx).SetBody(body).Put(fmt.Sprintf("/_plugins/_security/api/nodesdn/%s", name))
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}
	var result SecurityResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

// DeleteDistinguishedName deletes a node distinguished name entry by name.
func (s *DefaultSecurityService) DeleteDistinguishedName(ctx context.Context, name string) (*SecurityResponse, error) {
	if name == "" {
		return nil, fmt.Errorf("distinguished name is required")
	}
	resp, err := s.client.R().SetContext(ctx).Delete(fmt.Sprintf("/_plugins/_security/api/nodesdn/%s", name))
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}
	var result SecurityResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

// FlushCache flushes the Security plugin's internal caches.
func (s *DefaultSecurityService) FlushCache(ctx context.Context) (*SecurityResponse, error) {
	resp, err := s.client.R().SetContext(ctx).Delete("/_plugins/_security/api/cache")
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}
	var result SecurityResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

// AuthInfo retrieves the authentication information for the currently authenticated user.
func (s *DefaultSecurityService) AuthInfo(ctx context.Context) (*SecurityAuthInfoResponse, error) {
	resp, err := s.client.R().SetContext(ctx).Get("/_plugins/_security/authinfo")
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}
	var result SecurityAuthInfoResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

// GetConfig retrieves the current Security plugin dynamic configuration.
func (s *DefaultSecurityService) GetConfig(ctx context.Context) (*SecurityGetConfigResponse, error) {
	resp, err := s.client.R().SetContext(ctx).Get("/_plugins/_security/api/securityconfig")
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}
	var result SecurityGetConfigResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

// PutConfig updates the Security plugin's dynamic configuration.
func (s *DefaultSecurityService) PutConfig(ctx context.Context, body any) (*SecurityResponse, error) {
	resp, err := s.client.R().SetContext(ctx).SetBody(body).Put("/_plugins/_security/api/securityconfig/config")
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}
	var result SecurityResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

// GetAudit retrieves the current Security plugin audit logging configuration.
func (s *DefaultSecurityService) GetAudit(ctx context.Context) (*SecurityAudit, error) {
	resp, err := s.client.R().SetContext(ctx).Get("/_plugins/_security/api/audit")
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}
	var result struct {
		Config SecurityAudit `json:"config"`
	}
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result.Config, nil
}

// PutAudit updates the Security plugin's audit logging configuration.
func (s *DefaultSecurityService) PutAudit(ctx context.Context, body any) (*SecurityResponse, error) {
	resp, err := s.client.R().SetContext(ctx).SetBody(body).Put("/_plugins/_security/api/audit/config")
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}
	var result SecurityResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

// Health retrieves the health status of the Security plugin.
func (s *DefaultSecurityService) Health(ctx context.Context) (*SecurityHealthResponse, error) {
	resp, err := s.client.R().SetContext(ctx).Get("/_plugins/_security/health")
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}
	var result SecurityHealthResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

// WhoAmI retrieves the identity information of the currently authenticated user.
func (s *DefaultSecurityService) WhoAmI(ctx context.Context) (*SecurityWhoAmIResponse, error) {
	resp, err := s.client.R().SetContext(ctx).Get("/_plugins/_security/whoami")
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}
	var result SecurityWhoAmIResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

// TenantInfo retrieves the available tenant information from the Security plugin.
func (s *DefaultSecurityService) TenantInfo(ctx context.Context) (map[string]any, error) {
	resp, err := s.client.R().SetContext(ctx).Get("/_plugins/_security/tenantinfo")
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}
	var result map[string]any
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return result, nil
}

// DashboardsInfo retrieves the Dashboards integration information from the Security plugin.
func (s *DefaultSecurityService) DashboardsInfo(ctx context.Context) (map[string]any, error) {
	resp, err := s.client.R().SetContext(ctx).Get("/_plugins/_security/dashboardsinfo")
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}
	var result map[string]any
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return result, nil
}

// ConfigUpdate triggers a security configuration update across cluster nodes.
func (s *DefaultSecurityService) ConfigUpdate(ctx context.Context, configTypes []string) (*SecurityConfigUpdateResponse, error) {
	body := map[string]any{"config_type": configTypes}
	resp, err := s.client.R().SetContext(ctx).SetBody(body).Put("/_plugins/_security/configupdate")
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}
	var result SecurityConfigUpdateResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

// SSLInfo retrieves SSL certificate information from the Security plugin.
func (s *DefaultSecurityService) SSLInfo(ctx context.Context) (*SecuritySSLInfoResponse, error) {
	resp, err := s.client.R().SetContext(ctx).Get("/_opendistro/_security/sslinfo")
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}
	var result SecuritySSLInfoResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

// PermissionsInfo retrieves the current user's permission information from the Security plugin.
func (s *DefaultSecurityService) PermissionsInfo(ctx context.Context) (*SecurityPermissionsInfoResponse, error) {
	resp, err := s.client.R().SetContext(ctx).Get("/_plugins/_security/api/permissionsinfo")
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}
	var result SecurityPermissionsInfoResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

// GetAccount retrieves the current user's account information from the Security plugin.
func (s *DefaultSecurityService) GetAccount(ctx context.Context) (*SecurityAccountResponse, error) {
	resp, err := s.client.R().SetContext(ctx).Get("/_plugins/_security/api/account")
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}
	var result SecurityAccountResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

// PutAccount updates the current user's account in the Security plugin.
func (s *DefaultSecurityService) PutAccount(ctx context.Context, body any) (*SecurityResponse, error) {
	resp, err := s.client.R().SetContext(ctx).SetBody(body).Put("/_plugins/_security/api/account")
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}
	var result SecurityResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

// AuthToken requests an authentication token from the Security plugin.
func (s *DefaultSecurityService) AuthToken(ctx context.Context) (*SecurityAuthTokenResponse, error) {
	resp, err := s.client.R().SetContext(ctx).Post("/_plugins/_security/api/authtoken")
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}
	var result SecurityAuthTokenResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

// GetAllowlist retrieves the Security plugin REST API allowlist configuration.
func (s *DefaultSecurityService) GetAllowlist(ctx context.Context) (*SecurityAllowlist, error) {
	resp, err := s.client.R().SetContext(ctx).Get("/_plugins/_security/api/allowlist")
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}
	var result SecurityAllowlist
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

// PutAllowlist updates the Security plugin REST API allowlist configuration.
func (s *DefaultSecurityService) PutAllowlist(ctx context.Context, body any) (*SecurityResponse, error) {
	resp, err := s.client.R().SetContext(ctx).SetBody(body).Put("/_plugins/_security/api/allowlist")
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}
	var result SecurityResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

// GetCertificates retrieves the SSL certificates known to the Security plugin.
func (s *DefaultSecurityService) GetCertificates(ctx context.Context) (*SecurityCertificatesResponse, error) {
	resp, err := s.client.R().SetContext(ctx).Get("/_plugins/_security/api/certificates")
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}
	var result SecurityCertificatesResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

// GetMultiTenancyConfig retrieves the multi-tenancy configuration from the Security plugin.
func (s *DefaultSecurityService) GetMultiTenancyConfig(ctx context.Context) (*SecurityMultiTenancyConfigResponse, error) {
	resp, err := s.client.R().SetContext(ctx).Get("/_plugins/_security/api/tenancy/config")
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}
	var result SecurityMultiTenancyConfigResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

// PutMultiTenancyConfig updates the multi-tenancy configuration in the Security plugin.
func (s *DefaultSecurityService) PutMultiTenancyConfig(ctx context.Context, body any) (*SecurityResponse, error) {
	resp, err := s.client.R().SetContext(ctx).SetBody(body).Put("/_plugins/_security/api/tenancy/config")
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}
	var result SecurityResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

// GetRateLimiters retrieves the authentication failure rate limiter configurations.
func (s *DefaultSecurityService) GetRateLimiters(ctx context.Context) (map[string]SecurityRateLimiter, error) {
	resp, err := s.client.R().SetContext(ctx).Get("/_plugins/_security/api/authfailurelisteners")
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}
	var result map[string]SecurityRateLimiter
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return result, nil
}

// PutRateLimiter creates or updates an authentication failure rate limiter.
func (s *DefaultSecurityService) PutRateLimiter(ctx context.Context, name string, body any) (*SecurityResponse, error) {
	if name == "" {
		return nil, fmt.Errorf("rate limiter name is required")
	}
	resp, err := s.client.R().SetContext(ctx).SetBody(body).Put(fmt.Sprintf("/_plugins/_security/api/authfailurelisteners/%s", name))
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}
	var result SecurityResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

// DeleteRateLimiter deletes an authentication failure rate limiter by name.
func (s *DefaultSecurityService) DeleteRateLimiter(ctx context.Context, name string) (*SecurityResponse, error) {
	if name == "" {
		return nil, fmt.Errorf("rate limiter name is required")
	}
	resp, err := s.client.R().SetContext(ctx).Delete(fmt.Sprintf("/_plugins/_security/api/authfailurelisteners/%s", name))
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}
	var result SecurityResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

// ListRoles retrieves all security roles from the Security plugin.
func (s *DefaultSecurityService) ListRoles(ctx context.Context) (map[string]SecurityRole, error) {
	resp, err := s.client.R().SetContext(ctx).Get("/_plugins/_security/api/roles")
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}
	var result map[string]SecurityRole
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return result, nil
}

// ListUsers retrieves all internal users from the Security plugin.
func (s *DefaultSecurityService) ListUsers(ctx context.Context) (map[string]SecurityUser, error) {
	resp, err := s.client.R().SetContext(ctx).Get("/_plugins/_security/api/internalusers")
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}
	var result map[string]SecurityUser
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return result, nil
}

// ListRoleMappings retrieves all role mappings from the Security plugin.
func (s *DefaultSecurityService) ListRoleMappings(ctx context.Context) (map[string]SecurityRoleMapping, error) {
	resp, err := s.client.R().SetContext(ctx).Get("/_plugins/_security/api/rolesmapping")
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}
	var result map[string]SecurityRoleMapping
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return result, nil
}

// ListActionGroups retrieves all security action groups from the Security plugin.
func (s *DefaultSecurityService) ListActionGroups(ctx context.Context) (map[string]SecurityActionGroup, error) {
	resp, err := s.client.R().SetContext(ctx).Get("/_plugins/_security/api/actiongroups")
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}
	var result map[string]SecurityActionGroup
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return result, nil
}

// ListTenants retrieves all security tenants from the Security plugin.
func (s *DefaultSecurityService) ListTenants(ctx context.Context) (map[string]SecurityTenant, error) {
	resp, err := s.client.R().SetContext(ctx).Get("/_plugins/_security/api/tenants")
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}
	var result map[string]SecurityTenant
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return result, nil
}

// ListNodesDN retrieves all node distinguished name entries from the Security plugin.
func (s *DefaultSecurityService) ListNodesDN(ctx context.Context) (map[string]SecurityDistinguishedName, error) {
	resp, err := s.client.R().SetContext(ctx).Get("/_plugins/_security/api/nodesdn")
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}
	var result map[string]SecurityDistinguishedName
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return result, nil
}
