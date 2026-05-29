package api

// SecurityRole represents a security role returned from the OpenSearch Security plugin,
// including its permissions and metadata flags (reserved, hidden, static).
type SecurityRole struct {
	SecurityPutRole `json:",inline"`
	Reserved        *bool `json:"reserved,omitempty"`
	Hidden          *bool `json:"hidden,omitempty"`
	Static          *bool `json:"static,omitempty"`
}

// SecurityPutRole represents the request body for creating or updating a security role.
// It defines cluster-level permissions, index-level permissions, and tenant permissions.
type SecurityPutRole struct {
	Description        *string                     `json:"description,omitempty"`
	ClusterPermissions []string                    `json:"cluster_permissions,omitempty"`
	IndexPermissions   []SecurityIndexPermissions  `json:"index_permissions,omitempty"`
	TenantPermissions  []SecurityTenantPermissions `json:"tenant_permissions,omitempty"`
}

// SecurityIndexPermissions defines the index-level permissions within a security role,
// including index patterns, allowed actions, document-level security, field-level security, and masked fields.
type SecurityIndexPermissions struct {
	IndexPatterns         []string `json:"index_patterns"`
	MaskedFields          []string `json:"masked_fields,omitempty"`
	AllowedActions        []string `json:"allowed_actions"`
	DocumentLevelSecurity *string  `json:"dls,omitempty"`
	FieldLevelSecurity    []string `json:"fls,omitempty"`
}

// SecurityTenantPermissions defines the tenant-level permissions within a security role,
// specifying which tenants are accessible and what actions are allowed.
type SecurityTenantPermissions struct {
	TenantPatterns []string `json:"tenant_patterns"`
	AllowedActions []string `json:"allowed_actions"`
}

// SecurityRoleMapping represents a security role mapping returned from the OpenSearch Security plugin,
// linking backend roles, hosts, and users to a security role.
type SecurityRoleMapping struct {
	SecurityPutRoleMapping `json:",inline"`
	Reserved               *bool `json:"reserved,omitempty"`
	Hidden                 *bool `json:"hidden,omitempty"`
}

// SecurityPutRoleMapping represents the request body for creating or updating a role mapping.
// It maps backend roles, hosts, and users to a security role.
type SecurityPutRoleMapping struct {
	BackendRoles    []string `json:"backend_roles,omitempty"`
	AndBackendRoles []string `json:"and_backend_roles,omitempty"`
	Hosts           []string `json:"hosts,omitempty"`
	Users           []string `json:"users,omitempty"`
}

// SecurityUser represents an internal user returned from the OpenSearch Security plugin,
// including user attributes, backend roles, and metadata flags.
type SecurityUser struct {
	SecurityUserBase `json:",inline"`
	Reserved         *bool `json:"reserved,omitempty"`
	Hidden           *bool `json:"hidden,omitempty"`
	Static           *bool `json:"static,omitempty"`
}

// SecurityUserBase contains the common fields for a security user,
// including password hash, backend roles, security roles, and custom attributes.
type SecurityUserBase struct {
	Hash          string            `json:"hash,omitempty"`
	BackendRoles  []string          `json:"backend_roles,omitempty"`
	SecurityRoles []string          `json:"opendistro_security_roles,omitempty"`
	Attributes    map[string]string `json:"attributes,omitempty"`
	Description   *string           `json:"description,omitempty"`
	Service       *bool             `json:"service,omitempty"`
}

// SecurityPutUser represents the request body for creating or updating an internal user.
// It extends SecurityUserBase with an optional plaintext password field.
type SecurityPutUser struct {
	SecurityUserBase `json:",inline"`
	Password         *string `json:"password,omitempty"`
}

// SecurityActionGroup represents a security action group returned from the OpenSearch Security plugin.
// Action groups are named collections of allowed actions used in role definitions.
type SecurityActionGroup struct {
	SecurityPutActionGroup `json:",inline"`
	Reserved               *bool `json:"reserved,omitempty"`
	Hidden                 *bool `json:"hidden,omitempty"`
	Static                 *bool `json:"static,omitempty"`
}

// SecurityPutActionGroup represents the request body for creating or updating a security action group.
// It defines the group type, description, and the list of allowed actions.
type SecurityPutActionGroup struct {
	Description    *string  `json:"description,omitempty"`
	Type           *string  `json:"type,omitempty"`
	AllowedActions []string `json:"allowed_actions"`
}

// SecurityTenant represents a security tenant returned from the OpenSearch Security plugin.
// Tenants provide isolated spaces for Dashboards users.
type SecurityTenant struct {
	SecurityPutTenant `json:",inline"`
	Reserved          *bool `json:"reserved,omitempty"`
	Hidden            *bool `json:"hidden,omitempty"`
	Static            *bool `json:"static,omitempty"`
}

// SecurityPutTenant represents the request body for creating or updating a security tenant.
type SecurityPutTenant struct {
	Description *string `json:"description"`
}

// SecurityDistinguishedName represents a node distinguished name (DN) entry
// used for node-to-node authentication in the Security plugin.
// NodesDN contains the list of distinguished names for trusted nodes.
type SecurityDistinguishedName struct {
	NodesDN []string `json:"nodes_dn"`
}

// SecurityResponse represents a generic response from the Security plugin API,
// containing a status string and a human-readable message.
type SecurityResponse struct {
	Status  string `json:"status"`
	Message string `json:"message"`
}

// SecurityAuthInfoResponse represents the authentication information returned
// from the Security plugin's /_plugins/_security/authinfo endpoint.
// It includes the authenticated user's name, roles, tenants, peer certificates, and other metadata.
type SecurityAuthInfoResponse struct {
	User                   string          `json:"user"`
	UserName               string          `json:"user_name"`
	BackendRoles           []string        `json:"backend_roles"`
	CustomAttributeNames   []string        `json:"custom_attribute_names,omitempty"`
	Roles                  []string        `json:"roles"`
	Tenants                map[string]bool `json:"tenants"`
	Principal              *string         `json:"principal"`
	PeerCertificates       string          `json:"peer_certificates"`
	SSOLogoutURL           *string         `json:"sso_logout_url"`
	RemoteAddress          string          `json:"remote_address"`
	SizeOfUser             string          `json:"size_of_user,omitempty"`
	SizeOfBackendRoles     string          `json:"size_of_backendroles,omitempty"`
	SizeOfCustomAttributes string          `json:"size_of_custom_attributes,omitempty"`
	UserRequestedTenant    *string         `json:"user_requested_tenant,omitempty"`
}

// SecurityConfig represents the top-level dynamic configuration of the OpenSearch Security plugin.
// The Dynamic field contains all runtime-configurable settings.
type SecurityConfig struct {
	Dynamic SecurityConfigDynamic `json:"dynamic"`
}

// SecurityConfigDynamic contains the dynamic (runtime-configurable) settings
// for the OpenSearch Security plugin, including authentication backends, authorization,
// HTTPS configuration, and various security behavior toggles.
type SecurityConfigDynamic struct {
	FilteredAliasMode            *string                                       `json:"filtered_alias_mode,omitempty"`
	DisableRestAuth              *bool                                         `json:"disable_rest_auth,omitempty"`
	DisableIntertransportAuth    *bool                                         `json:"disable_intertransport_auth,omitempty"`
	RespectRequestIndicesOptions *bool                                         `json:"respect_request_indices_options,omitempty"`
	License                      *string                                       `json:"license,omitempty"`
	Kibana                       *SecurityConfigKibana                         `json:"kibana,omitempty"`
	Http                         *SecurityConfigHttp                           `json:"http,omitempty"`
	Authc                        map[string]SecurityConfigAuthc                `json:"authc,omitempty"`
	Authz                        map[string]SecurityConfigAuthz                `json:"authz,omitempty"`
	AuthFailureListeners         map[string]SecurityConfigAuthFailureListeners `json:"auth_failure_listeners,omitempty"`
	DoNotFailOnForbidden         *bool                                         `json:"do_not_fail_on_forbidden,omitempty"`
	MultiRolespanEnabled         *bool                                         `json:"multi_rolespan_enabled,omitempty"`
	HostsResolverMode            *string                                       `json:"hosts_resolver_mode,omitempty"`
	TransportUserrnameAttribute  *string                                       `json:"transport_userrname_attribute,omitempty"`
	DoNotFailOnForbiddenEmpty    *bool                                         `json:"do_not_fail_on_forbidden_empty,omitempty"`
	OnBehalfOfSettings           *SecurityConfigOnBehalfOfSettings             `json:"on_behalf_of,omitempty"`
}

// SecurityConfigKibana contains the Dashboards (Kibana) integration settings
// for the Security plugin, including multitenancy and server credentials.
type SecurityConfigKibana struct {
	MultitenancyEnabled  *bool   `json:"multitenancy_enabled,omitempty"`
	ServerUsername       *string `json:"server_username,omitempty"`
	ServerPassword       *string `json:"server_password,omitempty"`
	Index                *string `json:"index,omitempty"`
	DoNotFailOnForbidden *bool   `json:"doNotFailOnForbidden,omitempty"`
}

// SecurityConfigHttp contains HTTP-layer security settings for the Security plugin,
// including anonymous authentication and X-Forwarded-For configuration.
type SecurityConfigHttp struct {
	AnonymousAuthEnabled *bool                          `json:"anonymous_auth_enabled,omitempty"`
	Xff                  *SecurityConfigXff             `json:"xff,omitempty"`
	Authc                map[string]SecurityConfigAuthc `json:"authc,omitempty"`
}

// SecurityConfigXff configures X-Forwarded-For header handling for the Security plugin,
// controlling how client IP addresses are resolved behind proxies.
type SecurityConfigXff struct {
	Enabled         *bool   `json:"enabled,omitempty"`
	InternalProxies *string `json:"internalProxies,omitempty"`
	RemoteIpHeader  *string `json:"remoteIpHeader,omitempty"`
}

// SecurityConfigAuthc defines an authentication domain in the Security plugin,
// specifying the HTTP authenticator and authentication backend to use.
type SecurityConfigAuthc struct {
	Description           *string                              `json:"description,omitempty"`
	HttpEnabled           *bool                                `json:"http_enabled,omitempty"`
	TransportEnabled      *bool                                `json:"transport_enabled,omitempty"`
	Order                 *int                                 `json:"order,omitempty"`
	HttpAuthenticator     *SecurityConfigHttpAuthenticator     `json:"http_authenticator,omitempty"`
	AuthenticationBackend *SecurityConfigAuthenticationBackend `json:"authentication_backend,omitempty"`
}

// SecurityConfigHttpAuthenticator specifies the HTTP authenticator type and its configuration
// for a Security plugin authentication domain.
type SecurityConfigHttpAuthenticator struct {
	Type      string         `json:"type"`
	Challenge *bool          `json:"challenge,omitempty"`
	Config    map[string]any `json:"config,omitempty"`
}

// SecurityConfigAuthenticationBackend specifies the authentication backend type and its configuration
// for a Security plugin authentication domain.
type SecurityConfigAuthenticationBackend struct {
	Type   string         `json:"type"`
	Config map[string]any `json:"config,omitempty"`
}

// SecurityConfigAuthz defines an authorization domain in the Security plugin,
// specifying the authorization backend to use for resolving roles.
type SecurityConfigAuthz struct {
	Description          *string                             `json:"description,omitempty"`
	HttpEnabled          *bool                               `json:"http_enabled,omitempty"`
	AuthorizationBackend *SecurityConfigAuthorizationBackend `json:"authorization_backend,omitempty"`
}

// SecurityConfigAuthorizationBackend specifies the authorization backend type and its configuration
// for a Security plugin authorization domain.
type SecurityConfigAuthorizationBackend struct {
	Type   string         `json:"type"`
	Config map[string]any `json:"config,omitempty"`
}

// SecurityConfigAuthFailureListeners defines the configuration for authentication failure
// listeners in the Security plugin, which trigger actions on repeated auth failures.
type SecurityConfigAuthFailureListeners struct {
	Type           *string `json:"type,omitempty"`
	WindowDuration *string `json:"window_duration,omitempty"`
	MaxCount       *int    `json:"max_count,omitempty"`
	BlockDuration  *string `json:"block_duration,omitempty"`
}

// SecurityConfigOnBehalfOfSettings defines the "on behalf of" token settings
// for the Security plugin, allowing users to act on behalf of other users.
type SecurityConfigOnBehalfOfSettings struct {
	Enabled       *bool   `json:"enabled,omitempty"`
	SigningKey    *string `json:"signing_key,omitempty"`
	EncryptionKey *string `json:"encryption_key,omitempty"`
}

// SecurityGetConfigResponse wraps the Security plugin configuration returned from
// the GET /_plugins/_security/api/securityconfig endpoint.
type SecurityGetConfigResponse struct {
	Config SecurityConfig `json:"config"`
}

// SecurityAudit represents the audit logging configuration for the OpenSearch Security plugin,
// including the overall enabled flag, compliance settings, and audit event specifications.
type SecurityAudit struct {
	Enabled    *bool                   `json:"enabled,omitempty"`
	Compliance SecurityAuditCompliance `json:"compliance"`
	Audit      SecurityAuditSpec       `json:"audit"`
}

// SecurityAuditSpec defines the audit event logging settings, including which users and
// requests to ignore, which categories to disable, and various logging behavior options.
type SecurityAuditSpec struct {
	IgnoreUsers                 []string `json:"ignore_users,omitempty"`
	IgnoreRequests              []string `json:"ignore_requests,omitempty"`
	DisabledRestCategories      []string `json:"disabled_rest_categories,omitempty"`
	DisabledTransportCategories []string `json:"disabled_transport_categories,omitempty"`
	LogRequestBody              *bool    `json:"log_request_body,omitempty"`
	ResolveIndices              *bool    `json:"resolve_indices,omitempty"`
	ResolveBulkRequests         *bool    `json:"resolve_bulk_requests,omitempty"`
	ExcludeSensitiveHeaders     *bool    `json:"exclude_sensitive_headers,omitempty"`
	EnableTransport             *bool    `json:"enable_transport,omitempty"`
	EnableRest                  *bool    `json:"enable_rest,omitempty"`
}

// SecurityHealthResponse represents the health status of the Security plugin.
type SecurityHealthResponse struct {
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
}

// SecurityWhoAmIResponse represents the identity information of the currently authenticated user.
type SecurityWhoAmIResponse struct {
	Dn                       string `json:"dn"`
	IsAdmin                  bool   `json:"is_admin"`
	IsNodeCertificateRequest bool   `json:"is_node_certificate_request"`
}

// SecurityConfigUpdateResponse represents the result of a security configuration update.
type SecurityConfigUpdateResponse struct {
	Status      *string `json:"status,omitempty"`
	NodeSize    int     `json:"nodes_size,omitempty"`
	UpdatedNode int     `json:"updated_node,omitempty"`
}

// SecurityAuthTokenResponse represents an authentication token issued by the Security plugin.
type SecurityAuthTokenResponse struct {
	Authorization string `json:"authorization"`
}

// SecurityCertificatesResponse represents a collection of SSL certificates.
type SecurityCertificatesResponse struct {
	Certificates []SecurityCertificate `json:"certificates,omitempty"`
}

// SecurityCertificate represents a single SSL certificate entry.
type SecurityCertificate struct {
	IssuerDN     string `json:"issuer_dn,omitempty"`
	SubjectDN    string `json:"subject_dn,omitempty"`
	San          string `json:"san,omitempty"`
	NotBefore    string `json:"not_before,omitempty"`
	NotAfter     string `json:"not_after,omitempty"`
	SerialNumber string `json:"serial_number,omitempty"`
}

// SecurityMultiTenancyConfigResponse represents the multi-tenancy configuration.
type SecurityMultiTenancyConfigResponse struct {
	Enabled        *bool    `json:"enabled,omitempty"`
	DefaultTenant  *string  `json:"default_tenant,omitempty"`
	PrivateEnabled *bool    `json:"private_tenant_enabled,omitempty"`
	AdminUsernames []string `json:"admin_usernames,omitempty"`
}

// SecurityRateLimiter represents an authentication failure rate limiter configuration.
type SecurityRateLimiter struct {
	Type           string `json:"type,omitempty"`
	WindowDuration string `json:"window_duration,omitempty"`
	MaxCount       *int   `json:"max_count,omitempty"`
	BlockDuration  string `json:"block_duration,omitempty"`
}

// SecurityAllowlist represents the Security plugin REST API allowlist configuration.
type SecurityAllowlist struct {
	Enabled  *bool               `json:"enabled,omitempty"`
	Requests map[string][]string `json:"requests,omitempty"`
}

// SecurityPermissionsInfoResponse represents the current user's permission information.
type SecurityPermissionsInfoResponse struct {
	HasAccess           bool     `json:"has_access"`
	DisabledEndpoints   []string `json:"disabled_endpoints,omitempty"`
	DisabledEndpointsV2 []string `json:"disabledEndpoints,omitempty"`
	EnabledEndpoints    []string `json:"enabled_endpoints,omitempty"`
}

// SecuritySSLInfoResponse represents SSL certificate information from the Security plugin.
type SecuritySSLInfoResponse struct {
	HttpSslCertificatesInfo      *SecuritySSLInfoDetail `json:"http_sslinfo,omitempty"`
	TransportSslCertificatesInfo *SecuritySSLInfoDetail `json:"transport_sslinfo,omitempty"`
}

// SecuritySSLInfoDetail contains details about SSL certificates for either HTTP or transport layers.
type SecuritySSLInfoDetail struct {
	Principal string           `json:"principal,omitempty"`
	CertList  []map[string]any `json:"certs_list,omitempty"`
}

// SecurityAccountResponse represents the current user's account information.
type SecurityAccountResponse struct {
	UserName         string            `json:"user_name"`
	IsReserved       bool              `json:"is_reserved"`
	IsHidden         bool              `json:"is_hidden"`
	IsSystemUser     bool              `json:"is_system_user,omitempty"`
	BackendRoles     []string          `json:"backend_roles,omitempty"`
	Roles            []string          `json:"opendistro_security_roles,omitempty"`
	CustomAttributes map[string]string `json:"custom_attribute_names,omitempty"`
}

// SecurityAuditCompliance defines the compliance auditing settings for the Security plugin,
// including read/write access logging, metadata options, and user ignore lists.
type SecurityAuditCompliance struct {
	Enabled             *bool               `json:"enabled,omitempty"`
	WriteLogDiffs       *bool               `json:"write_log_diffs,omitempty"`
	ReadWatchedFields   map[string][]string `json:"read_watched_fields,omitempty"`
	ReadIgnoreUsers     []string            `json:"read_ignore_users,omitempty"`
	WriteWatchedIndices []string            `json:"write_watched_indices,omitempty"`
	WriteIgnoreUsers    []string            `json:"write_ignore_users,omitempty"`
	ReadMetadataOnly    *bool               `json:"read_metadata_only,omitempty"`
	WriteMetadataOnly   *bool               `json:"write_metadata_only,omitempty"`
	ExternalConfig      *bool               `json:"external_config,omitempty"`
	InternalConfig      *bool               `json:"internal_config,omitempty"`
}
