package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newSecurityTestServer() *httptest.Server {
	mux := http.NewServeMux()

	securityResponse := `{"status":"CREATED","message":"ok"}`

	mux.HandleFunc("/_plugins/_security/api/roles/test_role", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodGet:
			_, _ = w.Write([]byte(`{"test_role":{"cluster_permissions":["*"]}}`))
		case http.MethodPut:
			_, _ = w.Write([]byte(securityResponse))
		case http.MethodDelete:
			_, _ = w.Write([]byte(securityResponse))
		}
	})

	mux.HandleFunc("/_plugins/_security/api/roles", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"role1":{"cluster_permissions":["read"]},"role2":{"cluster_permissions":["write"]}}`))
	})

	mux.HandleFunc("/_plugins/_security/api/rolesmapping/test_role", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodGet:
			_, _ = w.Write([]byte(`{"test_role":{"backend_roles":["admin"]}}`))
		case http.MethodPut:
			_, _ = w.Write([]byte(securityResponse))
		case http.MethodDelete:
			_, _ = w.Write([]byte(securityResponse))
		}
	})

	mux.HandleFunc("/_plugins/_security/api/rolesmapping", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"role1":{"backend_roles":["admin"]}}`))
	})

	mux.HandleFunc("/_plugins/_security/api/internalusers/test_user", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodGet:
			_, _ = w.Write([]byte(`{"test_user":{"hash":"$2a$12$abc"}}`))
		case http.MethodPut:
			_, _ = w.Write([]byte(securityResponse))
		case http.MethodDelete:
			_, _ = w.Write([]byte(securityResponse))
		}
	})

	mux.HandleFunc("/_plugins/_security/api/internalusers", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"admin":{"hash":"$2a$12$abc"},"user1":{"hash":"$2a$12$def"}}`))
	})

	mux.HandleFunc("/_plugins/_security/api/actiongroups/test_ag", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodGet:
			_, _ = w.Write([]byte(`{"test_ag":{"allowed_actions":["indices:data/read/search"]}}`))
		case http.MethodPut:
			_, _ = w.Write([]byte(securityResponse))
		case http.MethodDelete:
			_, _ = w.Write([]byte(securityResponse))
		}
	})

	mux.HandleFunc("/_plugins/_security/api/actiongroups", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"read":{"allowed_actions":["indices:data/read/search"]}}`))
	})

	mux.HandleFunc("/_plugins/_security/api/tenants/test_tenant", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodGet:
			_, _ = w.Write([]byte(`{"test_tenant":{"description":"test"}}`))
		case http.MethodPut:
			_, _ = w.Write([]byte(securityResponse))
		case http.MethodDelete:
			_, _ = w.Write([]byte(securityResponse))
		}
	})

	mux.HandleFunc("/_plugins/_security/api/tenants", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"global_tenant":{"description":"Global tenant"}}`))
	})

	mux.HandleFunc("/_plugins/_security/api/nodesdn/test_dn", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodGet:
			_, _ = w.Write([]byte(`{"test_dn":{"nodes_dn":["CN=node1"]}}`))
		case http.MethodPut:
			_, _ = w.Write([]byte(securityResponse))
		case http.MethodDelete:
			_, _ = w.Write([]byte(securityResponse))
		}
	})

	mux.HandleFunc("/_plugins/_security/api/nodesdn", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"node1":{"nodes_dn":["CN=node1"]}}`))
	})

	mux.HandleFunc("/_plugins/_security/api/cache", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"OK","message":"Cache flushed"}`))
	})

	mux.HandleFunc("/_plugins/_security/authinfo", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"user":"admin","user_name":"admin","backend_roles":[],"roles":["all_access"],"tenants":{"global_tenant":true},"peer_certificates":"0","remote_address":"127.0.0.1"}`))
	})

	mux.HandleFunc("/_plugins/_security/api/securityconfig", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"config":{"dynamic":{}}}`))
	})

	mux.HandleFunc("/_plugins/_security/api/securityconfig/config", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(securityResponse))
	})

	mux.HandleFunc("/_plugins/_security/api/audit", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"config":{"enabled":true,"compliance":{},"audit":{}}}`))
	})

	mux.HandleFunc("/_plugins/_security/api/audit/config", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(securityResponse))
	})

	mux.HandleFunc("/_plugins/_security/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"UP","message":"Security plugin is healthy"}`))
	})

	mux.HandleFunc("/_plugins/_security/whoami", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"dn":"CN=admin,OU=ops,O=example,C=US","is_admin":true,"is_node_certificate_request":false}`))
	})

	mux.HandleFunc("/_plugins/_security/tenantinfo", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"global_tenant":{"description":"Global"}}`))
	})

	mux.HandleFunc("/_plugins/_security/dashboardsinfo", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"multitenancy_enabled":true}`))
	})

	mux.HandleFunc("/_plugins/_security/configupdate", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"OK","nodes_size":1,"updated_node":1}`))
	})

	mux.HandleFunc("/_opendistro/_security/sslinfo", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"http_sslinfo":{"principal":"CN=admin"},"transport_sslinfo":{"principal":"CN=node1"}}`))
	})

	mux.HandleFunc("/_plugins/_security/api/permissionsinfo", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"has_access":true,"disabled_endpoints":[],"enabled_endpoints":["api/account"]}`))
	})

	mux.HandleFunc("/_plugins/_security/api/account", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodGet:
			_, _ = w.Write([]byte(`{"user_name":"admin","is_reserved":false,"is_hidden":false,"is_system_user":false,"backend_roles":["admin"],"opendistro_security_roles":["all_access"]}`))
		case http.MethodPut:
			_, _ = w.Write([]byte(securityResponse))
		}
	})

	mux.HandleFunc("/_plugins/_security/api/authtoken", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"authorization":"Bearer abc123token"}`))
	})

	mux.HandleFunc("/_plugins/_security/api/allowlist", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodGet:
			_, _ = w.Write([]byte(`{"enabled":true,"requests":{"/_plugins/_security/api/account":["GET"]}}`))
		case http.MethodPut:
			_, _ = w.Write([]byte(securityResponse))
		}
	})

	mux.HandleFunc("/_plugins/_security/api/certificates", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"certificates":[{"issuer_dn":"CN=root","subject_dn":"CN=admin","serial_number":"1234"}]}`))
	})

	mux.HandleFunc("/_plugins/_security/api/tenancy/config", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodGet:
			_, _ = w.Write([]byte(`{"enabled":true,"default_tenant":"global_tenant","private_tenant_enabled":true,"admin_usernames":["admin"]}`))
		case http.MethodPut:
			_, _ = w.Write([]byte(securityResponse))
		}
	})

	mux.HandleFunc("/_plugins/_security/api/authfailurelisteners", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"IpRateLimiter":{"type":"ip","window_duration":"1m","max_count":10,"block_duration":"10m"}}`))
	})

	mux.HandleFunc("/_plugins/_security/api/authfailurelisteners/test_limiter", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodPut:
			_, _ = w.Write([]byte(securityResponse))
		case http.MethodDelete:
			_, _ = w.Write([]byte(securityResponse))
		}
	})

	return httptest.NewServer(mux)
}

func TestUnitSecurityService_GetRole(t *testing.T) {
	srv := newSecurityTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	result, err := svc.GetRole(ctx, "test_role")
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Contains(t, result, "test_role")
	assert.Equal(t, []string{"*"}, result["test_role"].ClusterPermissions)
}

func TestUnitSecurityService_PutRole(t *testing.T) {
	srv := newSecurityTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	resp, err := svc.PutRole(ctx, "test_role", &SecurityPutRole{
		ClusterPermissions: []string{"*"},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "CREATED", resp.Status)
}

func TestUnitSecurityService_DeleteRole(t *testing.T) {
	srv := newSecurityTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	resp, err := svc.DeleteRole(ctx, "test_role")
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "CREATED", resp.Status)
}

func TestUnitSecurityService_GetRoleMapping(t *testing.T) {
	srv := newSecurityTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	result, err := svc.GetRoleMapping(ctx, "test_role")
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Contains(t, result, "test_role")
}

func TestUnitSecurityService_PutRoleMapping(t *testing.T) {
	srv := newSecurityTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	resp, err := svc.PutRoleMapping(ctx, "test_role", &SecurityPutRoleMapping{
		BackendRoles: []string{"admin"},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "CREATED", resp.Status)
}

func TestUnitSecurityService_DeleteRoleMapping(t *testing.T) {
	srv := newSecurityTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	resp, err := svc.DeleteRoleMapping(ctx, "test_role")
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "CREATED", resp.Status)
}

func TestUnitSecurityService_GetUser(t *testing.T) {
	srv := newSecurityTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	result, err := svc.GetUser(ctx, "test_user")
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Contains(t, result, "test_user")
}

func TestUnitSecurityService_PutUser(t *testing.T) {
	srv := newSecurityTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	pw := "secret"
	resp, err := svc.PutUser(ctx, "test_user", &SecurityPutUser{
		SecurityUserBase: SecurityUserBase{
			BackendRoles: []string{"admin"},
		},
		Password: &pw,
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "CREATED", resp.Status)
}

func TestUnitSecurityService_DeleteUser(t *testing.T) {
	srv := newSecurityTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	resp, err := svc.DeleteUser(ctx, "test_user")
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "CREATED", resp.Status)
}

func TestUnitSecurityService_GetActionGroup(t *testing.T) {
	srv := newSecurityTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	result, err := svc.GetActionGroup(ctx, "test_ag")
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Contains(t, result, "test_ag")
}

func TestUnitSecurityService_PutActionGroup(t *testing.T) {
	srv := newSecurityTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	resp, err := svc.PutActionGroup(ctx, "test_ag", &SecurityPutActionGroup{
		AllowedActions: []string{"indices:data/read/search"},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "CREATED", resp.Status)
}

func TestUnitSecurityService_DeleteActionGroup(t *testing.T) {
	srv := newSecurityTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	resp, err := svc.DeleteActionGroup(ctx, "test_ag")
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "CREATED", resp.Status)
}

func TestUnitSecurityService_GetTenant(t *testing.T) {
	srv := newSecurityTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	result, err := svc.GetTenant(ctx, "test_tenant")
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Contains(t, result, "test_tenant")
}

func TestUnitSecurityService_PutTenant(t *testing.T) {
	srv := newSecurityTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	desc := "test tenant"
	resp, err := svc.PutTenant(ctx, "test_tenant", &SecurityPutTenant{
		Description: &desc,
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "CREATED", resp.Status)
}

func TestUnitSecurityService_DeleteTenant(t *testing.T) {
	srv := newSecurityTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	resp, err := svc.DeleteTenant(ctx, "test_tenant")
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "CREATED", resp.Status)
}

func TestUnitSecurityService_GetDistinguishedName(t *testing.T) {
	srv := newSecurityTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	result, err := svc.GetDistinguishedName(ctx, "test_dn")
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Contains(t, result, "test_dn")
	assert.Equal(t, []string{"CN=node1"}, result["test_dn"].NodesDN)
}

func TestUnitSecurityService_PutDistinguishedName(t *testing.T) {
	srv := newSecurityTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	resp, err := svc.PutDistinguishedName(ctx, "test_dn", &SecurityDistinguishedName{
		NodesDN: []string{"CN=node1"},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "CREATED", resp.Status)
}

func TestUnitSecurityService_DeleteDistinguishedName(t *testing.T) {
	srv := newSecurityTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	resp, err := svc.DeleteDistinguishedName(ctx, "test_dn")
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "CREATED", resp.Status)
}

func TestUnitSecurityService_FlushCache(t *testing.T) {
	srv := newSecurityTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	resp, err := svc.FlushCache(ctx)
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "OK", resp.Status)
}

func TestUnitSecurityService_AuthInfo(t *testing.T) {
	srv := newSecurityTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	resp, err := svc.AuthInfo(ctx)
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "admin", resp.UserName)
	assert.Contains(t, resp.Roles, "all_access")
}

func TestUnitSecurityService_GetConfig(t *testing.T) {
	srv := newSecurityTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	resp, err := svc.GetConfig(ctx)
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.NotNil(t, resp.Config)
}

func TestUnitSecurityService_PutConfig(t *testing.T) {
	srv := newSecurityTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	resp, err := svc.PutConfig(ctx, map[string]any{"dynamic": map[string]any{}})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "CREATED", resp.Status)
}

func TestUnitSecurityService_GetAudit(t *testing.T) {
	srv := newSecurityTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	resp, err := svc.GetAudit(ctx)
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.NotNil(t, resp.Enabled)
	assert.True(t, *resp.Enabled)
}

func TestUnitSecurityService_PutAudit(t *testing.T) {
	srv := newSecurityTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	resp, err := svc.PutAudit(ctx, map[string]any{"enabled": true})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "CREATED", resp.Status)
}

func TestUnitSecurityService_EmptyRoleNameErrors(t *testing.T) {
	srv := newSecurityTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	_, err := svc.GetRole(ctx, "")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "role name is required")

	_, err = svc.PutRole(ctx, "", nil)
	assert.Error(t, err)

	_, err = svc.DeleteRole(ctx, "")
	assert.Error(t, err)

	_, err = svc.GetRoleMapping(ctx, "")
	assert.Error(t, err)

	_, err = svc.PutRoleMapping(ctx, "", nil)
	assert.Error(t, err)

	_, err = svc.DeleteRoleMapping(ctx, "")
	assert.Error(t, err)
}

func TestUnitSecurityService_EmptyUsernameErrors(t *testing.T) {
	srv := newSecurityTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	_, err := svc.GetUser(ctx, "")
	assert.Error(t, err)

	_, err = svc.PutUser(ctx, "", nil)
	assert.Error(t, err)

	_, err = svc.DeleteUser(ctx, "")
	assert.Error(t, err)
}

func TestUnitSecurityService_EmptyActionGroupNameErrors(t *testing.T) {
	srv := newSecurityTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	_, err := svc.GetActionGroup(ctx, "")
	assert.Error(t, err)

	_, err = svc.PutActionGroup(ctx, "", nil)
	assert.Error(t, err)

	_, err = svc.DeleteActionGroup(ctx, "")
	assert.Error(t, err)
}

func TestUnitSecurityService_EmptyTenantNameErrors(t *testing.T) {
	srv := newSecurityTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	_, err := svc.GetTenant(ctx, "")
	assert.Error(t, err)

	_, err = svc.PutTenant(ctx, "", nil)
	assert.Error(t, err)

	_, err = svc.DeleteTenant(ctx, "")
	assert.Error(t, err)
}

func TestUnitSecurityService_EmptyDNNameErrors(t *testing.T) {
	srv := newSecurityTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	_, err := svc.GetDistinguishedName(ctx, "")
	assert.Error(t, err)

	_, err = svc.PutDistinguishedName(ctx, "", nil)
	assert.Error(t, err)

	_, err = svc.DeleteDistinguishedName(ctx, "")
	assert.Error(t, err)
}

func TestUnitSecurityService_Health(t *testing.T) {
	srv := newSecurityTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	resp, err := svc.Health(ctx)
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "UP", resp.Status)
	assert.Equal(t, "Security plugin is healthy", resp.Message)
}

func TestUnitSecurityService_Health_ServerError(t *testing.T) {
	srv := errServer(500)
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	_, err := svc.Health(ctx)
	require.Error(t, err)
}

func TestUnitSecurityService_Health_BadJSON(t *testing.T) {
	srv := badJSONServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	_, err := svc.Health(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}

func TestUnitSecurityService_Health_NetworkError(t *testing.T) {
	ctx := context.Background()
	svc := NewSecurityService(deadClient(), testLogger())

	_, err := svc.Health(ctx)
	require.Error(t, err)
}

func TestUnitSecurityService_WhoAmI(t *testing.T) {
	srv := newSecurityTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	resp, err := svc.WhoAmI(ctx)
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.True(t, resp.IsAdmin)
	assert.Contains(t, resp.Dn, "admin")
	assert.False(t, resp.IsNodeCertificateRequest)
}

func TestUnitSecurityService_WhoAmI_ServerError(t *testing.T) {
	srv := errServer(500)
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	_, err := svc.WhoAmI(ctx)
	require.Error(t, err)
}

func TestUnitSecurityService_WhoAmI_BadJSON(t *testing.T) {
	srv := badJSONServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	_, err := svc.WhoAmI(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}

func TestUnitSecurityService_WhoAmI_NetworkError(t *testing.T) {
	ctx := context.Background()
	svc := NewSecurityService(deadClient(), testLogger())

	_, err := svc.WhoAmI(ctx)
	require.Error(t, err)
}

func TestUnitSecurityService_TenantInfo(t *testing.T) {
	srv := newSecurityTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	resp, err := svc.TenantInfo(ctx)
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Contains(t, resp, "global_tenant")
}

func TestUnitSecurityService_TenantInfo_ServerError(t *testing.T) {
	srv := errServer(500)
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	_, err := svc.TenantInfo(ctx)
	require.Error(t, err)
}

func TestUnitSecurityService_TenantInfo_BadJSON(t *testing.T) {
	srv := badJSONServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	_, err := svc.TenantInfo(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}

func TestUnitSecurityService_TenantInfo_NetworkError(t *testing.T) {
	ctx := context.Background()
	svc := NewSecurityService(deadClient(), testLogger())

	_, err := svc.TenantInfo(ctx)
	require.Error(t, err)
}

func TestUnitSecurityService_DashboardsInfo(t *testing.T) {
	srv := newSecurityTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	resp, err := svc.DashboardsInfo(ctx)
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, true, resp["multitenancy_enabled"])
}

func TestUnitSecurityService_DashboardsInfo_ServerError(t *testing.T) {
	srv := errServer(500)
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	_, err := svc.DashboardsInfo(ctx)
	require.Error(t, err)
}

func TestUnitSecurityService_DashboardsInfo_BadJSON(t *testing.T) {
	srv := badJSONServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	_, err := svc.DashboardsInfo(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}

func TestUnitSecurityService_DashboardsInfo_NetworkError(t *testing.T) {
	ctx := context.Background()
	svc := NewSecurityService(deadClient(), testLogger())

	_, err := svc.DashboardsInfo(ctx)
	require.Error(t, err)
}

func TestUnitSecurityService_ConfigUpdate(t *testing.T) {
	srv := newSecurityTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	resp, err := svc.ConfigUpdate(ctx, []string{"config", "roles"})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, 1, resp.NodeSize)
	assert.Equal(t, 1, resp.UpdatedNode)
}

func TestUnitSecurityService_ConfigUpdate_ServerError(t *testing.T) {
	srv := errServer(500)
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	_, err := svc.ConfigUpdate(ctx, []string{"config"})
	require.Error(t, err)
}

func TestUnitSecurityService_ConfigUpdate_BadJSON(t *testing.T) {
	srv := badJSONServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	_, err := svc.ConfigUpdate(ctx, []string{"config"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}

func TestUnitSecurityService_ConfigUpdate_NetworkError(t *testing.T) {
	ctx := context.Background()
	svc := NewSecurityService(deadClient(), testLogger())

	_, err := svc.ConfigUpdate(ctx, []string{"config"})
	require.Error(t, err)
}

func TestUnitSecurityService_SSLInfo(t *testing.T) {
	srv := newSecurityTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	resp, err := svc.SSLInfo(ctx)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.NotNil(t, resp.HttpSslCertificatesInfo)
	assert.Equal(t, "CN=admin", resp.HttpSslCertificatesInfo.Principal)
	require.NotNil(t, resp.TransportSslCertificatesInfo)
	assert.Equal(t, "CN=node1", resp.TransportSslCertificatesInfo.Principal)
}

func TestUnitSecurityService_SSLInfo_ServerError(t *testing.T) {
	srv := errServer(500)
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	_, err := svc.SSLInfo(ctx)
	require.Error(t, err)
}

func TestUnitSecurityService_SSLInfo_BadJSON(t *testing.T) {
	srv := badJSONServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	_, err := svc.SSLInfo(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}

func TestUnitSecurityService_SSLInfo_NetworkError(t *testing.T) {
	ctx := context.Background()
	svc := NewSecurityService(deadClient(), testLogger())

	_, err := svc.SSLInfo(ctx)
	require.Error(t, err)
}

func TestUnitSecurityService_PermissionsInfo(t *testing.T) {
	srv := newSecurityTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	resp, err := svc.PermissionsInfo(ctx)
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.True(t, resp.HasAccess)
	assert.Contains(t, resp.EnabledEndpoints, "api/account")
}

func TestUnitSecurityService_PermissionsInfo_ServerError(t *testing.T) {
	srv := errServer(500)
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	_, err := svc.PermissionsInfo(ctx)
	require.Error(t, err)
}

func TestUnitSecurityService_PermissionsInfo_BadJSON(t *testing.T) {
	srv := badJSONServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	_, err := svc.PermissionsInfo(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}

func TestUnitSecurityService_PermissionsInfo_NetworkError(t *testing.T) {
	ctx := context.Background()
	svc := NewSecurityService(deadClient(), testLogger())

	_, err := svc.PermissionsInfo(ctx)
	require.Error(t, err)
}

func TestUnitSecurityService_GetAccount(t *testing.T) {
	srv := newSecurityTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	resp, err := svc.GetAccount(ctx)
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "admin", resp.UserName)
	assert.Contains(t, resp.Roles, "all_access")
}

func TestUnitSecurityService_GetAccount_ServerError(t *testing.T) {
	srv := errServer(500)
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	_, err := svc.GetAccount(ctx)
	require.Error(t, err)
}

func TestUnitSecurityService_GetAccount_BadJSON(t *testing.T) {
	srv := badJSONServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	_, err := svc.GetAccount(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}

func TestUnitSecurityService_GetAccount_NetworkError(t *testing.T) {
	ctx := context.Background()
	svc := NewSecurityService(deadClient(), testLogger())

	_, err := svc.GetAccount(ctx)
	require.Error(t, err)
}

func TestUnitSecurityService_PutAccount(t *testing.T) {
	srv := newSecurityTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	resp, err := svc.PutAccount(ctx, map[string]any{"password": "newpass"})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "CREATED", resp.Status)
}

func TestUnitSecurityService_PutAccount_ServerError(t *testing.T) {
	srv := errServer(500)
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	_, err := svc.PutAccount(ctx, map[string]any{"password": "newpass"})
	require.Error(t, err)
}

func TestUnitSecurityService_PutAccount_BadJSON(t *testing.T) {
	srv := badJSONServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	_, err := svc.PutAccount(ctx, map[string]any{"password": "newpass"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}

func TestUnitSecurityService_PutAccount_NetworkError(t *testing.T) {
	ctx := context.Background()
	svc := NewSecurityService(deadClient(), testLogger())

	_, err := svc.PutAccount(ctx, map[string]any{"password": "newpass"})
	require.Error(t, err)
}

func TestUnitSecurityService_AuthToken(t *testing.T) {
	srv := newSecurityTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	resp, err := svc.AuthToken(ctx)
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "Bearer abc123token", resp.Authorization)
}

func TestUnitSecurityService_AuthToken_ServerError(t *testing.T) {
	srv := errServer(500)
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	_, err := svc.AuthToken(ctx)
	require.Error(t, err)
}

func TestUnitSecurityService_AuthToken_BadJSON(t *testing.T) {
	srv := badJSONServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	_, err := svc.AuthToken(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}

func TestUnitSecurityService_AuthToken_NetworkError(t *testing.T) {
	ctx := context.Background()
	svc := NewSecurityService(deadClient(), testLogger())

	_, err := svc.AuthToken(ctx)
	require.Error(t, err)
}

func TestUnitSecurityService_GetAllowlist(t *testing.T) {
	srv := newSecurityTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	resp, err := svc.GetAllowlist(ctx)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.NotNil(t, resp.Enabled)
	assert.True(t, *resp.Enabled)
	assert.Contains(t, resp.Requests, "/_plugins/_security/api/account")
}

func TestUnitSecurityService_GetAllowlist_ServerError(t *testing.T) {
	srv := errServer(500)
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	_, err := svc.GetAllowlist(ctx)
	require.Error(t, err)
}

func TestUnitSecurityService_GetAllowlist_BadJSON(t *testing.T) {
	srv := badJSONServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	_, err := svc.GetAllowlist(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}

func TestUnitSecurityService_GetAllowlist_NetworkError(t *testing.T) {
	ctx := context.Background()
	svc := NewSecurityService(deadClient(), testLogger())

	_, err := svc.GetAllowlist(ctx)
	require.Error(t, err)
}

func TestUnitSecurityService_PutAllowlist(t *testing.T) {
	srv := newSecurityTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	resp, err := svc.PutAllowlist(ctx, map[string]any{"enabled": true})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "CREATED", resp.Status)
}

func TestUnitSecurityService_PutAllowlist_ServerError(t *testing.T) {
	srv := errServer(500)
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	_, err := svc.PutAllowlist(ctx, map[string]any{"enabled": true})
	require.Error(t, err)
}

func TestUnitSecurityService_PutAllowlist_BadJSON(t *testing.T) {
	srv := badJSONServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	_, err := svc.PutAllowlist(ctx, map[string]any{"enabled": true})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}

func TestUnitSecurityService_PutAllowlist_NetworkError(t *testing.T) {
	ctx := context.Background()
	svc := NewSecurityService(deadClient(), testLogger())

	_, err := svc.PutAllowlist(ctx, map[string]any{"enabled": true})
	require.Error(t, err)
}

func TestUnitSecurityService_GetCertificates(t *testing.T) {
	srv := newSecurityTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	resp, err := svc.GetCertificates(ctx)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Len(t, resp.Certificates, 1)
	assert.Equal(t, "CN=root", resp.Certificates[0].IssuerDN)
	assert.Equal(t, "1234", resp.Certificates[0].SerialNumber)
}

func TestUnitSecurityService_GetCertificates_ServerError(t *testing.T) {
	srv := errServer(500)
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	_, err := svc.GetCertificates(ctx)
	require.Error(t, err)
}

func TestUnitSecurityService_GetCertificates_BadJSON(t *testing.T) {
	srv := badJSONServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	_, err := svc.GetCertificates(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}

func TestUnitSecurityService_GetCertificates_NetworkError(t *testing.T) {
	ctx := context.Background()
	svc := NewSecurityService(deadClient(), testLogger())

	_, err := svc.GetCertificates(ctx)
	require.Error(t, err)
}

func TestUnitSecurityService_GetMultiTenancyConfig(t *testing.T) {
	srv := newSecurityTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	resp, err := svc.GetMultiTenancyConfig(ctx)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.NotNil(t, resp.Enabled)
	assert.True(t, *resp.Enabled)
	assert.Equal(t, "global_tenant", *resp.DefaultTenant)
}

func TestUnitSecurityService_GetMultiTenancyConfig_ServerError(t *testing.T) {
	srv := errServer(500)
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	_, err := svc.GetMultiTenancyConfig(ctx)
	require.Error(t, err)
}

func TestUnitSecurityService_GetMultiTenancyConfig_BadJSON(t *testing.T) {
	srv := badJSONServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	_, err := svc.GetMultiTenancyConfig(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}

func TestUnitSecurityService_GetMultiTenancyConfig_NetworkError(t *testing.T) {
	ctx := context.Background()
	svc := NewSecurityService(deadClient(), testLogger())

	_, err := svc.GetMultiTenancyConfig(ctx)
	require.Error(t, err)
}

func TestUnitSecurityService_PutMultiTenancyConfig(t *testing.T) {
	srv := newSecurityTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	resp, err := svc.PutMultiTenancyConfig(ctx, map[string]any{"enabled": true})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "CREATED", resp.Status)
}

func TestUnitSecurityService_PutMultiTenancyConfig_ServerError(t *testing.T) {
	srv := errServer(500)
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	_, err := svc.PutMultiTenancyConfig(ctx, map[string]any{"enabled": true})
	require.Error(t, err)
}

func TestUnitSecurityService_PutMultiTenancyConfig_BadJSON(t *testing.T) {
	srv := badJSONServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	_, err := svc.PutMultiTenancyConfig(ctx, map[string]any{"enabled": true})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}

func TestUnitSecurityService_PutMultiTenancyConfig_NetworkError(t *testing.T) {
	ctx := context.Background()
	svc := NewSecurityService(deadClient(), testLogger())

	_, err := svc.PutMultiTenancyConfig(ctx, map[string]any{"enabled": true})
	require.Error(t, err)
}

func TestUnitSecurityService_GetRateLimiters(t *testing.T) {
	srv := newSecurityTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	resp, err := svc.GetRateLimiters(ctx)
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Contains(t, resp, "IpRateLimiter")
	assert.Equal(t, "ip", resp["IpRateLimiter"].Type)
}

func TestUnitSecurityService_GetRateLimiters_ServerError(t *testing.T) {
	srv := errServer(500)
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	_, err := svc.GetRateLimiters(ctx)
	require.Error(t, err)
}

func TestUnitSecurityService_GetRateLimiters_BadJSON(t *testing.T) {
	srv := badJSONServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	_, err := svc.GetRateLimiters(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}

func TestUnitSecurityService_GetRateLimiters_NetworkError(t *testing.T) {
	ctx := context.Background()
	svc := NewSecurityService(deadClient(), testLogger())

	_, err := svc.GetRateLimiters(ctx)
	require.Error(t, err)
}

func TestUnitSecurityService_PutRateLimiter(t *testing.T) {
	srv := newSecurityTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	body := map[string]any{"type": "ip", "window_duration": "1m", "max_count": 10}
	resp, err := svc.PutRateLimiter(ctx, "test_limiter", body)
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "CREATED", resp.Status)
}

func TestUnitSecurityService_PutRateLimiter_ServerError(t *testing.T) {
	srv := errServer(500)
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	_, err := svc.PutRateLimiter(ctx, "test_limiter", map[string]any{})
	require.Error(t, err)
}

func TestUnitSecurityService_PutRateLimiter_BadJSON(t *testing.T) {
	srv := badJSONServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	_, err := svc.PutRateLimiter(ctx, "test_limiter", map[string]any{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}

func TestUnitSecurityService_PutRateLimiter_NetworkError(t *testing.T) {
	ctx := context.Background()
	svc := NewSecurityService(deadClient(), testLogger())

	_, err := svc.PutRateLimiter(ctx, "test_limiter", map[string]any{})
	require.Error(t, err)
}

func TestUnitSecurityService_PutRateLimiter_EmptyName(t *testing.T) {
	ctx := context.Background()
	svc := NewSecurityService(deadClient(), testLogger())

	_, err := svc.PutRateLimiter(ctx, "", map[string]any{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "rate limiter name is required")
}

func TestUnitSecurityService_DeleteRateLimiter(t *testing.T) {
	srv := newSecurityTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	resp, err := svc.DeleteRateLimiter(ctx, "test_limiter")
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "CREATED", resp.Status)
}

func TestUnitSecurityService_DeleteRateLimiter_ServerError(t *testing.T) {
	srv := errServer(500)
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	_, err := svc.DeleteRateLimiter(ctx, "test_limiter")
	require.Error(t, err)
}

func TestUnitSecurityService_DeleteRateLimiter_BadJSON(t *testing.T) {
	srv := badJSONServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	_, err := svc.DeleteRateLimiter(ctx, "test_limiter")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}

func TestUnitSecurityService_DeleteRateLimiter_NetworkError(t *testing.T) {
	ctx := context.Background()
	svc := NewSecurityService(deadClient(), testLogger())

	_, err := svc.DeleteRateLimiter(ctx, "test_limiter")
	require.Error(t, err)
}

func TestUnitSecurityService_DeleteRateLimiter_EmptyName(t *testing.T) {
	ctx := context.Background()
	svc := NewSecurityService(deadClient(), testLogger())

	_, err := svc.DeleteRateLimiter(ctx, "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "rate limiter name is required")
}

func TestUnitSecurityService_ListRoles(t *testing.T) {
	srv := newSecurityTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	resp, err := svc.ListRoles(ctx)
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Contains(t, resp, "role1")
	assert.Contains(t, resp, "role2")
}

func TestUnitSecurityService_ListRoles_ServerError(t *testing.T) {
	srv := errServer(500)
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	_, err := svc.ListRoles(ctx)
	require.Error(t, err)
}

func TestUnitSecurityService_ListRoles_BadJSON(t *testing.T) {
	srv := badJSONServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	_, err := svc.ListRoles(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}

func TestUnitSecurityService_ListRoles_NetworkError(t *testing.T) {
	ctx := context.Background()
	svc := NewSecurityService(deadClient(), testLogger())

	_, err := svc.ListRoles(ctx)
	require.Error(t, err)
}

func TestUnitSecurityService_ListUsers(t *testing.T) {
	srv := newSecurityTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	resp, err := svc.ListUsers(ctx)
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Contains(t, resp, "admin")
	assert.Contains(t, resp, "user1")
}

func TestUnitSecurityService_ListUsers_ServerError(t *testing.T) {
	srv := errServer(500)
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	_, err := svc.ListUsers(ctx)
	require.Error(t, err)
}

func TestUnitSecurityService_ListUsers_BadJSON(t *testing.T) {
	srv := badJSONServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	_, err := svc.ListUsers(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}

func TestUnitSecurityService_ListUsers_NetworkError(t *testing.T) {
	ctx := context.Background()
	svc := NewSecurityService(deadClient(), testLogger())

	_, err := svc.ListUsers(ctx)
	require.Error(t, err)
}

func TestUnitSecurityService_ListRoleMappings(t *testing.T) {
	srv := newSecurityTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	resp, err := svc.ListRoleMappings(ctx)
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Contains(t, resp, "role1")
}

func TestUnitSecurityService_ListRoleMappings_ServerError(t *testing.T) {
	srv := errServer(500)
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	_, err := svc.ListRoleMappings(ctx)
	require.Error(t, err)
}

func TestUnitSecurityService_ListRoleMappings_BadJSON(t *testing.T) {
	srv := badJSONServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	_, err := svc.ListRoleMappings(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}

func TestUnitSecurityService_ListRoleMappings_NetworkError(t *testing.T) {
	ctx := context.Background()
	svc := NewSecurityService(deadClient(), testLogger())

	_, err := svc.ListRoleMappings(ctx)
	require.Error(t, err)
}

func TestUnitSecurityService_ListActionGroups(t *testing.T) {
	srv := newSecurityTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	resp, err := svc.ListActionGroups(ctx)
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Contains(t, resp, "read")
}

func TestUnitSecurityService_ListActionGroups_ServerError(t *testing.T) {
	srv := errServer(500)
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	_, err := svc.ListActionGroups(ctx)
	require.Error(t, err)
}

func TestUnitSecurityService_ListActionGroups_BadJSON(t *testing.T) {
	srv := badJSONServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	_, err := svc.ListActionGroups(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}

func TestUnitSecurityService_ListActionGroups_NetworkError(t *testing.T) {
	ctx := context.Background()
	svc := NewSecurityService(deadClient(), testLogger())

	_, err := svc.ListActionGroups(ctx)
	require.Error(t, err)
}

func TestUnitSecurityService_ListTenants(t *testing.T) {
	srv := newSecurityTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	resp, err := svc.ListTenants(ctx)
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Contains(t, resp, "global_tenant")
}

func TestUnitSecurityService_ListTenants_ServerError(t *testing.T) {
	srv := errServer(500)
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	_, err := svc.ListTenants(ctx)
	require.Error(t, err)
}

func TestUnitSecurityService_ListTenants_BadJSON(t *testing.T) {
	srv := badJSONServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	_, err := svc.ListTenants(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}

func TestUnitSecurityService_ListTenants_NetworkError(t *testing.T) {
	ctx := context.Background()
	svc := NewSecurityService(deadClient(), testLogger())

	_, err := svc.ListTenants(ctx)
	require.Error(t, err)
}

func TestUnitSecurityService_ListNodesDN(t *testing.T) {
	srv := newSecurityTestServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	resp, err := svc.ListNodesDN(ctx)
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Contains(t, resp, "node1")
	assert.Equal(t, []string{"CN=node1"}, resp["node1"].NodesDN)
}

func TestUnitSecurityService_ListNodesDN_ServerError(t *testing.T) {
	srv := errServer(500)
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	_, err := svc.ListNodesDN(ctx)
	require.Error(t, err)
}

func TestUnitSecurityService_ListNodesDN_BadJSON(t *testing.T) {
	srv := badJSONServer()
	defer srv.Close()
	ctx := context.Background()
	svc := NewSecurityService(restyClient(srv), testLogger())

	_, err := svc.ListNodesDN(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}

func TestUnitSecurityService_ListNodesDN_NetworkError(t *testing.T) {
	ctx := context.Background()
	svc := NewSecurityService(deadClient(), testLogger())

	_, err := svc.ListNodesDN(ctx)
	require.Error(t, err)
}
