//go:build integration

package api_test

import (
	"context"
	"testing"

	"github.com/disaster37/opensearch/v4/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSecurityService_PutGetDeleteRole(t *testing.T) {
	client := newIntegrationClient()
	ctx := context.Background()
	roleName := "test-role"

	t.Cleanup(func() {
		client.Security().DeleteRole(ctx, roleName)
	})

	putResult, err := client.Security().PutRole(ctx, roleName, &api.SecurityPutRole{
		ClusterPermissions: []string{"cluster_monitor"},
	})
	require.NoError(t, err)
	assert.NotNil(t, putResult)
	assert.Equal(t, "CREATED", putResult.Status)

	getResult, err := client.Security().GetRole(ctx, roleName)
	require.NoError(t, err)
	assert.NotNil(t, getResult)
	assert.Contains(t, getResult, roleName)

	deleteResult, err := client.Security().DeleteRole(ctx, roleName)
	require.NoError(t, err)
	assert.NotNil(t, deleteResult)
	assert.Equal(t, "OK", deleteResult.Status)
}

func TestSecurityService_PutGetDeleteUser(t *testing.T) {
	client := newIntegrationClient()
	ctx := context.Background()
	username := "test-user"
	password := "TestPassword123!"

	t.Cleanup(func() {
		client.Security().DeleteUser(ctx, username)
	})

	putResult, err := client.Security().PutUser(ctx, username, &api.SecurityPutUser{
		Password: &password,
		SecurityUserBase: api.SecurityUserBase{
			BackendRoles: []string{"test"},
		},
	})
	require.NoError(t, err)
	assert.NotNil(t, putResult)
	assert.Equal(t, "CREATED", putResult.Status)

	getResult, err := client.Security().GetUser(ctx, username)
	require.NoError(t, err)
	assert.NotNil(t, getResult)
	assert.Contains(t, getResult, username)

	deleteResult, err := client.Security().DeleteUser(ctx, username)
	require.NoError(t, err)
	assert.NotNil(t, deleteResult)
	assert.Equal(t, "OK", deleteResult.Status)
}

func TestSecurityService_AuthInfo(t *testing.T) {
	client := newIntegrationClient()
	ctx := context.Background()

	result, err := client.Security().AuthInfo(ctx)
	require.NoError(t, err)
	assert.NotNil(t, result)
	assert.NotEmpty(t, result.UserName)
}

func TestSecurityService_GetConfig(t *testing.T) {
	client := newIntegrationClient()
	ctx := context.Background()

	result, err := client.Security().GetConfig(ctx)
	require.NoError(t, err)
	assert.NotNil(t, result)
}

func TestSecurityService_FlushCache(t *testing.T) {
	client := newIntegrationClient()
	ctx := context.Background()

	result, err := client.Security().FlushCache(ctx)
	require.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, "OK", result.Status)
}

func TestSecurityService_PutGetDeleteActionGroup(t *testing.T) {
	client := newIntegrationClient()
	ctx := context.Background()
	name := "test-action-group"
	desc := "test action group"

	t.Cleanup(func() {
		client.Security().DeleteActionGroup(ctx, name)
	})

	putResult, err := client.Security().PutActionGroup(ctx, name, &api.SecurityPutActionGroup{
		Description:    &desc,
		AllowedActions: []string{"indices:admin/create"},
	})
	require.NoError(t, err)
	assert.NotNil(t, putResult)
	assert.Equal(t, "CREATED", putResult.Status)

	getResult, err := client.Security().GetActionGroup(ctx, name)
	require.NoError(t, err)
	assert.NotNil(t, getResult)
	assert.Contains(t, getResult, name)

	deleteResult, err := client.Security().DeleteActionGroup(ctx, name)
	require.NoError(t, err)
	assert.NotNil(t, deleteResult)
	assert.Equal(t, "OK", deleteResult.Status)
}

func TestSecurityService_PutGetDeleteTenant(t *testing.T) {
	client := newIntegrationClient()
	ctx := context.Background()
	name := "test-tenant"
	desc := "test tenant"

	t.Cleanup(func() {
		client.Security().DeleteTenant(ctx, name)
	})

	putResult, err := client.Security().PutTenant(ctx, name, &api.SecurityPutTenant{
		Description: &desc,
	})
	require.NoError(t, err)
	assert.NotNil(t, putResult)
	assert.Equal(t, "CREATED", putResult.Status)

	getResult, err := client.Security().GetTenant(ctx, name)
	require.NoError(t, err)
	assert.NotNil(t, getResult)
	assert.Contains(t, getResult, name)

	deleteResult, err := client.Security().DeleteTenant(ctx, name)
	require.NoError(t, err)
	assert.NotNil(t, deleteResult)
	assert.Equal(t, "OK", deleteResult.Status)
}

func TestSecurityService_PutGetDeleteRoleMapping(t *testing.T) {
	client := newIntegrationClient()
	ctx := context.Background()
	roleName := "test-role"
	mappingRoleName := "test-role-mapping"

	_, err := client.Security().PutRole(ctx, mappingRoleName, &api.SecurityPutRole{
		ClusterPermissions: []string{"cluster_monitor"},
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		client.Security().DeleteRoleMapping(ctx, mappingRoleName)
		client.Security().DeleteRole(ctx, mappingRoleName)
		_ = roleName
	})

	putResult, err := client.Security().PutRoleMapping(ctx, mappingRoleName, &api.SecurityPutRoleMapping{
		BackendRoles: []string{"admin"},
		Users:        []string{"admin"},
	})
	require.NoError(t, err)
	assert.NotNil(t, putResult)
	assert.Equal(t, "CREATED", putResult.Status)

	getResult, err := client.Security().GetRoleMapping(ctx, mappingRoleName)
	require.NoError(t, err)
	assert.NotNil(t, getResult)
	assert.Contains(t, getResult, mappingRoleName)

	deleteResult, err := client.Security().DeleteRoleMapping(ctx, mappingRoleName)
	require.NoError(t, err)
	assert.NotNil(t, deleteResult)
	assert.Equal(t, "OK", deleteResult.Status)
}
