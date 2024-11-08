package opensearch

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSecurityRole(t *testing.T) {
	client := setupTestClient(t)
	var err error

	expectedRole := &SecurityPutRole{
		ClusterPermissions: []string{"*"},
		IndexPermissions: []SecurityIndexPermissions{
			{
				IndexPatterns:      []string{"*"},
				AllowedActions:     []string{"*"},
				MaskedFields:       []string{},
				FieldLevelSecurity: []string{},
			},
		},
		TenantPermissions: []SecurityTenantPermissions{
			{
				TenantPatterns: []string{"*"},
				AllowedActions: []string{"*"},
			},
		},
	}

	// Create role
	resPut, err := client.SecurityPutRole("superuser").Body(expectedRole).Do(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	assert.NotNil(t, resPut)

	// Get role
	resGet, err := client.SecurityGetRole("superuser").Do(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	assert.NotNil(t, resGet)
	assert.Equal(t, *expectedRole, (*resGet)["superuser"].SecurityPutRole)

	// Update role
	expectedRole.ClusterPermissions = []string{"cluster:admin/opendistro/alerting/alerts/get"}
	_, err = client.SecurityPutRole("superuser").Body(expectedRole).Do(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	resGet, err = client.SecurityGetRole("superuser").Do(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, *expectedRole, (*resGet)["superuser"].SecurityPutRole)

	// Delete role
	resDelete, err := client.SecurityDeleteRole("superuser").Do(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	assert.NotNil(t, resDelete)
	_, err = client.SecurityGetRole("superuser").Do(context.Background())
	assert.True(t, IsNotFound(err))
}
