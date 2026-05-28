package opensearch

import (
	"context"
	"testing"
)

func TestSecurityAuthInfoIntegration(t *testing.T) {
	client := setupTestClientAndCreateIndex(t)

	// Get auth info
	res, err := client.SecurityAuthInfo().Do(context.TODO())
	if err != nil {
		t.Fatal(err)
	}
	if res == nil {
		t.Fatal("expected response, got nil")
	}
	if res.UserName == "" {
		t.Errorf("expected user_name to be non-empty")
	}
	if res.Roles == nil {
		t.Errorf("expected roles to be non-nil")
	}
	if res.Tenants == nil {
		t.Errorf("expected tenants to be non-nil")
	}
}

func TestSecurityAuthInfoVerboseIntegration(t *testing.T) {
	client := setupTestClientAndCreateIndex(t)

	// Get verbose auth info
	res, err := client.SecurityAuthInfo().Verbose(true).Do(context.TODO())
	if err != nil {
		t.Fatal(err)
	}
	if res == nil {
		t.Fatal("expected response, got nil")
	}
	if res.UserName == "" {
		t.Errorf("expected user_name to be non-empty")
	}
	if res.SizeOfUser == "" {
		t.Errorf("expected size_of_user to be non-empty in verbose mode")
	}
	if res.SizeOfBackendRoles == "" {
		t.Errorf("expected size_of_backendroles to be non-empty in verbose mode")
	}
}
