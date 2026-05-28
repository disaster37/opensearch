package opensearch

import (
	"testing"
)

func TestSecurityAuthInfoBuildURL(t *testing.T) {
	client := setupTestClient(t)

	tests := []struct {
		Verbose      *bool
		AuthType     *string
		ExpectedPath string
		ExpectErr    bool
	}{
		{
			nil,
			nil,
			"/_plugins/_security/authinfo",
			false,
		},
	}

	for i, test := range tests {
		builder := client.SecurityAuthInfo()
		if test.Verbose != nil {
			builder = builder.Verbose(*test.Verbose)
		}
		if test.AuthType != nil {
			builder = builder.AuthType(*test.AuthType)
		}
		err := builder.Validate()
		if err != nil {
			if !test.ExpectErr {
				t.Errorf("case #%d: %v", i+1, err)
				continue
			}
		} else {
			if test.ExpectErr {
				t.Errorf("case #%d: expected error", i+1)
				continue
			}
			path, params, _ := builder.buildURL()
			if path != test.ExpectedPath {
				t.Errorf("case #%d: expected %q; got: %q", i+1, test.ExpectedPath, path)
			}
			if test.Verbose != nil {
				if params.Get("verbose") != "true" {
					t.Errorf("case #%d: expected verbose=true in params", i+1)
				}
			}
			if test.AuthType != nil {
				if params.Get("auth_type") != *test.AuthType {
					t.Errorf("case #%d: expected auth_type=%s in params", i+1, *test.AuthType)
				}
			}
		}
	}
}

func TestSecurityAuthInfoBuildURLWithVerbose(t *testing.T) {
	client := setupTestClient(t)

	builder := client.SecurityAuthInfo().Verbose(true)
	path, params, err := builder.buildURL()
	if err != nil {
		t.Fatal(err)
	}
	if path != "/_plugins/_security/authinfo" {
		t.Errorf("expected path %q; got: %q", "/_plugins/_security/authinfo", path)
	}
	if params.Get("verbose") != "true" {
		t.Errorf("expected verbose=true; got: %q", params.Get("verbose"))
	}
}

func TestSecurityAuthInfoBuildURLWithAuthType(t *testing.T) {
	client := setupTestClient(t)

	builder := client.SecurityAuthInfo().AuthType("basic")
	path, params, err := builder.buildURL()
	if err != nil {
		t.Fatal(err)
	}
	if path != "/_plugins/_security/authinfo" {
		t.Errorf("expected path %q; got: %q", "/_plugins/_security/authinfo", path)
	}
	if params.Get("auth_type") != "basic" {
		t.Errorf("expected auth_type=basic; got: %q", params.Get("auth_type"))
	}
}
