package api

import (
	"errors"
	"net/http"
	"testing"

	"github.com/disaster37/opensearch/v3/types"
	"github.com/go-resty/resty/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newMockClient() (*resty.Client, *http.Client) {
	rc := resty.New()
	return rc, rc.GetClient()
}

func TestParseErrorResponse(t *testing.T) {
	t.Run("with valid error body", func(t *testing.T) {
		client, _ := newMockClient()
		resp, err := client.R().
			SetError(map[string]any{}).
			Get("data:application/json;base64,eyJlcnJvciI6eyJ0eXBlIjoiaW5kZXhfbm90X2ZvdW5kX2V4Y2VwdGlvbiIsInJlYXNvbiI6Im5vIHN1Y2ggaW5kZXgiLCJpbmRleCI6Im15LWluZGV4In0sInN0YXR1cyI6NDA0fQ==")
		if err != nil {
			t.Skip("base64 data URI not supported by resty")
		}

		osErr := parseErrorResponse(resp)
		require.NotNil(t, osErr)
	})

	t.Run("with nil body", func(t *testing.T) {
		client, _ := newMockClient()
		resp, _ := client.R().Get("http://httpbin.org/status/500")
		if resp == nil {
			t.Skip("httpbin not available")
		}
	})
}

func TestWrapNetworkError(t *testing.T) {
	logger := testLogger()
	originalErr := errors.New("connection refused")
	result := wrapNetworkError(logger, originalErr)
	assert.Equal(t, originalErr, result)
}

func TestWrapUnmarshalError(t *testing.T) {
	logger := testLogger()

	t.Run("short body", func(t *testing.T) {
		client, _ := newMockClient()
		_, _ = client.R().Get("http://httpbin.org/status/200")
		err := wrapUnmarshalError(logger, &resty.Response{
			Request: &resty.Request{
				Method: "GET",
				URL:    "/test",
			},
		}, errors.New("json decode failed"))
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal response from GET /test")
	})
}

func TestValidationErrorFormat(t *testing.T) {
	baseErr := errors.New("Field validation for 'Index' failed on the 'required' tag")
	result := validationError(baseErr)
	assert.Contains(t, result.Error(), "validation:")
}

func TestValidationErrorNil(t *testing.T) {
	result := validationError(nil)
	assert.Nil(t, result)
}

func TestTypeAliases(t *testing.T) {
	t.Run("OpenSearchError works as expected", func(t *testing.T) {
		err := &types.OpenSearchError{
			Status: 404,
			Details: &types.OpenSearchErrorDetails{
				Type:   "index_not_found_exception",
				Reason: "no such index",
			},
		}
		assert.Equal(t, 404, err.StatusCode())
		assert.Contains(t, err.Error(), "404")
	})
}
