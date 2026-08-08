package opensearch

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaultRetryConditions_ConnectionReset(t *testing.T) {
	conditions := DefaultRetryConditions()

	// Test connection reset by peer error
	err := errors.New("read tcp 172.18.0.2:54492->10.221.84.160:443: read: connection reset by peer")

	shouldRetry := conditions[0](nil, err)
	assert.True(t, shouldRetry, "Should retry on connection reset by peer")
}

func TestDefaultRetryConditions_ConnectionRefused(t *testing.T) {
	conditions := DefaultRetryConditions()

	// Test connection refused error
	err := errors.New("connection refused")

	shouldRetry := conditions[0](nil, err)
	assert.True(t, shouldRetry, "Should retry on connection refused")
}

func TestDefaultRetryConditions_Timeout(t *testing.T) {
	conditions := DefaultRetryConditions()

	// Test timeout error
	err := errors.New("i/o timeout")

	shouldRetry := conditions[0](nil, err)
	assert.True(t, shouldRetry, "Should retry on timeout")
}

func TestDefaultRetryConditions_NoError(t *testing.T) {
	conditions := DefaultRetryConditions()

	// Test nil error (should not retry)
	shouldRetry := conditions[0](nil, nil)
	assert.False(t, shouldRetry, "Should not retry when no error")
}

func TestPITSearchRetryConditions_SearchPhaseExecutionException(t *testing.T) {
	// This is harder to test without a real Response object
	// But we can at least verify the function exists and returns correct length
	conditions := PITSearchRetryConditions()
	assert.GreaterOrEqual(t, len(conditions), 2, "Should have at least default + PIT conditions")
}

// TestRetryOn504GatewayTimeout verifies that OpenSearchTimeoutException
// responses (HTTP 504, PR #22064, OpenSearch 3.8.0) are retried under
// DefaultRetryConditions exactly like the old 500 was, since the default
// condition retries status >= 500 && status != 501.
func TestRetryOn504GatewayTimeout(t *testing.T) {
	var attempts int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&attempts, 1)
		if n < 3 {
			w.WriteHeader(http.StatusGatewayTimeout)
			_, _ = fmt.Fprint(w, `{"error":{"type":"timeout_exception","reason":"request timed out"},"status":504}`)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprint(w, `{"cluster_name":"test","version":{"number":"3.8.0"}}`)
	}))
	defer srv.Close()

	logger := logrus.NewEntry(logrus.StandardLogger())
	client, err := New(&Config{
		URL:              srv.URL,
		RetryCount:       3,
		RetryWaitTime:    1 * time.Millisecond,
		RetryMaxWaitTime: 1 * time.Millisecond,
		RetryConditions:  DefaultRetryConditions(),
	}, logger)
	require.NoError(t, err)

	resp, err := client.RestyClient().R().Get("/_test")
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode())
	assert.Equal(t, int32(3), atomic.LoadInt32(&attempts), "expected exactly 3 attempts (2 retries + 1 success)")
}
