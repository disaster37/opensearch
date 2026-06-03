package opensearch

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
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
