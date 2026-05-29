package types

import (
	"testing"
	"time"

	json "github.com/goccy/go-json"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUnixMilliTime_UnmarshalJSON(t *testing.T) {
	tests := []struct {
		name       string
		input      string
		expectZero bool
		expectMs   int64
	}{
		{
			name:       "valid timestamp",
			input:      "1704067200000",
			expectZero: false,
			expectMs:   1704067200000,
		},
		{
			name:       "null value",
			input:      "null",
			expectZero: true,
		},
		{
			name:       "empty string",
			input:      `""`,
			expectZero: true,
		},
		{
			name:       "zero value",
			input:      "0",
			expectZero: true,
		},
		{
			name:       "literal 0 integer",
			input:      "0",
			expectZero: true,
		},
		{
			name:       "negative zero",
			input:      "-0",
			expectZero: true,
		},
		{
			name:       "one millisecond",
			input:      "1",
			expectZero: false,
			expectMs:   1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var ut UnixMilliTime
			err := ut.UnmarshalJSON([]byte(tt.input))
			require.NoError(t, err)

			if tt.expectZero {
				assert.True(t, ut.IsZero())
			} else {
				assert.False(t, ut.IsZero())
				assert.Equal(t, tt.expectMs, ut.UnixMilli())
			}
		})
	}

	t.Run("invalid json", func(t *testing.T) {
		var ut UnixMilliTime
		err := ut.UnmarshalJSON([]byte(`"not-a-number"`))
		assert.Error(t, err)
	})
}

func TestUnixMilliTime_MarshalJSON(t *testing.T) {
	tests := []struct {
		name     string
		time     UnixMilliTime
		expected string
	}{
		{
			name:     "zero time",
			time:     UnixMilliTime{},
			expected: "0",
		},
		{
			name:     "valid time",
			time:     UnixMilliTime{Time: time.Unix(0, 1704067200000*int64(time.Millisecond))},
			expected: "1704067200000",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := tt.time.MarshalJSON()
			require.NoError(t, err)
			assert.Equal(t, tt.expected, string(data))
		})
	}
}

func TestUnixMilliTime_RoundTrip(t *testing.T) {
	original := UnixMilliTime{Time: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)}

	data, err := json.Marshal(original)
	require.NoError(t, err)

	var decoded UnixMilliTime
	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, original.UnixMilli(), decoded.UnixMilli())
}
