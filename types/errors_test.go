package types

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOpenSearchError_Error(t *testing.T) {
	tests := []struct {
		name     string
		err      *OpenSearchError
		expected string
	}{
		{
			name: "with details and reason",
			err: &OpenSearchError{
				Status: 404,
				Details: &OpenSearchErrorDetails{
					Type:   "index_not_found_exception",
					Reason: "no such index [my-index]",
				},
			},
			expected: "opensearch: Error 404: no such index [my-index] [type=index_not_found_exception]",
		},
		{
			name: "with nil details",
			err: &OpenSearchError{
				Status:  500,
				Details: nil,
			},
			expected: "opensearch: Error 500",
		},
		{
			name: "with empty reason",
			err: &OpenSearchError{
				Status: 400,
				Details: &OpenSearchErrorDetails{
					Type:   "illegal_argument_exception",
					Reason: "",
				},
			},
			expected: "opensearch: Error 400",
		},
		{
			name: "with details type only",
			err: &OpenSearchError{
				Status: 503,
				Details: &OpenSearchErrorDetails{
					Type: "service_unavailable",
				},
			},
			expected: "opensearch: Error 503",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.err.Error())
		})
	}
}

func TestOpenSearchError_StatusCode(t *testing.T) {
	tests := []struct {
		name   string
		err    *OpenSearchError
		expect int
	}{
		{
			name:   "404 status",
			err:    &OpenSearchError{Status: 404},
			expect: 404,
		},
		{
			name:   "0 status",
			err:    &OpenSearchError{Status: 0},
			expect: 0,
		},
		{
			name:   "500 status",
			err:    &OpenSearchError{Status: 500},
			expect: 500,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expect, tt.err.StatusCode())
		})
	}
}

func TestIsNotFound(t *testing.T) {
	tests := []struct {
		name   string
		err    error
		expect bool
	}{
		{
			name:   "404 error",
			err:    &OpenSearchError{Status: 404},
			expect: true,
		},
		{
			name:   "200 error",
			err:    &OpenSearchError{Status: 200},
			expect: false,
		},
		{
			name:   "500 error",
			err:    &OpenSearchError{Status: 500},
			expect: false,
		},
		{
			name:   "non-OpenSearchError",
			err:    errors.New("some other error"),
			expect: false,
		},
		{
			name:   "nil error",
			err:    nil,
			expect: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expect, IsNotFound(tt.err))
		})
	}
}

func TestIsConflict(t *testing.T) {
	tests := []struct {
		name   string
		err    error
		expect bool
	}{
		{
			name:   "409 error",
			err:    &OpenSearchError{Status: 409},
			expect: true,
		},
		{
			name:   "404 error",
			err:    &OpenSearchError{Status: 404},
			expect: false,
		},
		{
			name:   "500 error",
			err:    &OpenSearchError{Status: 500},
			expect: false,
		},
		{
			name:   "non-OpenSearchError",
			err:    errors.New("some error"),
			expect: false,
		},
		{
			name:   "nil error",
			err:    nil,
			expect: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expect, IsConflict(tt.err))
		})
	}
}
