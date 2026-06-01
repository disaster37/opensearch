package opensearch

import (
	"errors"
	"testing"

	"github.com/disaster37/opensearch/v4/types"
	"github.com/stretchr/testify/assert"
)

func TestIsNotFound(t *testing.T) {
	t.Run("404 error returns true", func(t *testing.T) {
		err := &types.OpenSearchError{Status: 404}
		assert.True(t, IsNotFound(err))
	})

	t.Run("non-404 error returns false", func(t *testing.T) {
		err := &types.OpenSearchError{Status: 500}
		assert.False(t, IsNotFound(err))
	})

	t.Run("non-OpenSearchError returns false", func(t *testing.T) {
		assert.False(t, IsNotFound(errors.New("random error")))
	})

	t.Run("nil error returns false", func(t *testing.T) {
		assert.False(t, IsNotFound(nil))
	})
}

func TestIsConflict(t *testing.T) {
	t.Run("409 error returns true", func(t *testing.T) {
		err := &types.OpenSearchError{Status: 409}
		assert.True(t, IsConflict(err))
	})

	t.Run("non-409 error returns false", func(t *testing.T) {
		err := &types.OpenSearchError{Status: 404}
		assert.False(t, IsConflict(err))
	})

	t.Run("non-OpenSearchError returns false", func(t *testing.T) {
		assert.False(t, IsConflict(errors.New("random error")))
	})

	t.Run("nil error returns false", func(t *testing.T) {
		assert.False(t, IsConflict(nil))
	})
}
