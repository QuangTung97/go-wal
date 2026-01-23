package wal

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSimpleByteReader(t *testing.T) {
	t.Run("read full", func(t *testing.T) {
		r := NewSimpleByteReader([]byte("test data 01"))
		assert.Equal(t, int64(12), r.Len())
		assert.Equal(t, "test data 01", string(r.Read(1000)))
	})

	t.Run("read partial", func(t *testing.T) {
		r := NewSimpleByteReader([]byte("test data 01"))
		assert.Equal(t, int64(12), r.Len())

		assert.Equal(t, "te", string(r.Read(2)))
		assert.Equal(t, int64(10), r.Len())

		assert.Equal(t, "st data ", string(r.Read(8)))
		assert.Equal(t, int64(2), r.Len())

		assert.Equal(t, "01", string(r.Read(100)))
		assert.Equal(t, int64(0), r.Len())
	})
}
