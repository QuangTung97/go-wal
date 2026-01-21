package wal

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPageVersion(t *testing.T) {
	assert.Equal(t, PageVersion(1), FirstVersion)
}

func TestEntryType(t *testing.T) {
	assert.Equal(t, EntryType(0), EntryTypeNone)
	assert.Equal(t, EntryType(1), EntryTypeFull)
}

func TestPageOffsets(t *testing.T) {
	assert.Equal(t, 1, checkSumOffset)
	assert.Equal(t, 5, flagsOffset)
	assert.Equal(t, 6, pageGenerationOffset)
	assert.Equal(t, 14, pageNumberOffset)
}
