package wal

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEntryType(t *testing.T) {
	assert.Equal(t, EntryType(0), EntryTypeNone)
	assert.Equal(t, EntryType(1), EntryTypeFull)
	assert.Equal(t, EntryType(2), EntryTypeFirst)
	assert.Equal(t, EntryType(3), EntryTypeMiddle)
	assert.Equal(t, EntryType(4), EntryTypeLast)
}

func TestLogEntry__Read_Write(t *testing.T) {
	page := newTestPage()
	err := WriteLogEntry(page.data, EntryTypeFull, []byte("test data 01"))
	assert.Equal(t, nil, err)

	entryType, data, err := ReadLogEntry(page.data)
	assert.Equal(t, nil, err)
	assert.Equal(t, EntryTypeFull, entryType)
	assert.Equal(t, "test data 01", string(data))
}
