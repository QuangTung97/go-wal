package wal

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEntryType(t *testing.T) {
	assert.Equal(t, EntryType(0), EntryTypeNone)
	assert.Equal(t, EntryType(1), EntryTypeNormal)
	assert.Equal(t, EntryType(2), EntryTypeFull)
	assert.Equal(t, EntryType(3), EntryTypeFirst)
	assert.Equal(t, EntryType(4), EntryTypeMiddle)
	assert.Equal(t, EntryType(5), EntryTypeLast)
}

func TestLogEntry__Read_Write(t *testing.T) {
	page := newTestPage()

	input := NewSimpleByteReader([]byte("test data 01"))
	n := WriteLogEntry(page.data, EntryTypeFull, input, input.Len())
	assert.Equal(t, int64(15), n)

	entryType, data, n := ReadLogEntry(page.data)
	assert.Equal(t, int64(15), n)
	assert.Equal(t, EntryTypeFull, entryType)
	assert.Equal(t, "test data 01", string(data))
	assert.Equal(t, 12, len(data))

	// read null entry
	page.data = page.data[n:]
	entryType, data, n = ReadLogEntry(page.data)
	assert.Equal(t, int64(1), n)
	assert.Equal(t, EntryTypeNone, entryType)
	assert.Equal(t, "", string(data))
}

func TestLogEntry__Read_Write__Partial(t *testing.T) {
	page := newTestPage()

	input := NewSimpleByteReader([]byte("test data 01 with remain"))
	n := WriteLogEntry(page.data, EntryTypeFull, input, 12)
	assert.Equal(t, int64(15), n)

	entryType, data, n := ReadLogEntry(page.data)
	assert.Equal(t, int64(15), n)
	assert.Equal(t, EntryTypeFull, entryType)
	assert.Equal(t, "test data 01", string(data))
	assert.Equal(t, 12, len(data))

	// read null entry
	page.data = page.data[n:]
	entryType, data, n = ReadLogEntry(page.data)
	assert.Equal(t, int64(1), n)
	assert.Equal(t, EntryTypeNone, entryType)
	assert.Equal(t, "", string(data))
}
