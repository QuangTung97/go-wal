package wal

import (
	"testing"
	"unsafe"

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
	assert.Equal(t, 6, pageEpochOffset)
	assert.Equal(t, 10, pageNumberOffset)
	assert.Equal(t, 18, pageHeaderSize)

	assert.Equal(t, pageHeaderSize-pageNumberOffset, int(unsafe.Sizeof(PageNum(0))))
	assert.Equal(t, pageNumberOffset-pageEpochOffset, int(unsafe.Sizeof(NewEpoch(0))))
	assert.Equal(t, unsafe.Sizeof(PageNum(0)), unsafe.Sizeof(LSN(0)))
	assert.Equal(t, unsafe.Sizeof(LSN(0)), unsafe.Sizeof(LogDataOffset(0)))
}
