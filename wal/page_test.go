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
	assert.Equal(t, 18, latestEntryOffsetOffset)
	assert.Equal(t, 20, pageHeaderSize)

	assert.Equal(t, pageHeaderSize-latestEntryOffsetOffset, int(unsafe.Sizeof(EntryOffset(0))))
	assert.Equal(t, latestEntryOffsetOffset-pageNumberOffset, int(unsafe.Sizeof(PageNum(0))))
	assert.Equal(t, pageNumberOffset-pageEpochOffset, int(unsafe.Sizeof(NewEpoch(0))))
	assert.Equal(t, unsafe.Sizeof(PageNum(0)), unsafe.Sizeof(LSN(0)))
	assert.Equal(t, unsafe.Sizeof(LSN(0)), unsafe.Sizeof(LogDataOffset(0)))
}

func newPageData() PageData {
	var data [PageSize]byte
	return data[:]
}

func TestInitPage(t *testing.T) {
	p := newPageData()
	InitPage(p, NewEpoch(21), 12<<32+PageSize+31)

	assert.Equal(t, PageVersion(1), p.GetVersion())
	assert.Equal(t, NewEpoch(21), p.GetEpoch())
	assert.Equal(t, PageNum(12<<32+PageSize+31), p.GetPageNum())
	assert.Equal(t, EntryOffset(pageHeaderSize-1), p.GetLatestOffset())
}
