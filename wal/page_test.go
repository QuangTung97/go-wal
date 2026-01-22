package wal

import (
	"bytes"
	"errors"
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

func newPageData() *Page {
	var data [PageSize]byte
	return &Page{
		data: data[:],
	}
}

func TestInitPage(t *testing.T) {
	p := newPageData()
	InitPage(p, NewEpoch(21), 12<<32+31)

	assert.Equal(t, PageVersion(1), p.GetVersion())
	assert.Equal(t, NewEpoch(21), p.GetEpoch())
	assert.Equal(t, PageNum(12<<32+31), p.GetPageNum())
}

func TestReadWritePage(t *testing.T) {
	page := newPageData()
	InitPage(page, NewEpoch(21), 12<<32+31)

	// write
	var buf bytes.Buffer
	err := page.Write(&buf)
	assert.Equal(t, nil, err)

	// read
	data := buf.Bytes()
	newPage := newPageData()
	err = ReadPage(newPage, bytes.NewReader(data))
	assert.Equal(t, nil, err)
	assert.Equal(t, FirstVersion, newPage.GetVersion())
	assert.Equal(t, page.data, newPage.data)

	// mismatch checksum
	data[511] = 11
	err = ReadPage(newPage, bytes.NewReader(data))
	assert.Equal(t, errors.New("mismatch page checksum"), err)
}
