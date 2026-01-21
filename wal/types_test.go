package wal

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPageSize(t *testing.T) {
	assert.Equal(t, 512, PageSize)
}

func TestLSN_ToPageNum(t *testing.T) {
	n := LSN(0b110111_1010_00101)
	assert.Equal(t, PageNum(0b110111), n.ToPageNum())
}

func TestLogDataOffset_ToLSN(t *testing.T) {
	offset := LogDataOffset(DataSizePerPage)
	assert.Equal(t, LSN(PageSize+pageHeaderSize), offset.ToLSN())

	offset = LogDataOffset(DataSizePerPage) + 1
	assert.Equal(t, LSN(PageSize+pageHeaderSize+1), offset.ToLSN())

	offset = LogDataOffset(2*DataSizePerPage) + DataSizePerPage - 1 // last byte of page 2
	assert.Equal(t, LSN(3*PageSize-1), offset.ToLSN())
}
