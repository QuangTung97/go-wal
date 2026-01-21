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
