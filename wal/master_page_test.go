package wal

import (
	"bytes"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMasterPageVersion(t *testing.T) {
	assert.Equal(t, MasterPageVersion(1), MasterPageFirstVersion)
}

func TestMasterPageHeaderOffset(t *testing.T) {
	assert.Equal(t, 1, masterPageChecksumOffset)
	assert.Equal(t, 5, masterPageLatestEpochOffset)
	assert.Equal(t, 9, masterPageCheckpointOffset)
}

func TestReadMasterPage__Write_And_Read(t *testing.T) {
	var writer bytes.Buffer

	page := MasterPage{
		Version:       MasterPageFirstVersion,
		LatestEpoch:   NewEpoch(31),
		CheckpointLSN: PageSize*3 + 123,
	}

	// write
	err := WriteMasterPage(&writer, &page)
	assert.Equal(t, nil, err)

	// read
	pageData := writer.Bytes()
	var readPage MasterPage
	err = ReadMasterPage(bytes.NewReader(pageData), &readPage)
	assert.Equal(t, nil, err)

	// check equal
	assert.Equal(t, page, readPage)

	pageData[511] = 11
	err = ReadMasterPage(bytes.NewReader(pageData), &readPage)
	assert.Equal(t, errors.New("mismatch master page checksum"), err)
}
