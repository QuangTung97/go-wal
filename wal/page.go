package wal

import (
	"encoding/binary"
	"io"
)

// --------------------------------------------------------------------
// Format of a page header
// version: 1 byte
// checksum: 4 bytes - crc32 (little endian)
// flags: 1 byte
// page epoch: 4 bytes (little endian)
// page number: 8 bytes (little endian)
// latest offset: 2 bytes (little endian)
// --------------------------------------------------------------------

// --------------------------------------------------------------------
// Format of a log entry
// type: 1 byte
// length: var-uint64 bytes
// data: length of bytes
// --------------------------------------------------------------------

const (
	checkSumOffset          = 1
	flagsOffset             = checkSumOffset + 4
	pageEpochOffset         = flagsOffset + 1
	pageNumberOffset        = pageEpochOffset + 4
	latestEntryOffsetOffset = pageNumberOffset + 8
	pageHeaderSize          = latestEntryOffsetOffset + 2
)

type PageVersion uint8

const (
	FirstVersion PageVersion = iota + 1
)

type PageData []byte

func InitPage(p PageData, epoch Epoch, num PageNum) {
	p[0] = uint8(FirstVersion)
	binary.LittleEndian.PutUint32(p[pageEpochOffset:], epoch.val)
	binary.LittleEndian.PutUint64(p[pageNumberOffset:], uint64(num))
	binary.LittleEndian.PutUint16(p[latestEntryOffsetOffset:], uint16(pageHeaderSize-1))
}

func (p PageData) GetVersion() PageVersion {
	return PageVersion(p[0])
}

func (p PageData) GetEpoch() Epoch {
	num := binary.LittleEndian.Uint32(p[pageEpochOffset:])
	return NewEpoch(num)
}

func (p PageData) GetPageNum() PageNum {
	num := binary.LittleEndian.Uint64(p[pageNumberOffset:])
	return PageNum(num)
}

func (p PageData) GetLatestOffset() EntryOffset {
	num := binary.LittleEndian.Uint16(p[latestEntryOffsetOffset:])
	return EntryOffset(num)
}

func (p PageData) Write(writer io.Writer) error {
	return nil
}

// EntryType is type of log entry
type EntryType uint8

const (
	EntryTypeNone EntryType = iota
	EntryTypeFull
)
