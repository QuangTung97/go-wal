package wal

import "io"

// --------------------------------------------------------------------
// Format of a page header
// version: 1 byte
// checksum: 4 bytes - crc32 (little endian)
// flags: 1 byte
// page generation: 8 bytes (little endian)
// page number: 8 bytes (little endian)
// total: 1 + 4 + 1 + 8 + 8 = 22 bytes
// --------------------------------------------------------------------

// --------------------------------------------------------------------
// Format of a log entry
// type: 1 byte
// length: var-uint64 bytes
// data: length of bytes
// --------------------------------------------------------------------

type PageVersion uint8

type CRC32Sum uint32

const (
	FirstVersion PageVersion = iota + 1
)

type PageData []byte

func (p PageData) Write(writer io.Writer) error {
	return nil
}

// EntryType is type of log entry
type EntryType uint8

const (
	EntryTypeNone EntryType = iota
	EntryTypeFull
)

const (
	checkSumOffset   = 1
	flagsOffset      = checkSumOffset + 4
	pageEpochOffset  = flagsOffset + 1
	pageNumberOffset = pageEpochOffset + 4
	pageHeaderSize   = pageNumberOffset + 8
)
