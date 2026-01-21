package wal

import (
	"fmt"
)

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

// EntryType is type of log entry
type EntryType uint8

const (
	EntryTypeNone EntryType = iota
	EntryTypeFull
)

type Page struct {
	data []byte

	generation PageGeneration
	pageNum    PageNum

	lastOffset int
}

func InitPage(page *Page, gen PageGeneration, num PageNum) {
	// TODO impl
}

func ParsePage(page *Page, data []byte) error {
	if len(data) != PageSize {
		return fmt.Errorf("invalid page size: %d", len(data))
	}
	return nil
}

func (p *Page) WriteTo(data []byte) {
	// TODO impl
}

func (p *Page) AddEntry(data []byte) {

}

const (
	checkSumOffset       = 1
	flagsOffset          = checkSumOffset + 4
	pageGenerationOffset = flagsOffset + 1
	pageNumberOffset     = pageGenerationOffset + 8
	pageHeaderSize       = pageNumberOffset + 8
)
