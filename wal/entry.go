package wal

import (
	"encoding/binary"
)

// --------------------------------------------------------------------
// Format of a log entry
// type: 1 byte
// length: 2 bytes
// data: length of bytes
// --------------------------------------------------------------------

const (
	logEntryDataLengthOffset = 1
	logEntryDataOffset       = logEntryDataLengthOffset + 2
)

// EntryType is type of log entry
type EntryType uint8

const (
	EntryTypeNone EntryType = iota
	EntryTypeFull
	EntryTypeFirst
	EntryTypeMiddle
	EntryTypeLast
)

func WriteLogEntry(pageData []byte, entryType EntryType, data []byte) error {
	pageData[0] = byte(entryType)
	binary.LittleEndian.PutUint16(
		pageData[logEntryDataLengthOffset:logEntryDataOffset],
		uint16(len(data)),
	)
	copy(pageData[logEntryDataOffset:], data)
	return nil
}

func ReadLogEntry(pageData []byte) (EntryType, []byte, error) {
	entryType := EntryType(pageData[0])
	dataLen := binary.LittleEndian.Uint16(
		pageData[logEntryDataLengthOffset:logEntryDataOffset],
	)
	return entryType, pageData[logEntryDataOffset : logEntryDataOffset+dataLen], nil
}
