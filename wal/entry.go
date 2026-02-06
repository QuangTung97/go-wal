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
	EntryTypeNormal
	EntryTypeFull // TODO remove
	EntryTypeFirst
	EntryTypeMiddle
	EntryTypeLast
)

func WriteLogEntry(
	pageData []byte, entryType EntryType,
	reader ByteReader, dataLen int64,
) int64 {
	pageData[0] = byte(entryType)

	binary.LittleEndian.PutUint16(
		pageData[logEntryDataLengthOffset:logEntryDataOffset],
		uint16(dataLen),
	)

	pageData = pageData[logEntryDataOffset:]
	dataLen = WriteLogEntryDataOnly(pageData, reader, dataLen)
	return logEntryDataOffset + dataLen
}

func WriteLogEntryDataOnly(pageData []byte, reader ByteReader, dataLen int64) int64 {
	newLen := reader.Len() - dataLen
	for reader.Len() > newLen {
		remainSize := reader.Len() - newLen
		tmpData := reader.Read(remainSize)
		copy(pageData, tmpData)
		pageData = pageData[len(tmpData):]
	}
	return dataLen
}

func ReadLogEntry(pageData []byte) (EntryType, []byte, int64) {
	entryType := EntryType(pageData[0])
	if entryType == EntryTypeNone {
		return EntryTypeNone, nil, 1
	}

	dataLen := binary.LittleEndian.Uint16(
		pageData[logEntryDataLengthOffset:logEntryDataOffset],
	)

	// TODO validate entry
	return entryType, pageData[logEntryDataOffset : logEntryDataOffset+dataLen], logEntryDataOffset + int64(dataLen)
}
