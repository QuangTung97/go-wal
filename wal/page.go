package wal

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
)

// --------------------------------------------------------------------
// Format of a page header
// version: 1 byte
// checksum: 4 bytes - crc32 (little endian)
// flags: 1 byte
// page epoch: 4 bytes (little endian)
// page number: 8 bytes (little endian)
// --------------------------------------------------------------------

const (
	checkSumOffset   = 1
	flagsOffset      = checkSumOffset + 4
	pageEpochOffset  = flagsOffset + 1
	pageNumberOffset = pageEpochOffset + 4
	pageHeaderSize   = pageNumberOffset + 8
)

type PageVersion uint8

type PageFlags uint8

const (
	PageFlagsNotFullMask PageFlags = 1 << iota
	PageFlagsTruncatedMask
)

func (f *PageFlags) IsNotFull() bool {
	return (*f & PageFlagsNotFullMask) != 0
}

func (f *PageFlags) SetNotFull(enabled bool) {
	if enabled {
		*f |= PageFlagsNotFullMask
	} else {
		*f &= ^PageFlagsNotFullMask
	}
}

func (f *PageFlags) IsTruncated() bool {
	return (*f & PageFlagsTruncatedMask) != 0
}

func (f *PageFlags) SetTruncated(enabled bool) {
	if enabled {
		*f |= PageFlagsTruncatedMask
	} else {
		*f &= ^PageFlagsTruncatedMask
	}
}

const (
	FirstVersion PageVersion = iota + 1
)

type Page struct {
	data []byte // must have cap = len = 512
}

var pageWithZeros [PageSize]byte

func InitPage(p *Page, epoch Epoch, num PageNum) {
	// clear page with zeros
	copy(p.data[:], pageWithZeros[:])

	p.data[0] = uint8(FirstVersion)
	binary.LittleEndian.PutUint32(p.data[pageEpochOffset:], epoch.val)
	binary.LittleEndian.PutUint64(p.data[pageNumberOffset:], uint64(num))
}

func (p *Page) GetVersion() PageVersion {
	return PageVersion(p.data[0])
}

func (p *Page) GetEpoch() Epoch {
	num := binary.LittleEndian.Uint32(p.data[pageEpochOffset:])
	return NewEpoch(num)
}

func (p *Page) GetPageNum() PageNum {
	num := binary.LittleEndian.Uint64(p.data[pageNumberOffset:])
	return PageNum(num)
}

func (p *Page) GetFlags() *PageFlags {
	return (*PageFlags)(&p.data[flagsOffset])
}

func (p *Page) GetLogData() []byte {
	return p.data[pageHeaderSize:]
}

func (p *Page) Write(writer io.Writer) error {
	crcSum := crc32.ChecksumIEEE(p.data[:])
	binary.LittleEndian.PutUint32(p.data[checkSumOffset:], crcSum)
	_, err := writer.Write(p.data[:])
	p.clearChecksum()
	return err
}

func (p *Page) clearChecksum() {
	// set crc sum to zero
	var zeroSum [4]byte
	copy(p.data[checkSumOffset:], zeroSum[:])
}

func ReadPage(p *Page, reader io.Reader) error {
	if _, err := io.ReadFull(reader, p.data[:]); err != nil {
		return err
	}

	crcSum := binary.LittleEndian.Uint32(p.data[checkSumOffset:])
	p.clearChecksum()
	computedSum := crc32.ChecksumIEEE(p.data[:])
	if computedSum != crcSum {
		return errors.New("mismatch page checksum")
	}

	return nil
}

type pageIterator struct {
	remainBytes []byte
	entryType   EntryType
	entryData   []byte
}

func (p *Page) newIterator() pageIterator {
	return pageIterator{
		remainBytes: p.data[pageHeaderSize:],
	}
}

func (i *pageIterator) next() bool {
	if len(i.remainBytes) == 0 {
		return false
	}

	var consumed int64
	i.entryType, i.entryData, consumed = ReadLogEntry(i.remainBytes)
	i.remainBytes = i.remainBytes[consumed:]

	return true
}
