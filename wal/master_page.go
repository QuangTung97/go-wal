package wal

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
)

// --------------------------------------------------------------------
// Format of master page
// version: 1 byte
// checksum: 4 bytes - crc32 (little endian)
// latest generation number: 8 bytes (little endian)
// checkpoint lsn: 8 bytes (little endian)
// --------------------------------------------------------------------

const (
	masterPageChecksumOffset    = 1
	masterPageLatestEpochOffset = masterPageChecksumOffset + 4
	masterPageCheckpointOffset  = masterPageLatestEpochOffset + 4
)

type MasterPageVersion uint8

const (
	MasterPageFirstVersion MasterPageVersion = iota + 1
)

type MasterPage struct {
	Version       MasterPageVersion
	LatestEpoch   Epoch
	CheckpointLSN LSN
}

func WriteMasterPage(w io.Writer, page *MasterPage) error {
	var data [PageSize]byte

	data[0] = byte(page.Version)
	binary.LittleEndian.PutUint64(
		data[masterPageLatestEpochOffset:],
		uint64(page.LatestEpoch.val),
	)
	binary.LittleEndian.PutUint64(
		data[masterPageCheckpointOffset:],
		uint64(page.CheckpointLSN),
	)

	// write checksum
	crcSum := crc32.ChecksumIEEE(data[:])
	binary.LittleEndian.PutUint32(
		data[masterPageChecksumOffset:],
		crcSum,
	)

	_, err := w.Write(data[:])
	return err
}

func ReadMasterPage(r io.Reader, page *MasterPage) error {
	var data [PageSize]byte

	if _, err := io.ReadFull(r, data[:]); err != nil {
		return err
	}

	crcSum := binary.LittleEndian.Uint32(data[masterPageChecksumOffset:])
	var zeroSum [4]byte
	copy(data[masterPageChecksumOffset:], zeroSum[:])

	computedSum := crc32.ChecksumIEEE(data[:])
	if computedSum != crcSum {
		return errors.New("mismatch master page checksum")
	}

	latestGen := binary.LittleEndian.Uint32(data[masterPageLatestEpochOffset:])
	checkpoint := binary.LittleEndian.Uint64(data[masterPageCheckpointOffset:])

	*page = MasterPage{
		Version:       MasterPageVersion(data[0]),
		LatestEpoch:   NewEpoch(latestGen),
		CheckpointLSN: LSN(checkpoint),
	}

	return nil
}
