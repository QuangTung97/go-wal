package wal

// --------------------------------------------------------------------
// Format of master page
// version: 1 byte
// checksum: 4 bytes - crc32 (little endian)
// latest generation number: 8 bytes (little endian)
// checkpoint lsn: 8 bytes (little endian)
// --------------------------------------------------------------------

type MasterPageVersion uint8

const (
	MasterPageVersionFirst MasterPageVersion = iota + 1
)

type MasterPage struct {
	Version          MasterPageVersion
	LatestGeneration PageGeneration
	CheckpointLSN    LSN
}
