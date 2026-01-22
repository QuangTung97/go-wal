package wal

import (
	"sync"

	"github.com/QuangTung97/go-wal/wal/filesys"
)

type WAL struct {
	mut sync.Mutex

	writeErr     error
	latestOffset LogDataOffset // => LSN => PageNum
}

func NewWAL(fs filesys.FileSystem, filename string, fileSize int64) (*WAL, error) {
	if err := createWalFileIfNotExists(fs, filename, fileSize); err != nil {
		return nil, err
	}

	return &WAL{
		latestOffset: DataSizePerPage - 1,
	}, nil
}

func createWalFileIfNotExists(fs filesys.FileSystem, filename string, fileSize int64) error {
	existed, err := fs.Exists(filename)
	if err != nil {
		return err
	}
	if existed {
		return nil
	}

	tempFileName := filename + ".tmp"
	if err := createTemporaryWalFile(fs, tempFileName, fileSize); err != nil {
		return err
	}

	if err := fs.Rename(tempFileName, filename); err != nil {
		return err
	}

	// TODO fsync the parent dir

	return nil
}

func createTemporaryWalFile(fs filesys.FileSystem, tempFileName string, fileSize int64) error {
	writer, err := fs.CreateEmptyFile(tempFileName, fileSize+PageSize)
	if err != nil {
		return err
	}

	// setup closer
	closer := filesys.NewIdempotentCloser(writer)
	defer closer.CloseIgnoreError()

	masterPage := &MasterPage{
		Version:       MasterPageFirstVersion,
		LatestEpoch:   NewEpoch(0),
		CheckpointLSN: PageSize - 1,
	}
	if err := WriteMasterPage(writer, masterPage); err != nil {
		return err
	}

	// Write to first page

	// TODO write to other pages

	// TODO fsync the tmp file

	return closer.Close()
}

type PrepareEntryRequest struct {
	lsn       LSN
	dataSize  int64
	totalSize int64
}

func (r *PrepareEntryRequest) GetLSN() LSN {
	return r.lsn
}

func (w *WAL) PrepareEntry(dataSize int64) (PrepareEntryRequest, error) {
	// TODO how to deal with WAL writer error?
	return PrepareEntryRequest{}, nil
}

func (w *WAL) GetLatestLSN() LSN {
	w.mut.Lock()
	offset := w.latestOffset
	w.mut.Unlock()
	return offset.ToLSN()
}
