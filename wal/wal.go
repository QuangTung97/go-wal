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

	writer, err := fs.CreateEmptyFile(filename, fileSize+PageSize)
	if err != nil {
		return err
	}

	// setup closer
	closer := filesys.NewIdempotentCloser(writer)
	defer closer.CloseIgnoreError()

	// TODO write to file

	if err := closer.Close(); err != nil {
		return err
	}

	// TODO rename

	return nil
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
