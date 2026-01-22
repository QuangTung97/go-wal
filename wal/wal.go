package wal

import (
	"sync"

	"github.com/QuangTung97/go-wal/wal/filesys"
)

type WAL struct {
	fs          filesys.FileSystem
	filename    string
	logFileSize int64

	mut          sync.Mutex
	logBuffer    []byte
	writeErr     error
	latestOffset LogDataOffset // => LSN => PageNum
}

func NewWAL(
	fs filesys.FileSystem, filename string,
	fileSize int64, logBufferSize int64,
) (*WAL, error) {
	w := &WAL{
		fs:          fs,
		filename:    filename,
		logFileSize: fileSize,

		logBuffer:    make([]byte, logBufferSize),
		latestOffset: DataSizePerPage - 1,
	}

	_, err := w.createWalFileIfNotExists()
	if err != nil {
		return nil, err
	}

	// TODO previous log existed

	// TODO round log buffer size

	return w, nil
}

func (w *WAL) createWalFileIfNotExists() (bool, error) {
	existed, err := w.fs.Exists(w.filename)
	if err != nil {
		return false, err
	}
	if existed {
		return true, nil
	}

	tempFileName := w.filename + ".tmp"
	if err := w.createTemporaryWalFile(tempFileName); err != nil {
		return false, err
	}

	if err := w.fs.Rename(tempFileName, w.filename); err != nil {
		return false, err
	}

	// TODO fsync the parent dir

	return false, nil
}

func (w *WAL) createTemporaryWalFile(tempFileName string) error {
	writer, err := w.fs.CreateEmptyFile(tempFileName, w.logFileSize)
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

	// TODO write to remaining pages

	// TODO fsync the tmp file

	return closer.Close()
}

type NewEntryRequest struct {
	dataSize int64
}

func (w *WAL) NewEntry(dataSize int64) (NewEntryRequest, error) {
	// TODO how to deal with WAL writer error?
	return NewEntryRequest{
		dataSize: dataSize,
	}, nil
}
