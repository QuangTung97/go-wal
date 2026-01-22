package wal

import (
	"github.com/QuangTung97/go-wal/wal/filesys"
)

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
	writer, err := w.fs.CreateEmptyFile(tempFileName, int64(w.diskNumPage)*PageSize)
	if err != nil {
		return err
	}

	// setup closer
	closer := filesys.NewIdempotentCloser(writer)
	defer closer.CloseIgnoreError()

	w.latestEpoch = NewEpoch(0)
	w.checkpointLsn = PageSize - 1

	masterPage := &MasterPage{
		Version:       MasterPageFirstVersion,
		LatestEpoch:   w.latestEpoch,
		CheckpointLSN: w.checkpointLsn,
	}

	if err := WriteMasterPage(writer, masterPage); err != nil {
		return err
	}

	// Write to first page

	// TODO write to remaining pages

	// TODO fsync the tmp file

	return closer.Close()
}
