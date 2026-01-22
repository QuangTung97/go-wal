package wal

import (
	"sync"

	"github.com/QuangTung97/go-wal/wal/filesys"
)

type WAL struct {
	fs          filesys.FileSystem
	filename    string
	diskNumPage PageNum
	memNumPage  PageNum

	mut          sync.Mutex
	logBuffer    []byte
	writeErr     error
	latestOffset LogDataOffset // => LSN => PageNum

	wg       sync.WaitGroup
	cond     *sync.Cond
	isClosed bool
}

var _ sync.Locker = &WAL{}

func NewWAL(
	fs filesys.FileSystem, filename string,
	fileSize int64, logBufferSize int64,
) (*WAL, error) {
	w := &WAL{
		fs:       fs,
		filename: filename,

		diskNumPage: PageNum(fileSize / PageSize),
		memNumPage:  PageNum(logBufferSize / PageSize),

		latestOffset: DataSizePerPage - 1,
	}

	w.cond = sync.NewCond(&w.mut)

	// TODO validate

	w.logBuffer = make([]byte, w.memNumPage*PageSize)

	_, err := w.createWalFileIfNotExists()
	if err != nil {
		return nil, err
	}

	// TODO previous log existed

	w.wg.Add(1)
	go w.runWriterInBackground()
	// TODO run goroutine

	return w, nil
}

func (w *WAL) Lock() {
	w.mut.Lock()
}

func (w *WAL) Unlock() {
	w.mut.Unlock()
}

// Shutdown does need to use Lock() & Unlock()
func (w *WAL) Shutdown() {
	w.Lock()
	prevClosed := w.isClosed
	w.isClosed = true
	w.Unlock()

	if prevClosed {
		return
	}

	w.cond.Signal()
	w.wg.Wait()
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
	writer, err := w.fs.CreateEmptyFile(tempFileName, int64(w.diskNumPage)*PageSize)
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
	wal *WAL

	fromOffset LogDataOffset
	dataSize   int64
}

func (r *NewEntryRequest) Write(data []byte) {
}

func (r *NewEntryRequest) Finish() {
}

func (r *NewEntryRequest) GetEndLSN() LSN {
	endOffset := r.fromOffset + LogDataOffset(r.dataSize)
	return endOffset.ToLSN()
}

func (w *WAL) NewEntry(dataSize int64) (NewEntryRequest, error) {
	// TODO wait on checkpoint

	// TODO how to deal with WAL writer error?

	from := w.latestOffset + 1
	w.latestOffset += LogDataOffset(dataSize)

	return NewEntryRequest{
		fromOffset: from,
		dataSize:   dataSize,
	}, nil
}

func (w *WAL) NotifyWriter() {
	w.cond.Signal()
}

func (w *WAL) runWriterInBackground() {
	defer w.wg.Done()
	for {
		closed := w.runWriterInBackgroundPerIteration()
		if closed {
			return
		}
	}
}

func (w *WAL) runWriterInBackgroundPerIteration() bool {
	w.mut.Lock()
	defer w.mut.Unlock()

	needWait := func() bool {
		if w.isClosed {
			return false
		}
		return true
	}

	for needWait() {
		w.cond.Wait()
	}

	return w.isClosed
}
