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

	mut       sync.Mutex
	logBuffer []byte
	writeErr  error

	latestOffset LogDataOffset
	writtenLsn   LSN

	latestEpoch   Epoch
	checkpointLsn LSN

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
	}

	w.cond = sync.NewCond(&w.mut)

	// TODO validate

	w.logBuffer = make([]byte, w.memNumPage*PageSize)

	_, err := w.createWalFileIfNotExists()
	if err != nil {
		return nil, err
	}

	w.latestOffset = DataSizePerPage - 1
	w.writtenLsn = PageSize - 1
	w.checkpointLsn = PageSize - 1

	// TODO init first page in memory

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
