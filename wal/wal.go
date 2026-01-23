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
	w.writtenLsn = w.checkpointLsn

	firstPage := w.getInMemPage(w.checkpointLsn.ToPageNum())
	InitPage(&firstPage, NewEpoch(0), w.checkpointLsn.ToPageNum())

	// TODO previous log existed

	return w, nil
}

func (w *WAL) FinishRecover() {
	w.latestEpoch.Inc()

	// TODO write the new latest epoch

	w.wg.Add(1)
	go w.runWriterInBackground()
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
	wal        *WAL
	remainSize int64
}

func (r *NewEntryRequest) Write(data []byte) {
	prevPageNum := r.wal.latestOffset.ToPageNum()

	nextLSN := (r.wal.latestOffset + 1).ToLSN()
	// pageEndLSN := (nextLSN & PageNumMask) + (PageSize - 1)

	nextPageNum := nextLSN.ToPageNum()
	if nextPageNum > prevPageNum {
		page := r.wal.getInMemPage(nextPageNum)
		InitPage(&page, r.wal.latestEpoch, nextPageNum)
	}
}

func (r *NewEntryRequest) Finish() {
}

func (w *WAL) NewEntry(dataSize int64) (NewEntryRequest, error) {
	// TODO wait on checkpoint

	// TODO how to deal with WAL writer error?

	return NewEntryRequest{
		wal:        w,
		remainSize: dataSize,
	}, nil
}

func (w *WAL) NotifyWriter() {
	w.cond.Signal()
}

func (w *WAL) getInMemPage(num PageNum) Page {
	offset := num % w.memNumPage
	return Page{
		data: w.logBuffer[offset*PageSize : (offset+1)*PageSize],
	}
}
