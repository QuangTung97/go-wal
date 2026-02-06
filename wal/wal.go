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

func (w *WAL) NextRecoverEntry() bool {
	return false
}

type EntryReader struct {
}

func (r *EntryReader) Read(data []byte) (n int, hasNext bool) {
	return len(data), false
}

func (w *WAL) GetRecoveryEntry() EntryReader {
	return EntryReader{}
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

// Write need to be called inside mutex lock
func (w *WAL) Write(reader ByteReader) {
	prevPageNum := w.latestOffset.ToPageNum()
	isSplit := false

	for {
		nextLSN := (w.latestOffset + 1).ToLSN()
		// pageEndLSN := (nextLSN & PageNumMask) + (PageSize - 1)

		nextPageNum := nextLSN.ToPageNum()
		if nextPageNum > prevPageNum {
			// init the page in memory
			page := w.getInMemPage(nextPageNum)
			InitPage(&page, w.latestEpoch, nextPageNum)
			prevPageNum = nextPageNum
		}

		page := w.getInMemPage(nextPageNum)
		offset := nextLSN.WithinPage()

		remainLen := PageSize - offset - logEntryDataOffset // must have the full entry header
		if isSplit {
			remainLen = PageSize - offset // if entry is split => not have to account for entry header
		}

		if remainLen <= 0 {
			// add the remaining bytes (ignoring entry header)
			w.latestOffset += LogDataOffset(PageSize - offset)
			continue
		}

		writeLen := reader.Len()
		withBreak := true
		if remainLen < uint64(reader.Len()) {
			writeLen = int64(remainLen)
			withBreak = false
		}

		if isSplit {
			written := WriteLogEntryDataOnly(page.data[offset:], reader, writeLen)
			w.latestOffset += LogDataOffset(written)
		} else {
			written := WriteLogEntry(page.data[offset:], EntryTypeNormal, reader, writeLen)
			w.latestOffset += LogDataOffset(written)
		}

		if withBreak {
			break
		}

		isSplit = true
	}
}

// NotifyWriter needs to be called inside mutex lock
func (w *WAL) NotifyWriter() {
	w.writtenLsn = w.latestOffset.ToLSN()
	w.cond.Signal()
}

func (w *WAL) getInMemPage(num PageNum) Page {
	offset := num % w.memNumPage
	return Page{
		data: w.logBuffer[offset*PageSize : (offset+1)*PageSize],
	}
}
