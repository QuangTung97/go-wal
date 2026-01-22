package wal

const (
	PageSizeLog = 9
	PageSize    = 1 << PageSizeLog
	PageNumMask = ^uint64(PageSize - 1)

	DataSizePerPage = PageSize - pageHeaderSize
)

type LSN uint64

func (n LSN) ToPageNum() PageNum {
	return PageNum(uint64(n)&PageNumMask) >> PageSizeLog
}

func (n LSN) ToOffset() LogDataOffset {
	pageNum := n.ToPageNum()
	startLSN := LSN(pageNum << PageSizeLog)
	index := LogDataOffset(n - startLSN)
	return LogDataOffset(pageNum)*DataSizePerPage + index - pageHeaderSize
}

type LogDataOffset uint64

func (o LogDataOffset) ToLSN() LSN {
	pageNum := uint64(o) / DataSizePerPage
	offsetInPage := uint64(o) % DataSizePerPage
	return LSN(pageNum<<PageSizeLog + pageHeaderSize + offsetInPage)
}

type PageNum uint64

type Epoch struct {
	val uint32
}

func NewEpoch(num uint32) Epoch {
	return Epoch{val: num}
}

func (e *Epoch) Inc() {
	e.val++
}
