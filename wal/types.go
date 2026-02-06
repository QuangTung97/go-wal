package wal

const (
	PageSizeLog    = 9
	PageSize       = 1 << PageSizeLog
	WithinPageMask = PageSize - 1
	PageNumMask    = ^LSN(WithinPageMask)

	DataSizePerPage = PageSize - pageHeaderSize
)

type LSN uint64

func (n LSN) ToPageNum() PageNum {
	return PageNum(n&PageNumMask) >> PageSizeLog
}

func (n LSN) ToOffset() LogDataOffset {
	pageNum := n.ToPageNum()
	startLSN := LSN(pageNum << PageSizeLog)
	index := LogDataOffset(n - startLSN)
	return LogDataOffset(pageNum)*DataSizePerPage + index - pageHeaderSize
}

// WithinPage get the byte offset within the page
func (n LSN) WithinPage() uint64 {
	return uint64(n) & WithinPageMask
}

type LogDataOffset uint64

func (o LogDataOffset) ToPageNum() PageNum {
	pageNum := uint64(o) / DataSizePerPage
	return PageNum(pageNum)
}

func (o LogDataOffset) ToLSN() LSN {
	pageNum := o.ToPageNum()
	offsetInPage := uint64(o) % DataSizePerPage
	return LSN(uint64(pageNum)<<PageSizeLog + pageHeaderSize + offsetInPage)
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

// TODO add compare
