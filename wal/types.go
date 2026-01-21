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

type LogDataOffset uint64

func (o LogDataOffset) ToLSN() LSN {
	pageNum := uint64(o) / DataSizePerPage
	offsetInPage := uint64(o) % DataSizePerPage
	return LSN(pageNum<<PageSizeLog + pageHeaderSize + offsetInPage)
}

type PageNum uint64

type PageGeneration uint32
