package wal

const (
	PageSizeLog = 9
	PageSize    = 1 << PageSizeLog
	PageNumMask = ^uint64(PageSize - 1)
)

type LSN uint64

func (n LSN) ToPageNum() PageNum {
	return PageNum(uint64(n)&PageNumMask) >> PageSizeLog
}

type PageNum uint64

type PageGeneration uint32
