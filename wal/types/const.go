package types

const (
	PageSizeLog     = 9
	PageSize        = 1 << PageSizeLog
	WithinPageMask  = PageSize - 1
	PageNumMask     = ^LSN(WithinPageMask)
	DataSizePerPage = PageSize - PageHeaderSize
)

const (
	PageCheckSumOffset = 1
	PageFlagsOffset    = PageCheckSumOffset + 4
	PageEpochOffset    = PageFlagsOffset + 1
	PageNumberOffset   = PageEpochOffset + 4
	PageHeaderSize     = PageNumberOffset + 8
)
