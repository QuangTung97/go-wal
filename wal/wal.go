package wal

type WAL struct {
	nextOffset LogDataOffset
}

type PrepareEntryRequest struct {
	lsn       LSN
	dataSize  int64
	totalSize int64
}

func (r PrepareEntryRequest) GetLSN() LSN {
	return r.lsn
}

func (w *WAL) PrepareEntry(dataSize int64) PrepareEntryRequest {
	return PrepareEntryRequest{}
}
