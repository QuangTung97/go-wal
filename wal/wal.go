package wal

type WAL struct {
}

func (w *WAL) AddLogEntry(data []byte) LSN {
	return 0
}
