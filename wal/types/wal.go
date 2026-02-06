package types

// WalWriter is thread safe
type WalWriter interface {
	NewEntry(dataLen int64) LogEntryWriter
}

// LogEntryWriter is NOT thread safe
type LogEntryWriter interface {
	GetLastLSN() LSN // lsn of the highest byte
	Write(data []byte)
	Finish()
}
