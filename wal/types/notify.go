package types

type LSNWaiter interface {
	WaitLSN(lsn LSN)
	SetLSN(lsn LSN)
}
