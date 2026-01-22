package wal

func (w *WAL) runWriterInBackground() {
	defer w.wg.Done()
	for {
		closed := w.runWriterInBackgroundPerIteration()
		if closed {
			return
		}
	}
}

func (w *WAL) runWriterInBackgroundPerIteration() bool {
	w.mut.Lock()
	defer w.mut.Unlock()

	needWait := func() bool {
		if w.isClosed {
			return false
		}
		return true
	}

	for needWait() {
		w.cond.Wait()
	}

	return w.isClosed
}
