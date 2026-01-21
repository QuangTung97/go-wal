package filesys

import "io"

type IdempotentCloser interface {
	Close() error
	CloseIgnoreError()
}

func NewIdempotentCloser(closer io.Closer) IdempotentCloser {
	return &idempotentCloserImpl{
		closer: closer,
	}
}

type idempotentCloserImpl struct {
	closer io.Closer

	closed bool
	err    error
}

func (c *idempotentCloserImpl) Close() error {
	if c.closed {
		return c.err
	}
	c.err = c.closer.Close()
	return c.err
}

func (c *idempotentCloserImpl) CloseIgnoreError() {
	_ = c.Close()
}
