package wal

type ByteReader interface {
	Read(maxSize int64) []byte
	Len() int64
}

type simpleByteReader struct {
	data []byte
}

func NewSimpleByteReader(data []byte) ByteReader {
	return &simpleByteReader{
		data: data,
	}
}

func (r *simpleByteReader) Read(maxSize int64) []byte {
	n := int64(len(r.data))
	n = min(n, maxSize)

	output := r.data[:n]
	r.data = r.data[n:]
	return output
}

func (r *simpleByteReader) Len() int64 {
	return int64(len(r.data))
}
