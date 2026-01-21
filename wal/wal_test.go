package wal

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/QuangTung97/go-wal/wal/filesys"
)

type walTest struct {
	filename string
	wal      *WAL
}

func newWalTest(t *testing.T) *walTest {
	w := &walTest{}

	tempDir := t.TempDir()
	w.filename = filepath.Join(tempDir, "wal01")
	fs := filesys.NewFileSystem()

	var err error
	w.wal, err = NewWAL(fs, w.filename, PageSize*4)
	if err != nil {
		panic(err)
	}

	return w
}

func TestWAL__Normal(t *testing.T) {
	w := newWalTest(t)
	assert.Equal(t, LSN(PageSize-1), w.wal.GetLatestLSN())

	// check file size
	fileStat, err := os.Stat(w.filename)
	require.Equal(t, nil, err)
	assert.Equal(t, int64(512*5), fileStat.Size())
}
