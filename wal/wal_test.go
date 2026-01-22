package wal

import (
	"bytes"
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
	// 2 pages in mem
	// 4 pages on disk
	w.wal, err = NewWAL(fs, w.filename, PageSize*5, PageSize*2)
	if err != nil {
		panic(err)
	}

	t.Cleanup(w.wal.Shutdown)

	return w
}

func TestWAL__Init_And_Check_Master_Page(t *testing.T) {
	w := newWalTest(t)

	// check file size
	fileStat, err := os.Stat(w.filename)
	require.Equal(t, nil, err)
	assert.Equal(t, int64(512*5), fileStat.Size())

	// check file content
	allData, err := os.ReadFile(w.filename)
	require.Equal(t, nil, err)
	reader := bytes.NewReader(allData)

	// check master page
	var masterPage MasterPage
	err = ReadMasterPage(reader, &masterPage)
	require.Equal(t, nil, err)
	require.Equal(t, MasterPage{
		Version:       1,
		LatestEpoch:   NewEpoch(0),
		CheckpointLSN: 511,
	}, masterPage)

	// check init data
	assert.Equal(t, PageSize*2, len(w.wal.logBuffer))
	assert.Equal(t, LogDataOffset(DataSizePerPage-1), w.wal.latestOffset)
	assert.Equal(t, LSN(PageSize-1), w.wal.writtenLsn)
	assert.Equal(t, NewEpoch(0), w.wal.latestEpoch)
	assert.Equal(t, LSN(PageSize-1), w.wal.checkpointLsn)

	// shutdown
	w.wal.Shutdown()
}
