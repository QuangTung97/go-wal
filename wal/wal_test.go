package wal

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/QuangTung97/go-wal/wal/filesys"
)

func joinStrings(list ...string) string {
	return strings.Join(list, "")
}

type walTest struct {
	filename string
	wal      *WAL
}

func newWalTest(t *testing.T, pageOnDisk int64, pageOnMem int64) *walTest {
	w := &walTest{}

	tempDir := t.TempDir()
	w.filename = filepath.Join(tempDir, "wal01")
	fs := filesys.NewFileSystem()

	var err error
	// 2 pages in mem
	// 4 pages on disk
	w.wal, err = NewWAL(fs, w.filename, PageSize*pageOnDisk, PageSize*pageOnMem)
	if err != nil {
		panic(err)
	}

	t.Cleanup(w.wal.Shutdown)

	return w
}

func TestWAL__Init_And_Check_Master_Page(t *testing.T) {
	w := newWalTest(t, 5, 2)

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

	// get first page
	firstPage := w.wal.getInMemPage(0)
	assert.Equal(t, FirstVersion, firstPage.GetVersion())
	assert.Equal(t, NewEpoch(0), firstPage.GetEpoch())
	assert.Equal(t, PageNum(0), firstPage.GetPageNum())

	w.wal.FinishRecover()

	// shutdown
	w.wal.Shutdown()
}

func (w *walTest) addEntry(input string) {
	reader := NewSimpleByteReader([]byte(input))
	w.wal.Write(reader)
}

func TestWAL__Add_Entry__Check_In_Memory(t *testing.T) {
	w := newWalTest(t, 100, 20)

	w.wal.FinishRecover()
	w.addEntry("test 01")

	// check second page
	secondPage := w.wal.getInMemPage(1)
	assert.Equal(t, FirstVersion, secondPage.GetVersion())
	assert.Equal(t, NewEpoch(1), secondPage.GetEpoch())
	assert.Equal(t, PageNum(1), secondPage.GetPageNum())

	it := secondPage.newIterator()
	assert.Equal(t, true, it.next())
	assert.Equal(t, EntryTypeNormal, it.entryType)
	assert.Equal(t, "test 01", string(it.entryData))

	// check third page, not yet init
	page3 := w.wal.getInMemPage(2)
	assert.Equal(t, PageVersion(0), page3.GetVersion())
	assert.Equal(t, NewEpoch(0), page3.GetEpoch())
	assert.Equal(t, PageNum(0), page3.GetPageNum())
}

func TestWAL__Add_Big_Entry__Check_In_Memory(t *testing.T) {
	w := newWalTest(t, 100, 20)

	w.wal.FinishRecover()
	w.addEntry("input01") // add simple entry

	inputStr := joinStrings(
		strings.Repeat("A", 200),
		strings.Repeat("B", 300),
		strings.Repeat("C", 500),
	)
	w.addEntry(inputStr) // add second big entry

	// ----------------------------
	// check second page
	// ----------------------------
	page2 := w.wal.getInMemPage(1)
	assert.Equal(t, FirstVersion, page2.GetVersion())
	assert.Equal(t, NewEpoch(1), page2.GetEpoch())
	assert.Equal(t, PageNum(1), page2.GetPageNum())

	// check first entry
	it := page2.newIterator()
	assert.Equal(t, true, it.next())
	assert.Equal(t, EntryTypeNormal, it.entryType)
	assert.Equal(t, "input01", string(it.entryData))

	// next entry
	assert.Equal(t, true, it.next())
	assert.Equal(t, EntryTypeNormal, it.entryType)
	assert.Equal(t, strings.Repeat("A", 200)+strings.Repeat("B", 281), string(it.entryData))

	// no next
	assert.Equal(t, false, it.next())

	// ----------------------------
	// check third page
	// ----------------------------
	page3 := w.wal.getInMemPage(2)
	assert.Equal(t, FirstVersion, page3.GetVersion())
	assert.Equal(t, NewEpoch(1), page3.GetEpoch())
	assert.Equal(t, PageNum(2), page3.GetPageNum())

	assert.Equal(t,
		strings.Repeat("B", 19)+strings.Repeat("C", 512-19-pageHeaderSize),
		string(page3.GetLogData()),
	)

	// ----------------------------
	// check forth page
	// ----------------------------
	page4 := w.wal.getInMemPage(3)
	assert.Equal(t, FirstVersion, page4.GetVersion())
	assert.Equal(t, NewEpoch(1), page4.GetEpoch())
	assert.Equal(t, PageNum(3), page4.GetPageNum())

	assert.Equal(t, strings.Repeat("C", 25)+"\x00", string(page4.GetLogData()[:26]))
}

func TestWAL__Add_Entry__Over_Max_Page(t *testing.T) {
	w := newWalTest(t, 100, 20)

	w.wal.FinishRecover()
	w.addEntry("input01") // add simple entry

	inputStr := joinStrings(
		strings.Repeat("A", 200),
		strings.Repeat("B", 281-3),
	)
	w.addEntry(inputStr) // add big entry

	w.addEntry("y") // add single byte

	// ----------------------------
	// check second page
	// ----------------------------
	page2 := w.wal.getInMemPage(1)
	assert.Equal(t, FirstVersion, page2.GetVersion())
	assert.Equal(t, NewEpoch(1), page2.GetEpoch())
	assert.Equal(t, PageNum(1), page2.GetPageNum())

	// check first entry
	it := page2.newIterator()
	assert.Equal(t, true, it.next())
	assert.Equal(t, EntryTypeFull, it.entryType)
	assert.Equal(t, "input01", string(it.entryData))

	// next entry
	assert.Equal(t, true, it.next())
	assert.Equal(t, EntryTypeFull, it.entryType)
	assert.Equal(t, strings.Repeat("A", 200)+strings.Repeat("B", 281-3), string(it.entryData))

	// none entry
	assert.Equal(t, true, it.next())
	assert.Equal(t, EntryTypeNone, it.entryType)
	assert.Equal(t, true, it.next())
	assert.Equal(t, EntryTypeNone, it.entryType)
	assert.Equal(t, true, it.next())
	assert.Equal(t, EntryTypeNone, it.entryType)
	assert.Equal(t, false, it.next()) // end here

	// ----------------------------
	// check third page
	// ----------------------------
	page3 := w.wal.getInMemPage(2)
	assert.Equal(t, FirstVersion, page3.GetVersion())
	assert.Equal(t, NewEpoch(1), page3.GetEpoch())
	assert.Equal(t, PageNum(2), page3.GetPageNum())

	// check first entry of third page
	it = page3.newIterator()
	assert.Equal(t, true, it.next())
	assert.Equal(t, EntryTypeFull, it.entryType)
	assert.Equal(t, "y", string(it.entryData))
}

func TestWAL__Add_3_Entry__Fit_Page(t *testing.T) {
	w := newWalTest(t, 100, 20)

	w.wal.FinishRecover()
	w.addEntry("input01") // add simple entry

	inputStr := joinStrings(
		strings.Repeat("A", 200),
		strings.Repeat("B", 281-4),
	)
	w.addEntry(inputStr) // add big entry

	w.addEntry("y") // add single byte

	// ----------------------------
	// check second page
	// ----------------------------
	page2 := w.wal.getInMemPage(1)
	assert.Equal(t, FirstVersion, page2.GetVersion())
	assert.Equal(t, NewEpoch(1), page2.GetEpoch())
	assert.Equal(t, PageNum(1), page2.GetPageNum())

	// check first entry
	it := page2.newIterator()
	assert.Equal(t, true, it.next())
	assert.Equal(t, EntryTypeFull, it.entryType)
	assert.Equal(t, "input01", string(it.entryData))

	// next entry
	assert.Equal(t, true, it.next())
	assert.Equal(t, EntryTypeFull, it.entryType)
	assert.Equal(t, strings.Repeat("A", 200)+strings.Repeat("B", 281-4), string(it.entryData))

	// next entry
	assert.Equal(t, true, it.next())
	assert.Equal(t, EntryTypeFull, it.entryType)
	assert.Equal(t, "y", string(it.entryData))

	// end
	assert.Equal(t, false, it.next())

	// ----------------------------
	// check third page
	// ----------------------------
	page3 := w.wal.getInMemPage(2)
	assert.Equal(t, PageVersion(0), page3.GetVersion())
	assert.Equal(t, NewEpoch(0), page3.GetEpoch())
	assert.Equal(t, PageNum(0), page3.GetPageNum())
}

func TestWAL__Add_Entry_Then_Recover(t *testing.T) {
	w := newWalTest(t, 100, 20)
	w.wal.FinishRecover()

	w.wal.Shutdown()
}
