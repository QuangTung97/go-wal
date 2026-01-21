package filesys

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFileSystem__Exists_And_Create_Empty_File(t *testing.T) {
	tempDir := t.TempDir()
	filename := filepath.Join(tempDir, "file01")

	fs := NewFileSystem()

	// not exist
	existed, err := fs.Exists(filename)
	assert.Equal(t, nil, err)
	assert.Equal(t, false, existed)

	// create file
	writer, err := fs.CreateEmptyFile(filename, 721)
	assert.Equal(t, nil, err)

	// check existed
	existed, err = fs.Exists(filename)
	assert.Equal(t, nil, err)
	assert.Equal(t, true, existed)

	// check size
	fileStat, err := os.Stat(filename)
	assert.Equal(t, nil, err)
	assert.Equal(t, int64(721), fileStat.Size())

	// close writer
	assert.Equal(t, nil, writer.Close())

	// rename
	newName := filepath.Join(tempDir, "file02")
	err = fs.Rename(filename, newName)
	assert.Equal(t, nil, err)

	// check not exist
	existed, err = fs.Exists(filename)
	assert.Equal(t, nil, err)
	assert.Equal(t, false, existed)

	// check existed
	existed, err = fs.Exists(newName)
	assert.Equal(t, nil, err)
	assert.Equal(t, true, existed)
}
