package wal

import (
	"path/filepath"
	"testing"
)

func TestWAL(t *testing.T) {
	tempDir := t.TempDir()
	filename := filepath.Join(tempDir, "wal01")

	NewWAL(filename, PageSize*5)
}
