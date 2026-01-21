package filesys

import (
	"io"
	"os"
	"syscall"
)

type FileSystem interface {
	Exists(path string) (bool, error)
	CreateEmptyFile(name string, fileSize int64) (io.WriteCloser, error)
	Rename(oldPath, newPath string) error
}

func NewFileSystem() FileSystem {
	return &fileSystemImpl{}
}

type fileSystemImpl struct{}

func (f *fileSystemImpl) Exists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (f *fileSystemImpl) CreateEmptyFile(name string, fileSize int64) (io.WriteCloser, error) {
	file, err := os.OpenFile(name, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return nil, err
	}

	fd := file.Fd()
	if err := syscall.Fallocate(int(fd), 0, 0, fileSize); err != nil {
		_ = file.Close()
		return nil, err
	}

	return file, nil
}

func (f *fileSystemImpl) Rename(oldPath, newPath string) error {
	return os.Rename(oldPath, newPath)
}
