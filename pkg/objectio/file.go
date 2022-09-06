package objectio

import (
	"io"
	"os"
)

type ObjectFile interface {
	io.Reader
	io.WriterAt
	io.Closer
}

type LocalFile struct {
	path string
	file *os.File
}

func NewLocalFile(path string) (ObjectFile, error) {
	var err error
	lf := &LocalFile{
		path: path,
	}
	if _, err = os.Stat(path); os.IsNotExist(err) {
		lf.file, err = os.Create(path)
		return lf, err
	}
	if lf.file, err = os.OpenFile(path, os.O_RDWR, os.ModePerm); err != nil {
		return nil, err
	}
	return lf, err
}

func (l LocalFile) Read(p []byte) (n int, err error) {
	return l.file.Read(p)
}

func (l LocalFile) WriteAt(p []byte, off int64) (n int, err error) {
	return l.file.WriteAt(p, off)
}

func (l LocalFile) Close() error {
	return l.Close()
}
