package common

import "io"

type FileType uint8

const (
	InvalidFile FileType = iota
	MemFile
	DiskFile
)

type FileInfo interface {
	Name() string
	Size() int64
	OriginSize() int64
	CompressAlgo() int
}

type IVFile interface {
	io.Reader
	Ref()
	Unref()
	Stat() FileInfo
	GetFileType() FileType
}

type baseFileInfo struct {
	size int64
}

func (i *baseFileInfo) Name() string      { return "" }
func (i *baseFileInfo) Size() int64       { return i.size }
func (i *baseFileInfo) OriginSize() int64 { return i.size }
func (i *baseFileInfo) CompressAlgo() int { return 0 }

type baseMemFile struct {
	stat baseFileInfo
}

func NewMemFile(size int64) IVFile {
	return &baseMemFile{
		stat: baseFileInfo{
			size: size,
		},
	}
}

func (f *baseMemFile) Ref()                             {}
func (f *baseMemFile) Unref()                           {}
func (f *baseMemFile) Read(p []byte) (n int, err error) { return n, err }
func (f *baseMemFile) Stat() FileInfo                   { return &f.stat }
func (f *baseMemFile) GetFileType() FileType            { return MemFile }
