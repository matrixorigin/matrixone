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
