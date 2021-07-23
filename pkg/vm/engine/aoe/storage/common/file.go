package common

import "io"

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
}
