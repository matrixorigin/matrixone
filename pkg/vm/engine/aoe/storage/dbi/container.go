package dbi

import (
	"io"
	ro "matrixone/pkg/container/vector"
)

type IBatchReader interface {
	IsReadonly() bool
	Length() int
	GetAttrs() []int
	Close() error
	CloseVector(idx int) error
	IsVectorClosed(idx int) bool
	GetReaderByAttr(attr int) IVectorReader
}

type IVectorReader interface {
	io.Closer
	GetValue(int) interface{}
	IsNull(int) bool
	HasNull() bool
	NullCnt() int
	Length() int
	Capacity() int
	SliceReference(start, end int) IVectorReader
	CopyToVector() *ro.Vector
}
