package dbi

import (
	"io"
	ro "matrixone/pkg/container/vector"
	"matrixone/pkg/vm/process"
)

type VectorType uint8

const (
	StdVec VectorType = iota
	StrVec
	Wrapper
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
	GetType() VectorType
	GetValue(int) interface{}
	IsNull(int) bool
	HasNull() bool
	NullCnt() int
	Length() int
	Capacity() int
	GetMemorySize() uint64
	SliceReference(start, end int) IVectorReader
	CopyToVector() *ro.Vector
	CopyToVectorWithProc(uint64, *process.Process) (*ro.Vector, error)
}
