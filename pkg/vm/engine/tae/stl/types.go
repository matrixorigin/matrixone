package stl

import (
	"io"
	"unsafe"
)

func Sizeof[T any]() int {
	var v T
	return int(unsafe.Sizeof(v))
}

func SizeOfMany[T any](cnt int) int {
	var v T
	return int(unsafe.Sizeof(v)) * cnt
}

type Bytes struct {
	Data   []byte
	Offset []uint32
	Length []uint32
}

type Vector[T any] interface {
	Close()

	Clone(offset, length int) Vector[T]
	IsView() bool
	Data() []byte
	Slice() []T
	Bytes() *Bytes
	ReadBytes(*Bytes)
	DataWindow(offset, length int) []byte
	SliceWindow(offset, length int) []T

	Append(v T)
	AppendMany(vals ...T)
	Get(i int) (v T)
	Update(i int, v T)
	Delete(i int) (deleted T)
	RangeDelete(offset, length int)
	Reset()

	Capacity() int
	Length() int
	Allocated() int
	String() string
	Desc() string
	WriteTo(io.Writer) (int64, error)
	ReadFrom(io.Reader) (int64, error)

	GetAllocator() MemAllocator
}
