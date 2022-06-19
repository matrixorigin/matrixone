package stl

import "unsafe"

func Sizeof[T any]() int {
	var v T
	return int(unsafe.Sizeof(v))
}

func LengthOfVals[T any](cnt int) int {
	var v T
	return int(unsafe.Sizeof(v)) * cnt
}

type Vector[T any] interface {
	Close()

	Clone(offset, length int) Vector[T]
	IsView() bool
	Data() []byte
	Slice() []T
	DataWindow(offset, length int) []byte
	SliceWindow(offset, length int) []T

	Append(v T)
	AppendMany(vals ...T)
	Get(i int) (v T)
	Update(i int, v T)
	Delete(i int) (deleted T)
	RangeDelete(offset, length int)

	Capacity() int
	Length() int
	Allocated() int
	String() string
	Desc() string

	GetAllocator() MemAllocator
}
