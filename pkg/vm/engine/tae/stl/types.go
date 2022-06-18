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

	Data() []byte
	Slice() []T

	Append(v T)
	AppendMany(vals ...T)
	Get(i int) (v T)
	Set(i int, v T) (old T)
	Delete(i int) (deleted T)
	RangeDelete(offset, length int)

	Capacity() int
	Length() int
	Allocated() int
	String() string

	GetAllocator() MemAllocator
}
