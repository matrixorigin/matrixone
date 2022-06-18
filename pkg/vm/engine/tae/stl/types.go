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
	Get(i int) (v T, err error)
	Set(i int, v T) (old T, err error)
	Delete(i int) (deleted T, err error)

	Capacity() int
	Length() int
	Allocated() int
	String() string

	GetAllocator() MemAllocator[T]
}
