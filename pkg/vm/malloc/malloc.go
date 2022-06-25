package malloc

import "unsafe"

func Malloc(n int) []byte {
	ptr := mallocgc(uintptr(n), nil, false)
	return (*[maxArrayLen]byte)(ptr)[:n:n]
}

//go:linkname mallocgc runtime.mallocgc
func mallocgc(uintptr, unsafe.Pointer, bool) unsafe.Pointer
