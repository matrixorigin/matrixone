package buffer

// #include <stdlib.h>
import "C"
import "unsafe"

func malloc(size int) unsafe.Pointer {
	ptr := C.calloc(C.size_t(size), 1)
	if ptr == nil {
		// NB: throw is like panic, except it guarantees the process will be
		// terminated. The call below is exactly what the Go runtime invokes when
		// it cannot allocate memory.
		panic("out of memory")
	}
	return ptr
}

// free frees the specified slice.
func free(ptr unsafe.Pointer) {
	C.free(ptr)
}
