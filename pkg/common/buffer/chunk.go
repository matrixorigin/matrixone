package buffer

import (
	"unsafe"

	"golang.org/x/sys/unix"
)

func init() {
	var c chunk

	ChunkSize = int(unsafe.Sizeof(c))
}

func (c *chunk) alloc(sz int) []byte {
	c.Lock()
	defer c.Unlock()
	if int(c.off)+sz+int(PointerSize) >= len(c.data) {
		c.flag |= FULL
		return nil
	}
	data := c.data[c.off : int(c.off)+sz+int(PointerSize)]
	*((*unsafe.Pointer)(unsafe.Pointer(unsafe.SliceData(data)))) = unsafe.Pointer(c)
	c.off += uint32(sz + int(PointerSize))
	c.numAlloc++
	return data[PointerSize:]
}

func (c *chunk) free() {
	c.Lock()
	if c.numFree = c.numFree + 1; c.numFree == c.numAlloc &&
		c.flag&FULL == FULL { // this chunk is no longer needed
		unix.Munmap(c.data)
		c.data = nil
		c.Unlock()
		free(unsafe.Pointer(c))
		return
	}
	c.Unlock()
}
