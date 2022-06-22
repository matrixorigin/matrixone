package stl

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

func TestAlloctor(t *testing.T) {
	allocator := NewSimpleAllocator()
	node := allocator.Alloc(10)
	assert.True(t, int(unsafe.Sizeof(int32(0)))*10 <= allocator.Usage())
	allocator.Free(node)
	assert.Equal(t, 0, allocator.Usage())
}
