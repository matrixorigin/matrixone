package cursor_spl

import (
	"math/rand"
	"sync/atomic"
	"unsafe"
)

const (
	maxHeight  = 20   // Maximum height of the skip list
	heightBits = 32   // Number of bits used to store height
	pValue     = 0.25 // Probability of increasing height
)

// node represents a node in the skip list
type node struct {
	// The actual height is stored in the lower heightBits bits
	// The upper bits are used for the reference count
	heightAndRefs uint64

	// The value stored in this node
	value unsafe.Pointer

	// next holds the pointers to the next nodes at each level
	// The array is allocated with the node to ensure memory locality
	next [maxHeight]unsafe.Pointer
}

// newNode creates a new node with the given value and height
func newNode(alloc Allocator, value unsafe.Pointer, height int) *node {
	size := unsafe.Sizeof(node{}) + (uintptr(maxHeight)-1)*unsafe.Sizeof(unsafe.Pointer(nil))
	ptr := alloc.Malloc(int(size))
	if ptr == nil {
		return nil
	}

	n := (*node)(ptr)
	n.heightAndRefs = uint64(height) | (1 << heightBits) // Initial ref count of 1
	n.value = value

	return n
}

// height returns the height of the node
func (n *node) height() int {
	return int(atomic.LoadUint64(&n.heightAndRefs) & ((1 << heightBits) - 1))
}

// ref increases the reference count of the node
func (n *node) ref() {
	atomic.AddUint64(&n.heightAndRefs, 1<<heightBits)
}

// deref decreases the reference count and returns true if the node should be freed
func (n *node) deref() bool {
	for {
		old := atomic.LoadUint64(&n.heightAndRefs)
		refs := old >> heightBits
		if refs <= 0 {
			return true
		}
		height := old & ((1 << heightBits) - 1)
		new := ((refs - 1) << heightBits) | height
		if atomic.CompareAndSwapUint64(&n.heightAndRefs, old, new) {
			return refs == 1
		}
	}
}

// getNext returns the next node at the given level
func (n *node) getNext(level int) *node {
	if n == nil {
		return nil
	}
	return (*node)(atomic.LoadPointer(&n.next[level]))
}

// casNext performs a compare-and-swap operation on the next pointer at the given level
func (n *node) casNext(level int, old, new *node) bool {
	if n == nil {
		return false
	}
	return atomic.CompareAndSwapPointer(&n.next[level], unsafe.Pointer(old), unsafe.Pointer(new))
}

// randomHeight returns a random height for a new node
func randomHeight() int {
	h := 1
	for h < maxHeight && rand.Float64() < pValue {
		h++
	}
	return h
}

// freeNode frees the memory allocated for the node
func freeNode(alloc Allocator, n *node) {
	if n != nil {
		alloc.Free(unsafe.Pointer(n))
	}
}
