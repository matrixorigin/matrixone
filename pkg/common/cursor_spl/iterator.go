package cursor_spl

import (
	"unsafe"
)

// Iterator represents an iterator over the skip list
type Iterator struct {
	list *SkipList
	node *node
}

// NewIterator creates a new iterator for the skip list
func (s *SkipList) NewIterator() *Iterator {
	return &Iterator{
		list: s,
		node: s.head,
	}
}

// Valid returns true if the iterator is positioned at a valid node
func (it *Iterator) Valid() bool {
	return it != nil && it.list != nil && it.node != nil && it.node != it.list.head
}

// Next advances the iterator to the next node
func (it *Iterator) Next() {
	if !it.Valid() {
		it.node = it.list.head.getNext(0)
	} else {
		it.node = it.node.getNext(0)
	}
}

// Seek positions the iterator at the first node greater than or equal to the given value
func (it *Iterator) Seek(value unsafe.Pointer) {
	var prev [maxHeight]*node
	it.node = it.list.findPath(value, prev, true)
}

// Value returns the value at the current iterator position
func (it *Iterator) Value() unsafe.Pointer {
	if it == nil || !it.Valid() || it.node == nil {
		return nil
	}
	return it.node.value
}

// Close releases any resources used by the iterator
func (it *Iterator) Close() {
	it.list = nil
	it.node = nil
}

// SeekToFirst positions the iterator at the first node in the skip list
func (it *Iterator) SeekToFirst() {
	if it == nil || it.list == nil || it.list.head == nil {
		it.node = nil
		return
	}
	it.node = it.list.head.getNext(0)
}

// SeekToLast positions the iterator at the last node in the skip list
func (it *Iterator) SeekToLast() {
	curr := it.list.head
	for level := maxHeight - 1; level >= 0; level-- {
		for {
			next := curr.getNext(level)
			if next == nil {
				break
			}
			curr = next
		}
	}
	if curr == it.list.head {
		it.node = nil
	} else {
		it.node = curr
	}
}

// Prev moves the iterator to the previous node
func (it *Iterator) Prev() {
	if !it.Valid() {
		it.SeekToLast()
		return
	}

	var prev [maxHeight]*node
	target := it.node

	// Find the path to the current node
	curr := it.list.head
	found := false

	for level := maxHeight - 1; level >= 0; level-- {
		for {
			next := curr.getNext(level)
			if next == nil || next == target {
				prev[level] = curr
				if next == target {
					found = true
				}
				break
			}
			if it.list.less(next.value, target.value) {
				curr = next
				continue
			}
			break
		}
	}

	if !found {
		it.node = nil
		return
	}

	// The previous node is the last non-nil node in prev
	for i := 0; i < maxHeight; i++ {
		if prev[i] != it.list.head {
			it.node = prev[i]
			return
		}
	}

	it.node = nil
}
