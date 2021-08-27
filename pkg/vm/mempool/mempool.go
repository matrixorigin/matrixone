package mempool

import (
	"matrixone/pkg/encoding"
	"reflect"
	"sync/atomic"
	"unsafe"
)

var NullPointer unsafe.Pointer

func init() {
	var PageOffsets = map[int]int{
		0:  2,
		1:  4,
		2:  8,
		3:  16,
		4:  32,
		5:  64,
		6:  128,
		7:  256,
		8:  512,
		9:  1024,
		10: 2048,
		11: 4096,
		12: 8192,
		13: 16384,
	}
	PageOffset = PageOffsets[PageSize]
}

var OneCount = []byte{1, 0, 0, 0, 0, 0, 0, 0}

func New(maxSize, factor int) *Mempool {
	m := &Mempool{maxSize, make([]bucket, 0, 16)}
	for size := PageSize; size <= maxSize; size *= factor {
		m.buckets = append(m.buckets, bucket{
			size:  size,
			nslot: 8,
			slots: make([]unsafe.Pointer, 8),
		})
	}
	return m
}

func Realloc(data []byte, size int64) int64 {
	if data == nil {
		return size
	}
	n := int64(cap(data) - CountSize)
	newcap := n
	doublecap := n + n
	if size > doublecap {
		newcap = size
	} else {
		if len(data)-CountSize < 1024 {
			newcap = doublecap
		} else {
			for 0 < newcap && newcap < size {
				newcap += newcap / 4
			}
			if newcap <= 0 {
				newcap = size
			}
		}
	}
	return newcap
}

func (m *Mempool) Alloc(size int) []byte {
	size = ((size + PageSize - 1 + CountSize) >> PageOffset) << PageOffset
	if size <= m.maxSize {
		for _, b := range m.buckets {
			if b.size >= size {
				for i := 0; i < b.nslot; i++ {
					old := b.slots[i]
					if old == NullPointer {
						continue
					}
					if atomic.CompareAndSwapPointer(&b.slots[i], old, NullPointer) {
						hp := *(*reflect.SliceHeader)(old)
						data := *(*[]byte)(unsafe.Pointer(&hp))
						copy(data, OneCount)
						return data[:size]
					}
				}
				data := make([]byte, size)
				copy(data, OneCount)
				return data
			}
		}
	}
	data := make([]byte, size)
	copy(data, OneCount)
	return data
}

func (m *Mempool) Free(data []byte) bool {
	count := encoding.DecodeUint64(data[:8])
	copy(data, encoding.EncodeUint64(count-1))
	if count > 1 {
		return false
	}
	size := cap(data)
	if size <= m.maxSize {
		for i, j := 0, len(m.buckets)-1; i < j; i++ {
			b := m.buckets[i]
			if size >= b.size && size < m.buckets[i+1].size {
				b.cnt = (b.cnt + 1) % b.nslot
				b.slots[b.cnt] = unsafe.Pointer(&data)
				return true
			}
		}
	}
	return true
}
