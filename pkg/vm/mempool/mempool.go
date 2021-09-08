package mempool

import (
	"matrixone/pkg/internal/rawalloc"
	"sync"
)

var Pool = sync.Pool{
	New: func() interface{} {
		return New()
	},
}

func New() *Mempool {
	m := &Mempool{buckets: make([]bucket, 0, 10)}
	for size := PageSize; size <= MaxSize; size *= Factor {
		m.buckets = append(m.buckets, bucket{
			size:  size,
			slots: make([][]byte, 0, 8),
		})
	}
	return m
}

func Realloc(data []byte, size int64) int64 {
	if data == nil {
		return size
	}
	n := int64(cap(data))
	newcap := n
	doublecap := n + n
	if size > doublecap {
		newcap = size
	} else {
		if len(data) < 1024 {
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
	size = ((size + PageSize - 1) >> PageOffset) << PageOffset
	if size <= MaxSize {
		for i, b := range m.buckets {
			if b.size >= size {
				if len(b.slots) > 0 {
					data := b.slots[0]
					m.buckets[i].slots = m.buckets[i].slots[1:]
					return data[:size]
				}
				return rawalloc.New(size, size)
			}
		}
	}
	for i, buf := range m.buffers {
		if cap(buf) >= size {
			m.buffers[i] = m.buffers[len(m.buffers)-1]
			m.buffers = m.buffers[:len(m.buffers)-1]
			return buf[:size]
		}
	}
	return rawalloc.New(size, size)
}

func (m *Mempool) Free(data []byte) {
	size := cap(data)
	if size <= MaxSize {
		for i, b := range m.buckets {
			if b.size >= size {
				m.buckets[i].slots = append(m.buckets[i].slots, data)
				return
			}
		}
		return
	}
	m.buffers = append(m.buffers, data)
	return
}
