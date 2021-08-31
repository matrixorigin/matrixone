package mempool

import (
	"matrixone/pkg/internal/rawalloc"
	"reflect"
	"runtime"
	"sync/atomic"
	"unsafe"
)

var OneCount = []byte{1, 0, 0, 0, 0, 0, 0, 0}

func New(maxSize, factor int) *Mempool {
	m := &Mempool{make([]class, 0, 10), 64, maxSize}
	for chunkSize := 64; chunkSize <= maxSize && chunkSize <= PageSize; chunkSize *= factor {
		c := class{
			size:   chunkSize,
			page:   make([]byte, PageSize),
			chunks: make([]chunk, PageSize/chunkSize),
			head:   (1 << 32),
		}
		for i := 0; i < len(c.chunks); i++ {
			chk := &c.chunks[i]
			chk.mem = c.page[i*chunkSize : (i+1)*chunkSize : (i+1)*chunkSize]
			if i < len(c.chunks)-1 {
				chk.next = uint64(i+1+1) << 32
			} else {
				c.pageBegin = uintptr(unsafe.Pointer(&c.page[0]))
				c.pageEnd = uintptr(unsafe.Pointer(&chk.mem[0]))
			}
		}
		m.classes = append(m.classes, c)
	}
	return m
}

func (m *Mempool) Alloc(size int) []byte {
	size += CountSize
	if size <= m.maxSize {
		for i := 0; i < len(m.classes); i++ {
			if m.classes[i].size >= size {
				mem := m.classes[i].Pop()
				if mem != nil {
					return mem[:size]
				}
				break
			}
		}
	}
	return rawalloc.New(size, size)
}

func (m *Mempool) Free(data []byte) bool {
	count := *(*uint64)(unsafe.Pointer(&data[0])) - 1
	hp := reflect.SliceHeader{Data: uintptr(unsafe.Pointer(&count)), Len: 8, Cap: 8}
	copy(data, *(*[]byte)(unsafe.Pointer(&hp)))
	if count > 1 {
		return false
	}
	size := cap(data)
	for i := 0; i < len(m.classes); i++ {
		if m.classes[i].size == size {
			m.classes[i].Push(data)
			break
		}
	}
	return true
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

func (c *class) Push(mem []byte) {
	ptr := (*reflect.SliceHeader)(unsafe.Pointer(&mem)).Data
	if c.pageBegin <= ptr && ptr <= c.pageEnd {
		i := (ptr - c.pageBegin) / uintptr(c.size)
		chk := &c.chunks[i]
		if chk.next != 0 {
			panic("Double Free")
		}
		chk.aba++
		new := uint64(i+1)<<32 + uint64(chk.aba)
		for {
			old := atomic.LoadUint64(&c.head)
			atomic.StoreUint64(&chk.next, old)
			if atomic.CompareAndSwapUint64(&c.head, old, new) {
				break
			}
			runtime.Gosched()
		}
	}
}

func (c *class) Pop() []byte {
	for {
		old := atomic.LoadUint64(&c.head)
		if old == 0 {
			return nil
		}
		chk := &c.chunks[old>>32-1]
		nxt := atomic.LoadUint64(&chk.next)
		if atomic.CompareAndSwapUint64(&c.head, old, nxt) {
			atomic.StoreUint64(&chk.next, 0)
			return chk.mem
		}
		runtime.Gosched()
	}
}
