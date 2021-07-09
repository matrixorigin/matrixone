package common

import (
	"fmt"
	// log "github.com/sirupsen/logrus"
	"sync"
	"sync/atomic"
)

const (
	K uint64 = 1024
	M uint64 = 1024 * 1024
)

var (
	PageSizes = []uint64{
		64,
		256,
		1 * K,
		4 * K,
		16 * K,
		64 * K,
		256 * K,
	}
	pools = []sync.Pool{
		sync.Pool{
			New: func() interface{} {
				n := MemNode{
					idx: uint8(0),
					Buf: make([]byte, PageSizes[0]),
				}
				return &n
			},
		},
		sync.Pool{
			New: func() interface{} {
				n := MemNode{
					idx: uint8(1),
					Buf: make([]byte, PageSizes[1]),
				}
				return &n
			},
		},
		sync.Pool{
			New: func() interface{} {
				n := MemNode{
					idx: uint8(2),
					Buf: make([]byte, PageSizes[2]),
				}
				return &n
			},
		},
		sync.Pool{
			New: func() interface{} {
				n := MemNode{
					idx: uint8(3),
					Buf: make([]byte, PageSizes[3]),
				}
				return &n
			},
		},
		sync.Pool{
			New: func() interface{} {
				n := MemNode{
					idx: uint8(4),
					Buf: make([]byte, PageSizes[4]),
				}
				return &n
			},
		},
		sync.Pool{
			New: func() interface{} {
				n := MemNode{
					idx: uint8(5),
					Buf: make([]byte, PageSizes[5]),
				}
				return &n
			},
		},
		sync.Pool{
			New: func() interface{} {
				n := MemNode{
					idx: uint8(6),
					Buf: make([]byte, PageSizes[6]),
				}
				return &n
			},
		},
	}
)

func findPage(size uint64) (idx int, ok bool) {
	if size > PageSizes[len(PageSizes)-1] {
		return idx, false
	}
	l := 0
	h := len(PageSizes) - 1
	for {
		if l > h {
			break
		}
		m := (l + h) / 2
		if PageSizes[m] < size {
			l = m + 1
		} else if PageSizes[m] > size {
			h = m - 1
		} else {
			return m, true
		}
	}
	return l, true
}

type poolWrapper struct {
	sync.Pool
	count uint64
	idx   int
}

func (p *poolWrapper) Get() interface{} {
	atomic.AddUint64(&p.count, uint64(1))
	return p.Pool.Get()
}

func (p *poolWrapper) Put(x interface{}) {
	atomic.AddUint64(&p.count, ^uint64(0))
	p.Pool.Put(x)
}

func (p *poolWrapper) Count() int {
	return int(atomic.LoadUint64(&p.count))
}

type Mempool struct {
	pools    []poolWrapper
	capacity uint64
	usage    uint64
}

type MemNode struct {
	idx uint8
	Buf []byte
}

func (n *MemNode) Size() int {
	return cap(n.Buf)
}

func (n *MemNode) PageIdx() int {
	return int(n.idx)
}

func NewMempool(capacity uint64) *Mempool {
	mp := &Mempool{
		capacity: capacity,
		pools:    make([]poolWrapper, len(PageSizes)),
	}

	for idx, _ := range PageSizes {
		mp.pools[idx].idx = idx
		mp.pools[idx].Pool = pools[idx]
	}

	return mp
}

func (mp *Mempool) Alloc(size uint64) *MemNode {
	pageIdx, ok := findPage(size)
	if ok {
		size = PageSizes[pageIdx]
	}
	postsize := atomic.AddUint64(&mp.usage, size)
	if postsize > mp.capacity {
		atomic.AddUint64(&mp.usage, ^uint64(size-1))
		return nil
	}

	if !ok {
		node := new(MemNode)
		node.Buf = make([]byte, size)
		return node
	}
	return mp.pools[pageIdx].Get().(*MemNode)
}

func (mp *Mempool) Free(n *MemNode) {
	if n == nil {
		return
	}
	size := n.Size()
	mp.pools[n.PageIdx()].Put(n)
	// log.Infof("Free size %d", size)
	usage := atomic.AddUint64(&mp.usage, ^uint64(uint64(size)-1))
	if usage > mp.capacity {
		panic("")
	}
}

func (mp *Mempool) Usage() uint64 {
	return atomic.LoadUint64(&mp.usage)
}

func (mp *Mempool) Capacity() uint64 {
	return mp.capacity
}

func (mp *Mempool) String() string {
	usage := atomic.LoadUint64(&mp.usage)
	s := fmt.Sprintf("<Mempool>(Cap=%d)(Usage=%d)", mp.capacity, usage)
	for _, pool := range mp.pools {
		s = fmt.Sprintf("%s\nPage: %d, Count: %d", s, pool.idx, pool.Count())
	}
	return s
}
