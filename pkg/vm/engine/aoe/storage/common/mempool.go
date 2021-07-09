package common

import (
	"fmt"
	log "github.com/sirupsen/logrus"
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

func findPageIdx(size uint64) (idx int, ok bool) {
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
	pools      []poolWrapper
	capacity   uint64
	usage      uint64
	quotausage uint64
	other      uint64
}

type MemNode struct {
	size uint32
	idx  uint8
	Buf  []byte
}

func (n *MemNode) Size() int {
	if n.Buf != nil {
		return cap(n.Buf)
	}
	return int(n.size)
}

func (n *MemNode) PageIdx() int {
	return int(n.idx)
}

func (n *MemNode) IsQuota() bool {
	return n.Buf == nil
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
	pageIdx, ok := findPageIdx(size)
	if ok {
		size = PageSizes[pageIdx]
	}
	preusage := atomic.LoadUint64(&mp.usage)
	postsize := preusage + size
	if postsize > mp.capacity {
		return nil
	}
	for !atomic.CompareAndSwapUint64(&mp.usage, preusage, postsize) {
		preusage = atomic.LoadUint64(&mp.usage)
		postsize = preusage + size
		if postsize > mp.capacity {
			return nil
		}
	}

	if !ok {
		atomic.AddUint64(&mp.other, uint64(1))
		node := new(MemNode)
		node.idx = uint8(len(PageSizes))
		node.Buf = make([]byte, size)
		return node
	}
	return mp.pools[pageIdx].Get().(*MemNode)
}

func (mp *Mempool) ApplyQuota(size uint64) *MemNode {
	preusage := atomic.LoadUint64(&mp.usage)
	postsize := preusage + size
	if postsize > mp.capacity {
		return nil
	}
	for !atomic.CompareAndSwapUint64(&mp.usage, preusage, postsize) {
		preusage = atomic.LoadUint64(&mp.usage)
		postsize = preusage + size
		if postsize > mp.capacity {
			return nil
		}
	}
	atomic.AddUint64(&mp.quotausage, size)
	node := new(MemNode)
	node.size = uint32(size)
	return node
}

func (mp *Mempool) Free(n *MemNode) {
	if n == nil {
		return
	}
	size := n.Size()
	if n.IsQuota() {
		atomic.AddUint64(&mp.quotausage, ^uint64(uint64(size)-1))
		n = nil
	} else {
		if n.idx < uint8(len(PageSizes)) {
			mp.pools[n.PageIdx()].Put(n)
		} else {
			atomic.AddUint64(&mp.other, ^uint64(0))
			n = nil
		}
	}
	// log.Infof("Free size %d", size)
	usage := atomic.AddUint64(&mp.usage, ^uint64(uint64(size)-1))
	if usage > mp.capacity {
		log.Error("logic error")
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
	s := fmt.Sprintf("<Mempool>(Cap=%d)(Usage=%d)(Quota=%d)", mp.capacity, usage, atomic.LoadUint64(&mp.quotausage))
	for _, pool := range mp.pools {
		s = fmt.Sprintf("%s\nPage: %d, Count: %d", s, pool.idx, pool.Count())
	}
	s = fmt.Sprintf("%s\nPage: [UDEF], Count: %d", s, atomic.LoadUint64(&mp.other))
	return s
}
