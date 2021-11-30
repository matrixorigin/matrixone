// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package common

import (
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"sync"
	"sync/atomic"
)

const (
	UNLIMIT uint64 = ^uint64(0)
)

const (
	K uint64 = 1024
	M uint64 = 1024 * 1024
	G        = K * M
)

var (
	// GPool is the global mem pool used by AOE storage.
	// It's initialized automatically during init phase.
	GPool *Mempool
)

// Here we defined different page sizes, and each size has a certain
// pool managing all pages in that size.
var (
	PageSizes = []uint64{
		64,
		128,
		256,
		512,
		1 * K,
		4 * K,
		8 * K,
		16 * K,
		32 * K,
		64 * K,
		128 * K,
		256 * K,
		512 * K,
		M,
	}
	pools []sync.Pool
)

func init() {
	pools = make([]sync.Pool, len(PageSizes))
	for idx, _ := range PageSizes {
		pools[idx] = sync.Pool{
			New: func(i int) func() interface{} {
				return func() interface{} {
					n := &MemNode{
						idx: uint8(i),
						Buf: make([]byte, PageSizes[i]),
					}
					return n
				}
			}(idx),
		}
	}
	GPool = NewMempool(UNLIMIT)
}

func ToH(size uint64) string {
	var s string
	if size < K {
		s = fmt.Sprintf("%d B", size)
	} else if size < M {
		s = fmt.Sprintf("%.4f KB", float64(size)/float64(K))
	} else if size < G {
		s = fmt.Sprintf("%.4f MB", float64(size)/float64(M))
	} else {
		s = fmt.Sprintf("%.4f GB", float64(size)/float64(G))
	}

	return s
}

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

// poolWrapper wrapped a single pool for one certain page size.
// It collects the current page count and idx in Mempool.
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

// Mempool wraps an easy-to-use memory pool for the
// AOE storage.
//
// It manages memory through different granularity.
type Mempool struct {
	pools      []poolWrapper
	capacity   uint64
	usage      uint64
	quotausage uint64
	other      uint64
	peakusage  uint64
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
		mp.pools[idx].Pool = sync.Pool{New: pools[idx].New}
		mp.pools[idx].Pool.Put(pools[idx].Get())
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

	peak := atomic.LoadUint64(&mp.peakusage)
	if postsize > peak {
		atomic.CompareAndSwapUint64(&mp.peakusage, peak, postsize)
	}

	if !ok {
		atomic.AddUint64(&mp.other, uint64(1))
		node := new(MemNode)
		node.idx = uint8(len(PageSizes))
		node.Buf = make([]byte, size)
		// runtime.SetFinalizer(node, func(o *MemNode) {
		// 	log.Infof("[GC]: MemNode: %d", len(o.Buf))
		// })
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
			n.Buf = nil
			n = nil
		}
	}
	usage := atomic.AddUint64(&mp.usage, ^uint64(uint64(size)-1))
	if usage > mp.capacity {
		logutil.Panicf("logic error")
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
	peak := atomic.LoadUint64(&mp.peakusage)
	s := fmt.Sprintf("<Mempool>(Cap=%s)(Usage=%s)(Quota=%s)(Peak=%s)", ToH(mp.capacity), ToH(usage), ToH(atomic.LoadUint64(&mp.quotausage)), ToH(peak))
	for idx := range mp.pools {
		s = fmt.Sprintf("%s\nPage: %s, Count: %d", s, ToH(PageSizes[idx]), mp.pools[idx].Count())
	}
	s = fmt.Sprintf("%s\nPage: [UDEF], Count: %d", s, atomic.LoadUint64(&mp.other))
	return s
}
