// Copyright 2023 Matrix Origin
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

package lockservice

import (
	"fmt"
	"math"
	"sync"
	"sync/atomic"
)

var (
	cowSlicePool = sync.Pool{
		New: func() any {
			return &cowSlice{}
		},
	}
)

// cowSlice is used to store information about all the locks occupied by a
// transaction. This structure is specially designed to avoid [][]byte expansion
// and is lock-free. The cowSlice supports a single-write-multiple-read concurrency
// model for a transaction.
type cowSlice struct {
	fsp *fixedSlicePool
	fs  atomic.Value // *fixedSlice
	v   atomic.Uint64

	// just for testing
	hack struct {
		replace func()
		slice   func()
	}
}

func newCowSlice(
	fsp *fixedSlicePool,
	values [][]byte) *cowSlice {
	cs := cowSlicePool.Get().(*cowSlice)
	cs.fsp = fsp
	fs := fsp.acquire(len(values))
	fs.append(values)
	cs.fs.Store(fs)
	return cs
}

func (cs *cowSlice) append(values [][]byte) {
	old := cs.mustGet()
	capacity := old.cap()
	newLen := len(values) + old.len()
	if capacity >= newLen {
		old.append(values)
		return
	}

	// COW(copy-on-write), which needs to be copied once for each expansion, but for
	// [][]byte lock information, only the []byte pointer needs to be copied and the
	// overhead can be ignored.
	new := cs.fsp.acquire(newLen)
	new.join(old, values)
	cs.replace(old, new)
}

func (cs *cowSlice) replace(old, new *fixedSlice) {
	cs.fs.Store(new)
	for {
		v := cs.v.Load()
		if cs.hack.replace != nil {
			cs.hack.replace()
		}
		// we cannot unref old at will as there may be concurrent reads.
		if cs.v.CompareAndSwap(v, v+1) {
			old.unref()
			return
		}
	}
}

func (cs *cowSlice) slice() *fixedSlice {
	for {
		v := cs.v.Load()
		// In either case, if we get an incorrect fs, the following atomic operation
		// will not succeed, the data will not be read.
		fs := cs.mustGet()
		fs.ref()
		if cs.hack.slice != nil {
			cs.hack.slice()
		}
		if cs.v.CompareAndSwap(v, v+1) {
			return fs
		}
		// anyway, adding a ref does not produce an error. When we find that the fs we
		// get is incorrect, we unref
		fs.unref()
	}
}

func (cs *cowSlice) close() {
	cs.v.Store(0)
	cs.mustGet().unref()
	cowSlicePool.Put(cs)
}

func (cs *cowSlice) mustGet() *fixedSlice {
	v := cs.fs.Load()
	if v == nil {
		panic("BUG: must can get slice")
	}
	return v.(*fixedSlice)
}

// fixedSlice is fixed capacity [][]byte slice
type fixedSlice struct {
	values [][]byte
	sp     *fixedSlicePool
	atomic struct {
		len atomic.Uint32
		ref atomic.Int32
	}
}

func (s *fixedSlice) all() [][]byte {
	return s.values[:s.len()]
}

func (s *fixedSlice) join(
	other *fixedSlice,
	values [][]byte) {
	n := other.len()
	copy(s.values, other.values[:n])
	copy(s.values[n:n+len(values)], values)
	s.atomic.len.Store(uint32(n + len(values)))
}

func (s *fixedSlice) append(values [][]byte) {
	n := len(values)
	offset := s.len()
	for i := 0; i < n; i++ {
		s.values[offset+i] = values[i]
	}
	s.atomic.len.Add(uint32(len(values)))
}

func (s *fixedSlice) iter(fn func([]byte) bool) {
	n := s.len()
	for i := 0; i < n; i++ {
		if !fn(s.values[i]) {
			return
		}
	}
}

func (s *fixedSlice) len() int {
	return int(s.atomic.len.Load())
}

func (s *fixedSlice) cap() int {
	return cap(s.values)
}

func (s *fixedSlice) ref() {
	s.atomic.ref.Add(1)
}

func (s *fixedSlice) unref() {
	n := s.atomic.ref.Add(-1)
	if n == 0 {
		s.close()
		return
	}
	if n < 0 {
		panic("BUG: invalid ref count")
	}
}

func (s *fixedSlice) close() {
	for i := range s.values {
		s.values[i] = nil
	}
	s.atomic.len.Store(0)
	s.sp.release(s)
}

// fixedSlicePool maintains a set of sync.pool, each of which is used to store a specified
// size of slice. The size of the slice increases in multiples of 2.
type fixedSlicePool struct {
	slices   []sync.Pool
	acquireV atomic.Uint64
	releaseV atomic.Uint64
}

func newFixedSlicePool(max int) *fixedSlicePool {
	sp := &fixedSlicePool{}
	max = roundUp(max)
	for i := 1; i <= max; {
		cap := i
		sp.slices = append(sp.slices, sync.Pool{
			New: func() any {
				s := &fixedSlice{values: make([][]byte, cap), sp: sp}
				return s
			}})
		i = i << 1
	}
	return sp
}

func (sp *fixedSlicePool) acquire(n int) *fixedSlice {
	if n == 0 {
		n = 4
	}
	sp.acquireV.Add(1)
	n = roundUp(n)
	i := int(math.Log2(float64(n)))
	if i >= len(sp.slices) {
		panic(fmt.Sprintf("too large fixed slice %d, max is %d",
			n,
			1<<(len(sp.slices)-1)))
	}
	s := sp.slices[i].Get().(*fixedSlice)
	s.ref()
	return s
}

func (sp *fixedSlicePool) release(s *fixedSlice) {
	sp.releaseV.Add(1)
	n := s.cap()
	i := int(math.Log2(float64(n)))
	if i >= len(sp.slices) {
		panic(fmt.Sprintf("too large fixed slice %d, max is %d",
			n,
			2<<(len(sp.slices)-1)))
	}
	sp.slices[i].Put(s)
}

// roundUp takes a int greater than 0 and rounds it up to the next
// power of 2.
func roundUp(v int) int {
	v--
	v |= v >> 1
	v |= v >> 2
	v |= v >> 4
	v |= v >> 8
	v |= v >> 16
	v |= v >> 32
	v++
	return v
}
