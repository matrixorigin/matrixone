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
	"math"
	"sync"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
)

// cowSlice is used to store information about all the locks occupied by a
// transaction. This structure is specially designed to avoid [][]byte expansion
// and is lock-free. The cowSlice supports a single-write-multiple-read concurrency
// model for a transaction.
type cowSlice struct {
	fsp *fixedSlicePool
	// Populated by the single writer only after exact keys outgrow one segment.
	exactKeys map[string]struct{}
	// Set on the requesting CN once this table has seen an exact lock request.
	// Only the owner knows which keys remain after a range merge.
	remoteExactRows bool
	fs              *atomic.Value // *fixedSlice
	v               *atomic.Uint64

	// just for testing
	hack struct {
		replace func()
		slice   func()
	}
}

func newCowSlice(
	fsp *fixedSlicePool,
	values [][]byte) (*cowSlice, error) {
	fs, err := fsp.acquire(len(values))
	if err != nil {
		return nil, err
	}
	cs := reuse.Alloc[cowSlice](nil)
	cs.fsp = fsp
	fs.append(values)
	cs.fs.Store(fs)
	return cs, nil
}

func (cs *cowSlice) append(values [][]byte) error {
	old := cs.mustGet()
	if len(values) > cs.fsp.maxSize()-old.totalLen() {
		return moerr.NewLockNeedUpgradeNoCtx()
	}
	cs.appendToTail(old, values)
	if cs.exactKeys != nil {
		for _, key := range values {
			cs.exactKeys[string(key)] = struct{}{}
		}
	}
	return nil
}

// Exact catalog rows cannot escalate. Keep full segments immutable and grow
// only the tail. A retained tail pins its predecessors for concurrent readers.
func (cs *cowSlice) appendExact(values [][]byte) {
	old := cs.mustGet()
	if cs.exactKeys == nil {
		if len(values) <= cs.fsp.maxSize()-old.totalLen() {
			cs.appendToTail(old, values)
			return
		}
		// A remote caller records request keys even when the owner already
		// held them. Repeated batches must not allocate more and more segments.
		cs.exactKeys = make(map[string]struct{}, old.totalLen())
		old.iter(func(key []byte) bool {
			cs.exactKeys[string(key)] = struct{}{}
			return true
		})
	}
	start := 0
	for i, key := range values {
		if _, ok := cs.exactKeys[string(key)]; ok {
			cs.appendSegments(values[start:i])
			start = i + 1
			continue
		}
		cs.exactKeys[string(key)] = struct{}{}
	}
	cs.appendSegments(values[start:])
}

func (cs *cowSlice) appendSegments(values [][]byte) {
	maxSize := cs.fsp.maxSize()
	for len(values) > 0 {
		old := cs.mustGet()
		n := min(maxSize-old.len(), len(values))
		if n > 0 {
			cs.appendToTail(old, values[:n])
		} else {
			n = min(maxSize, len(values))
			next := cs.fsp.get(n)
			old.retain()
			next.prev, next.prefixLen = old, old.totalLen()
			next.append(values[:n])
			cs.replace(old, next)
		}
		values = values[n:]
	}
}

func (cs *cowSlice) appendToTail(old *fixedSlice, values [][]byte) {
	capacity := old.cap()
	newLen := len(values) + old.len()
	if capacity >= newLen {
		old.append(values)
		return
	}

	// COW(copy-on-write), which needs to be copied once for each expansion, but for
	// [][]byte lock information, only the []byte pointer needs to be copied and the
	// overhead can be ignored.
	new := cs.fsp.get(newLen)
	if old.prev != nil {
		old.prev.retain()
		new.prev, new.prefixLen = old.prev, old.prefixLen
	}
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
		if cs.hack.slice != nil {
			cs.hack.slice()
		}
		if !fs.tryRef() {
			continue
		}
		if cs.v.CompareAndSwap(v, v+1) {
			return fs
		}
		// When we find that the fs we get is incorrect, we unref.
		fs.unref()
	}
}

func (cs *cowSlice) close() {
	reuse.Free(cs, nil)
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
	prev      *fixedSlice // immutable, full segments in acquisition order
	prefixLen int
	values    [][]byte
	sp        *fixedSlicePool
	atomic    struct {
		len atomic.Uint32
		ref atomic.Int32
	}
}

func (s *fixedSlice) all() [][]byte {
	if s.prev == nil {
		return s.values[:s.len()]
	}
	values := make([][]byte, 0, s.totalLen())
	s.iter(func(key []byte) bool {
		values = append(values, key)
		return true
	})
	return values
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
	if s.prev != nil {
		var previous []*fixedSlice
		for p := s.prev; p != nil; p = p.prev {
			previous = append(previous, p)
		}
		for i := len(previous) - 1; i >= 0; i-- {
			if !previous[i].iterSegment(fn) {
				return
			}
		}
	}
	s.iterSegment(fn)
}

func (s *fixedSlice) iterSegment(fn func([]byte) bool) bool {
	for _, key := range s.values[:s.len()] {
		if !fn(key) {
			return false
		}
	}
	return true
}

func (s *fixedSlice) totalLen() int {
	return s.prefixLen + s.len()
}

func (s *fixedSlice) len() int {
	return int(s.atomic.len.Load())
}

func (s *fixedSlice) cap() int {
	return cap(s.values)
}

func (s *fixedSlice) initRef() {
	if !s.atomic.ref.CompareAndSwap(0, 1) {
		panic("BUG: invalid initial ref count")
	}
}

// retain pins a live segment while the writer still owns a reference to it.
func (s *fixedSlice) retain() {
	if !s.tryRef() {
		panic("BUG: retaining a retired segment")
	}
}

func (s *fixedSlice) tryRef() bool {
	for {
		v := s.atomic.ref.Load()
		if v <= 0 {
			return false
		}
		if s.atomic.ref.CompareAndSwap(v, v+1) {
			return true
		}
	}
}

func (s *fixedSlice) unref() {
	for s != nil {
		n := s.atomic.ref.Add(-1)
		if n > 0 {
			return
		}
		if n < 0 {
			panic("BUG: invalid ref count")
		}
		prev := s.prev
		s.prev, s.prefixLen = nil, 0
		s.close()
		s = prev
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

func (sp *fixedSlicePool) maxSize() int {
	return 1 << (len(sp.slices) - 1)
}

func (sp *fixedSlicePool) acquire(n int) (*fixedSlice, error) {
	if n == 0 {
		n = min(4, sp.maxSize())
	}
	if n > sp.maxSize() {
		return nil, moerr.NewLockNeedUpgradeNoCtx()
	}
	return sp.get(n), nil
}

// get takes a nonempty segment whose requested size fits the pool.
func (sp *fixedSlicePool) get(n int) *fixedSlice {
	sp.acquireV.Add(1)
	i := int(math.Log2(float64(roundUp(n))))
	s := sp.slices[i].Get().(*fixedSlice)
	s.initRef()
	return s
}

func (sp *fixedSlicePool) release(s *fixedSlice) error {
	sp.releaseV.Add(1)
	n := s.cap()
	i := int(math.Log2(float64(n)))
	if i >= len(sp.slices) {
		return moerr.NewLockNeedUpgradeNoCtx()
	}
	sp.slices[i].Put(s)
	return nil
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

func (cs cowSlice) TypeName() string {
	return "lockservice.cowSlice"
}
