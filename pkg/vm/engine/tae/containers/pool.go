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

package containers

import (
	"fmt"
	"sync"
	"sync/atomic"
	_ "unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/util/metric/stats"
)

const (
	// fix sized / (varlen + fix sized)
	defaultFixedSizeRatio = 0.6

	// default max alloaction size for a vector
	defaultAllocLimit = 1024 * 1024 * 2 // 2MB
)

var _vectorPoolAlloactor *mpool.MPool

func init() {
	var err error
	if _vectorPoolAlloactor, err = mpool.NewMPool(
		"taeVectorPool", 0, mpool.NoFixed,
	); err != nil {
		panic(err)
	}
}

func GetDefaultVectorPoolALLocator() *mpool.MPool {
	return _vectorPoolAlloactor
}

type VectorPoolOption func(*VectorPool)

func WithAllocationLimit(maxv int) VectorPoolOption {
	return func(p *VectorPool) {
		p.maxAlloc = maxv
	}
}

func WithMPool(mp *mpool.MPool) VectorPoolOption {
	return func(p *VectorPool) {
		p.mp = mp
	}
}

type vectorPoolElement struct {
	pool  *VectorPool
	mp    *mpool.MPool
	vec   *vector.Vector
	inUse atomic.Bool
	// lent is the vector holding vec's buffers while they are lent.
	lent atomic.Pointer[vector.Vector]
}

// allocated is the memory of the element's buffers, lent or not. Reading a
// lent vector's size races with its borrower; it is a statistic.
func (e *vectorPoolElement) allocated() int {
	if l := e.lent.Load(); l != nil {
		return l.Allocated()
	}
	return e.vec.Allocated()
}

// lentVectors maps each vector a VectorPool has lent and not yet taken back
// to its element. A lent vector is a fresh struct holding the element's
// buffers (see lend), so a stale second release of it can never reach a
// later borrower of the same buffers.
var lentVectors sync.Map // *vector.Vector -> *vectorPoolElement

func (e *vectorPoolElement) tryReuse(t *types.Type) bool {
	if e.inUse.CompareAndSwap(false, true) {
		e.vec.ResetWithNewType(t)
		return true
	}
	return false
}

// lend moves the element's buffers into a new vector struct and records it
// as lent. The element must be in use (tryReuse).
func (e *vectorPoolElement) lend() *vector.Vector {
	lent := new(vector.Vector)
	*lent = *e.vec
	*e.vec = vector.Vector{}
	e.lent.Store(lent)
	lentVectors.Store(lent, e)
	return lent
}

// takeBack moves a lent vector's buffers back into the element and leaves
// the lent struct empty, then returns the element to the pool.
func (e *vectorPoolElement) takeBack(lent *vector.Vector) {
	*e.vec = *lent
	*lent = vector.Vector{}
	e.lent.Store(nil)
	e.put()
}

// returnToPool returns vec to the VectorPool that lent it, and reports
// whether it was lent by one.
func returnToPool(vec *vector.Vector) bool {
	e, ok := lentVectors.LoadAndDelete(vec)
	if !ok {
		return false
	}
	e.(*vectorPoolElement).takeBack(vec)
	return true
}

// isLent reports whether vec was lent by a VectorPool and not yet returned.
func isLent(vec *vector.Vector) bool {
	_, ok := lentVectors.Load(vec)
	return ok
}

func (e *vectorPoolElement) put() {
	if e.vec.Allocated() > e.pool.maxAlloc {
		newVec := vector.NewVec(*e.vec.GetType(), e.mp)
		e.vec.Free(e.mp)
		e.vec = newVec
		e.pool.resetStats.Add(1)
	}
	if !e.inUse.CompareAndSwap(true, false) {
		panic("vectorPoolElement.put: vector is not in use")
	}
	// Close may have skipped this slot while it was checked out.
	if e.pool.closed.Load() {
		e.freeIfIdle()
	}
}

func (e *vectorPoolElement) freeIfIdle() {
	if e.inUse.CompareAndSwap(false, true) {
		e.vec.Free(e.mp)
		e.inUse.Store(false)
	}
}

func (e *vectorPoolElement) isIdle() bool {
	return !e.inUse.Load()
}

type VectorPool struct {
	name           string
	closed         atomic.Bool
	maxAlloc       int
	ratio          float64
	fixSizedPool   []*vectorPoolElement
	varlenPool     []*vectorPoolElement
	fixHitStats    stats.Counter
	varlenHitStats stats.Counter
	resetStats     stats.Counter
	totalStats     stats.Counter
	mp             *mpool.MPool
}

func NewVectorPool(name string, cnt int, opts ...VectorPoolOption) *VectorPool {
	p := &VectorPool{
		name:  name,
		ratio: -1,
	}
	for _, opt := range opts {
		opt(p)
	}
	if p.mp == nil {
		p.mp = _vectorPoolAlloactor
	}
	if p.maxAlloc <= 0 {
		p.maxAlloc = defaultAllocLimit
	}
	if p.ratio < 0 {
		p.ratio = defaultFixedSizeRatio
	}

	cnt1 := int(float64(cnt) * p.ratio)
	cnt2 := cnt - cnt1
	p.fixSizedPool = make([]*vectorPoolElement, 0, cnt1)
	p.varlenPool = make([]*vectorPoolElement, 0, cnt2)

	for i := 0; i < cnt1; i++ {
		t := types.T_int64.ToType()
		p.fixSizedPool = append(p.fixSizedPool, newVectorElement(p, &t, p.mp))
	}
	for i := 0; i < cnt2; i++ {
		t := types.T_varchar.ToType()
		p.varlenPool = append(p.varlenPool, newVectorElement(p, &t, p.mp))
	}
	return p
}

func (p *VectorPool) String() string {
	fixedUsedCnt, _ := p.FixedSizeUsed(false)
	varlenUsedCnt, _ := p.VarlenUsed(false)
	usedCnt := fixedUsedCnt + varlenUsedCnt
	fixHit := p.fixHitStats.Load()
	varlenHit := p.varlenHitStats.Load()
	hit := fixHit + varlenHit
	str := fmt.Sprintf(
		"VectorPool[%s][%d/%d]: FixSizedVec[%d/%d] VarlenVec[%d/%d], Reset/Hit/totalStats:[%d/(%d,%d)%d/%d]",
		p.name,                                /* name */
		usedCnt,                               /* totalStats used vector cnt */
		len(p.fixSizedPool)+len(p.varlenPool), /* totalStats vector cnt */
		fixedUsedCnt,                          /* used fixed sized vector cnt */
		len(p.fixSizedPool),                   /* totalStats fixed sized vector cnt */
		varlenUsedCnt,                         /* used varlen vector cnt */
		len(p.varlenPool),                     /* totalStats varlen vector cnt */
		p.resetStats.Load(),                   /* reset cnt */
		fixHit,                                /* fixed sized vector hit cnt */
		varlenHit,                             /* varlen vector hit cnt */
		hit,                                   /* hit cnt */
		p.totalStats.Load(),                   /* totalStats cnt */
	)
	return str
}

// GetVector lends a vector of type t; release it with ReleaseVector, which
// returns it to the pool.
func (p *VectorPool) GetVector(t *types.Type) Vector {
	p.totalStats.Add(1)
	if p.closed.Load() {
		return MakeVector(*t, p.mp)
	}
	if t.IsFixedLen() {
		if len(p.fixSizedPool) > 0 {
			for i := 0; i < 4; i++ {
				idx := fastrand() % uint32(len(p.fixSizedPool))
				element := p.fixSizedPool[idx]
				if element.tryReuse(t) {
					p.fixHitStats.Add(1)
					return element.lend()
				}
			}
		}
	} else {
		if len(p.varlenPool) > 0 {
			for i := 0; i < 4; i++ {
				idx := fastrand() % uint32(len(p.varlenPool))
				element := p.varlenPool[idx]
				if element.tryReuse(t) {
					p.varlenHitStats.Add(1)
					return element.lend()
				}
			}
		}
	}

	return MakeVector(*t, p.mp)
}

// CloneWindow copies rows [offset, offset+length) of src into a vector lent
// by p; release it with ReleaseVector.
func (p *VectorPool) CloneWindow(src Vector, offset, length int) (Vector, error) {
	cloned := p.GetVector(src.GetType())
	if err := src.CloneWindowTo(cloned, offset, offset+length, cloned.MPool()); err != nil {
		ReleaseVector(cloned)
		return nil, err
	}
	return cloned, nil
}

func (p *VectorPool) GetMPool() *mpool.MPool {
	return p.mp
}

// Close releases idle buffers. Checked-out vectors remain valid and release
// their backing memory when returned; allocations after Close are unpooled.
func (p *VectorPool) Close() {
	if !p.closed.CompareAndSwap(false, true) {
		return
	}
	for _, e := range p.fixSizedPool {
		e.freeIfIdle()
	}
	for _, e := range p.varlenPool {
		e.freeIfIdle()
	}
}

func (p *VectorPool) Allocated() int {
	size := 0
	size += p.FixedSizeAllocated()
	size += p.VarlenaSizeAllocated()
	return size
}

func (p *VectorPool) FixedSizeAllocated() int {
	size := 0
	for _, element := range p.fixSizedPool {
		size += element.allocated()
	}
	return size
}

func (p *VectorPool) VarlenaSizeAllocated() int {
	size := 0
	for _, element := range p.varlenPool {
		size += element.allocated()
	}
	return size
}

func (p *VectorPool) FixedSizeUsed(isUnsafe bool) (int, int) {
	size := 0
	cnt := 0
	for _, element := range p.fixSizedPool {
		if element.isIdle() {
			continue
		}
		if isUnsafe {
			size += element.allocated()
		}
		cnt++
	}
	return cnt, size
}

func (p *VectorPool) VarlenUsed(isUnsafe bool) (int, int) {
	size := 0
	cnt := 0
	for _, element := range p.varlenPool {
		if element.isIdle() {
			continue
		}
		if isUnsafe {
			size += element.allocated()
		}
		cnt++
	}
	return cnt, size
}

func (p *VectorPool) Used(isUnsafe bool) (int, int) {
	cnt, size := p.FixedSizeUsed(isUnsafe)
	cnt2, size2 := p.VarlenUsed(isUnsafe)
	cnt += cnt2
	size += size2
	return cnt, size
}

// Only for test
// It is not safe to call Destory
func (p *VectorPool) Destory() {
	fixSizedPool := p.fixSizedPool
	varlenPool := p.varlenPool
	mp := p.mp
	p.mp = nil
	p.fixSizedPool = nil
	p.varlenPool = nil

	for _, elem := range fixSizedPool {
		elem.vec.Free(mp)
	}
	for _, elem := range varlenPool {
		elem.vec.Free(mp)
	}
}

func newVectorElement(pool *VectorPool, t *types.Type, mp *mpool.MPool) *vectorPoolElement {
	vec := vector.NewVec(*t, mp)
	element := &vectorPoolElement{
		pool: pool,
		mp:   mp,
		vec:  vec,
	}
	return element
}

// ReleaseVector releases vec: a vector lent by a VectorPool goes back to its
// pool, any other vector is freed. Releasing a vector again, or the shared
// EMPTY_VECTOR, does nothing.
func ReleaseVector(vec Vector) {
	if vec == nil || vec == EMPTY_VECTOR {
		return
	}
	if returnToPool(vec) {
		return
	}
	vec.Free(vec.MPool())
}
