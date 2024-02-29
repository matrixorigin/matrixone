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

func WithFixedSizeRatio(ratio float64) VectorPoolOption {
	return func(p *VectorPool) {
		p.ratio = ratio
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
}

func (e *vectorPoolElement) tryReuse(t *types.Type) bool {
	if e.inUse.CompareAndSwap(false, true) {
		e.vec.ResetWithNewType(t)
		return true
	}
	return false
}

func (e *vectorPoolElement) put() {
	if e.vec.Allocated() > e.pool.maxAlloc {
		newVec := vector.NewVec(*e.vec.GetType())
		e.vec.Free(e.mp)
		e.vec = newVec
		e.pool.resetStats.Add(1)
	}
	if !e.inUse.CompareAndSwap(true, false) {
		panic("vectorPoolElement.put: vector is not in use")
	}
}

func (e *vectorPoolElement) isIdle() bool {
	return !e.inUse.Load()
}

type VectorPool struct {
	name           string
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

func (p *VectorPool) GetVector(t *types.Type) *vectorWrapper {
	p.totalStats.Add(1)
	if t.IsFixedLen() {
		if len(p.fixSizedPool) > 0 {
			for i := 0; i < 4; i++ {
				idx := fastrand() % uint32(len(p.fixSizedPool))
				element := p.fixSizedPool[idx]
				if element.tryReuse(t) {
					p.fixHitStats.Add(1)
					return &vectorWrapper{
						wrapped: element.vec,
						mpool:   element.mp,
						element: element,
					}
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
					return &vectorWrapper{
						wrapped: element.vec,
						mpool:   element.mp,
						element: element,
					}
				}
			}
		}
	}

	return NewVector(*t, Options{Allocator: p.mp})
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
		size += element.vec.Allocated()
	}
	return size
}

func (p *VectorPool) VarlenaSizeAllocated() int {
	size := 0
	for _, element := range p.varlenPool {
		size += element.vec.Allocated()
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
			size += element.vec.Allocated()
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
			size += element.vec.Allocated()
		}
		cnt++
	}
	return cnt, size
}

func (p *VectorPool) GetAllocator() *mpool.MPool {
	return p.mp
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
	vec := vector.NewVec(*t)
	element := &vectorPoolElement{
		pool: pool,
		mp:   mp,
		vec:  vec,
	}
	return element
}

//go:linkname fastrand runtime.fastrand
func fastrand() uint32
