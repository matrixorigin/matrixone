package containers

import (
	"fmt"
	_ "unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// fix sized / (varlen + fix sized)
const fixedSizeRatio = 0.6

var _vectorPoolAlloactor *mpool.MPool

func init() {
	var err error
	if _vectorPoolAlloactor, err = mpool.NewMPool(
		"taeVectorPool", 0, mpool.NoFixed,
	); err != nil {
		panic(err)
	}
}

type VectorPool struct {
	name         string
	fixSizedPool []*vectorWrapper
	varlenPool   []*vectorWrapper
}

func _putVectorPool(vec *vectorWrapper) {
	vec.toIdle()
}

func NewVectorPool(name string, cnt int) *VectorPool {
	cnt1 := int(float64(cnt) * fixedSizeRatio)
	cnt2 := cnt - cnt1
	p := &VectorPool{
		name:         name,
		fixSizedPool: make([]*vectorWrapper, 0, cnt1),
		varlenPool:   make([]*vectorWrapper, 0, cnt2),
	}
	for i := 0; i < cnt1; i++ {
		t := types.T_int64.ToType()
		p.fixSizedPool = append(p.fixSizedPool, newVectorElement(&t))
	}
	for i := 0; i < cnt2; i++ {
		t := types.T_varchar.ToType()
		p.varlenPool = append(p.varlenPool, newVectorElement(&t))
	}
	return p
}

func (p *VectorPool) String() string {
	fixedUsedCnt, fixedUsedSize := p.FixedSizeUsed()
	varlenUsedCnt, varlenUsedSize := p.VarlenUsed()
	usedCnt, usedSize := fixedUsedCnt+varlenUsedCnt, fixedUsedSize+varlenUsedSize
	str := fmt.Sprintf(
		"VectorPool[%s][%d/%d:%d/%d]: FixSizedVec[%d/%d:%d/%d] VarlenVec[%d/%d:%d/%d]",
		p.name,                                /* name */
		usedCnt,                               /* total used vector cnt */
		len(p.fixSizedPool)+len(p.varlenPool), /* total vector cnt */
		usedSize,                              /* total used vector size */
		p.Allocated(),                         /* total vector size */
		fixedUsedCnt,                          /* used fixed sized vector cnt */
		len(p.fixSizedPool),                   /* total fixed sized vector cnt */
		fixedUsedSize,                         /* used fixed sized vector size */
		p.FixedSizeAllocated(),                /* total fixed sized vector size */
		varlenUsedCnt,                         /* used varlen vector cnt */
		len(p.varlenPool),                     /* total varlen vector cnt */
		varlenUsedSize,                        /* used varlen vector size */
		p.VarlenaSizeAllocated(),              /* total varlen vector size */
	)
	return str
}

func (p *VectorPool) GetVector(t *types.Type) *vectorWrapper {
	if t.IsFixedLen() {
		for i := 0; i < 4; i++ {
			idx := fastrand() % uint32(len(p.fixSizedPool))
			vec := p.fixSizedPool[idx]
			// if !vec.GetType().IsFixedLen() {
			// 	panic("logic error: vector type should be fix sized")
			// }
			if vec.tryReuse(t) {
				return vec
			}
		}
	} else {
		for i := 0; i < 4; i++ {
			idx := fastrand() % uint32(len(p.varlenPool))
			vec := p.varlenPool[idx]
			if vec.tryReuse(t) {
				return vec
			}
		}
	}

	return NewVector(*t)
}

func (p *VectorPool) Allocated() int {
	size := 0
	size += p.FixedSizeAllocated()
	size += p.VarlenaSizeAllocated()
	return size
}

func (p *VectorPool) FixedSizeAllocated() int {
	size := 0
	for _, vec := range p.fixSizedPool {
		size += vec.Allocated()
	}
	return size
}

func (p *VectorPool) VarlenaSizeAllocated() int {
	size := 0
	for _, vec := range p.varlenPool {
		size += vec.Allocated()
	}
	return size
}

func (p *VectorPool) FixedSizeUsed() (int, int) {
	size := 0
	cnt := 0
	for _, vec := range p.fixSizedPool {
		if vec.isIdle() {
			continue
		}
		size += vec.Allocated()
		cnt++
	}
	return cnt, size
}

func (p *VectorPool) VarlenUsed() (int, int) {
	size := 0
	cnt := 0
	for _, vec := range p.varlenPool {
		if vec.isIdle() {
			continue
		}
		size += vec.Allocated()
		cnt++
	}
	return cnt, size
}

func (p *VectorPool) Used() (int, int) {
	cnt, size := p.FixedSizeUsed()
	cnt2, size2 := p.VarlenUsed()
	cnt += cnt2
	size += size2
	return cnt, size
}

func newVectorElement(t *types.Type) *vectorWrapper {
	opts := Options{
		Allocator: _vectorPoolAlloactor,
	}
	vec := NewVector(*t, opts)
	vec.put = _putVectorPool
	return vec
}

//go:linkname fastrand runtime.fastrand
func fastrand() uint32
