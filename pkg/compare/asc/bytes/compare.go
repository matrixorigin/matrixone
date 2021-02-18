package bytes

import (
	"bytes"
	"matrixbase/pkg/container/nulls"
	"matrixbase/pkg/container/types"
	"matrixbase/pkg/container/vector"
)

func New() *compare {
	return &compare{
		vs: make([]*vector.Bytes, 2),
		ns: make([]*nulls.Nulls, 2),
	}
}

func (c *compare) Vector() *vector.Vector {
	return &vector.Vector{
		Col: c.vs[0],
		Nsp: c.ns[0],
		Typ: types.T_bytes,
	}
}

func (c *compare) Set(idx int, v *vector.Vector) {
	c.ns[idx] = v.Nsp
	c.vs[idx] = v.Bytes()
}

func (c *compare) Copy(vecSrc, vecDst int, src, dst int64) int {
	if c.ns[vecSrc].Any() && c.ns[vecSrc].Contains(uint64(src)) {
		c.ns[vecDst].Add(uint64(dst))
		return 0
	} else {
		c.ns[vecDst].Del(uint64(dst))
		return c.vs[vecDst].Set(dst, c.vs[vecSrc], src)
	}
	return 0
}

func (c *compare) Compare(veci, vecj int, vi, vj int64) int {
	x, y := c.vs[veci], c.vs[vecj]
	return bytes.Compare(x.Data[x.Os[vi]:x.Os[vi]+x.Ns[vi]], y.Data[y.Os[vj]:y.Os[vj]+y.Ns[vj]])
}
