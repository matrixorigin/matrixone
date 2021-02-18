package ints

import (
	"matrixbase/pkg/container/nulls"
	"matrixbase/pkg/container/types"
	"matrixbase/pkg/container/vector"
)

func New() *compare {
	return &compare{
		vs: make([][]int64, 2),
		ns: make([]*nulls.Nulls, 2),
	}
}

func (c *compare) Vector() *vector.Vector {
	return &vector.Vector{
		Col: c.vs[0],
		Nsp: c.ns[0],
		Typ: types.T_int,
	}
}

func (c *compare) Set(idx int, v *vector.Vector) {
	c.ns[idx] = v.Nsp
	c.vs[idx] = v.Ints()
}

func (c *compare) Copy(vecSrc, vecDst int, src, dst int64) int {
	if c.ns[vecSrc].Any() && c.ns[vecSrc].Contains(uint64(src)) {
		c.ns[vecDst].Add(uint64(dst))
	} else {
		c.ns[vecDst].Del(uint64(dst))
		c.vs[vecDst][dst] = c.vs[vecSrc][src]
	}
	return 0
}

func (c *compare) Compare(veci, vecj int, vi, vj int64) int {
	if c.vs[veci][vi] == c.vs[vecj][vj] {
		return 0
	}
	if c.vs[veci][vi] < c.vs[vecj][vj] {
		return -1
	}
	return +1
}
