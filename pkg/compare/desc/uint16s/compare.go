package uint16s

import (
	"matrixbase/pkg/container/nulls"
	"matrixbase/pkg/container/vector"
	"matrixbase/pkg/vm/process"
)

func New() *compare {
	return &compare{
		xs: make([][]uint16, 2),
		ns: make([]*nulls.Nulls, 2),
		vs: make([]*vector.Vector, 2),
	}
}

func (c *compare) Vector() *vector.Vector {
	return c.vs[0]
}

func (c *compare) Set(idx int, v *vector.Vector) {
	c.vs[idx] = v
	c.ns[idx] = v.Nsp
	c.xs[idx] = v.Col.([]uint16)
}

func (c *compare) Compare(veci, vecj int, vi, vj int64) int {
	if c.xs[veci][vi] == c.xs[vecj][vj] {
		return 0
	}
	if c.xs[veci][vi] < c.xs[vecj][vj] {
		return +1
	}
	return -1
}

func (c *compare) Copy(vecSrc, vecDst int, src, dst int64, _ *process.Process) error {
	if c.ns[vecSrc].Any() && c.ns[vecSrc].Contains(uint64(src)) {
		c.ns[vecDst].Add(uint64(dst))
	} else {
		c.ns[vecDst].Del(uint64(dst))
		c.xs[vecDst][dst] = c.xs[vecSrc][src]
	}
	return nil
}
