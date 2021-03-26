package varchar

import (
	"bytes"
	"matrixbase/pkg/container/types"
	"matrixbase/pkg/container/vector"
	"matrixbase/pkg/vm/process"
)

func New() *compare {
	return &compare{
		vs: make([]*vector.Vector, 2),
	}
}

func (c *compare) Vector() *vector.Vector {
	return c.vs[0]
}

func (c *compare) Set(idx int, v *vector.Vector) {
	c.vs[idx] = v
}

func (c *compare) Copy(vecSrc, vecDst int, src, dst int64, proc *process.Process) error {
	if c.vs[vecSrc].Nsp.Any() && c.vs[vecSrc].Nsp.Contains(uint64(src)) {
		c.vs[vecDst].Nsp.Add(uint64(dst))
		return nil
	}
	c.vs[vecDst].Nsp.Del(uint64(dst))
	return c.vs[vecDst].Copy(c.vs[vecSrc], dst, src, proc)
}

func (c *compare) Compare(veci, vecj int, vi, vj int64) int {
	x, y := c.vs[veci].Col.(*types.Bytes), c.vs[vecj].Col.(*types.Bytes)
	r := bytes.Compare(x.Get(int(vi)), y.Get(int(vj)))
	switch r {
	case +1:
		r = -1
	case -1:
		r = +1
	}
	return r
}
