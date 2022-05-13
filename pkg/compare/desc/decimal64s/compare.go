package decimal64s

import (
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func New() *compare {
	return &compare{
		xs: make([][]types.Decimal64, 2),
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
	c.xs[idx] = v.Col.([]types.Decimal64)
}

func (c *compare) Compare(veci, vecj int, vi, vj int64) int {
	return -int(types.CompareDecimal64Decimal64(c.xs[veci][vi], c.xs[vecj][vj], c.vs[0].Typ.Scale, c.vs[1].Typ.Scale))
}

func (c *compare) Copy(vecSrc, vecDst int, src, dst int64, _ *process.Process) error {
	if nulls.Any(c.ns[vecSrc]) && nulls.Contains(c.ns[vecSrc], (uint64(src))) {
		nulls.Add(c.ns[vecDst], (uint64(dst)))
	} else {
		nulls.Del(c.ns[vecDst], (uint64(dst)))
		c.xs[vecDst][dst] = c.xs[vecSrc][src]
	}
	return nil
}
