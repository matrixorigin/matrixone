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

package compare

import (
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorize/moarray"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func (c arrayCompare) Vector() *vector.Vector {
	return c.vs[0]
}

func (c arrayCompare) Set(idx int, v *vector.Vector) {
	c.vs[idx] = v
	c.isConstNull[idx] = v.IsConstNull()
}

func (c arrayCompare) Copy(vecSrc, vecDst int, src, dst int64, proc *process.Process) error {
	if c.vs[vecSrc].GetRollups().Contains(uint64(src)) {
		nulls.Add(c.vs[vecDst].GetRollups(), uint64(dst))
	} else {
		nulls.Del(c.vs[vecDst].GetRollups(), uint64(dst))
		c.vs[vecDst].Copy(c.vs[vecSrc], dst, src, proc.Mp())
	}
	if c.isConstNull[vecSrc] || c.vs[vecSrc].GetNulls().Contains(uint64(src)) {
		nulls.Add(c.vs[vecDst].GetNulls(), uint64(dst))
		return nil
	} else {
		nulls.Del(c.vs[vecDst].GetNulls(), uint64(dst))
		return c.vs[vecDst].Copy(c.vs[vecSrc], dst, src, proc.Mp())
	}
}

func (c arrayCompare) Compare(veci, vecj int, vi, vj int64) int {
	n0 := c.isConstNull[veci] || c.vs[veci].GetNulls().Contains(uint64(vi))
	n1 := c.isConstNull[vecj] || c.vs[vecj].GetNulls().Contains(uint64(vj))
	cmp := nullsCompare(n0, n1, c.nullsLast)
	if cmp != 0 {
		return cmp - nullsCompareFlag
	}
	_x := c.vs[veci].GetBytesAt(int(vi))
	_y := c.vs[vecj].GetBytesAt(int(vj))

	switch c.vs[veci].GetType().Oid {
	case types.T_array_float32:
		return CompareArrayFromBytes[float32](_x, _y, c.desc)
	case types.T_array_float64:
		return CompareArrayFromBytes[float64](_x, _y, c.desc)
	default:
		panic("Compare Not supported")
	}
}

func CompareArrayFromBytes[T types.RealNumbers](_x, _y []byte, desc bool) int {
	x := types.BytesToArray[T](_x)
	y := types.BytesToArray[T](_y)

	if desc {
		return moarray.Compare[T](y, x)
	}
	return moarray.Compare[T](x, y)
}
