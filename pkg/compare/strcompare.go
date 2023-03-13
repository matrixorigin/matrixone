// Copyright 2021 - 2022 Matrix Origin
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
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func (c *strCompare) Vector() *vector.Vector {
	return c.vs[0]
}

func (c *strCompare) Set(idx int, v *vector.Vector) {
	c.vs[idx] = v
	c.isConstNull[idx] = v.IsConstNull()
}

func (c *strCompare) Copy(vecSrc, vecDst int, src, dst int64, proc *process.Process) error {
	if c.isConstNull[vecSrc] || c.vs[vecSrc].GetNulls().Contains(uint64(src)) {
		nulls.Add(c.vs[vecDst].GetNulls(), uint64(dst))
		return nil
	} else {
		nulls.Del(c.vs[vecDst].GetNulls(), uint64(dst))
		return c.vs[vecDst].Copy(c.vs[vecSrc], dst, src, proc.Mp())
	}
}

func (c *strCompare) Compare(veci, vecj int, vi, vj int64) int {
	n0 := c.isConstNull[veci] || c.vs[veci].GetNulls().Contains(uint64(vi))
	n1 := c.isConstNull[vecj] || c.vs[vecj].GetNulls().Contains(uint64(vj))
	cmp := nullsCompare(n0, n1, c.nullsLast)
	if cmp != 0 {
		return cmp - nullsCompareFlag
	}
	x := c.vs[veci].GetBytesAt(int(vi))
	y := c.vs[vecj].GetBytesAt(int(vj))
	if c.desc {
		return bytes.Compare(y, x)
	}
	return bytes.Compare(x, y)
}
