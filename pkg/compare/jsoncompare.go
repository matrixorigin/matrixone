// Copyright 2026 Matrix Origin
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
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// jsonCompare implements the SQL ORDER BY relation for JSON. The generic
// string comparator remains the physical byte-order comparator used by
// identity consumers and persisted JSON keys.
type jsonCompare struct {
	desc        bool
	nullsLast   bool
	vs          []*vector.Vector
	isConstNull []bool
}

func (c *jsonCompare) Vector() *vector.Vector {
	return c.vs[0]
}

func (c *jsonCompare) Set(idx int, v *vector.Vector) {
	c.vs[idx] = v
	c.isConstNull[idx] = v.IsConstNull()
}

func (c *jsonCompare) Copy(vecSrc, vecDst int, src, dst int64, proc *process.Process) error {
	return c.vs[vecDst].Copy(c.vs[vecSrc], dst, src, proc.Mp())
}

func (c *jsonCompare) Compare(veci, vecj int, vi, vj int64) int {
	n0 := c.isConstNull[veci] || c.vs[veci].GetNulls().Contains(uint64(vi)) ||
		c.vs[veci].GetGrouping().Contains(uint64(vi))
	n1 := c.isConstNull[vecj] || c.vs[vecj].GetNulls().Contains(uint64(vj)) ||
		c.vs[vecj].GetGrouping().Contains(uint64(vj))
	cmp := nullsCompare(n0, n1, c.nullsLast)
	if cmp != 0 {
		return cmp - nullsCompareFlag
	}

	left := types.DecodeJson(c.vs[veci].GetBytesAt(int(vi)))
	right := types.DecodeJson(c.vs[vecj].GetBytesAt(int(vj)))
	cmp = bytejson.CompareByteJson(left, right)
	if c.desc {
		return -cmp
	}
	return cmp
}
