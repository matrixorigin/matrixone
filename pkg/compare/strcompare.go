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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
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
	if c.typ.Oid == types.T_any && !c.vs[vecSrc].IsNull(uint64(src)) &&
		!c.vs[vecSrc].GetGrouping().Contains(uint64(src)) {
		return moerr.NewNotSupportedNoCtx("non-NULL ANY comparison")
	}
	return c.vs[vecDst].Copy(c.vs[vecSrc], dst, src, proc.Mp())
}

func (c *strCompare) Compare(veci, vecj int, vi, vj int64) int {
	n0 := c.isConstNull[veci] || c.vs[veci].GetNulls().Contains(uint64(vi)) ||
		c.vs[veci].GetGrouping().Contains(uint64(vi))
	n1 := c.isConstNull[vecj] || c.vs[vecj].GetNulls().Contains(uint64(vj)) ||
		c.vs[vecj].GetGrouping().Contains(uint64(vj))
	// ANY is the domainless NULL type. Reuse NULL comparison without reading
	// payload bytes; concrete values still require a concrete comparison type.
	if c.typ.Oid == types.T_any && (!n0 || !n1) {
		panic(moerr.NewNotSupportedNoCtx("non-NULL ANY comparison"))
	}
	cmp := nullsCompare(n0, n1, c.nullsLast)
	if cmp != 0 {
		return cmp - nullsCompareFlag
	}
	x := c.vs[veci].GetBytesAt(int(vi))
	y := c.vs[vecj].GetBytesAt(int(vj))
	// The plan type is the semantic owner of the comparison domain. Runtime
	// vectors can be materialized through a storage or spill path that only
	// preserves the physical string OID, so deriving collation from the vector
	// would silently fall back to byte order.
	cmp = types.CompareStringOrderValues(c.typ, x, y)
	if c.desc {
		return -cmp
	}
	return cmp
}
