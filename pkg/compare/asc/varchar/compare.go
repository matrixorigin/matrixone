// Copyright 2021 Matrix Origin
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

package varchar

import (
	"bytes"
	"matrixone/pkg/container/nulls"
	"matrixone/pkg/container/types"
	"matrixone/pkg/container/vector"
	"matrixone/pkg/vm/process"
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
	if nulls.Any(c.vs[vecSrc].Nsp) && nulls.Contains(c.vs[vecSrc].Nsp, uint64(src)) {
		nulls.Add(c.vs[vecDst].Nsp, uint64(dst))
		return nil
	}
	nulls.Del(c.vs[vecDst].Nsp, uint64(dst))
	return vector.Copy(c.vs[vecDst], c.vs[vecSrc], dst, src, proc.Mp)
}

func (c *compare) Compare(veci, vecj int, vi, vj int64) int {
	x, y := c.vs[veci].Col.(*types.Bytes), c.vs[vecj].Col.(*types.Bytes)
	return bytes.Compare(x.Get(vi), y.Get(vj))
}
