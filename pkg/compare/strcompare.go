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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func (c *strCompare) Vector() *vector.Vector {
	return c.vs[0]
}

func (c *strCompare) Set(idx int, v *vector.Vector) {
	c.vs[idx] = v
}

func (c *strCompare) Copy(vecSrc, vecDst int, src, dst int64, proc *process.Process) error {
	if nulls.Any(c.vs[vecSrc].Nsp) && nulls.Contains(c.vs[vecSrc].Nsp, uint64(src)) {
		nulls.Add(c.vs[vecDst].Nsp, uint64(dst))
		return nil
	}
	nulls.Del(c.vs[vecDst].Nsp, uint64(dst))
	return vector.Copy(c.vs[vecDst], c.vs[vecSrc], dst, src, proc.Mp)
}

func (c *strCompare) Compare(veci, vecj int, vi, vj int64) int {
	x, y := c.vs[veci].Col.(*types.Bytes), c.vs[vecj].Col.(*types.Bytes)
	if c.desc {
		return bytes.Compare(x.Get(vi), y.Get(vj)) * -1
	}
	return bytes.Compare(x.Get(vi), y.Get(vj))
}
