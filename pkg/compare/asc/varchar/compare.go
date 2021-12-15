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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
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
	return bytes.Compare(x.Get(vi), y.Get(vj))
}
