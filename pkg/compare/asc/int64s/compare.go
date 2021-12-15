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

package int64s

import (
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func New() *compare {
	return &compare{
		xs: make([][]int64, 2),
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
	c.xs[idx] = v.Col.([]int64)
}

func (c *compare) Compare(veci, vecj int, vi, vj int64) int {
	if c.xs[veci][vi] == c.xs[vecj][vj] {
		return 0
	}
	if c.xs[veci][vi] < c.xs[vecj][vj] {
		return -1
	}
	return +1
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
