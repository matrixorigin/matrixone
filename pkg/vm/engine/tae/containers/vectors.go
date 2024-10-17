// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package containers

import (
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

type Vectors []vector.Vector

func NewVectors(n int) Vectors {
	return make([]vector.Vector, n)
}

func (vs Vectors) Free(mp *mpool.MPool) {
	for i := range vs {
		vs[i].Free(mp)
	}
}

func (vs Vectors) Allocated() int {
	n := 0
	for i := range vs {
		n += vs[i].Allocated()
	}
	return n
}

func (vs Vectors) Rows() int {
	if len(vs) == 0 {
		return 0
	}
	return vs[0].Length()
}
