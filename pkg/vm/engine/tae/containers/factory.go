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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
)

func MakeVector(typ types.Type, mp *mpool.MPool) (vec Vector) {
	if mp == nil {
		mp = common.DefaultAllocator
	}
	return vector.NewVec(typ, mp)
}

func BuildBatchWithPool(
	attrs []string, colTypes []types.Type, capacity int, pool *VectorPool,
) *Batch {
	bat := &Batch{
		Attrs:   make([]string, 0, len(attrs)),
		Nameidx: make(map[string]int, len(attrs)),
		Vecs:    make([]Vector, 0, len(attrs)),
		Pool:    pool,
	}
	for i, attr := range attrs {
		vec := pool.GetVector(&colTypes[i])
		if capacity > 0 {
			if err := vec.PreExtend(capacity, vec.MPool()); err != nil {
				panic(err)
			}
		}
		bat.AddVector(attr, vec)
	}
	return bat
}

func BuildBatch(attrs []string, colTypes []types.Type, opts Options) *Batch {
	bat := &Batch{
		Attrs:   make([]string, 0, len(attrs)),
		Nameidx: make(map[string]int, len(attrs)),
		Vecs:    make([]Vector, 0, len(attrs)),
	}
	for i, attr := range attrs {
		vec := MakeVector(colTypes[i], opts.Allocator)
		if opts.Capacity > 0 {
			if err := vec.PreExtend(opts.Capacity, vec.MPool()); err != nil {
				panic(err)
			}
		}
		bat.AddVector(attr, vec)
	}
	return bat
}

func NewEmptyBatch() *Batch {
	return &Batch{
		Attrs:   make([]string, 0),
		Vecs:    make([]Vector, 0),
		Nameidx: make(map[string]int),
	}
}
