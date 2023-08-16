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

package agg

import (
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

func (a *UnaryAgg[T1, T2]) Free2(pool *mpool.MPool) {
	if a.otyp.IsVarlen() {
		return
	}
	if cap(a.da) > 0 {
		pool.Free(a.da)
	}
}

func (a *UnaryAgg[T1, T2]) Grows2(count int, pool *mpool.MPool) error {
	a.grows(count)

	finalCount := len(a.es) + count
	// allocate memory from pool except for string type.
	if a.otyp.IsVarlen() {
		// first time.
		if len(a.es) == 0 {
			a.vs = make([]T2, 0, count)
			a.es = make([]bool, count)
			for i := range a.es {
				a.es[i] = true
			}
		} else {
			var emptyResult T2
			for i := len(a.es); i < finalCount; i++ {
				a.es = append(a.es, true)
				a.vs = append(a.vs, emptyResult)
			}
		}

	} else {
		itemSize := a.otyp.TypeSize()
		if len(a.es) == 0 {
			data, err := pool.Alloc(count * itemSize)
			if err != nil {
				return err
			}
			a.da = data
			a.vs = types.DecodeSlice[T2](a.da)

		} else {
			data, err := pool.Grow(a.da, (count+len(a.es))*itemSize)
			if err != nil {
				return err
			}
			a.da = data
			a.vs = types.DecodeSlice[T2](a.da)
		}

		a.vs = a.vs[:finalCount]
		a.da = a.da[:finalCount*itemSize]
		for i := len(a.es); i < finalCount; i++ {
			a.es = append(a.es, true)
		}
		a.es = a.es[:finalCount]
	}
	return nil
}

func (a *UnaryAgg[T1, T2]) Fill2(groupIdx int64, rowIndex int64, vectors []*vector.Vector) error {
	return nil
}
