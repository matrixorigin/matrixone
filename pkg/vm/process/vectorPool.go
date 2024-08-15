// Copyright 2024 Matrix Origin
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

package process

import (
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"sync"
)

// defaultVectorPoolSize is the default pool capacity of vector pool for one query.
const defaultVectorPoolSize = 32

// defaultMaxVectorItemSize is the max size of accepted item can enter the vector pool.
const defaultMaxVectorItemSize = 8192 * 64

const (
	// pool index.
	poolWithoutArea = 0
	poolWithArea    = 1
)

// cachedVectorPool is a session-level attribute that is used to store a batch of vectors for reuse.
// It provides external methods such as getVector, putVector and free.
//
// It should be noted that "poolCapacity" refers to the size of each type of pool, not the total size.
//
// todo: in the future, it should be considered to use the size of memory as the capacity.
// todo: session-level pool seems to be somewhat redundant.
type cachedVectorPool struct {
	sync.Mutex
	poolCapacity int

	//memoryCapacity int

	// pool[0] stores vector without area.
	// pool[1] stores vector with area.
	pool [2][]*vector.Vector
}

func initCachedVectorPool() *cachedVectorPool {
	return &cachedVectorPool{
		poolCapacity: defaultVectorPoolSize,
		pool: [2][]*vector.Vector{
			make([]*vector.Vector, 0, defaultVectorPoolSize),
			make([]*vector.Vector, 0, defaultVectorPoolSize),
		},
	}
}

// vectorCannotPut return true if vector cannot match pool requirement.
// the following types of vectors will not be put into the pool.
// 1. const vector.
// 2. vector with any cantFree flag.
// 3. vector with very big memory.
// todo: should we reject the very small vector ?.
func vectorCannotPut(vec *vector.Vector) bool {
	return vec.IsConst() || vec.NeedDup() || vec.Allocated() > defaultMaxVectorItemSize
}

func (vp *cachedVectorPool) putVectorIntoPool(vec *vector.Vector) bool {
	// put into specific pool.
	putToSpecificPool := func(v *vector.Vector, idx int) bool {
		vp.Lock()
		defer vp.Unlock()

		if len(vp.pool[idx]) < vp.poolCapacity {
			vp.pool[idx] = append(vp.pool[idx], v)
			return true
		}
		return false
	}

	if vectorCannotPut(vec) {
		return false
	}
	if cap(vec.GetArea()) > 0 {
		return putToSpecificPool(vec, poolWithArea)
	}
	return putToSpecificPool(vec, poolWithoutArea)
}

func (vp *cachedVectorPool) getVectorFromPool(typ types.Type) *vector.Vector {
	// get from specific pool.
	getFromSpecificPool := func(idx int) *vector.Vector {
		k := len(vp.pool[idx]) - 1
		if k >= 0 {
			vec := vp.pool[idx][k]
			vp.pool[idx] = vp.pool[idx][:k]
			return vec
		}
		return nil
	}

	// get vector from pools in order.
	getFromPoolsInOrder := func(first, second int) *vector.Vector {
		if vec := getFromSpecificPool(first); vec != nil {
			return vec
		}
		return getFromSpecificPool(second)
	}

	isVar := typ.IsVarlen()
	var result *vector.Vector

	vp.Lock()
	if isVar {
		result = getFromPoolsInOrder(poolWithArea, poolWithoutArea)
	} else {
		result = getFromSpecificPool(poolWithoutArea)
	}
	vp.Unlock()

	return result
}

func (vp *cachedVectorPool) memorySize() int64 {
	vp.Lock()
	defer vp.Unlock()

	total := int64(0)
	for _, v := range vp.pool[poolWithoutArea] {
		total += int64(v.Allocated())
	}
	for _, v := range vp.pool[poolWithArea] {
		total += int64(v.Allocated())
	}
	return total
}

func (vp *cachedVectorPool) modifyCapacity(vectorNumber int, mp *mpool.MPool) {
	vp.Lock()
	defer vp.Unlock()

	vp.poolCapacity = vectorNumber

	// modify pool 0.
	if len(vp.pool[poolWithoutArea]) > vectorNumber {
		for i := vectorNumber - 1; i < len(vp.pool[poolWithoutArea]); i++ {
			vp.pool[poolWithoutArea][i].Free(mp)
		}
		vp.pool[poolWithoutArea] = vp.pool[poolWithoutArea][:vectorNumber:vectorNumber]
	}

	// modify pool 1.
	if len(vp.pool[poolWithArea]) > vectorNumber {
		for i := vectorNumber - 1; i < len(vp.pool[poolWithArea]); i++ {
			vp.pool[poolWithArea][i].Free(mp)
		}
		vp.pool[poolWithArea] = vp.pool[poolWithArea][:vectorNumber:vectorNumber]
	}
}

func (vp *cachedVectorPool) free(mp *mpool.MPool) {
	vp.Lock()
	defer vp.Unlock()

	// clean pool 0.
	for _, vec := range vp.pool[poolWithoutArea] {
		vec.Free(mp)
	}
	vp.pool[poolWithoutArea] = vp.pool[poolWithoutArea][:0]

	// clean pool 1.
	for _, vec := range vp.pool[poolWithArea] {
		vec.Free(mp)
	}
	vp.pool[poolWithArea] = vp.pool[poolWithArea][:0]
}
