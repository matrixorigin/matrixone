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
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

// PutBatch updates the reference count of the batch.
// when this batch is no longer in use, places all vectors into the pool.
func (proc *Process) PutBatch(bat *batch.Batch) {
	// situations that batch was still in use.
	// we use `!= 0` but not `>0` to avoid the situation that the batch was cleaned more than required.
	if bat == batch.EmptyBatch || atomic.AddInt64(&bat.Cnt, -1) != 0 {
		return
	}

	for _, vec := range bat.Vecs {
		if vec != nil {
			proc.PutVector(vec)
			bat.ReplaceVector(vec, nil)
		}
	}
	for _, agg := range bat.Aggs {
		if agg != nil {
			agg.Free()
		}
	}
	bat.Aggs = nil
	bat.Vecs = nil
	bat.Attrs = nil
	bat.SetRowCount(0)
}

// PutVector attempts to put the vector to the pool.
// It should be noted that, for performance and correct memory usage, we won't call the reset() action here.
//
// If the put operation fails, it releases the memory of the vector.
func (proc *Process) PutVector(vec *vector.Vector) {
	if !proc.Base.vp.putVectorIntoPool(vec) {
		vec.Free(proc.Mp())
	}
}

// GetVector attempts to retrieve a vector of a specified type from the pool.
//
// If the get operation fails, it allocates a new vector to return.
func (proc *Process) GetVector(typ types.Type) *vector.Vector {
	if typ.Oid == types.T_any {
		return vector.NewVec(typ)
	}
	if vec := proc.Base.vp.getVectorFromPool(typ); vec != nil {
		vec.Reset(typ)
		return vec
	}
	return vector.NewVec(typ)
}

// FreeVectors release the vector pool.
func (proc *Process) FreeVectors() {
	proc.Base.vp.free(proc.Base.mp)
}
