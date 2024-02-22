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

package aggexec

import (
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type aggFuncResult[T types.FixedSizeTExceptStrType] struct {
	proc   *process.Process
	mp     *mpool.MPool
	typ    types.Type
	res    *vector.Vector
	values []T // for quick get/set

	groupToSet int // row index for aggGet() and aggSet()
}

type aggFuncBytesResult struct {
	proc       *process.Process
	mp         *mpool.MPool
	typ        types.Type
	res        *vector.Vector
	groupToSet int // row index for aggGet() and aggSet()
}

func initFixedAggFuncResult[T types.FixedSizeTExceptStrType](proc *process.Process, typ types.Type) aggFuncResult[T] {
	return aggFuncResult[T]{
		proc:       proc,
		mp:         proc.Mp(),
		typ:        typ,
		res:        proc.GetVector(typ),
		groupToSet: 0,
	}
}

func (r *aggFuncResult[T]) grows(more int) error {
	newLen := r.res.Length() + more
	if err := r.res.PreExtend(newLen, r.mp); err != nil {
		return err
	}
	r.res.SetLength(newLen)
	r.values = vector.MustFixedCol[T](r.res)
	return nil
}

func (r *aggFuncResult[T]) aggGet() T {
	return r.values[r.groupToSet]
}

// for agg private structure's fill.
func (r *aggFuncResult[T]) aggSet(v T) {
	r.values[r.groupToSet] = v
}

func (r *aggFuncResult[T]) flush() *vector.Vector {
	result := r.res
	r.res = nil
	return result
}

func (r *aggFuncResult[T]) free() {
	if r.res == nil {
		return
	}
	r.proc.PutVector(r.res)
}

func initBytesAggFuncResult(proc *process.Process, typ types.Type) aggFuncBytesResult {
	return aggFuncBytesResult{
		proc:       proc,
		mp:         proc.Mp(),
		typ:        typ,
		res:        proc.GetVector(typ),
		groupToSet: 0,
	}
}

func (r *aggFuncBytesResult) grows(more int) error {
	return r.res.PreExtend(r.res.Length()+more, r.mp)
}

func (r *aggFuncBytesResult) aggGet() []byte {
	// todo: we cannot do simple optimization here because the set method may change
	//  the max length of the vector.
	//  if we want, we should add a flag to indicate that the vector item's length is <= types.VarlenaInlineSize.
	return r.res.GetBytesAt(r.groupToSet)
}

func (r *aggFuncBytesResult) aggSet(v []byte) error {
	return vector.SetBytesAt(r.res, r.groupToSet, v, r.mp)
}

func (r *aggFuncBytesResult) flush() *vector.Vector {
	result := r.res
	r.res = nil
	return result
}

func (r *aggFuncBytesResult) free() {
	if r.res == nil {
		return
	}
	r.proc.PutVector(r.res)
}
