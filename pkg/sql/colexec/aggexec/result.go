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
)

type aggFuncResult[T types.FixedSizeTExceptStrType] struct {
	mg     AggMemoryManager
	mp     *mpool.MPool
	typ    types.Type
	res    *vector.Vector
	values []T // for quick get/set

	groupToSet  int  // row index for aggGet() and aggSet()
	emptyBeNull bool // indicate that if we should set null to the new row.
}

type aggFuncBytesResult struct {
	mg          AggMemoryManager
	mp          *mpool.MPool
	typ         types.Type
	res         *vector.Vector
	groupToSet  int  // row index for aggGet() and aggSet()
	emptyBeNull bool // indicate that if we should set null to the new row.
}

func initFixedAggFuncResult[T types.FixedSizeTExceptStrType](
	mg AggMemoryManager, typ types.Type,
	emptyNull bool) aggFuncResult[T] {
	if mg == nil {
		return aggFuncResult[T]{
			typ:         typ,
			res:         vector.NewVec(typ),
			emptyBeNull: emptyNull,
		}
	}
	return aggFuncResult[T]{
		mg:          mg,
		mp:          mg.Mp(),
		typ:         typ,
		res:         mg.GetVector(typ),
		groupToSet:  0,
		emptyBeNull: emptyNull,
	}
}

// todo: there is a bug here if we set result as null when group grows.
//
//	this row cannot be set value again because the null flag was set.
//	should use a bool array to indicate that if a group was empty.
func (r *aggFuncResult[T]) grows(more int) error {
	oldLen, newLen := r.res.Length(), r.res.Length()+more
	if err := r.res.PreExtend(newLen, r.mp); err != nil {
		return err
	}
	r.res.SetLength(newLen)
	r.values = vector.MustFixedCol[T](r.res)

	// reset the new row.
	{
		var v T
		for i, j := oldLen, newLen; i < j; i++ {
			r.values[i] = v
		}
		if r.emptyBeNull {
			for i, j := uint64(oldLen), uint64(newLen); i < j; i++ {
				r.res.GetNulls().Set(i)
			}
		}
	}
	return nil
}

func (r *aggFuncResult[T]) aggGet() T {
	return r.values[r.groupToSet]
}

// for agg private structure's Fill.
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
	if r.res.NeedDup() {
		r.res.Free(r.mp)
	}
	r.mg.PutVector(r.res)
}

func (r *aggFuncResult[T]) marshal() ([]byte, error) {
	return r.res.MarshalBinary()
}

func (r *aggFuncResult[T]) unmarshal(data []byte) error {
	return r.res.UnmarshalBinary(data)
}

func initBytesAggFuncResult(
	mg AggMemoryManager, typ types.Type,
	emptyNull bool) aggFuncBytesResult {
	if mg == nil {
		return aggFuncBytesResult{
			typ:         typ,
			res:         vector.NewVec(typ),
			emptyBeNull: emptyNull,
		}
	}
	return aggFuncBytesResult{
		mg:          mg,
		mp:          mg.Mp(),
		typ:         typ,
		res:         mg.GetVector(typ),
		groupToSet:  0,
		emptyBeNull: emptyNull,
	}
}

func (r *aggFuncBytesResult) grows(more int) error {
	oldLen, newLen := r.res.Length(), r.res.Length()+more
	if err := r.res.PreExtend(newLen, r.mp); err != nil {
		return err
	}
	r.res.SetLength(newLen)

	// reset the new row.
	{
		var v = []byte("")
		for i, j := oldLen, newLen; i < j; i++ {
			// this will never cause error.
			_ = vector.SetBytesAt(r.res, i, v, r.mp)
		}
		if r.emptyBeNull {
			for i, j := uint64(oldLen), uint64(newLen); i < j; i++ {
				r.res.GetNulls().Set(i)
			}
		}
	}
	return nil
}

func (r *aggFuncBytesResult) aggGet() []byte {
	// todo: we cannot do simple optimization to get bytes here because result was not read-only.
	//  the set method may change the max length of the vector.
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
	if r.res == nil || r.mg == nil {
		return
	}
	if r.res.NeedDup() {
		r.res.Free(r.mp)
	}
	r.mg.PutVector(r.res)
}

func (r *aggFuncBytesResult) marshal() ([]byte, error) {
	return r.res.MarshalBinary()
}

func (r *aggFuncBytesResult) unmarshal(data []byte) error {
	return r.res.UnmarshalBinary(data)
}
