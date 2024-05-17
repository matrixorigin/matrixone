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
	"bytes"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

type basicResult struct {
	mg          AggMemoryManager
	mp          *mpool.MPool
	typ         types.Type
	res         *vector.Vector
	ess         *vector.Vector // empty situation.
	empty       []bool
	groupToSet  int  // row index for aggGet() and aggSet()
	emptyBeNull bool // indicate that if we should set null to the new row.
}

func (r *basicResult) init(
	mg AggMemoryManager, typ types.Type,
	emptyNull bool) {
	r.typ = typ
	r.emptyBeNull = emptyNull
	r.groupToSet = 0
	if mg == nil {
		return
	}
	r.mg = mg
	r.mp = mg.Mp()
	r.res = mg.GetVector(typ)
	r.ess = mg.GetVector(types.T_bool.ToType())
}

func (r *basicResult) extend(more int) (oldLen, newLen int, err error) {
	oldLen, newLen = r.res.Length(), r.res.Length()+more
	if err = r.res.PreExtend(more, r.mp); err != nil {
		return oldLen, oldLen, err
	}
	if err = r.ess.PreExtend(more, r.mp); err != nil {
		return oldLen, oldLen, err
	}
	r.res.SetLength(newLen)
	r.ess.SetLength(newLen)

	r.empty = vector.MustFixedCol[bool](r.ess)
	for i := oldLen; i < newLen; i++ {
		r.empty[i] = true
	}
	return oldLen, newLen, nil
}

func (r *basicResult) preAllocate(more int) (err error) {
	oldLen := r.res.Length()
	if err = r.res.PreExtend(more, r.mp); err != nil {
		return err
	}
	if err = r.ess.PreExtend(more, r.mp); err != nil {
		return err
	}
	r.res.SetLength(oldLen)
	r.ess.SetLength(oldLen)
	return nil
}

func (r *basicResult) mergeEmpty(other basicResult, i, j int) {
	r.empty[i] = r.empty[i] && other.empty[j]
}

func (r *basicResult) groupIsEmpty(i int) bool {
	return r.empty[i]
}

func (r *basicResult) setGroupNotEmpty(i int) {
	r.empty[i] = false
}

func (r *basicResult) flush() *vector.Vector {
	if r.emptyBeNull {
		nsp := nulls.NewWithSize(len(r.empty))
		for i, j := uint64(0), uint64(len(r.empty)); i < j; i++ {
			if r.empty[i] {
				nsp.Add(i)
			}
		}
		r.res.SetNulls(nsp)
	}
	result := r.res
	r.res = nil
	return result
}

func (r *basicResult) free() {
	if r.mg == nil {
		return
	}
	if r.res != nil {
		if r.res.NeedDup() {
			r.res.Free(r.mp)
		} else {
			r.mg.PutVector(r.res)
		}
	}
	if r.ess != nil {
		if r.ess.NeedDup() {
			r.ess.Free(r.mp)
		} else {
			r.mg.PutVector(r.ess)
		}
	}
}

func (r *basicResult) eq0(other basicResult) bool {
	if !r.typ.Eq(other.typ) {
		return false
	}
	bs1 := vector.MustFixedCol[bool](r.ess)
	bs2 := vector.MustFixedCol[bool](other.ess)
	if len(bs1) != len(bs2) {
		return false
	}
	for i, j := 0, len(bs1); i < j; i++ {
		if bs1[i] != bs2[i] {
			return false
		}
	}
	return true
}

func (r *basicResult) marshal() ([]byte, error) {
	d1, err := r.res.MarshalBinary()
	if err != nil {
		return nil, err
	}
	d2, err := r.ess.MarshalBinary()
	if err != nil {
		return nil, err
	}
	d := make([]byte, 0, 4+len(d1)+len(d2))
	length := uint32(len(d1))
	d = append(d, types.EncodeUint32(&length)...)
	d = append(d, d1...)
	d = append(d, d2...)
	return d, nil
}

func (r *basicResult) unmarshal0(data []byte) error {
	if r.mg == nil {
		r.res = vector.NewVec(r.typ)
		r.ess = vector.NewVec(types.T_bool.ToType())
	} else {
		r.res = r.mg.GetVector(r.typ)
		r.ess = r.mg.GetVector(types.T_bool.ToType())
	}

	length := types.DecodeUint32(data[:4])
	data = data[4:]

	var mp *mpool.MPool = nil
	if r.mg != nil {
		mp = r.mg.Mp()
	}

	if err := vectorUnmarshal(r.res, data[:length], mp); err != nil {
		return err
	}
	data = data[length:]
	if err := vectorUnmarshal(r.ess, data, mp); err != nil {
		r.res.Free(mp)
		return err
	}
	r.empty = vector.MustFixedCol[bool](r.ess)
	return nil
}

type aggFuncResult[T types.FixedSizeTExceptStrType] struct {
	basicResult
	values []T // for quick get/set
}

type aggFuncBytesResult struct {
	basicResult
}

func initFixedAggFuncResult[T types.FixedSizeTExceptStrType](
	mg AggMemoryManager, typ types.Type,
	emptyNull bool) aggFuncResult[T] {
	r := aggFuncResult[T]{}
	r.init(mg, typ, emptyNull)
	return r
}

func (r *aggFuncResult[T]) grows(more int) error {
	oldLen, newLen, err := r.extend(more)
	if err != nil {
		return err
	}
	r.values = vector.MustFixedCol[T](r.res)
	// reset the new row.
	var v T
	for i, j := oldLen, newLen; i < j; i++ {
		r.values[i] = v
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

func (r *aggFuncResult[T]) unmarshal(data []byte) error {
	if err := r.unmarshal0(data); err != nil {
		return err
	}
	r.values = vector.MustFixedCol[T](r.res)
	return nil
}

func (r *aggFuncResult[T]) eq(other aggFuncResult[T]) bool {
	if !r.basicResult.eq0(other.basicResult) {
		return false
	}
	vs1 := vector.MustFixedCol[T](r.res)
	vs2 := vector.MustFixedCol[T](other.res)
	if len(vs1) != len(vs2) {
		return false
	}
	bs1 := vector.MustFixedCol[bool](r.ess)
	for i, j := 0, len(vs1); i < j; i++ {
		if bs1[i] {
			continue
		}
		if vs1[i] != vs2[i] {
			return false
		}
	}
	return true
}

func initBytesAggFuncResult(
	mg AggMemoryManager, typ types.Type,
	emptyNull bool) aggFuncBytesResult {
	r := aggFuncBytesResult{}
	r.init(mg, typ, emptyNull)
	return r
}

func (r *aggFuncBytesResult) grows(more int) error {
	oldLen, newLen, err := r.extend(more)
	if err != nil {
		return err
	}

	var v = []byte("")
	for i, j := oldLen, newLen; i < j; i++ {
		// this will never cause error.
		_ = vector.SetBytesAt(r.res, i, v, r.mp)
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

func (r *aggFuncBytesResult) unmarshal(data []byte) error {
	return r.unmarshal0(data)
}

func (r *aggFuncBytesResult) eq(other aggFuncBytesResult) bool {
	if !r.basicResult.eq0(other.basicResult) {
		return false
	}
	vs1 := vector.MustBytesCol(r.res)
	vs2 := vector.MustBytesCol(other.res)
	if len(vs1) != len(vs2) {
		return false
	}
	bs1 := vector.MustFixedCol[bool](r.ess)
	for i, j := 0, len(vs1); i < j; i++ {
		if bs1[i] {
			continue
		}
		if !bytes.Equal(vs1[i], vs2[i]) {
			return false
		}
	}
	return true
}
