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
	r.res = vector.NewVec(typ)
	r.ess = vector.NewVec(types.T_bool.ToType())
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

	r.empty = vector.MustFixedColWithTypeCheck[bool](r.ess)
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
		r.res.Free(r.mp)
		r.res = nil
	}
	if r.ess != nil {
		r.ess.Free(r.mp)
		r.ess = nil
	}
}

func (r *basicResult) eq0(other basicResult) bool {
	if !r.typ.Eq(other.typ) {
		return false
	}
	bs1 := vector.MustFixedColWithTypeCheck[bool](r.ess)
	bs2 := vector.MustFixedColWithTypeCheck[bool](other.ess)
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
	r.res = vector.NewVec(r.typ)
	r.ess = vector.NewVec(types.T_bool.ToType())

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
	r.empty = vector.MustFixedColWithTypeCheck[bool](r.ess)
	return nil
}

type aggFuncResult[T types.FixedSizeTExceptStrType] struct {
	basicResult
	requireInit    bool
	requiredResult T

	values []T // for quick get/set
}

type aggFuncBytesResult struct {
	basicResult
	requireInit    bool
	requiredResult []byte
}

func initFixedAggFuncResult[T types.FixedSizeTExceptStrType](
	mg AggMemoryManager, typ types.Type,
	emptyNull bool) aggFuncResult[T] {
	r := aggFuncResult[T]{}
	r.init(mg, typ, emptyNull)
	return r
}

// initFixedAggFuncResult2 is used to initialize the result with a fixed value.
// fixed value is the required first value of the result.
// e.g. max(int) = math.MinInt64, min(int) = math.MaxInt64.
func initFixedAggFuncResult2[T types.FixedSizeTExceptStrType](
	mg AggMemoryManager, typ types.Type, emptyNull bool, value T) aggFuncResult[T] {
	r := initFixedAggFuncResult[T](mg, typ, emptyNull)
	r.requireInit = true
	r.requiredResult = value
	return r
}

func (r *aggFuncResult[T]) grows(more int) error {
	oldLen, newLen, err := r.extend(more)
	if err != nil {
		return err
	}
	r.values = vector.MustFixedColWithTypeCheck[T](r.res)
	// reset the new row.
	var v T
	if r.requireInit {
		v = r.requiredResult
	}
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

func (r *aggFuncResult[T]) marshal() ([]byte, error) {
	d, err := r.basicResult.marshal()
	if err != nil {
		return nil, err
	}
	rLen := int64(len(d))
	bs := types.EncodeInt64(&rLen)
	bs = append(bs, d...)
	bs = append(bs, types.EncodeBool(&r.requireInit)...)
	if r.requireInit {
		bs = append(bs, types.EncodeFixed[T](r.requiredResult)...)
	}
	return bs, nil
}

func (r *aggFuncResult[T]) unmarshal(data []byte) error {
	l := types.DecodeInt64(data[:8])
	d1, d2 := data[8:8+l], data[8+l:]
	if err := r.unmarshal0(d1); err != nil {
		return err
	}
	r.values = vector.MustFixedColWithTypeCheck[T](r.res)
	r.requireInit = types.DecodeBool(d2[:1])
	if r.requireInit {
		r.requiredResult = types.DecodeFixed[T](d2[1:])
	}
	return nil
}

func (r *aggFuncResult[T]) eq(other aggFuncResult[T]) bool {
	if !r.basicResult.eq0(other.basicResult) {
		return false
	}
	vs1 := vector.MustFixedColWithTypeCheck[T](r.res)
	vs2 := vector.MustFixedColWithTypeCheck[T](other.res)
	if len(vs1) != len(vs2) {
		return false
	}
	bs1 := vector.MustFixedColWithTypeCheck[bool](r.ess)
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

func initBytesAggFuncResult2(
	mg AggMemoryManager, typ types.Type, emptyNull bool, value []byte) aggFuncBytesResult {
	r := initBytesAggFuncResult(mg, typ, emptyNull)
	r.requireInit = true
	r.requiredResult = value
	return r
}

func (r *aggFuncBytesResult) grows(more int) error {
	oldLen, newLen, err := r.extend(more)
	if err != nil {
		return err
	}

	if r.requireInit {
		v := r.requiredResult
		for i, j := oldLen, newLen; i < j; i++ {
			if err = vector.SetBytesAt(r.res, i, v, r.mp); err != nil {
				return err
			}
		}

	} else {
		v := []byte("")
		for i, j := oldLen, newLen; i < j; i++ {
			// this will never cause error.
			_ = vector.SetBytesAt(r.res, i, v, r.mp)
		}
	}
	return nil
}

func (r *aggFuncBytesResult) aggGet() []byte {
	// never return the source pointer directly.
	//
	// if not, append action outside like `r = append(r, "more")` will cause memory contamination to other row.
	newr := r.res.GetBytesAt(r.groupToSet)
	newr = newr[:len(newr):len(newr)]
	return newr
}

func (r *aggFuncBytesResult) aggSet(v []byte) error {
	return vector.SetBytesAt(r.res, r.groupToSet, v, r.mp)
}

func (r *aggFuncBytesResult) marshal() ([]byte, error) {
	d, err := r.basicResult.marshal()
	if err != nil {
		return nil, err
	}
	rLen := int64(len(d))
	bs := types.EncodeInt64(&rLen)
	bs = append(bs, d...)
	bs = append(bs, types.EncodeBool(&r.requireInit)...)
	if r.requireInit {
		bs = append(bs, r.requiredResult...)
	}
	return bs, nil
}

func (r *aggFuncBytesResult) unmarshal(data []byte) error {
	l := types.DecodeInt64(data[:8])
	d1, d2 := data[8:8+l], data[8+l:]
	if err := r.unmarshal0(d1); err != nil {
		return err
	}
	r.requireInit = types.DecodeBool(d2[:1])
	if r.requireInit {
		r.requiredResult = append(r.requiredResult, d2[1:]...)
	}
	return nil
}

func (r *aggFuncBytesResult) eq(other aggFuncBytesResult) bool {
	if !r.basicResult.eq0(other.basicResult) {
		return false
	}
	vs1, area1 := vector.MustVarlenaRawData(r.res)
	vs2, area2 := vector.MustVarlenaRawData(other.res)
	if len(vs1) != len(vs2) {
		return false
	}
	bs1 := vector.MustFixedColWithTypeCheck[bool](r.ess)
	for i, j := 0, len(vs1); i < j; i++ {
		if bs1[i] {
			continue
		}
		if !bytes.Equal(vs1[i].GetByteSlice(area1), vs2[i].GetByteSlice(area2)) {
			return false
		}
	}
	return true
}
