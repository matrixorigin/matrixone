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
	hll "github.com/axiomhq/hyperloglog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

// approx_count() returns the approximate number of count(distinct) values in a group.
type approxCountFixedExec[T types.FixedSizeTExceptStrType] struct {
	singleAggInfo
	singleAggExecExtraInformation
	arg sFixedArg[T]
	ret aggResultWithFixedType[uint64]

	groups []*hll.Sketch
}

func (exec *approxCountFixedExec[T]) GetOptResult() SplitResult {
	return &exec.ret.optSplitResult
}

func (exec *approxCountFixedExec[T]) marshal() ([]byte, error) {
	d := exec.singleAggInfo.getEncoded()
	r, em, err := exec.ret.marshalToBytes()
	if err != nil {
		return nil, err
	}

	encoded := EncodedAgg{
		Info:    d,
		Result:  r,
		Empties: em,
		Groups:  nil,
	}
	if len(exec.groups) > 0 {
		encoded.Groups = make([][]byte, len(exec.groups))
		for i := range encoded.Groups {
			encoded.Groups[i], err = exec.groups[i].MarshalBinary()
			if err != nil {
				return nil, err
			}
		}
	}
	return encoded.Marshal()
}

func (exec *approxCountFixedExec[T]) unmarshal(_ *mpool.MPool, result, empties, groups [][]byte) error {
	err := exec.ret.unmarshalFromBytes(result, empties)
	if err != nil {
		return err
	}
	if len(groups) > 0 {
		exec.groups = make([]*hll.Sketch, len(groups))
		for i := range exec.groups {
			exec.groups[i] = hll.New()
			if err = exec.groups[i].UnmarshalBinary(groups[i]); err != nil {
				return err
			}
		}
	}
	return nil
}

type approxCountVarExec struct {
	singleAggInfo
	singleAggExecExtraInformation
	arg sBytesArg
	ret aggResultWithFixedType[uint64]

	groups []*hll.Sketch
}

func (exec *approxCountVarExec) GetOptResult() SplitResult {
	return &exec.ret.optSplitResult
}

func (exec *approxCountVarExec) marshal() ([]byte, error) {
	d := exec.singleAggInfo.getEncoded()
	r, em, err := exec.ret.marshalToBytes()
	if err != nil {
		return nil, err
	}

	encoded := EncodedAgg{
		Info:    d,
		Result:  r,
		Empties: em,
		Groups:  nil,
	}
	if len(exec.groups) > 0 {
		encoded.Groups = make([][]byte, len(exec.groups))
		for i := range encoded.Groups {
			encoded.Groups[i], err = exec.groups[i].MarshalBinary()
			if err != nil {
				return nil, err
			}
		}
	}
	return encoded.Marshal()
}

func (exec *approxCountVarExec) unmarshal(_ *mpool.MPool, result, empties, groups [][]byte) error {
	err := exec.ret.unmarshalFromBytes(result, empties)
	if err != nil {
		return err
	}
	if len(groups) > 0 {
		exec.groups = make([]*hll.Sketch, len(groups))
		for i := range exec.groups {
			exec.groups[i] = hll.New()
			if err = exec.groups[i].UnmarshalBinary(groups[i]); err != nil {
				return err
			}
		}
	}
	return nil
}

func newApproxCountFixedExec[T types.FixedSizeTExceptStrType](mg AggMemoryManager, info singleAggInfo) AggFuncExec {
	return &approxCountFixedExec[T]{
		singleAggInfo: info,
		ret:           initAggResultWithFixedTypeResult[uint64](mg, info.retType, false, 0),
	}
}

func makeApproxCount(mg AggMemoryManager, id int64, arg types.Type) AggFuncExec {
	info := singleAggInfo{
		aggID:     id,
		distinct:  false,
		argType:   arg,
		retType:   types.T_uint64.ToType(),
		emptyNull: false,
	}

	if info.argType.IsVarlen() {
		return &approxCountVarExec{
			singleAggInfo: info,
			ret:           initAggResultWithFixedTypeResult[uint64](mg, info.retType, false, 0),
		}
	}

	switch info.argType.Oid {
	case types.T_bool:
		return newApproxCountFixedExec[bool](mg, info)
	case types.T_bit, types.T_uint64:
		return newApproxCountFixedExec[uint64](mg, info)
	case types.T_int8:
		return newApproxCountFixedExec[int8](mg, info)
	case types.T_int16:
		return newApproxCountFixedExec[int16](mg, info)
	case types.T_int32:
		return newApproxCountFixedExec[int32](mg, info)
	case types.T_int64:
		return newApproxCountFixedExec[int64](mg, info)
	case types.T_uint8:
		return newApproxCountFixedExec[uint8](mg, info)
	case types.T_uint16:
		return newApproxCountFixedExec[uint16](mg, info)
	case types.T_uint32:
		return newApproxCountFixedExec[uint32](mg, info)
	case types.T_float32:
		return newApproxCountFixedExec[float32](mg, info)
	case types.T_float64:
		return newApproxCountFixedExec[float64](mg, info)
	case types.T_decimal64:
		return newApproxCountFixedExec[types.Decimal64](mg, info)
	case types.T_decimal128:
		return newApproxCountFixedExec[types.Decimal128](mg, info)
	case types.T_date:
		return newApproxCountFixedExec[types.Date](mg, info)
	case types.T_datetime:
		return newApproxCountFixedExec[types.Datetime](mg, info)
	case types.T_timestamp:
		return newApproxCountFixedExec[types.Timestamp](mg, info)
	case types.T_time:
		return newApproxCountFixedExec[types.Time](mg, info)
	case types.T_enum:
		return newApproxCountFixedExec[types.Enum](mg, info)
	case types.T_uuid:
		return newApproxCountFixedExec[types.Uuid](mg, info)
	default:
		panic("unsupported type for approx_count()")
	}
}

func (exec *approxCountFixedExec[T]) GroupGrow(more int) error {
	oldLen, newLen := len(exec.groups), len(exec.groups)+more
	exec.groups = append(exec.groups, make([]*hll.Sketch, more)...)
	for i := oldLen; i < newLen; i++ {
		exec.groups[i] = hll.New()
	}
	return exec.ret.grows(more)
}

func (exec *approxCountFixedExec[T]) PreAllocateGroups(more int) error {
	return exec.ret.preExtend(more)
}

func (exec *approxCountFixedExec[T]) Fill(groupIndex int, row int, vectors []*vector.Vector) error {
	if vectors[0].IsNull(uint64(row)) {
		return nil
	}
	if vectors[0].IsConst() {
		row = 0
	}
	v := vector.MustFixedColWithTypeCheck[T](vectors[0])[row]
	exec.groups[groupIndex].Insert(types.EncodeFixed[T](v))
	return nil
}

func (exec *approxCountFixedExec[T]) BulkFill(groupIndex int, vectors []*vector.Vector) error {
	if vectors[0].IsConstNull() {
		return nil
	}
	if vectors[0].IsConst() {
		v := vector.MustFixedColWithTypeCheck[T](vectors[0])[0]
		exec.groups[groupIndex].Insert(types.EncodeFixed[T](v))
		return nil
	}
	exec.arg.prepare(vectors[0])
	if exec.arg.w.WithAnyNullValue() {
		for i, j := uint64(0), uint64(vectors[0].Length()); i < j; i++ {
			if v, null := exec.arg.w.GetValue(i); !null {
				exec.groups[groupIndex].Insert(types.EncodeFixed[T](v))
			}
		}
		return nil
	}

	for i, j := uint64(0), uint64(vectors[0].Length()); i < j; i++ {
		v, _ := exec.arg.w.GetValue(i)
		exec.groups[groupIndex].Insert(types.EncodeFixed[T](v))
	}
	return nil
}

func (exec *approxCountFixedExec[T]) BatchFill(offset int, groups []uint64, vectors []*vector.Vector) error {
	if vectors[0].IsConstNull() {
		return nil
	}
	if vectors[0].IsConst() {
		v := vector.MustFixedColWithTypeCheck[T](vectors[0])[0]
		for _, group := range groups {
			if group != GroupNotMatched {
				exec.groups[group-1].Insert(types.EncodeFixed[T](v))
			}
		}
		return nil
	}

	exec.arg.prepare(vectors[0])
	u64Offset := uint64(offset)
	if exec.arg.w.WithAnyNullValue() {
		for i, j := uint64(0), uint64(len(groups)); i < j; i++ {
			if groups[i] != GroupNotMatched {
				v, null := exec.arg.w.GetValue(i + u64Offset)
				if !null {
					exec.groups[groups[i]-1].Insert(types.EncodeFixed[T](v))
				}
			}
		}
		return nil
	}

	for i, j := uint64(0), uint64(len(groups)); i < j; i++ {
		if groups[i] != GroupNotMatched {
			v, _ := exec.arg.w.GetValue(i + u64Offset)
			exec.groups[groups[i]-1].Insert(types.EncodeFixed[T](v))
		}
	}
	return nil
}

func (exec *approxCountFixedExec[T]) Merge(next AggFuncExec, groupIdx1, groupIdx2 int) error {
	nextExec := next.(*approxCountFixedExec[T])
	return exec.groups[groupIdx1].Merge(nextExec.groups[groupIdx2])
}

func (exec *approxCountFixedExec[T]) BatchMerge(next AggFuncExec, offset int, groups []uint64) error {
	other := next.(*approxCountFixedExec[T])

	for i := range groups {
		if groups[i] == GroupNotMatched {
			continue
		}
		g1, g2 := int(groups[i])-1, i+offset
		if err := exec.groups[g1].Merge(other.groups[g2]); err != nil {
			return err
		}
	}
	return nil
}

func (exec *approxCountFixedExec[T]) Flush() ([]*vector.Vector, error) {
	setter := exec.ret.set
	for i, group := range exec.groups {
		exec.ret.updateNextAccessIdx(i)
		setter(group.Estimate())
	}

	if exec.partialResult != nil {
		getter := exec.ret.get
		exec.ret.updateNextAccessIdx(exec.partialGroup)
		setter(getter() + exec.partialResult.(uint64))
	}
	return exec.ret.flushAll(), nil
}

func (exec *approxCountFixedExec[T]) Free() {
	exec.ret.free()
	exec.groups = nil
}

func (exec *approxCountFixedExec[T]) Size() int64 {
	var size int64
	for _, s := range exec.groups {
		if s != nil {
			if data, err := s.MarshalBinary(); err == nil {
				size += int64(len(data))
			}
		}
	}
	// 8 is the size of a pointer.
	size += int64(cap(exec.groups)) * 8
	return exec.ret.Size() + size
}

func (exec *approxCountVarExec) GroupGrow(more int) error {
	oldLen, newLen := len(exec.groups), len(exec.groups)+more
	if cap(exec.groups) >= newLen {
		exec.groups = exec.groups[:newLen]
	} else {
		exec.groups = append(exec.groups, make([]*hll.Sketch, more)...)
	}

	for i := oldLen; i < newLen; i++ {
		exec.groups[i] = hll.New()
	}
	return exec.ret.grows(more)
}

func (exec *approxCountVarExec) PreAllocateGroups(more int) error {
	if len(exec.groups) == 0 {
		exec.groups = make([]*hll.Sketch, 0, more)
	} else {
		oldLength := len(exec.groups)
		exec.groups = append(exec.groups, make([]*hll.Sketch, more)...)
		exec.groups = exec.groups[:oldLength]
	}

	return exec.ret.preExtend(more)
}

func (exec *approxCountVarExec) Fill(groupIndex int, row int, vectors []*vector.Vector) error {
	if vectors[0].IsNull(uint64(row)) {
		return nil
	}
	if vectors[0].IsConst() {
		row = 0
	}
	exec.groups[groupIndex].Insert(vectors[0].GetBytesAt(row))
	return nil
}

func (exec *approxCountVarExec) BulkFill(groupIndex int, vectors []*vector.Vector) error {
	if vectors[0].IsConstNull() {
		return nil
	}
	if vectors[0].IsConst() {
		exec.groups[groupIndex].Insert(vectors[0].GetBytesAt(0))
		return nil
	}
	exec.arg.prepare(vectors[0])
	if exec.arg.w.WithAnyNullValue() {
		for i, j := uint64(0), uint64(vectors[0].Length()); i < j; i++ {
			if v, null := exec.arg.w.GetStrValue(i); !null {
				exec.groups[groupIndex].Insert(v)
			}
		}
		return nil
	}

	for i, j := uint64(0), uint64(vectors[0].Length()); i < j; i++ {
		v, _ := exec.arg.w.GetStrValue(i)
		exec.groups[groupIndex].Insert(v)
	}
	return nil
}

func (exec *approxCountVarExec) BatchFill(offset int, groups []uint64, vectors []*vector.Vector) error {
	if vectors[0].IsConstNull() {
		return nil
	}
	if vectors[0].IsConst() {
		v := vectors[0].GetBytesAt(0)
		for _, group := range groups {
			if group != GroupNotMatched {
				exec.groups[group-1].Insert(v)
			}
		}
		return nil
	}

	exec.arg.prepare(vectors[0])
	u64Offset := uint64(offset)
	if exec.arg.w.WithAnyNullValue() {
		for i, j := uint64(0), uint64(len(groups)); i < j; i++ {
			if groups[i] != GroupNotMatched {
				v, null := exec.arg.w.GetStrValue(i + u64Offset)
				if !null {
					exec.groups[groups[i]-1].Insert(v)
				}
			}
		}
		return nil
	}

	for i, j := uint64(0), uint64(len(groups)); i < j; i++ {
		if groups[i] != GroupNotMatched {
			v, _ := exec.arg.w.GetStrValue(i + u64Offset)
			exec.groups[groups[i]-1].Insert(v)
		}
	}
	return nil
}

func (exec *approxCountVarExec) Merge(next AggFuncExec, groupIdx1, groupIdx2 int) error {
	nextExec := next.(*approxCountVarExec)
	return exec.groups[groupIdx1].Merge(nextExec.groups[groupIdx2])
}

func (exec *approxCountVarExec) BatchMerge(next AggFuncExec, offset int, groups []uint64) error {
	other := next.(*approxCountVarExec)

	for i := range groups {
		if groups[i] == GroupNotMatched {
			continue
		}
		g1, g2 := int(groups[i])-1, i+offset
		if err := exec.groups[g1].Merge(other.groups[g2]); err != nil {
			return err
		}
	}
	return nil
}

func (exec *approxCountVarExec) Flush() ([]*vector.Vector, error) {
	setter := exec.ret.set
	for i, group := range exec.groups {
		exec.ret.updateNextAccessIdx(i)
		setter(group.Estimate())
	}

	if exec.partialResult != nil {
		getter := exec.ret.get
		exec.ret.updateNextAccessIdx(exec.partialGroup)
		setter(getter() + exec.partialResult.(uint64))
	}
	return exec.ret.flushAll(), nil
}

func (exec *approxCountVarExec) Free() {
	exec.ret.free()
	exec.groups = nil
}

func (exec *approxCountVarExec) Size() int64 {
	var size int64
	for _, s := range exec.groups {
		if s != nil {
			if data, err := s.MarshalBinary(); err == nil {
				size += int64(len(data))
			}
		}
	}
	// 8 is the size of a pointer.
	size += int64(cap(exec.groups)) * 8
	return exec.ret.Size() + size
}
