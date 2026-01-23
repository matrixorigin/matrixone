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
	"fmt"
	io "io"
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

// group_concat is a special string aggregation function.
type groupConcatExec struct {
	multiAggInfo
	ret aggResultWithBytesType

	separator []byte
}

func (exec *groupConcatExec) GetOptResult() SplitResult {
	return &exec.ret.optSplitResult
}

func (exec *groupConcatExec) marshal() ([]byte, error) {
	d := exec.multiAggInfo.getEncoded()
	r, em, dist, err := exec.ret.marshalToBytes()
	if err != nil {
		return nil, err
	}
	encoded := EncodedAgg{
		Info:    d,
		Result:  r,
		Empties: em,
		// Oh, this is so f**ked.
		Groups: [][]byte{exec.separator},
	}

	if dist != nil {
		encoded.Groups = append(encoded.Groups, dist...)
	}
	return encoded.Marshal()
}

func (exec *groupConcatExec) unmarshal(_ *mpool.MPool, result, empties, groups [][]byte) error {
	if err := exec.SetExtraInformation(groups[0], 0); err != nil {
		return err
	}
	return exec.ret.unmarshalFromBytes(result, empties, groups[1:])
}

func (exec *groupConcatExec) SaveIntermediateResult(cnt int64, flags [][]uint8, buf *bytes.Buffer) error {
	err := marshalRetAndGroupsToBuffer[dummyBinaryMarshaler](
		cnt, flags, buf,
		&exec.ret.optSplitResult, nil, [][]byte{exec.separator})
	if err != nil {
		return err
	}

	if err = types.WriteSizeBytes(exec.separator, buf); err != nil {
		return err
	}
	return nil
}

func (exec *groupConcatExec) SaveIntermediateResultOfChunk(chunk int, buf *bytes.Buffer) error {
	err := marshalChunkToBuffer[dummyBinaryMarshaler](chunk, buf,
		&exec.ret.optSplitResult, nil, [][]byte{exec.separator})
	if err != nil {
		return err
	}

	if err = types.WriteSizeBytes(exec.separator, buf); err != nil {
		return err
	}
	return nil
}

func (exec *groupConcatExec) UnmarshalFromReader(reader io.Reader, mp *mpool.MPool) error {
	_, sep, err := unmarshalFromReader[dummyBinaryUnmarshaler](reader, &exec.ret.optSplitResult)
	if err != nil {
		return err
	}
	//
	// exec.ret.setupT()
	exec.separator = sep[0]
	return nil
}

func GroupConcatReturnType(args []types.Type) types.Type {
	for _, p := range args {
		if p.Oid == types.T_binary || p.Oid == types.T_varbinary || p.Oid == types.T_blob {
			return types.T_blob.ToType()
		}
	}
	return types.T_text.ToType()
}

func newGroupConcatExec(mg *mpool.MPool, info multiAggInfo, separator string) AggFuncExec {
	exec := &groupConcatExec{
		multiAggInfo: info,
		ret:          initAggResultWithBytesTypeResult(mg, info.retType, info.emptyNull, "", info.distinct),
		separator:    []byte(separator),
	}
	return exec
}

func isValidGroupConcatUnit(value []byte) error {
	if len(value) > math.MaxUint16 {
		return moerr.NewInternalErrorNoCtx("group_concat: the length of the value is too long")
	}
	return nil
}

func (exec *groupConcatExec) GroupGrow(more int) error {
	return exec.ret.grows(more)
}

func (exec *groupConcatExec) PreAllocateGroups(more int) error {
	return exec.ret.preExtend(more)
}

func (exec *groupConcatExec) Fill(groupIndex int, row int, vectors []*vector.Vector) error {
	// if any value was null, there is no need to Fill.
	u64Row := uint64(row)
	for _, v := range vectors {
		if v.IsNull(u64Row) {
			return nil
		}
	}

	x, y := exec.ret.updateNextAccessIdx(groupIndex)
	exec.ret.setGroupNotEmpty(x, y)

	if need, err := exec.ret.distinctFill(x, y, vectors, row); err != nil || !need {
		return err
	}

	r := exec.ret.get()
	if len(r) > 0 {
		r = append(r, exec.separator...)
	}

	var err error
	for i, v := range vectors {
		if r, err = oidToConcatFunc[exec.multiAggInfo.argTypes[i].Oid](v, row, r); err != nil {
			return err
		}
	}
	return exec.ret.set(r)
}

func (exec *groupConcatExec) BulkFill(groupIndex int, vectors []*vector.Vector) error {
	for row, end := 0, vectors[0].Length(); row < end; row++ {
		if err := exec.Fill(groupIndex, row, vectors); err != nil {
			return err
		}
	}
	return nil
}

func (exec *groupConcatExec) BatchFill(offset int, groups []uint64, vectors []*vector.Vector) error {
	for i, j, idx := offset, offset+len(groups), 0; i < j; i++ {
		if groups[idx] != GroupNotMatched {
			if err := exec.Fill(int(groups[idx]-1), i, vectors); err != nil {
				return err
			}
		}
		idx++
	}
	return nil
}

func (exec *groupConcatExec) SetExtraInformation(partialResult any, _ int) error {
	// todo: too bad here.
	exec.separator = partialResult.([]byte)
	return nil
}

func (exec *groupConcatExec) merge(other *groupConcatExec, idx1, idx2 int) error {
	x1, y1 := exec.ret.updateNextAccessIdx(idx1)
	x2, y2 := other.ret.updateNextAccessIdx(idx2)

	if err := exec.ret.distinctMerge(x1, &other.ret.optSplitResult, x2); err != nil {
		return err
	}
	empty1, empty2 := exec.ret.isGroupEmpty(x1, y1), other.ret.isGroupEmpty(x2, y2)

	if empty2 {
		return nil
	}
	exec.ret.MergeAnotherEmpty(x1, y1, empty2)
	v2 := other.ret.get()
	if empty1 {
		return exec.ret.set(v2)
	}
	v1 := exec.ret.get()
	v1 = append(v1, exec.separator...)
	v1 = append(v1, v2...)
	return exec.ret.set(v1)
}

func (exec *groupConcatExec) Merge(next AggFuncExec, groupIdx1, groupIdx2 int) error {
	return exec.merge(next.(*groupConcatExec), groupIdx1, groupIdx2)
}

func (exec *groupConcatExec) BatchMerge(next AggFuncExec, offset int, groups []uint64) error {
	other := next.(*groupConcatExec)
	for i := range groups {
		if groups[i] == GroupNotMatched {
			continue
		}
		if err := exec.merge(other, int(groups[i])-1, i+offset); err != nil {
			return err
		}
	}
	return nil
}

func (exec *groupConcatExec) Flush() ([]*vector.Vector, error) {
	return exec.ret.flushAll(), nil
}

func (exec *groupConcatExec) Free() {
	exec.ret.free()
}

func (exec *groupConcatExec) Size() int64 {
	return exec.ret.Size() + int64(cap(exec.separator))
}

var GroupConcatUnsupportedTypes = []types.T{
	types.T_tuple,
}

func IsGroupConcatSupported(t types.Type) bool {
	for _, unsupported := range GroupConcatUnsupportedTypes {
		if t.Oid == unsupported {
			return false
		}
	}
	return true
}

var oidToConcatFunc = map[types.T]func(*vector.Vector, int, []byte) ([]byte, error){
	types.T_bit:           concatFixedTypeChecked[uint64],
	types.T_bool:          concatFixedTypeChecked[bool],
	types.T_int8:          concatFixedTypeChecked[int8],
	types.T_int16:         concatFixedTypeChecked[int16],
	types.T_int32:         concatFixedTypeChecked[int32],
	types.T_int64:         concatFixedTypeChecked[int64],
	types.T_uint8:         concatFixedTypeChecked[uint8],
	types.T_uint16:        concatFixedTypeChecked[uint16],
	types.T_uint32:        concatFixedTypeChecked[uint32],
	types.T_uint64:        concatFixedTypeChecked[uint64],
	types.T_float32:       concatFixedTypeChecked[float32],
	types.T_float64:       concatFixedTypeChecked[float64],
	types.T_decimal64:     concatDecimal64,
	types.T_decimal128:    concatDecimal128,
	types.T_date:          concatTime[types.Date],
	types.T_datetime:      concatTime[types.Datetime],
	types.T_timestamp:     concatTime[types.Timestamp],
	types.T_time:          concatTime[types.Time],
	types.T_varchar:       concatVar,
	types.T_char:          concatVar,
	types.T_blob:          concatVar,
	types.T_text:          concatVar,
	types.T_datalink:      concatVar,
	types.T_varbinary:     concatVar,
	types.T_binary:        concatVar,
	types.T_json:          concatJson,
	types.T_enum:          concatVar,
	types.T_interval:      concatFixedTypeChecked[types.IntervalType],
	types.T_TS:            concatFixedTypeChecked[types.TS],
	types.T_Rowid:         concatFixedTypeChecked[types.Rowid],
	types.T_Blockid:       concatFixedTypeChecked[types.Blockid],
	types.T_array_float32: concatVar,
	types.T_array_float64: concatVar,
}

func concatFixedTypeChecked[T types.FixedSizeTExceptStrType](v *vector.Vector, row int, src []byte) ([]byte, error) {
	val := vector.GetFixedAtNoTypeCheck[T](v, row)
	return fmt.Appendf(src, "%v", val), nil
}

func concatVar(v *vector.Vector, row int, src []byte) ([]byte, error) {
	val := v.GetBytesAt(row)

	if err := isValidGroupConcatUnit(val); err != nil {
		return nil, err
	}
	return append(src, val...), nil
}

func concatDecimal64(v *vector.Vector, row int, src []byte) ([]byte, error) {
	value := vector.GetFixedAtNoTypeCheck[types.Decimal64](v, row)
	return fmt.Appendf(src, "%v", value.Format(v.GetType().Scale)), nil
}

func concatDecimal128(v *vector.Vector, row int, src []byte) ([]byte, error) {
	value := vector.GetFixedAtNoTypeCheck[types.Decimal128](v, row)
	return fmt.Appendf(src, "%v", value.Format(v.GetType().Scale)), nil
}

func concatTime[T fmt.Stringer](v *vector.Vector, row int, src []byte) ([]byte, error) {
	value := vector.GetFixedAtNoTypeCheck[T](v, row)
	return fmt.Appendf(src, "%v", value.String()), nil
}

func concatJson(v *vector.Vector, row int, src []byte) ([]byte, error) {
	value := v.GetBytesAt(row)
	if err := isValidGroupConcatUnit(value); err != nil {
		return nil, err
	}
	// Decode the bytejson binary format and convert to JSON string
	bj := types.DecodeJson(value)
	jsonStr := bj.String()
	return append(src, jsonStr...), nil
}
