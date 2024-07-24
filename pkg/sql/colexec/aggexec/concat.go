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
	"fmt"
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

const (
	groupConcatMaxLen = 1024
)

// group_concat is a special string aggregation function.
type groupConcatExec struct {
	multiAggInfo
	ret aggFuncBytesResult
	distinctHash

	separator []byte
}

func (exec *groupConcatExec) marshal() ([]byte, error) {
	d := exec.multiAggInfo.getEncoded()
	r, err := exec.ret.marshal()
	if err != nil {
		return nil, err
	}
	encoded := &EncodedAgg{
		Info:   d,
		Result: r,
		Groups: [][]byte{exec.separator},
	}
	return encoded.Marshal()
}

func (exec *groupConcatExec) unmarshal(mp *mpool.MPool, result []byte, groups [][]byte) error {
	if err := exec.SetExtraInformation(groups[0], 0); err != nil {
		return err
	}
	return exec.ret.unmarshal(result)
}

func GroupConcatReturnType(args []types.Type) types.Type {
	for _, p := range args {
		if p.Oid == types.T_binary || p.Oid == types.T_varbinary || p.Oid == types.T_blob {
			return types.T_blob.ToType()
		}
	}
	return types.T_text.ToType()
}

func newGroupConcatExec(mg AggMemoryManager, info multiAggInfo, separator string) AggFuncExec {
	exec := &groupConcatExec{
		multiAggInfo: info,
		ret:          initBytesAggFuncResult(mg, info.retType, info.emptyNull),
		separator:    []byte(separator),
	}
	if info.distinct {
		exec.distinctHash = newDistinctHash(mg.Mp(), false)
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
	if exec.IsDistinct() {
		if err := exec.distinctHash.grows(more); err != nil {
			return err
		}
	}
	return exec.ret.grows(more)
}

func (exec *groupConcatExec) PreAllocateGroups(more int) error {
	return exec.ret.preAllocate(more)
}

func (exec *groupConcatExec) Fill(groupIndex int, row int, vectors []*vector.Vector) error {
	// if any value was null, there is no need to Fill.
	u64Row := uint64(row)
	for _, v := range vectors {
		if v.IsNull(u64Row) {
			return nil
		}
	}

	if exec.IsDistinct() {
		if need, err := exec.distinctHash.fill(groupIndex, vectors, row); err != nil || !need {
			return err
		}
	}

	exec.ret.groupToSet = groupIndex
	exec.ret.setGroupNotEmpty(groupIndex)
	r := exec.ret.aggGet()
	if len(r) > groupConcatMaxLen {
		return nil
	}
	if len(r) > 0 {
		r = append(r, exec.separator...)
	}

	var err error
	for i, v := range vectors {
		if r, err = oidToConcatFunc[exec.multiAggInfo.argTypes[i].Oid](v, row, r); err != nil {
			return err
		}
	}
	if err = exec.ret.aggSet(r); err != nil {
		return err
	}
	return nil
}

func (exec *groupConcatExec) BulkFill(groupIndex int, vectors []*vector.Vector) error {
	exec.ret.groupToSet = groupIndex
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

func (exec *groupConcatExec) SetExtraInformation(partialResult any, groupIndex int) error {
	// todo: too bad here.
	exec.separator = partialResult.([]byte)
	return nil
}

func (exec *groupConcatExec) merge(other *groupConcatExec, idx1, idx2 int) error {
	exec.ret.groupToSet = idx1
	other.ret.groupToSet = idx2
	if err := exec.distinctHash.merge(&other.distinctHash); err != nil {
		return err
	}

	v1 := exec.ret.aggGet()
	v2 := other.ret.aggGet()
	if len(v2) == 0 || len(v1) > groupConcatMaxLen {
		return nil
	}
	if len(v1) > 0 && len(v2) > 0 {
		v1 = append(v1, exec.separator...)
		v1 = append(v1, v2...)
		return exec.ret.aggSet(v1)
	}
	if len(v1) == 0 {
		return exec.ret.aggSet(v2)
	}
	return exec.ret.aggSet(v1)
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

func (exec *groupConcatExec) Flush() (*vector.Vector, error) {
	return exec.ret.flush(), nil
}

func (exec *groupConcatExec) Free() {
	exec.distinctHash.free()
	exec.ret.free()
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
	types.T_bit:           concatFixed[uint64],
	types.T_bool:          concatFixed[bool],
	types.T_int8:          concatFixed[int8],
	types.T_int16:         concatFixed[int16],
	types.T_int32:         concatFixed[int32],
	types.T_int64:         concatFixed[int64],
	types.T_uint8:         concatFixed[uint8],
	types.T_uint16:        concatFixed[uint16],
	types.T_uint32:        concatFixed[uint32],
	types.T_uint64:        concatFixed[uint64],
	types.T_float32:       concatFixed[float32],
	types.T_float64:       concatFixed[float64],
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
	types.T_json:          concatVar,
	types.T_enum:          concatVar,
	types.T_interval:      concatFixed[types.IntervalType],
	types.T_TS:            concatFixed[types.TS],
	types.T_Rowid:         concatFixed[types.Rowid],
	types.T_Blockid:       concatFixed[types.Blockid],
	types.T_array_float32: concatVar,
	types.T_array_float64: concatVar,
}

func concatFixed[T types.FixedSizeTExceptStrType](v *vector.Vector, row int, src []byte) ([]byte, error) {
	value := vector.GetFixedAt[T](v, row)
	return fmt.Appendf(src, "%v", value), nil
}

func concatVar(v *vector.Vector, row int, src []byte) ([]byte, error) {
	value := v.GetBytesAt(row)

	if err := isValidGroupConcatUnit(value); err != nil {
		return nil, err
	}
	return append(src, value...), nil
}

func concatDecimal64(v *vector.Vector, row int, src []byte) ([]byte, error) {
	value := vector.GetFixedAt[types.Decimal64](v, row)
	return fmt.Appendf(src, "%v", value.Format(v.GetType().Scale)), nil
}

func concatDecimal128(v *vector.Vector, row int, src []byte) ([]byte, error) {
	value := vector.GetFixedAt[types.Decimal128](v, row)
	return fmt.Appendf(src, "%v", value.Format(v.GetType().Scale)), nil
}

func concatTime[T fmt.Stringer](v *vector.Vector, row int, src []byte) ([]byte, error) {
	value := vector.GetFixedAt[T](v, row)
	return fmt.Appendf(src, "%v", value.String()), nil
}
