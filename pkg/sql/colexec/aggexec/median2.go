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
	"slices"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

var MedianSupportedType = []types.T{
	types.T_bit, types.T_int8, types.T_int16, types.T_int32, types.T_int64,
	types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
	types.T_float32, types.T_float64, types.T_decimal64, types.T_decimal128,
}

func MedianReturnType(args []types.Type) types.Type {
	if args[0].IsDecimal() {
		return types.New(types.T_decimal128, 38, args[0].Scale+1)
	}
	return types.T_float64.ToType()
}

type medianExec struct {
	aggExec
}

func newMedianExec(mp *mpool.MPool, aggID int64, isDistinct bool, param types.Type) (AggFuncExec, error) {
	switch param.Oid {
	case types.T_bit, types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_float32, types.T_float64, types.T_decimal64, types.T_decimal128:
	default:
		return nil, moerr.NewInternalErrorNoCtx("unsupported type for median()")
	}

	var exec medianExec
	exec.mp = mp
	exec.aggInfo = aggInfo{
		aggId:      aggID,
		isDistinct: isDistinct,
		argTypes:   []types.Type{param},
		retType:    MedianReturnType([]types.Type{param}),
		emptyNull:  true,
		saveArg:    true,
	}
	return &exec, nil
}

func (exec *medianExec) Fill(groupIndex int, row int, vectors []*vector.Vector) error {
	return exec.BatchFill(row, []uint64{uint64(groupIndex + 1)}, vectors)
}

func (exec *medianExec) BulkFill(groupIndex int, vectors []*vector.Vector) error {
	return exec.BatchFill(0, slices.Repeat([]uint64{uint64(groupIndex + 1)}, vectors[0].Length()), vectors)
}

func (exec *medianExec) BatchFill(offset int, groups []uint64, vectors []*vector.Vector) error {
	return exec.batchFillArgs(offset, groups, vectors, exec.IsDistinct())
}

func (exec *medianExec) Merge(next AggFuncExec, groupIdx1, groupIdx2 int) error {
	return exec.BatchMerge(next, groupIdx2, []uint64{uint64(groupIdx1 + 1)})
}

func (exec *medianExec) BatchMerge(next AggFuncExec, offset int, groups []uint64) error {
	other := next.(*medianExec)
	return exec.batchMergeArgs(&other.aggExec, offset, groups, exec.IsDistinct())
}

func (exec *medianExec) SetExtraInformation(partialResult any, groupIndex int) error {
	return nil
}

func (exec *medianExec) Flush() (_ []*vector.Vector, retErr error) {
	vecs := make([]*vector.Vector, len(exec.state))
	defer func() {
		if retErr != nil {
			for _, v := range vecs {
				if v != nil {
					v.Free(exec.mp)
				}
			}
		}
	}()
	for i, st := range exec.state {
		vecs[i] = vector.NewOffHeapVecWithType(exec.retType)
		if err := vecs[i].PreExtend(int(st.length), exec.mp); err != nil {
			return nil, err
		}
		for j := 0; j < int(st.length); j++ {
			if st.argCnt[j] == 0 {
				vector.AppendNull(vecs[i], exec.mp)
				continue
			}
			switch exec.argTypes[0].Oid {
			case types.T_bit, types.T_uint64:
				v, err := medianNumericFromState[uint64](st, uint16(j), &exec.aggInfo)
				if err != nil {
					return nil, err
				}
				vector.AppendFixed(vecs[i], v, false, exec.mp)
			case types.T_int8:
				v, err := medianNumericFromState[int8](st, uint16(j), &exec.aggInfo)
				if err != nil {
					return nil, err
				}
				vector.AppendFixed(vecs[i], v, false, exec.mp)
			case types.T_int16:
				v, err := medianNumericFromState[int16](st, uint16(j), &exec.aggInfo)
				if err != nil {
					return nil, err
				}
				vector.AppendFixed(vecs[i], v, false, exec.mp)
			case types.T_int32:
				v, err := medianNumericFromState[int32](st, uint16(j), &exec.aggInfo)
				if err != nil {
					return nil, err
				}
				vector.AppendFixed(vecs[i], v, false, exec.mp)
			case types.T_int64:
				v, err := medianNumericFromState[int64](st, uint16(j), &exec.aggInfo)
				if err != nil {
					return nil, err
				}
				vector.AppendFixed(vecs[i], v, false, exec.mp)
			case types.T_uint8:
				v, err := medianNumericFromState[uint8](st, uint16(j), &exec.aggInfo)
				if err != nil {
					return nil, err
				}
				vector.AppendFixed(vecs[i], v, false, exec.mp)
			case types.T_uint16:
				v, err := medianNumericFromState[uint16](st, uint16(j), &exec.aggInfo)
				if err != nil {
					return nil, err
				}
				vector.AppendFixed(vecs[i], v, false, exec.mp)
			case types.T_uint32:
				v, err := medianNumericFromState[uint32](st, uint16(j), &exec.aggInfo)
				if err != nil {
					return nil, err
				}
				vector.AppendFixed(vecs[i], v, false, exec.mp)
			case types.T_float32:
				v, err := medianNumericFromState[float32](st, uint16(j), &exec.aggInfo)
				if err != nil {
					return nil, err
				}
				vector.AppendFixed(vecs[i], v, false, exec.mp)
			case types.T_float64:
				v, err := medianNumericFromState[float64](st, uint16(j), &exec.aggInfo)
				if err != nil {
					return nil, err
				}
				vector.AppendFixed(vecs[i], v, false, exec.mp)
			case types.T_decimal64:
				v, err := medianDecimal64FromState(st, uint16(j), &exec.aggInfo)
				if err != nil {
					return nil, err
				}
				vector.AppendFixed(vecs[i], v, false, exec.mp)
			case types.T_decimal128:
				v, err := medianDecimal128FromState(st, uint16(j), &exec.aggInfo)
				if err != nil {
					return nil, err
				}
				vector.AppendFixed(vecs[i], v, false, exec.mp)
			default:
				return nil, moerr.NewInternalErrorNoCtx("unsupported type for median()")
			}
		}
	}
	return vecs, nil
}

func (exec *medianExec) Size() int64 {
	var size int64
	for _, st := range exec.state {
		size += int64(len(st.argbuf))
		size += int64(cap(st.argCnt)) * 4
	}
	return size
}

func (exec *medianExec) Free() {
	exec.aggExec.Free()
}

func medianNumericFromState[T numeric](st aggState, idx uint16, info *aggInfo) (float64, error) {
	vals := make([]T, 0, st.argCnt[idx])
	if err := st.iter(idx, func(k []byte) error {
		vals = append(vals, *util.UnsafeFromBytes[T](aggPayloadFromKey(info, k)))
		return nil
	}); err != nil {
		return 0, err
	}
	lessFnFactory := func(nums []T) func(a, b int) bool {
		return func(i, j int) bool {
			return nums[i] < nums[j]
		}
	}
	rows := len(vals)
	if rows&1 == 1 {
		return float64(quickSelect(vals, lessFnFactory, rows>>1)), nil
	}
	v1 := quickSelect(vals, lessFnFactory, rows>>1-1)
	v2 := quickSelect(vals, lessFnFactory, rows>>1)
	return float64(v1+v2) / 2, nil
}

func medianDecimal64FromState(st aggState, idx uint16, info *aggInfo) (types.Decimal128, error) {
	vals := make([]types.Decimal64, 0, st.argCnt[idx])
	if err := st.iter(idx, func(k []byte) error {
		vals = append(vals, types.DecodeDecimal64(aggPayloadFromKey(info, k)))
		return nil
	}); err != nil {
		return types.Decimal128{}, err
	}
	lessFnFactory := func(nums []types.Decimal64) func(a, b int) bool {
		return func(i, j int) bool {
			return nums[i].Compare(nums[j]) < 0
		}
	}
	rows := len(vals)
	if rows&1 == 1 {
		return FromD64ToD128(quickSelect(vals, lessFnFactory, rows>>1)).Scale(1)
	}

	v1 := FromD64ToD128(quickSelect(vals, lessFnFactory, rows>>1-1))
	v2 := FromD64ToD128(quickSelect(vals, lessFnFactory, rows>>1))
	ret, err := v1.Add128(v2)
	if err != nil {
		return types.Decimal128{}, err
	}
	if ret.Sign() {
		if ret, err = ret.Minus().Scale(1); err != nil {
			return types.Decimal128{}, err
		}
		return ret.Right(1).Minus(), nil
	}
	if ret, err = ret.Scale(1); err != nil {
		return types.Decimal128{}, err
	}
	return ret.Right(1), nil
}

func medianDecimal128FromState(st aggState, idx uint16, info *aggInfo) (types.Decimal128, error) {
	vals := make([]types.Decimal128, 0, st.argCnt[idx])
	if err := st.iter(idx, func(k []byte) error {
		vals = append(vals, types.DecodeDecimal128(aggPayloadFromKey(info, k)))
		return nil
	}); err != nil {
		return types.Decimal128{}, err
	}
	lessFnFactory := func(nums []types.Decimal128) func(a, b int) bool {
		return func(i, j int) bool {
			return nums[i].Compare(nums[j]) < 0
		}
	}
	rows := len(vals)
	if rows&1 == 1 {
		ret := quickSelect(vals, lessFnFactory, rows>>1)
		return ret.Scale(1)
	}
	v1 := quickSelect(vals, lessFnFactory, rows>>1-1)
	v2 := quickSelect(vals, lessFnFactory, rows>>1)
	ret, err := v1.Add128(v2)
	if err != nil {
		return types.Decimal128{}, err
	}
	if ret.Sign() {
		if ret, err = ret.Minus().Scale(1); err != nil {
			return types.Decimal128{}, err
		}
		return ret.Right(1).Minus(), nil
	}
	if ret, err = ret.Scale(1); err != nil {
		return types.Decimal128{}, err
	}
	return ret.Right(1), nil
}
