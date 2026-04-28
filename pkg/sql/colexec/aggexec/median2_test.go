// Copyright 2026 Matrix Origin
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
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestMedianExecAcrossSupportedTypes(t *testing.T) {
	mp := mpool.MustNewZero()

	cases := []struct {
		name   string
		typ    types.Type
		values any
		want   any
	}{
		{name: "bit", typ: types.T_bit.ToType(), values: []uint64{1, 3, 2}, want: 2.0},
		{name: "int8", typ: types.T_int8.ToType(), values: []int8{1, 3, 2}, want: 2.0},
		{name: "int16", typ: types.T_int16.ToType(), values: []int16{1, 3, 2}, want: 2.0},
		{name: "int32", typ: types.T_int32.ToType(), values: []int32{1, 3, 2}, want: 2.0},
		{name: "int64", typ: types.T_int64.ToType(), values: []int64{1, 3, 2}, want: 2.0},
		{name: "uint8", typ: types.T_uint8.ToType(), values: []uint8{1, 3, 2}, want: 2.0},
		{name: "uint16", typ: types.T_uint16.ToType(), values: []uint16{1, 3, 2}, want: 2.0},
		{name: "uint32", typ: types.T_uint32.ToType(), values: []uint32{1, 3, 2}, want: 2.0},
		{name: "uint64", typ: types.T_uint64.ToType(), values: []uint64{1, 3, 2}, want: 2.0},
		{name: "float32", typ: types.T_float32.ToType(), values: []float32{1, 3, 2}, want: 2.0},
		{name: "float64", typ: types.T_float64.ToType(), values: []float64{1, 3, 2}, want: 2.0},
		{name: "decimal64", typ: types.New(types.T_decimal64, 10, 2), values: mustDecimal64s(t, "1.00", "2.00", "3.00"), want: "2.000"},
		{name: "decimal128", typ: types.New(types.T_decimal128, 20, 2), values: mustDecimal128s(t, "1.00", "2.00", "3.00"), want: "2.000"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			exec, err := makeMedian(mp, 1, false, tc.typ)
			require.NoError(t, err)
			require.NoError(t, exec.GroupGrow(1))

			vec := medianTestVector(t, mp, tc.typ, tc.values)
			require.NoError(t, exec.BulkFill(0, []*vector.Vector{vec}))
			require.NoError(t, exec.SetExtraInformation(nil, 0))
			require.GreaterOrEqual(t, exec.Size(), int64(0))

			ret, err := exec.Flush()
			require.NoError(t, err)
			require.Len(t, ret, 1)

			switch want := tc.want.(type) {
			case float64:
				require.Equal(t, want, vector.GetFixedAtNoTypeCheck[float64](ret[0], 0))
			case string:
				require.Equal(t, want, vector.GetFixedAtNoTypeCheck[types.Decimal128](ret[0], 0).Format(ret[0].GetType().Scale))
			}

			vec.Free(mp)
			ret[0].Free(mp)
			exec.Free()
		})
	}
}

func TestMedianDistinctAndErrorPaths(t *testing.T) {
	mp := mpool.MustNewZero()

	exec, err := makeMedian(mp, 2, true, types.T_int64.ToType())
	require.NoError(t, err)
	require.NoError(t, exec.GroupGrow(1))
	vec := buildFixedVec(t, mp, types.T_int64.ToType(), []int64{1, 1, 3})
	require.NoError(t, exec.BulkFill(0, []*vector.Vector{vec}))
	other, err := makeMedian(mp, 2, true, types.T_int64.ToType())
	require.NoError(t, err)
	require.NoError(t, other.GroupGrow(1))
	require.NoError(t, other.Fill(0, 0, []*vector.Vector{vec}))
	require.NoError(t, exec.Merge(other, 0, 0))
	require.NoError(t, exec.BatchMerge(other, 0, []uint64{1}))
	ret, err := exec.Flush()
	require.NoError(t, err)
	require.Equal(t, 2.0, vector.GetFixedAtNoTypeCheck[float64](ret[0], 0))

	_, err = makeMedian(mp, 3, false, types.T_varchar.ToType())
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported type for median")

	ret[0].Free(mp)
	vec.Free(mp)
	exec.Free()
	other.Free()

	info := aggInfo{argTypes: []types.Type{types.New(types.T_decimal64, 10, 2)}}
	state := aggState{}
	require.NoError(t, state.init(mp, 1, 1, &aggInfo{argTypes: info.argTypes, saveArg: true}, false))
	require.NoError(t, state.fillArg(mp, 0, types.EncodeDecimal64(ptr(mustDecimal64s(t, "-1.00")[0])), false))
	require.NoError(t, state.fillArg(mp, 0, types.EncodeDecimal64(ptr(mustDecimal64s(t, "-2.00")[0])), false))
	v64, err := medianDecimal64FromState(state, 0, &info)
	require.NoError(t, err)
	require.Equal(t, "-1.500", v64.Format(3))
	state.free(mp)

	info128 := aggInfo{argTypes: []types.Type{types.New(types.T_decimal128, 20, 2)}}
	state128 := aggState{}
	require.NoError(t, state128.init(mp, 1, 1, &aggInfo{argTypes: info128.argTypes, saveArg: true}, false))
	vals128 := mustDecimal128s(t, "1.00", "3.00")
	require.NoError(t, state128.fillArg(mp, 0, types.EncodeDecimal128(&vals128[0]), false))
	require.NoError(t, state128.fillArg(mp, 0, types.EncodeDecimal128(&vals128[1]), false))
	v128, err := medianDecimal128FromState(state128, 0, &info128)
	require.NoError(t, err)
	require.Equal(t, "2.000", v128.Format(3))
	state128.free(mp)
}

func TestMedianMultipleGroupsAndNullHandling(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() {
		require.Equal(t, int64(0), mp.CurrNB())
	}()

	exec, err := makeMedian(mp, AggIdOfMedian, false, types.T_int64.ToType())
	require.NoError(t, err)
	require.NoError(t, exec.GroupGrow(3))

	vec := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixedList(vec, []int64{1, 2, 10, 20, 0, 0, 100}, []bool{false, false, false, false, true, true, false}, mp))
	defer vec.Free(mp)

	require.NoError(t, exec.BatchFill(0, []uint64{1, 1, 2, 2, 2, GroupNotMatched, 3}, []*vector.Vector{vec}))

	ret, err := exec.Flush()
	require.NoError(t, err)
	require.Len(t, ret, 1)
	require.Equal(t, 1.5, vector.GetFixedAtNoTypeCheck[float64](ret[0], 0))
	require.Equal(t, 15.0, vector.GetFixedAtNoTypeCheck[float64](ret[0], 1))
	require.Equal(t, 100.0, vector.GetFixedAtNoTypeCheck[float64](ret[0], 2))
	ret[0].Free(mp)
	exec.Free()
}

func TestMedianDistinctConstAndBatchFill(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() {
		require.Equal(t, int64(0), mp.CurrNB())
	}()

	exec, err := makeMedian(mp, AggIdOfMedian, true, types.T_int64.ToType())
	require.NoError(t, err)
	require.NoError(t, exec.GroupGrow(2))

	constVec, err := vector.NewConstFixed(types.T_int64.ToType(), int64(7), 4, mp)
	require.NoError(t, err)
	defer constVec.Free(mp)
	require.NoError(t, exec.BatchFill(0, []uint64{1, 1, 1, 1}, []*vector.Vector{constVec}))
	require.NoError(t, exec.BatchFill(0, []uint64{2, 2, 2, 2}, []*vector.Vector{constVec}))

	nonConst := buildFixedVec(t, mp, types.T_int64.ToType(), []int64{7, 8, 8, 9})
	defer nonConst.Free(mp)
	require.NoError(t, exec.BatchFill(0, []uint64{1, 1, 1, 1}, []*vector.Vector{nonConst}))

	ret, err := exec.Flush()
	require.NoError(t, err)
	require.Len(t, ret, 1)
	require.Equal(t, 8.0, vector.GetFixedAtNoTypeCheck[float64](ret[0], 0))
	require.Equal(t, 7.0, vector.GetFixedAtNoTypeCheck[float64](ret[0], 1))
	ret[0].Free(mp)
	exec.Free()
}

func TestMedianBatchMergeAcrossGroups(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() {
		require.Equal(t, int64(0), mp.CurrNB())
	}()

	left, err := makeMedian(mp, AggIdOfMedian, false, types.T_int64.ToType())
	require.NoError(t, err)
	right, err := makeMedian(mp, AggIdOfMedian, false, types.T_int64.ToType())
	require.NoError(t, err)
	require.NoError(t, left.GroupGrow(2))
	require.NoError(t, right.GroupGrow(2))

	vecLeft := buildFixedVec(t, mp, types.T_int64.ToType(), []int64{1, 9, 3, 11})
	vecRight := buildFixedVec(t, mp, types.T_int64.ToType(), []int64{5, 13, 7, 15})
	defer vecLeft.Free(mp)
	defer vecRight.Free(mp)

	require.NoError(t, left.BatchFill(0, []uint64{1, 1, 2, 2}, []*vector.Vector{vecLeft}))
	require.NoError(t, right.BatchFill(0, []uint64{1, 1, 2, 2}, []*vector.Vector{vecRight}))
	require.NoError(t, left.BatchMerge(right, 0, []uint64{1, 2}))

	ret, err := left.Flush()
	require.NoError(t, err)
	require.Len(t, ret, 1)
	require.Equal(t, 7.0, vector.GetFixedAtNoTypeCheck[float64](ret[0], 0))
	require.Equal(t, 9.0, vector.GetFixedAtNoTypeCheck[float64](ret[0], 1))
	ret[0].Free(mp)
	left.Free()
	right.Free()
}

func TestMedianDistinctBatchMergeDeduplicates(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() {
		require.Equal(t, int64(0), mp.CurrNB())
	}()

	left, err := makeMedian(mp, AggIdOfMedian, true, types.T_int64.ToType())
	require.NoError(t, err)
	right, err := makeMedian(mp, AggIdOfMedian, true, types.T_int64.ToType())
	require.NoError(t, err)
	require.NoError(t, left.GroupGrow(1))
	require.NoError(t, right.GroupGrow(1))

	vecLeft := buildFixedVec(t, mp, types.T_int64.ToType(), []int64{1, 3, 5})
	vecRight := buildFixedVec(t, mp, types.T_int64.ToType(), []int64{3, 5, 7})
	defer vecLeft.Free(mp)
	defer vecRight.Free(mp)

	require.NoError(t, left.BulkFill(0, []*vector.Vector{vecLeft}))
	require.NoError(t, right.BulkFill(0, []*vector.Vector{vecRight}))
	require.NoError(t, left.BatchMerge(right, 0, []uint64{1}))

	ret, err := left.Flush()
	require.NoError(t, err)
	require.Len(t, ret, 1)
	require.Equal(t, 4.0, vector.GetFixedAtNoTypeCheck[float64](ret[0], 0))
	ret[0].Free(mp)
	left.Free()
	right.Free()
}

func TestMedianIntermediateRoundTripMultipleGroups(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() {
		require.Equal(t, int64(0), mp.CurrNB())
	}()

	exec, err := makeMedian(mp, AggIdOfMedian, false, types.T_int64.ToType())
	require.NoError(t, err)
	require.NoError(t, exec.GroupGrow(3))

	vec := buildFixedVec(t, mp, types.T_int64.ToType(), []int64{1, 1, 2, 4, 6, 8, 10})
	defer vec.Free(mp)
	require.NoError(t, exec.BatchFill(0, []uint64{1, 1, 1, 2, 2, 3, 3}, []*vector.Vector{vec}))

	var buf bytes.Buffer
	require.NoError(t, exec.SaveIntermediateResult(3, [][]uint8{{1, 1, 1}}, &buf))

	restored, err := makeMedian(mp, AggIdOfMedian, false, types.T_int64.ToType())
	require.NoError(t, err)
	require.NoError(t, restored.UnmarshalFromReader(bytes.NewReader(buf.Bytes()), mp))

	ret, err := restored.Flush()
	require.NoError(t, err)
	require.Len(t, ret, 1)
	require.Equal(t, 1.0, vector.GetFixedAtNoTypeCheck[float64](ret[0], 0))
	require.Equal(t, 5.0, vector.GetFixedAtNoTypeCheck[float64](ret[0], 1))
	require.Equal(t, 9.0, vector.GetFixedAtNoTypeCheck[float64](ret[0], 2))
	ret[0].Free(mp)
	exec.Free()
	restored.Free()
}

func TestMedianDecimalDistinctMerge(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() {
		require.Equal(t, int64(0), mp.CurrNB())
	}()

	typ := types.New(types.T_decimal64, 10, 2)
	left, err := makeMedian(mp, AggIdOfMedian, true, typ)
	require.NoError(t, err)
	right, err := makeMedian(mp, AggIdOfMedian, true, typ)
	require.NoError(t, err)
	require.NoError(t, left.GroupGrow(1))
	require.NoError(t, right.GroupGrow(1))

	vecLeft := medianTestVector(t, mp, typ, mustDecimal64s(t, "1.00", "3.00", "5.00"))
	vecRight := medianTestVector(t, mp, typ, mustDecimal64s(t, "3.00", "7.00", "9.00"))
	defer vecLeft.Free(mp)
	defer vecRight.Free(mp)

	require.NoError(t, left.BulkFill(0, []*vector.Vector{vecLeft}))
	require.NoError(t, right.BulkFill(0, []*vector.Vector{vecRight}))
	require.NoError(t, left.Merge(right, 0, 0))

	ret, err := left.Flush()
	require.NoError(t, err)
	require.Equal(t, "5.000", vector.GetFixedAtNoTypeCheck[types.Decimal128](ret[0], 0).Format(ret[0].GetType().Scale))
	ret[0].Free(mp)
	left.Free()
	right.Free()
}

func TestMedianNumericValsAvoidsInt64Overflow(t *testing.T) {
	vals := []int64{math.MaxInt64, math.MaxInt64}
	require.Equal(t, float64(math.MaxInt64), medianNumericVals(vals))
}

func TestMedianIntermediateRoundTripRejectsInvalidGroupCount(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() {
		require.Equal(t, int64(0), mp.CurrNB())
	}()

	exec, err := makeMedian(mp, AggIdOfMedian, false, types.T_int64.ToType())
	require.NoError(t, err)
	require.NoError(t, exec.GroupGrow(1))

	vec := buildFixedVec(t, mp, types.T_int64.ToType(), []int64{1, 2, 3})
	defer vec.Free(mp)
	require.NoError(t, exec.BulkFill(0, []*vector.Vector{vec}))

	var buf bytes.Buffer
	require.NoError(t, exec.SaveIntermediateResult(1, [][]uint8{{1}}, &buf))

	broken := append([]byte(nil), buf.Bytes()...)
	reader := bytes.NewReader(broken)
	_, err = types.ReadInt64(reader)
	require.NoError(t, err)

	probe, err := makeMedian(mp, AggIdOfMedian, false, types.T_int64.ToType())
	require.NoError(t, err)
	probeExec := probe.(*medianColumnNumericExec[int64])
	require.NoError(t, unmarshalFromReaderNoGroup(reader, &probeExec.ret.optSplitResult))
	offset := len(broken) - reader.Len()
	copy(broken[offset:offset+8], types.EncodeInt64(ptr(int64(1<<30))))
	probe.Free()

	restored, err := makeMedian(mp, AggIdOfMedian, false, types.T_int64.ToType())
	require.NoError(t, err)
	err = restored.UnmarshalFromReader(bytes.NewReader(broken), mp)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid group count")
	restored.Free()
	exec.Free()
}

func TestMedianDistinctRoundTripRebuildsDistinctState(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() {
		require.Equal(t, int64(0), mp.CurrNB())
	}()

	exec, err := makeMedian(mp, AggIdOfMedian, true, types.T_int64.ToType())
	require.NoError(t, err)
	require.NoError(t, exec.GroupGrow(1))

	vec := buildFixedVec(t, mp, types.T_int64.ToType(), []int64{1, 3, 3})
	defer vec.Free(mp)
	require.NoError(t, exec.BulkFill(0, []*vector.Vector{vec}))

	var buf bytes.Buffer
	require.NoError(t, exec.SaveIntermediateResult(1, [][]uint8{{1}}, &buf))

	restored, err := makeMedian(mp, AggIdOfMedian, true, types.T_int64.ToType())
	require.NoError(t, err)
	require.NoError(t, restored.UnmarshalFromReader(bytes.NewReader(buf.Bytes()), mp))

	more := buildFixedVec(t, mp, types.T_int64.ToType(), []int64{3, 5})
	defer more.Free(mp)
	require.NoError(t, restored.BulkFill(0, []*vector.Vector{more}))

	ret, err := restored.Flush()
	require.NoError(t, err)
	require.Len(t, ret, 1)
	require.Equal(t, 3.0, vector.GetFixedAtNoTypeCheck[float64](ret[0], 0))
	ret[0].Free(mp)
	restored.Free()
	exec.Free()
}

func TestSelectKthFuncHandlesDuplicateHeavyInput(t *testing.T) {
	vals := make([]int, 4096)
	comparisons := 0

	got := selectKthFunc(vals, len(vals)/2, func(a, b int) int {
		comparisons++
		switch {
		case a < b:
			return -1
		case a > b:
			return 1
		default:
			return 0
		}
	})

	require.Equal(t, 0, got)
	require.LessOrEqual(t, comparisons, len(vals)*2)
}

func medianTestVector(t *testing.T, mp *mpool.MPool, typ types.Type, values any) *vector.Vector {
	t.Helper()
	v := vector.NewVec(typ)
	switch typ.Oid {
	case types.T_bit, types.T_uint64:
		require.NoError(t, vector.AppendFixedList(v, values.([]uint64), nil, mp))
	case types.T_int8:
		require.NoError(t, vector.AppendFixedList(v, values.([]int8), nil, mp))
	case types.T_int16:
		require.NoError(t, vector.AppendFixedList(v, values.([]int16), nil, mp))
	case types.T_int32:
		require.NoError(t, vector.AppendFixedList(v, values.([]int32), nil, mp))
	case types.T_int64:
		require.NoError(t, vector.AppendFixedList(v, values.([]int64), nil, mp))
	case types.T_uint8:
		require.NoError(t, vector.AppendFixedList(v, values.([]uint8), nil, mp))
	case types.T_uint16:
		require.NoError(t, vector.AppendFixedList(v, values.([]uint16), nil, mp))
	case types.T_uint32:
		require.NoError(t, vector.AppendFixedList(v, values.([]uint32), nil, mp))
	case types.T_float32:
		require.NoError(t, vector.AppendFixedList(v, values.([]float32), nil, mp))
	case types.T_float64:
		require.NoError(t, vector.AppendFixedList(v, values.([]float64), nil, mp))
	case types.T_decimal64:
		require.NoError(t, vector.AppendFixedList(v, values.([]types.Decimal64), nil, mp))
	case types.T_decimal128:
		require.NoError(t, vector.AppendFixedList(v, values.([]types.Decimal128), nil, mp))
	default:
		t.Fatalf("unsupported test type %v", typ.Oid)
	}
	return v
}

func mustDecimal64s(t *testing.T, vals ...string) []types.Decimal64 {
	t.Helper()
	ret := make([]types.Decimal64, len(vals))
	for i, v := range vals {
		d, err := types.ParseDecimal64(v, 10, 2)
		require.NoError(t, err)
		ret[i] = d
	}
	return ret
}

func mustDecimal128s(t *testing.T, vals ...string) []types.Decimal128 {
	t.Helper()
	ret := make([]types.Decimal128, len(vals))
	for i, v := range vals {
		d, err := types.ParseDecimal128(v, 20, 2)
		require.NoError(t, err)
		ret[i] = d
	}
	return ret
}
