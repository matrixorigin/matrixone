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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestAvgTwCacheNumericRoundTrip(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	defer func() { require.Equal(t, int64(0), mp.CurrNB()) }()

	input := vector.NewVec(types.T_int32.ToType())
	require.NoError(t, vector.AppendFixedList(input, []int32{3, 5}, nil, mp))

	exec := newAvgTwCacheNumericExec[int32](mp, 1, types.T_int32.ToType())
	require.NoError(t, exec.GroupGrow(1))
	require.NoError(t, exec.BatchFill(0, []uint64{1, 1}, []*vector.Vector{input}))

	var buf bytes.Buffer
	require.NoError(t, exec.SaveIntermediateResult(1, [][]uint8{{1}}, &buf))

	restored := newAvgTwCacheNumericExec[int32](mp, 1, types.T_int32.ToType())
	require.NoError(t, restored.UnmarshalFromReader(bytes.NewReader(buf.Bytes()), mp))

	vecs, err := restored.Flush()
	require.NoError(t, err)
	require.Len(t, vecs, 1)
	require.False(t, vecs[0].IsNull(0))
	payload := vecs[0].GetBytesAt(0)
	require.Len(t, payload, 16)
	require.Equal(t, 8.0, types.DecodeFloat64(payload[0:]))
	require.Equal(t, int64(2), types.DecodeInt64(payload[8:]))

	vecs[0].Free(mp)
	exec.Free()
	restored.Free()
	input.Free(mp)
}

func TestAvgTwResultFloatRoundTrip(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	defer func() { require.Equal(t, int64(0), mp.CurrNB()) }()

	input := vector.NewVec(types.T_int32.ToType())
	require.NoError(t, vector.AppendFixedList(input, []int32{2, 4, 6}, nil, mp))

	cacheExec := newAvgTwCacheNumericExec[int32](mp, 1, types.T_int32.ToType())
	require.NoError(t, cacheExec.GroupGrow(1))
	require.NoError(t, cacheExec.BatchFill(0, []uint64{1, 1, 1}, []*vector.Vector{input}))
	cacheVecs, err := cacheExec.Flush()
	require.NoError(t, err)

	resultExec := newAvgTwResultFloatExec(mp, 2, types.T_char.ToType())
	require.NoError(t, resultExec.GroupGrow(1))
	require.NoError(t, resultExec.BatchFill(0, []uint64{1}, cacheVecs))

	var buf bytes.Buffer
	require.NoError(t, resultExec.SaveIntermediateResult(1, [][]uint8{{1}}, &buf))

	restored := newAvgTwResultFloatExec(mp, 2, types.T_char.ToType())
	require.NoError(t, restored.UnmarshalFromReader(bytes.NewReader(buf.Bytes()), mp))

	resultVecs, err := restored.Flush()
	require.NoError(t, err)
	require.Len(t, resultVecs, 1)
	require.False(t, resultVecs[0].IsNull(0))
	require.Equal(t, 4.0, vector.GetFixedAtNoTypeCheck[float64](resultVecs[0], 0))

	resultVecs[0].Free(mp)
	cacheVecs[0].Free(mp)
	cacheExec.Free()
	resultExec.Free()
	restored.Free()
	input.Free(mp)
}

func TestAvgTwDecimalRoundTrip(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	defer func() { require.Equal(t, int64(0), mp.CurrNB()) }()

	typ := types.New(types.T_decimal64, 10, 2)
	v1, err := types.ParseDecimal64("1.10", 10, 2)
	require.NoError(t, err)
	v2, err := types.ParseDecimal64("2.30", 10, 2)
	require.NoError(t, err)

	input := vector.NewVec(typ)
	require.NoError(t, vector.AppendFixedList(input, []types.Decimal64{v1, v2}, nil, mp))

	cacheExec := newAvgTwCacheDecimalExec[types.Decimal64](mp, 1, typ)
	require.NoError(t, cacheExec.GroupGrow(1))
	require.NoError(t, cacheExec.BatchFill(0, []uint64{1, 1}, []*vector.Vector{input}))
	cacheVecs, err := cacheExec.Flush()
	require.NoError(t, err)

	resultExec := newAvgTwResultDecimalExec(mp, 2, AvgTwCacheReturnType([]types.Type{typ}))
	require.NoError(t, resultExec.GroupGrow(1))
	require.NoError(t, resultExec.BatchFill(0, []uint64{1}, cacheVecs))
	resultVecs, err := resultExec.Flush()
	require.NoError(t, err)
	require.Len(t, resultVecs, 1)
	require.False(t, resultVecs[0].IsNull(0))
	require.Equal(t, "1.70000000", vector.GetFixedAtNoTypeCheck[types.Decimal128](resultVecs[0], 0).Format(8))

	resultVecs[0].Free(mp)
	cacheVecs[0].Free(mp)
	cacheExec.Free()
	resultExec.Free()
	input.Free(mp)
}

func TestAvgTwFactoriesAndErrorPaths(t *testing.T) {
	mp := mpool.MustNewZero()

	for _, typ := range []types.Type{
		types.T_int8.ToType(),
		types.T_uint16.ToType(),
		types.T_float64.ToType(),
		types.New(types.T_decimal64, 10, 2),
		types.New(types.T_decimal128, 20, 2),
	} {
		exec, err := makeAvgTwCacheExec(mp, 1, typ)
		require.NoError(t, err)
		exec.Free()
	}

	for _, typ := range []types.Type{
		types.T_char.ToType(),
		types.New(types.T_varchar, types.MaxVarcharLen, 2),
	} {
		exec, err := makeAvgTwResultExec(mp, 1, typ)
		require.NoError(t, err)
		exec.Free()
	}

	_, err := makeAvgTwCacheExec(mp, 1, types.T_varchar.ToType())
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported type")

	_, err = makeAvgTwResultExec(mp, 1, types.T_int64.ToType())
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported type")

	floatExec := newAvgTwResultFloatExec(mp, 1, types.T_char.ToType())
	require.NoError(t, floatExec.GroupGrow(1))
	badFloat := buildVarlenVec(t, mp, types.T_char.ToType(), []string{"x"})
	err = floatExec.Fill(0, 0, []*vector.Vector{badFloat})
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid float cache payload")

	decimalExec := newAvgTwResultDecimalExec(mp, 1, types.New(types.T_varchar, types.MaxVarcharLen, 2))
	require.NoError(t, decimalExec.GroupGrow(1))
	badDecimal := buildVarlenVec(t, mp, types.T_varchar.ToType(), []string{"short"})
	err = decimalExec.Fill(0, 0, []*vector.Vector{badDecimal})
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid decimal cache payload")

	badFloat.Free(mp)
	badDecimal.Free(mp)
	floatExec.Free()
	decimalExec.Free()
}

func TestAvgTwCacheAndResultMergeBranches(t *testing.T) {
	mp := mpool.MustNewZero()

	left := newAvgTwCacheNumericExec[int32](mp, 1, types.T_int32.ToType())
	right := newAvgTwCacheNumericExec[int32](mp, 1, types.T_int32.ToType())
	require.NoError(t, left.GroupGrow(2))
	require.NoError(t, right.GroupGrow(2))
	require.NoError(t, left.SetExtraInformation(nil, 0))

	constVec, err := vector.NewConstFixed(types.T_int32.ToType(), int32(5), 3, mp)
	require.NoError(t, err)
	require.NoError(t, left.BatchFill(0, []uint64{1, GroupNotMatched, 2}, []*vector.Vector{constVec}))
	require.NoError(t, left.Fill(0, 0, []*vector.Vector{constVec}))
	require.NoError(t, left.BulkFill(1, []*vector.Vector{constVec}))

	rightVec := buildFixedVec(t, mp, types.T_int32.ToType(), []int32{7, 9})
	require.NoError(t, right.BatchFill(0, []uint64{1, 2}, []*vector.Vector{rightVec}))
	require.NoError(t, left.Merge(right, 0, 0))
	require.NoError(t, left.BatchMerge(right, 0, []uint64{1, 2}))

	cacheVecs, err := left.Flush()
	require.NoError(t, err)
	require.Len(t, cacheVecs, 1)
	require.Equal(t, 24.0, types.DecodeFloat64(cacheVecs[0].GetBytesAt(0)[0:]))
	require.Equal(t, int64(4), types.DecodeInt64(cacheVecs[0].GetBytesAt(0)[8:]))
	require.Equal(t, 29.0, types.DecodeFloat64(cacheVecs[0].GetBytesAt(1)[0:]))
	require.Equal(t, int64(5), types.DecodeInt64(cacheVecs[0].GetBytesAt(1)[8:]))

	resultExec := newAvgTwResultFloatExec(mp, 2, types.T_char.ToType())
	require.NoError(t, resultExec.GroupGrow(3))
	require.NoError(t, resultExec.BatchFill(0, []uint64{1, 2, GroupNotMatched}, cacheVecs))
	resultVecs, err := resultExec.Flush()
	require.NoError(t, err)
	require.Equal(t, 6.0, vector.GetFixedAtNoTypeCheck[float64](resultVecs[0], 0))
	require.Equal(t, 5.8, vector.GetFixedAtNoTypeCheck[float64](resultVecs[0], 1))
	require.True(t, resultVecs[0].IsNull(2))

	cacheVecs[0].Free(mp)
	resultVecs[0].Free(mp)
	constVec.Free(mp)
	rightVec.Free(mp)
	left.Free()
	right.Free()
	resultExec.Free()
}

func TestAvgTwDecimalAndResultWrapperPaths(t *testing.T) {
	mp := mpool.MustNewZero()
	typ := types.New(types.T_decimal64, 10, 2)

	cacheLeft := newAvgTwCacheDecimalExec[types.Decimal64](mp, 1, typ)
	cacheRight := newAvgTwCacheDecimalExec[types.Decimal64](mp, 1, typ)
	require.NoError(t, cacheLeft.GroupGrow(1))
	require.NoError(t, cacheRight.GroupGrow(1))
	require.NoError(t, cacheLeft.SetExtraInformation(nil, 0))

	input := vector.NewVec(typ)
	require.NoError(t, vector.AppendFixedList(input, mustDecimal64s(t, "1.00", "3.00"), nil, mp))
	require.NoError(t, cacheLeft.Fill(0, 0, []*vector.Vector{input}))
	require.NoError(t, cacheLeft.BulkFill(0, []*vector.Vector{input}))
	require.NoError(t, cacheRight.Fill(0, 1, []*vector.Vector{input}))
	require.NoError(t, cacheLeft.Merge(cacheRight, 0, 0))
	require.NoError(t, cacheLeft.BatchMerge(cacheRight, 0, []uint64{1}))

	cacheVecs, err := cacheLeft.Flush()
	require.NoError(t, err)

	resultExec := newAvgTwResultDecimalExec(mp, 2, AvgTwCacheReturnType([]types.Type{typ}))
	resultOther := newAvgTwResultDecimalExec(mp, 2, AvgTwCacheReturnType([]types.Type{typ}))
	require.NoError(t, resultExec.GroupGrow(1))
	require.NoError(t, resultOther.GroupGrow(1))
	require.NoError(t, resultExec.SetExtraInformation(nil, 0))
	require.NoError(t, resultExec.Fill(0, 0, cacheVecs))
	require.NoError(t, resultOther.BulkFill(0, cacheVecs))
	require.NoError(t, resultExec.Merge(resultOther, 0, 0))
	require.NoError(t, resultExec.BatchMerge(resultOther, 0, []uint64{1}))

	resultVecs, err := resultExec.Flush()
	require.NoError(t, err)
	require.False(t, resultVecs[0].IsNull(0))

	cacheVecs[0].Free(mp)
	resultVecs[0].Free(mp)
	input.Free(mp)
	cacheLeft.Free()
	cacheRight.Free()
	resultExec.Free()
	resultOther.Free()
}

func TestAvgTwResultFloatWrapperPaths(t *testing.T) {
	mp := mpool.MustNewZero()

	cacheExec := newAvgTwCacheNumericExec[int32](mp, 1, types.T_int32.ToType())
	require.NoError(t, cacheExec.GroupGrow(1))
	input := buildFixedVec(t, mp, types.T_int32.ToType(), []int32{2, 4})
	require.NoError(t, cacheExec.BulkFill(0, []*vector.Vector{input}))
	cacheVecs, err := cacheExec.Flush()
	require.NoError(t, err)

	left := newAvgTwResultFloatExec(mp, 2, types.T_char.ToType())
	right := newAvgTwResultFloatExec(mp, 2, types.T_char.ToType())
	require.NoError(t, left.GroupGrow(1))
	require.NoError(t, right.GroupGrow(1))
	require.NoError(t, left.SetExtraInformation(nil, 0))
	require.NoError(t, left.BulkFill(0, cacheVecs))
	require.NoError(t, right.Fill(0, 0, cacheVecs))
	require.NoError(t, left.Merge(right, 0, 0))
	require.NoError(t, left.BatchMerge(right, 0, []uint64{1}))

	vecs, err := left.Flush()
	require.NoError(t, err)
	require.False(t, vecs[0].IsNull(0))

	cacheVecs[0].Free(mp)
	vecs[0].Free(mp)
	input.Free(mp)
	cacheExec.Free()
	left.Free()
	right.Free()
}
