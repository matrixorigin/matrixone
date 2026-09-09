// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package function

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestLastInsertIDExprResolution(t *testing.T) {
	zero, err := GetFunctionByName(context.Background(), "last_insert_id", nil)
	require.NoError(t, err)
	require.Equal(t, int64(0), zero.GetEncodedOverloadID()&0xffffffff)

	one, err := GetFunctionByName(context.Background(), "last_insert_id", []types.Type{types.T_int64.ToType()})
	require.NoError(t, err)
	require.Equal(t, int64(LastInsertIDExprOverload), one.GetEncodedOverloadID()&0xffffffff)
	require.Equal(t, types.T_uint64, one.GetReturnType().Oid)
	prepared, err := GetFunctionByName(context.Background(), "last_insert_id", []types.Type{types.T_any.ToType()})
	require.NoError(t, err)
	require.Equal(t, int64(LastInsertIDExprOverload), prepared.GetEncodedOverloadID()&0xffffffff)

	for _, typ := range []types.T{types.T_float64, types.T_decimal64, types.T_varchar} {
		_, err = GetFunctionByName(context.Background(), "last_insert_id", []types.Type{typ.ToType()})
		require.NoError(t, err, "conversion from %s follows CAST AS UNSIGNED", typ)
	}
}

func TestLastInsertIDExprExecutionDoesNotPublishUntilSuccess(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	proc.SetLastInsertID(7)

	input := testutil.NewVector(3, types.T_int64.ToType(), proc.Mp(), false, []int64{0, 42, 9})
	defer input.Free(proc.Mp())
	out, err := RunFunctionDirectly(proc, EncodeOverloadID(LAST_INSERT_ID, LastInsertIDExprOverload), []*vector.Vector{input}, 3)
	require.NoError(t, err)
	require.Equal(t, []uint64{0, 42, 9}, vector.MustFixedColNoTypeCheck[uint64](out))
	require.Equal(t, uint64(7), proc.GetLastInsertID())
	value, valid := proc.GetLastInsertIDExpr()
	require.True(t, valid)
	require.Equal(t, uint64(9), value)
	out.Free(proc.Mp())

	proc.ResetLastInsertIDExpr()
	negative := testutil.NewVector(1, types.T_int64.ToType(), proc.Mp(), false, []int64{-1})
	defer negative.Free(proc.Mp())
	out, err = RunFunctionDirectly(proc, EncodeOverloadID(LAST_INSERT_ID, LastInsertIDExprOverload), []*vector.Vector{negative}, 1)
	require.NoError(t, err)
	require.Equal(t, uint64(^uint64(0)), vector.MustFixedColNoTypeCheck[uint64](out)[0])
	out.Free(proc.Mp())
	_, valid = proc.GetLastInsertIDExpr()
	require.True(t, valid)
	require.Equal(t, uint64(7), proc.GetLastInsertID())
}

func TestLastInsertIDExprNullAndMaxUint64(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	proc.SetLastInsertID(13)

	nullInput := vector.NewConstNull(types.T_any.ToType(), 1, proc.Mp())
	out, err := RunFunctionDirectly(proc, EncodeOverloadID(LAST_INSERT_ID, LastInsertIDExprOverload), []*vector.Vector{nullInput}, 1)
	require.NoError(t, err)
	require.True(t, out.IsNull(0))
	require.Equal(t, uint64(13), proc.GetLastInsertID())
	_, valid, isNull := proc.GetLastInsertIDExprState()
	require.True(t, valid)
	require.True(t, isNull)
	out.Free(proc.Mp())
	nullInput.Free(proc.Mp())

	maxInput, err := vector.NewConstFixed(types.T_uint64.ToType(), ^uint64(0), 1, proc.Mp())
	require.NoError(t, err)
	out, err = RunFunctionDirectly(proc, EncodeOverloadID(LAST_INSERT_ID, LastInsertIDExprOverload), []*vector.Vector{maxInput}, 1)
	require.NoError(t, err)
	require.Equal(t, ^uint64(0), vector.MustFixedColNoTypeCheck[uint64](out)[0])
	value, valid := proc.GetLastInsertIDExpr()
	require.True(t, valid)
	require.Equal(t, ^uint64(0), value)
	out.Free(proc.Mp())
	maxInput.Free(proc.Mp())
}

func TestLastInsertIDExprConstNullHonorsSelectedRows(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	input := vector.NewConstNull(types.T_any.ToType(), 3, proc.Mp())
	defer input.Free(proc.Mp())
	run := func(length int, selectList *FunctionSelectList) *vector.Vector {
		result := vector.NewFunctionResultWrapper(types.T_uint64.ToType(), proc.Mp())
		require.NoError(t, result.PreExtendAndReset(length))
		require.NoError(t, LastInsertIDExpr([]*vector.Vector{input}, result, proc, length, selectList))
		return result.GetResultVector()
	}

	proc.ResetLastInsertIDExpr()
	proc.SetLastInsertIDExpr(77)
	run(0, nil)
	_, valid, _ := proc.GetLastInsertIDExprState()
	require.True(t, valid, "zero-length evaluation must not replace an existing generation")
	value, _, _ := proc.GetLastInsertIDExprState()
	require.Equal(t, uint64(77), value)

	proc.ResetLastInsertIDExpr()
	out := run(3, &FunctionSelectList{AnyNull: true, AllNull: true, SelectList: []bool{false, false, false}})
	require.True(t, out.IsNull(0))
	_, valid, _ = proc.GetLastInsertIDExprState()
	require.False(t, valid, "an all-masked const NULL must not publish")
	out.Free(proc.Mp())

	proc.ResetLastInsertIDExpr()
	out = run(3, &FunctionSelectList{AnyNull: true, SelectList: []bool{false, true, false}})
	require.True(t, out.IsNull(0))
	require.True(t, out.IsNull(1))
	require.True(t, out.IsNull(2))
	_, valid, isNull := proc.GetLastInsertIDExprState()
	require.True(t, valid)
	require.True(t, isNull)
	out.Free(proc.Mp())
}

func TestLastInsertIDExprPreparedIntegerUsesConversionMetadata(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	proc.SetLastInsertID(13)

	input := vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(input, []byte("18446744073709551615"), false, proc.Mp()))
	input.ToConst()
	input.SetType(types.T_any.ToType())
	input.SetPrepareParamKind(vector.PrepareParamInteger)
	defer input.Free(proc.Mp())

	out, err := RunFunctionDirectly(proc, EncodeOverloadID(LAST_INSERT_ID, LastInsertIDExprOverload), []*vector.Vector{input}, 1)
	require.NoError(t, err)
	defer out.Free(proc.Mp())
	require.Equal(t, ^uint64(0), vector.MustFixedColNoTypeCheck[uint64](out)[0])
	value, valid := proc.GetLastInsertIDExpr()
	require.True(t, valid)
	require.Equal(t, ^uint64(0), value)

	proc.ResetLastInsertIDExpr()
	negative := vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(negative, []byte("-1"), false, proc.Mp()))
	negative.ToConst()
	negative.SetType(types.T_any.ToType())
	negative.SetPrepareParamKind(vector.PrepareParamInteger)
	defer negative.Free(proc.Mp())
	out, err = RunFunctionDirectly(proc, EncodeOverloadID(LAST_INSERT_ID, LastInsertIDExprOverload), []*vector.Vector{negative}, 1)
	require.NoError(t, err)
	require.Equal(t, ^uint64(0), vector.MustFixedColNoTypeCheck[uint64](out)[0])
	out.Free(proc.Mp())
	_, valid = proc.GetLastInsertIDExpr()
	require.True(t, valid)
}

func TestLastInsertIDExprUsesNumericAndStringConversions(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	floatInput := testutil.NewVector(1, types.T_float64.ToType(), proc.Mp(), false, []float64{2.5})
	out, err := RunFunctionDirectly(proc, EncodeOverloadID(LAST_INSERT_ID, LastInsertIDExprOverload), []*vector.Vector{floatInput}, 1)
	require.NoError(t, err)
	require.Equal(t, uint64(2), vector.MustFixedColNoTypeCheck[uint64](out)[0])
	out.Free(proc.Mp())
	floatInput.Free(proc.Mp())

	stringInput := vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(stringInput, []byte("-1"), false, proc.Mp()))
	out, err = RunFunctionDirectly(proc, EncodeOverloadID(LAST_INSERT_ID, LastInsertIDExprOverload), []*vector.Vector{stringInput}, 1)
	require.NoError(t, err)
	require.Equal(t, ^uint64(0), vector.MustFixedColNoTypeCheck[uint64](out)[0])
	out.Free(proc.Mp())
	stringInput.Free(proc.Mp())

	decimal, err := types.Decimal64FromFloat64(2.5, 18, 1)
	require.NoError(t, err)
	decimalInput, err := vector.NewConstFixed(types.New(types.T_decimal64, 18, 1), decimal, 1, proc.Mp())
	require.NoError(t, err)
	out, err = RunFunctionDirectly(proc, EncodeOverloadID(LAST_INSERT_ID, LastInsertIDExprOverload), []*vector.Vector{decimalInput}, 1)
	require.NoError(t, err)
	require.Equal(t, uint64(3), vector.MustFixedColNoTypeCheck[uint64](out)[0])
	out.Free(proc.Mp())
	decimalInput.Free(proc.Mp())
}
