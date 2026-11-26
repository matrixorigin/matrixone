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

package function

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestHasEvaluableRows(t *testing.T) {
	t.Run("zero length", func(t *testing.T) {
		require.False(t, hasEvaluableRows(mustNullsOK(nulls.NewWithSize(0, bitmapTestMP)), 0))
	})

	t.Run("empty bitmap fast path", func(t *testing.T) {
		require.True(t, hasEvaluableRows(mustNullsOK(nulls.NewWithSize(2, bitmapTestMP)), 2))
	})

	t.Run("all current rows null with stale tail", func(t *testing.T) {
		rsNull := mustNullsOK(nulls.NewWithSize(3, bitmapTestMP))
		rsNull.Add(0)
		rsNull.Add(1)
		rsNull.Add(2)
		require.False(t, hasEvaluableRows(rsNull, 2))
	})

	t.Run("partial null", func(t *testing.T) {
		rsNull := mustNullsOK(nulls.NewWithSize(2, bitmapTestMP))
		rsNull.Add(1)
		require.True(t, hasEvaluableRows(rsNull, 2))
	})

	t.Run("stale tail only", func(t *testing.T) {
		rsNull := mustNullsOK(nulls.NewWithSize(3, bitmapTestMP))
		rsNull.Add(2)
		require.True(t, hasEvaluableRows(rsNull, 2))
	})
}

type arithmeticMatrixInput interface {
	int64 | uint64 | float32 | float64 | types.Decimal64 | types.Decimal128 | types.Decimal256
}

type arithmeticMatrixOperation struct {
	name string
	id   int
	fn   executeLogicOfOverload
}

func TestArithmeticDivisionByZeroMatrix(t *testing.T) {
	divMod := []arithmeticMatrixOperation{
		{name: "div", id: INTEGER_DIV, fn: integerDivFn},
		{name: "mod", id: MOD, fn: modFn},
	}
	allOps := []arithmeticMatrixOperation{
		{name: "slash", id: DIV, fn: divFn},
		{name: "div", id: INTEGER_DIV, fn: integerDivFn},
		{name: "mod", id: MOD, fn: modFn},
	}

	runArithmeticMatrixType(t, types.T_int64.ToType(), int64(1), int64(0), divMod)
	runArithmeticMatrixType(t, types.T_uint64.ToType(), uint64(1), uint64(0), divMod)
	runArithmeticMatrixType(t, types.T_float32.ToType(), float32(1), float32(0), allOps)
	runArithmeticMatrixType(t, types.T_float64.ToType(), float64(1), float64(0), allOps)
	runArithmeticMatrixType(t, types.New(types.T_decimal64, 18, 2), types.Decimal64(100), types.Decimal64(0), allOps)
	runArithmeticMatrixType(t, types.New(types.T_decimal128, 38, 2),
		types.Decimal128{B0_63: 100}, types.Decimal128{}, allOps)
	runArithmeticMatrixType(t, types.New(types.T_decimal256, 65, 2),
		types.Decimal256{B0_63: 100}, types.Decimal256{}, allOps)
}

func runArithmeticMatrixType[T arithmeticMatrixInput](
	t *testing.T,
	typ types.Type,
	one T,
	zero T,
	operations []arithmeticMatrixOperation,
) {
	t.Helper()
	type shape struct {
		name                  string
		leftConst, rightConst bool
	}
	shapes := []shape{
		{name: "const_const", leftConst: true, rightConst: true},
		{name: "const_vector", leftConst: true},
		{name: "vector_const", rightConst: true},
		{name: "vector_vector"},
	}
	type rowState struct {
		name string
	}
	states := []rowState{{name: "all_null"}, {name: "mixed_null"}}
	modes := []struct {
		name   string
		strict bool
	}{{name: "strict", strict: true}, {name: "non_strict"}}

	for _, op := range operations {
		for _, shape := range shapes {
			for _, state := range states {
				// A const/const pair has no row-varying operand, so mixed NULL is
				// not a representable execution shape.
				if shape.leftConst && shape.rightConst && state.name == "mixed_null" {
					continue
				}
				for _, mode := range modes {
					name := typ.Oid.String() + "/" + op.name + "/" + shape.name + "/" + state.name + "/" + mode.name
					t.Run(name, func(t *testing.T) {
						leftValues := []T{one, one}
						rightValues := []T{zero, zero}
						leftNulls := []bool{false, false}
						rightNulls := []bool{false, false}

						switch state.name {
						case "all_null":
							leftNulls = []bool{true, true}
						case "mixed_null":
							if !shape.leftConst {
								leftNulls = []bool{true, false}
							} else {
								rightNulls = []bool{true, false}
							}
						}

						if shape.leftConst {
							leftValues = leftValues[:1]
							leftNulls = leftNulls[:1]
						}
						if shape.rightConst {
							rightValues = rightValues[:1]
							rightNulls = rightNulls[:1]
						}

						makeInput := func(isConst bool, values []T, nullList []bool) FunctionTestInput {
							if isConst {
								return NewFunctionTestConstInput(typ, values, nullList)
							}
							return NewFunctionTestInput(typ, values, nullList)
						}
						inputs := []FunctionTestInput{
							makeInput(shape.leftConst, leftValues, leftNulls),
							makeInput(shape.rightConst, rightValues, rightNulls),
						}
						resultType := resolvedReturnType(t, op.id, []types.Type{typ, typ})
						proc := testutil.NewProcess(t)
						defer proc.Free()
						sink := &uncompressWarningSink{}
						proc.WarningSink = sink
						proc.GetSessionInfo().SqlMode = "ERROR_FOR_DIVISION_BY_ZERO"
						profile := &process.StmtProfile{}
						proc.SetStmtProfile(profile)
						if mode.strict {
							profile.SetStatementRuntimeProfile("Insert", "DML", false)
						} else {
							profile.SetStatementRuntimeProfile("Select", "DQL", false)
						}
						if mode.strict {
							atomic.StoreInt32(&proc.Base.DivByZeroErrorMode, 1)
						} else {
							atomic.StoreInt32(&proc.Base.DivByZeroErrorMode, 0)
						}
						tcc := NewFunctionTestCase(proc, inputs,
							NewFunctionTestResult(resultType, false, nil, nil), op.fn)
						defer tcc.Free()
						if shape.leftConst && shape.rightConst {
							require.Equal(t, !mode.strict && state.name != "all_null", ScalarDivisionNeedsRowEvaluation(tcc.parameters, proc))
						}
						require.NoError(t, tcc.result.PreExtendAndReset(2))
						err := tcc.fn(tcc.parameters, tcc.result, proc, 2)

						wantError := mode.strict && state.name != "all_null"
						if wantError {
							require.Error(t, err)
							return
						}
						require.NoError(t, err)
						wantWarnings := uint64(0)
						if !mode.strict && state.name != "all_null" {
							wantWarnings = 1
						}
						require.Equal(t, wantWarnings, sink.total)
						require.Len(t, sink.records, int(wantWarnings))
						for _, warning := range sink.records {
							require.Equal(t, uint16(1365), warning.code)
							require.Equal(t, "Division by 0", warning.message)
						}
						rsVec := tcc.GetResultVectorDirectly()
						require.Equal(t, resultType, *rsVec.GetType())
						require.False(t, rsVec.GetNulls().Contains(2), "must not retain a NULL outside length")
						require.True(t, rsVec.GetNulls().Contains(0))
						require.True(t, rsVec.GetNulls().Contains(1))
					})
				}
			}
		}
	}
}

func TestIntegerSlashResolverMatrix(t *testing.T) {
	for _, typ := range []types.Type{types.T_int64.ToType(), types.T_uint64.ToType()} {
		t.Run(typ.Oid.String(), func(t *testing.T) {
			got, err := GetFunctionByName(context.Background(), "/", []types.Type{typ, typ})
			require.NoError(t, err)
			targets, needCast := got.ShouldDoImplicitTypeCast()
			require.True(t, needCast)
			require.Len(t, targets, 2)
			require.Equal(t, got.GetReturnType(), resolvedReturnType(t, DIV, targets))
		})
	}
}
