// Copyright 2021 Matrix Origin
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

package colexec

import (
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestListExpressionExecutor(t *testing.T) {
	proc := testutil.NewProcess(t)

	bat := testutil.NewBatch(
		[]types.Type{types.T_int64.ToType()},
		true, 10, proc.Mp())

	// build plan_list
	exprList := []*plan.Expr{
		makePlan2Int64ConstExprWithType(1),
		makePlan2Int64ConstExprWithType(2),
	}

	evalExpr := &plan.Expr{
		Expr: &plan.Expr_List{
			List: &plan.ExprList{
				List: exprList,
			},
		},
		Typ: plan.Type{
			Id:          int32(types.T_int64),
			NotNullable: true,
		},
	}
	curr := proc.Mp().CurrNB()

	listExprExecutor, err := NewExpressionExecutor(proc, evalExpr)
	require.NoError(t, err)
	tree, err := DebugShowExecutor(listExprExecutor)
	require.NoError(t, err)
	t.Log(tree)
	require.NoError(t, err)
	require.Equal(t, listExprExecutor.IsColumnExpr(), false)

	vec, err := listExprExecutor.Eval(proc, []*batch.Batch{bat}, nil)
	require.NoError(t, err)
	vals := vector.MustFixedColNoTypeCheck[int64](vec)
	require.Equal(t, int64(1), vals[0])
	require.Equal(t, int64(2), vals[1])

	listExprExecutor.ResetForNextQuery()

	vec, err = listExprExecutor.Eval(proc, []*batch.Batch{bat}, nil)
	require.NoError(t, err)
	tree, err = DebugShowExecutor(listExprExecutor)
	require.NoError(t, err)
	t.Log(tree)
	vals = vector.MustFixedColNoTypeCheck[int64](vec)
	require.Equal(t, int64(1), vals[0])
	require.Equal(t, int64(2), vals[1])

	vec, err = listExprExecutor.EvalWithoutResultReusing(proc, []*batch.Batch{bat}, nil)
	require.NoError(t, err)
	vals = vector.MustFixedColNoTypeCheck[int64](vec)
	require.Equal(t, int64(1), vals[0])
	require.Equal(t, int64(2), vals[1])
	vec.Free(proc.GetMPool())

	listExprExecutor.Free()

	require.Equal(t, curr, proc.Mp().CurrNB())
}

func TestParamExpressionExecutorPreservesBinaryFlagPerParameter(t *testing.T) {
	proc := testutil.NewProcess(t)
	params := vector.NewVec(types.T_text.ToType())
	require.NoError(t, vector.AppendBytes(params, []byte("AB\x00\x00"), false, proc.Mp()))
	require.NoError(t, vector.AppendBytes(params, []byte("text"), false, proc.Mp()))
	proc.SetPrepareParamsWithIsBin(params, []bool{true, false})
	t.Cleanup(func() { params.Free(proc.Mp()) })

	binaryExpr := NewParamExpressionExecutor(proc.Mp(), 0, types.T_text.ToType())
	textExpr := NewParamExpressionExecutor(proc.Mp(), 1, types.T_text.ToType())
	t.Cleanup(binaryExpr.Free)
	t.Cleanup(textExpr.Free)

	binaryVec, err := binaryExpr.Eval(proc, nil, nil)
	require.NoError(t, err)
	require.True(t, binaryVec.GetIsBin())
	require.Equal(t, "AB\x00\x00", binaryVec.GetStringAt(0))

	textVec, err := textExpr.Eval(proc, nil, nil)
	require.NoError(t, err)
	require.False(t, textVec.GetIsBin())
	require.Equal(t, "text", textVec.GetStringAt(0))
}

func TestFixedExpressionExecutor(t *testing.T) {
	proc := testutil.NewProcess(t)

	// Expr_C
	con := makePlan2Int64ConstExprWithType(218311)
	conExprExecutor, err := NewExpressionExecutor(proc, con)
	require.NoError(t, err)
	tree, err := DebugShowExecutor(conExprExecutor)
	require.NoError(t, err)
	t.Log(tree)

	emptyBatch := &batch.Batch{}
	emptyBatch.SetRowCount(10)
	vec, err := conExprExecutor.Eval(proc, []*batch.Batch{emptyBatch}, nil)
	require.NoError(t, err)
	curr1 := proc.Mp().CurrNB()
	{
		require.Equal(t, 10, vec.Length())
		require.Equal(t, types.T_int64.ToType(), *vec.GetType())
		require.Equal(t, int64(218311), vector.MustFixedColWithTypeCheck[int64](vec)[0])
		require.Equal(t, false, vec.GetNulls().Contains(0))
	}
	_, err = conExprExecutor.Eval(proc, []*batch.Batch{emptyBatch}, nil)
	require.NoError(t, err)
	tree, err = DebugShowExecutor(conExprExecutor)
	require.NoError(t, err)
	t.Log(tree)
	require.Equal(t, curr1, proc.Mp().CurrNB()) // check memory reuse
	conExprExecutor.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())

	// Expr_T
	ety := &plan.Expr{
		Expr: &plan.Expr_T{T: &plan.TargetType{}},
		Typ: plan.Type{
			Id:          int32(types.T_decimal128),
			Width:       30,
			Scale:       6,
			NotNullable: true,
		},
	}
	curr2 := proc.Mp().CurrNB()
	typExpressionExecutor, err := NewExpressionExecutor(proc, ety)
	require.NoError(t, err)

	emptyBatch.SetRowCount(5)
	vec, err = typExpressionExecutor.Eval(proc, []*batch.Batch{emptyBatch}, nil)
	require.NoError(t, err)
	tree, err = DebugShowExecutor(typExpressionExecutor)
	require.NoError(t, err)
	t.Log(tree)
	{
		require.Equal(t, 5, vec.Length())
		require.Equal(t, types.T_decimal128, vec.GetType().Oid)
		require.Equal(t, int32(30), vec.GetType().Width)
		require.Equal(t, int32(6), vec.GetType().Scale)
	}
	typExpressionExecutor.Free()
	require.Equal(t, curr2, proc.Mp().CurrNB())
}

func TestGeometryLiteralExpressionExecutor(t *testing.T) {
	proc := testutil.NewProcess(t)

	expr := &plan.Expr{
		Typ: plan.Type{
			Id:          int32(types.T_geometry),
			NotNullable: true,
		},
		Expr: &plan.Expr_Lit{
			Lit: &plan.Literal{
				Isnull: false,
				Value: &plan.Literal_Sval{
					Sval: "POINT(1 1)",
				},
			},
		},
	}

	executor, err := NewExpressionExecutor(proc, expr)
	require.NoError(t, err)
	defer executor.Free()

	emptyBatch := &batch.Batch{}
	emptyBatch.SetRowCount(3)
	vec, err := executor.Eval(proc, []*batch.Batch{emptyBatch}, nil)
	require.NoError(t, err)
	require.Equal(t, types.T_geometry, vec.GetType().Oid)
	require.Equal(t, 3, vec.Length())
	require.Equal(t, "POINT(1 1)", vec.GetStringAt(0))
}

func TestVarExpressionExecutor(t *testing.T) {
	proc := testutil.NewProcess(t)

	// Create a variable expression
	varExpr := &plan.Expr{
		Expr: &plan.Expr_V{
			V: &plan.VarRef{
				Name:   "test_var",
				System: false,
				Global: false,
			},
		},
		Typ: plan.Type{
			Id:          int32(types.T_int64),
			NotNullable: true,
		},
	}

	// Mock the variable resolution function
	proc.SetResolveVariableFunc(func(name string, system, global bool) (interface{}, error) {
		if name == "test_var" {
			return int64(12345), nil
		}
		return nil, moerr.NewInternalErrorNoCtx("variable not found")
	})

	varExprExecutor, err := NewExpressionExecutor(proc, varExpr)
	require.NoError(t, err)
	tree, err := DebugShowExecutor(varExprExecutor)
	require.NoError(t, err)
	t.Log(tree)

	// after vector.SetConstBytes pass go test -v, can comment out below line
	// vec, err := varExprExecutor.Eval(proc, []*batch.Batch{nil}, nil)
	// require.NoError(t, err)
	// curr := proc.Mp().CurrNB()
	// {
	// 	require.Equal(t, 1, vec.Length())
	// 	require.Equal(t, types.T_int64.ToType(), *vec.GetType())
	// 	val := string(vec.GetBytesAt(0))
	// 	result, err := strconv.ParseInt(val, 10, 64)
	// 	require.NoError(t, err)
	// 	require.Equal(t, int64(12345), result)
	// 	require.Equal(t, false, vec.GetNulls().Contains(0))
	// }

	// varExprExecutor.ResetForNextQuery()
	// _, err = varExprExecutor.Eval(proc, []*batch.Batch{nil}, nil)
	// require.NoError(t, err)
	// tree, err = DebugShowExecutor(varExprExecutor)
	// require.NoError(t, err)
	// t.Log(tree)
	// require.Equal(t, curr, proc.Mp().CurrNB()) // check memory reuse
	// varExprExecutor.Free()
	// require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestVarExpressionExecutorPreservesBinaryFlagOnReuse(t *testing.T) {
	proc := testutil.NewProcess(t)
	value := "AB\x00\x00"
	isBin := true
	proc.SetResolveVariableFunc(func(string, bool, bool) (interface{}, error) {
		return value, nil
	})
	proc.SetResolveVariableIsBinFunc(func(string, bool, bool) (bool, error) {
		return isBin, nil
	})
	expr := &plan.Expr{
		Expr: &plan.Expr_V{V: &plan.VarRef{Name: "copied_var"}},
		Typ:  plan.Type{Id: int32(types.T_text)},
	}
	executor, err := NewExpressionExecutor(proc, expr)
	require.NoError(t, err)
	t.Cleanup(executor.Free)

	vec, err := executor.Eval(proc, nil, nil)
	require.NoError(t, err)
	require.True(t, vec.GetIsBin())
	require.Equal(t, "AB\x00\x00", vec.GetStringAt(0))

	value, isBin = "text", false
	vec, err = executor.Eval(proc, nil, nil)
	require.NoError(t, err)
	require.False(t, vec.GetIsBin())
	require.Equal(t, "text", vec.GetStringAt(0))

	value, isBin = "CD\x00\x00", true
	vec, err = executor.Eval(proc, nil, nil)
	require.NoError(t, err)
	require.True(t, vec.GetIsBin())
	require.Equal(t, "CD\x00\x00", vec.GetStringAt(0))
}

func TestVarExpressionExecutorWithoutResolveVariableFunc(t *testing.T) {
	proc := testutil.NewProcess(t)
	varExpr := &plan.Expr{
		Expr: &plan.Expr_V{
			V: &plan.VarRef{
				Name:   "test_var",
				System: true,
			},
		},
		Typ: plan.Type{
			Id: int32(types.T_text),
		},
	}

	varExprExecutor, err := NewExpressionExecutor(proc, varExpr)
	require.NoError(t, err)
	_, err = varExprExecutor.Eval(proc, nil, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "resolve variable function is not set")
}

func TestColumnExpressionExecutor(t *testing.T) {
	proc := testutil.NewProcess(t)

	col := &plan.Expr{
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: 0,
				ColPos: 2,
			},
		},
		Typ: plan.Type{
			Id:          int32(types.T_int32),
			NotNullable: true,
		},
	}
	colExprExecutor, err := NewExpressionExecutor(proc, col)
	require.NoError(t, err)
	tree, err := DebugShowExecutor(colExprExecutor)
	require.NoError(t, err)
	t.Log(tree)

	bat := testutil.NewBatch(
		[]types.Type{types.T_int8.ToType(), types.T_int16.ToType(), types.T_int32.ToType(), types.T_int64.ToType()},
		true, 10, proc.Mp())
	curr := proc.Mp().CurrNB()
	vec, err := colExprExecutor.Eval(proc, []*batch.Batch{bat}, nil)
	require.NoError(t, err)
	tree, err = DebugShowExecutor(colExprExecutor)
	require.NoError(t, err)
	t.Log(tree)
	{
		require.Equal(t, types.T_int32.ToType(), *vec.GetType())
		require.Equal(t, 10, vec.Length())
	}
	colExprExecutor.Free() // cannot free the vec of batch
	require.Equal(t, curr, proc.Mp().CurrNB())
}

// TestColumnExpressionExecutor_RelIndexOutOfRange verifies that Eval returns
// an error instead of panicking when relIndex >= len(batches).
// This reproduces the crash seen when IVF-Flat entries table contains NULL
// vectors and L2_DISTANCE + ORDER BY LIMIT triggers the Top operator.
func TestColumnExpressionExecutor_RelIndexOutOfRange(t *testing.T) {
	proc := testutil.NewProcess(t)

	// relIndex=2 but we will only pass 2 batches (valid indices: 0, 1).
	col := &plan.Expr{
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: 2,
				ColPos: 0,
			},
		},
		Typ: plan.Type{
			Id:          int32(types.T_int32),
			NotNullable: true,
		},
	}
	executor, err := NewExpressionExecutor(proc, col)
	require.NoError(t, err)
	defer executor.Free()

	bat := testutil.NewBatch(
		[]types.Type{types.T_int32.ToType()},
		true, 5, proc.Mp())

	// Two batches → relIndex 2 is out of range.
	_, err = executor.Eval(proc, []*batch.Batch{bat, bat}, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "relIndex 2 out of range")

	// Single batch → the existing len==1 hack forces relIndex to 0, should succeed.
	vec, err := executor.Eval(proc, []*batch.Batch{bat}, nil)
	require.NoError(t, err)
	require.Equal(t, 5, vec.Length())
}

func TestFunctionExpressionExecutor(t *testing.T) {
	{
		proc := testutil.NewProcess(t)

		bat := testutil.NewBatchWithVectors(
			[]*vector.Vector{
				testutil.NewVector(2, types.T_int64.ToType(), proc.Mp(), false, []int64{1, 2}),
			}, make([]int64, 2))

		currStart := proc.Mp().CurrNB()
		fExprExecutor := &FunctionExpressionExecutor{}
		err := fExprExecutor.Init(proc, 2, types.T_int64.ToType())
		fExprExecutor.evalFn = func(params []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *function.FunctionSelectList) error {
			v1 := vector.GenerateFunctionFixedTypeParameter[int64](params[0])
			v2 := vector.GenerateFunctionFixedTypeParameter[int64](params[1])
			rs := vector.MustFunctionResult[int64](result)
			for i := 0; i < length; i++ {
				v11, null11 := v1.GetValue(uint64(i))
				v22, null22 := v2.GetValue(uint64(i))
				if null11 || null22 {
					err := rs.Append(0, true)
					if err != nil {
						return err
					}
				} else {
					err := rs.Append(v11+v22, false)
					if err != nil {
						return err
					}
				}
			}
			return nil
		}
		fExprExecutor.freeFn = nil
		require.NoError(t, err)

		col1 := &plan.Expr{
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: 0,
					ColPos: 0,
				},
			},
			Typ: plan.Type{
				Id:          int32(types.T_int64),
				NotNullable: true,
			},
		}
		col2 := makePlan2Int64ConstExprWithType(100)
		executor1, err := NewExpressionExecutor(proc, col1)
		require.NoError(t, err)
		executor2, err := NewExpressionExecutor(proc, col2)
		require.NoError(t, err)
		fExprExecutor.SetParameter(0, executor1)
		fExprExecutor.SetParameter(1, executor2)

		tree, err := DebugShowExecutor(fExprExecutor)
		require.NoError(t, err)
		t.Log(tree)

		vec, err := fExprExecutor.Eval(proc, []*batch.Batch{bat}, nil)
		require.NoError(t, err)
		tree, err = DebugShowExecutor(fExprExecutor)
		require.NoError(t, err)
		t.Log(tree)

		curr3 := proc.Mp().CurrNB()
		{
			require.Equal(t, 2, vec.Length())
			require.Equal(t, types.T_int64.ToType(), *vec.GetType())
			require.Equal(t, int64(101), vector.MustFixedColWithTypeCheck[int64](vec)[0]) // 1+100
			require.Equal(t, int64(102), vector.MustFixedColWithTypeCheck[int64](vec)[1]) // 2+100
		}
		_, err = fExprExecutor.Eval(proc, []*batch.Batch{bat}, nil)
		require.NoError(t, err)
		require.Equal(t, curr3, proc.Mp().CurrNB())
		fExprExecutor.Free()
		proc.Free()
		require.Equal(t, currStart, proc.Mp().CurrNB())
	}

	// test memory leak if constant fold happens
	{
		proc := testutil.NewProcess(t)

		col1 := makePlan2BoolConstExprWithType(true)
		col2 := makePlan2BoolConstExprWithType(true)
		fExpr := &plan.Expr{
			Typ: plan.Type{
				Id:          int32(types.T_bool),
				NotNullable: true,
			},
			Expr: &plan.Expr_F{
				F: &plan.Function{
					Func: &plan.ObjectRef{
						ObjName: function.AndFunctionName,
						Obj:     function.AndFunctionEncodedID,
					},
					Args: []*plan.Expr{col1, col2},
				},
			},
		}
		currNb := proc.Mp().CurrNB()
		executor, err := NewExpressionExecutor(proc, fExpr)
		require.NoError(t, err)
		result, err := executor.Eval(proc, nil, nil)
		require.NoError(t, err)
		require.Equal(t, true, result != nil && result.IsConst())
		executor.Free()
		proc.Free()
		require.Equal(t, currNb, proc.Mp().CurrNB())
	}
}

func TestFunctionExpressionExecutorShrinkingSelectList(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	floatType := types.T_float64.ToType()
	div, err := function.GetFunctionByName(proc.Ctx, "/", []types.Type{floatType, floatType})
	require.NoError(t, err)
	resultType := div.GetReturnType()

	column := &plan.Expr{
		Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 0, ColPos: 0}},
		Typ:  plan.Type{Id: int32(types.T_float64), NotNullable: true},
	}
	constant := &plan.Expr{
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{
			Value: &plan.Literal_Dval{Dval: 2},
		}},
		Typ: plan.Type{Id: int32(types.T_float64), NotNullable: true},
	}
	expr := &plan.Expr{
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{ObjName: "/", Obj: div.GetEncodedOverloadID()},
			Args: []*plan.Expr{column, constant},
		}},
		Typ: plan.Type{Id: int32(resultType.Oid), Width: resultType.Width, Scale: resultType.Scale},
	}

	executor, err := NewExpressionExecutor(proc, expr)
	require.NoError(t, err)
	defer executor.Free()

	largeBatch := testutil.NewBatchWithVectors(
		[]*vector.Vector{testutil.NewVector(3, floatType, proc.Mp(), false, []float64{5, 5, 5})},
		make([]int64, 3))
	defer largeBatch.Clean(proc.Mp())
	_, err = executor.Eval(proc, []*batch.Batch{largeBatch}, []bool{true, true, false})
	require.NoError(t, err)

	smallBatch := testutil.NewBatchWithVectors(
		[]*vector.Vector{testutil.NewVector(2, floatType, proc.Mp(), false, []float64{5, 5})},
		make([]int64, 2))
	defer smallBatch.Clean(proc.Mp())
	result, err := executor.Eval(proc, []*batch.Batch{smallBatch}, []bool{true, false})
	require.NoError(t, err)
	require.Equal(t, 2, result.Length())
	require.Equal(t, float64(2.5), vector.MustFixedColWithTypeCheck[float64](result)[0])
	require.False(t, result.GetNulls().Contains(0))
	require.True(t, result.GetNulls().Contains(1))
}

func TestExpressionReset(t *testing.T) {
	proc := testutil.NewProcess(t)

	// functions will be folded.
	{
		col1 := makePlan2BoolConstExprWithType(true)
		col2 := makePlan2BoolConstExprWithType(true)
		fExpr := &plan.Expr{
			Typ: plan.Type{
				Id:          int32(types.T_bool),
				NotNullable: true,
			},
			Expr: &plan.Expr_F{
				F: &plan.Function{
					Func: &plan.ObjectRef{
						ObjName: function.AndFunctionName,
						Obj:     function.AndFunctionEncodedID,
					},
					Args: []*plan.Expr{col1, col2},
				},
			},
		}

		originNb := proc.Mp().CurrNB()

		executor, err := NewExpressionExecutor(proc, fExpr)
		require.NoError(t, err)

		tree, err := DebugShowExecutor(executor)
		require.NoError(t, err)
		t.Log(tree)

		result, err := executor.Eval(proc, nil, nil)
		require.NoError(t, err)
		require.Equal(t, true, result != nil && result.IsConst() && result.Length() == 1)

		tree, err = DebugShowExecutor(executor)
		require.NoError(t, err)
		t.Log(tree)

		inputs := []*batch.Batch{
			batch.New(nil),
		}
		inputs[0].SetRowCount(100)
		result, err = executor.Eval(proc, inputs, nil)
		require.NoError(t, err)
		require.Equal(t, true, result != nil && result.IsConst() && result.Length() == 100)

		executor.ResetForNextQuery()

		result, err = executor.Eval(proc, nil, nil)
		require.NoError(t, err)
		require.Equal(t, true, result != nil && result.IsConst() && result.Length() == 1)

		executor.Free()
		proc.Free()
		require.Equal(t, originNb, proc.Mp().CurrNB())
	}
}

func TestJsonOrderingWithTextPrepareParamExact(t *testing.T) {
	tests := []struct {
		name       string
		op         string
		jsonOnLeft bool
		jsonValue  string
		paramValue string
		paramNull  bool
		want       bool
		wantNull   bool
		wantErr    bool
	}{
		{name: "adjacent integers json left", op: "<", jsonOnLeft: true, jsonValue: "9007199254740992", paramValue: "9007199254740993", want: true},
		{name: "adjacent integers json right", op: ">", jsonOnLeft: false, jsonValue: "9007199254740992", paramValue: "9007199254740993", want: true},
		{name: "maximum uint64", op: "<", jsonOnLeft: true, jsonValue: "18446744073709551614", paramValue: "18446744073709551615", want: true},
		{name: "precise decimals", op: "<", jsonOnLeft: true, jsonValue: "0.123456789123456788", paramValue: "0.123456789123456789", want: true},
		{name: "null parameter", op: "<", jsonOnLeft: true, jsonValue: "1", paramNull: true, wantNull: true},
		{name: "invalid string parameter", op: "<", jsonOnLeft: true, jsonValue: "1", paramValue: "not-json", wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			params := vector.NewVec(types.T_text.ToType())
			require.NoError(t, vector.AppendBytes(params, []byte(test.paramValue), test.paramNull, proc.Mp()))
			proc.SetPrepareParams(params)

			jsonType := types.T_json.ToType()
			textType := types.T_text.ToType()
			normalizeFn, err := function.GetFunctionByName(proc.Ctx, function.JsonOrderingParamFunctionName, []types.Type{textType})
			require.NoError(t, err)
			paramExpr := &plan.Expr{
				Typ: plan.Type{Id: int32(types.T_json)},
				Expr: &plan.Expr_F{F: &plan.Function{
					Func: &plan.ObjectRef{ObjName: function.JsonOrderingParamFunctionName, Obj: normalizeFn.GetEncodedOverloadID()},
					Args: []*plan.Expr{
						{Typ: plan.Type{Id: int32(types.T_text)}, Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: 0}}},
					},
				}},
			}
			jsonExpr := &plan.Expr{
				Typ:  plan.Type{Id: int32(types.T_json)},
				Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 0, ColPos: 0}},
			}
			args := []*plan.Expr{jsonExpr, paramExpr}
			if !test.jsonOnLeft {
				args[0], args[1] = args[1], args[0]
			}
			compareFn, err := function.GetFunctionByName(proc.Ctx, test.op, []types.Type{jsonType, jsonType})
			require.NoError(t, err)
			expr := &plan.Expr{
				Typ: plan.Type{Id: int32(types.T_bool)},
				Expr: &plan.Expr_F{F: &plan.Function{
					Func: &plan.ObjectRef{ObjName: test.op, Obj: compareFn.GetEncodedOverloadID()},
					Args: args,
				}},
			}

			json, err := types.ParseStringToByteJson(test.jsonValue)
			require.NoError(t, err)
			encoded, err := types.EncodeJson(json)
			require.NoError(t, err)
			jsonVec := vector.NewVec(jsonType)
			require.NoError(t, vector.AppendBytes(jsonVec, encoded, false, proc.Mp()))
			input := batch.NewWithSize(1)
			input.Vecs[0] = jsonVec
			input.SetRowCount(1)

			executor, err := NewExpressionExecutor(proc, expr)
			require.NoError(t, err)
			result, err := executor.Eval(proc, []*batch.Batch{input}, nil)
			if test.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.wantNull, result.GetNulls().Contains(0))
				if !test.wantNull {
					require.Equal(t, test.want, vector.MustFixedColWithTypeCheck[bool](result)[0])
				}
			}

			executor.Free()
			input.Clean(proc.Mp())
			proc.SetPrepareParams(nil)
			params.Free(proc.Mp())
			proc.Free()
		})
	}
}

func TestFunctionFold(t *testing.T) {
	t.Skip("todo: implement this test")
}

func TestModifyResultOwnerToOuter(t *testing.T) {
	// we cannot modify the column expression's memory owner.
	// because its owner is never own to executor.
	columnExecutor := &ColumnExpressionExecutor{}
	require.False(t, modifyResultOwnerToOuter(columnExecutor))
}

// some util code copied from package `plan`.
func makePlan2Int64ConstExprWithType(v int64) *plan.Expr {
	return &plan.Expr{
		Expr: makePlan2Int64ConstExpr(v),
		Typ: plan.Type{
			Id:          int32(types.T_int64),
			NotNullable: true,
		},
	}
}

func makePlan2Int64ConstExpr(v int64) *plan.Expr_Lit {
	return &plan.Expr_Lit{Lit: &plan.Literal{
		Isnull: false,
		Value: &plan.Literal_I64Val{
			I64Val: v,
		},
	}}
}

func makePlan2BoolConstExprWithType(b bool) *plan.Expr {
	return &plan.Expr{
		Expr: makePlan2BoolConstExpr(b),
		Typ: plan.Type{
			Id:          int32(types.T_bool),
			NotNullable: true,
		},
	}
}

func makePlan2BoolConstExpr(b bool) *plan.Expr_Lit {
	return &plan.Expr_Lit{Lit: &plan.Literal{
		Isnull: false,
		Value: &plan.Literal_Bval{
			Bval: b,
		},
	}}
}

func makePlan2TimestampConstExprWithType(v int64, scale int32) *plan.Expr {
	return &plan.Expr{
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{
			Isnull: false,
			Value: &plan.Literal_Timestampval{
				Timestampval: v,
			},
		}},
		Typ: plan.Type{
			Id:          int32(types.T_timestamp),
			Scale:       scale,
			NotNullable: true,
		},
	}
}

// TestTimestampLiteral_ScaleValidation tests scale validation for TIMESTAMP literals
func TestTimestampLiteral_ScaleValidation(t *testing.T) {
	proc := testutil.NewProcess(t)

	// Test valid scales (0-6)
	for scale := int32(0); scale <= 6; scale++ {
		t.Run(fmt.Sprintf("valid_scale_%d", scale), func(t *testing.T) {
			expr := makePlan2TimestampConstExprWithType(1609459200000000, scale) // 2021-01-01 00:00:00
			executor, err := NewExpressionExecutor(proc, expr)
			require.NoError(t, err)

			emptyBatch := &batch.Batch{}
			emptyBatch.SetRowCount(1)
			vec, err := executor.Eval(proc, []*batch.Batch{emptyBatch}, nil)
			require.NoError(t, err)
			require.Equal(t, 1, vec.Length())
			require.Equal(t, types.T_timestamp, vec.GetType().Oid)
			require.Equal(t, scale, vec.GetType().Scale)

			executor.Free()
		})
	}

	// Test invalid scale: negative
	// Note: Scale validation happens during NewExpressionExecutor creation,
	// not during Eval, because generateConstExpressionExecutor validates scale
	t.Run("invalid_scale_negative", func(t *testing.T) {
		expr := makePlan2TimestampConstExprWithType(1609459200000000, -1)
		executor, err := NewExpressionExecutor(proc, expr)
		require.Error(t, err) // Executor creation should fail due to invalid scale
		require.Nil(t, executor)
		require.Contains(t, err.Error(), "Too-big precision")
		require.Contains(t, err.Error(), "TIMESTAMP")
		require.Contains(t, err.Error(), "Maximum is 6")
	})

	// Test invalid scale: greater than 6
	// Note: Scale validation happens during NewExpressionExecutor creation,
	// not during Eval, because generateConstExpressionExecutor validates scale
	t.Run("invalid_scale_too_large", func(t *testing.T) {
		expr := makePlan2TimestampConstExprWithType(1609459200000000, 7)
		executor, err := NewExpressionExecutor(proc, expr)
		require.Error(t, err) // Executor creation should fail due to invalid scale
		require.Nil(t, executor)
		require.Contains(t, err.Error(), "Too-big precision")
		require.Contains(t, err.Error(), "TIMESTAMP")
		require.Contains(t, err.Error(), "Maximum is 6")
	})
}

func TestGetExprZoneMapConstantFold(t *testing.T) {
	proc := testutil.NewProcess(t)
	ctx := proc.Ctx

	// Build abs(-42): a function in the "default" case with all-constant args.
	// This exercises the constant-fold path and the defer cleanup for ivecs.
	argExpr := &plan.Expr{
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{
			Isnull: false,
			Value:  &plan.Literal_I64Val{I64Val: -42},
		}},
		Typ:   plan.Type{Id: int32(types.T_int64), NotNullable: true},
		AuxId: 0,
	}

	// Resolve the "abs" function for int64
	fGet, err := function.GetFunctionByName(ctx, "abs", []types.Type{types.T_int64.ToType()})
	require.NoError(t, err)
	funcID := fGet.GetEncodedOverloadID()
	retType := fGet.GetReturnType()

	funcExpr := &plan.Expr{
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{
				Obj:     funcID,
				ObjName: "abs",
			},
			Args: []*plan.Expr{argExpr},
		}},
		Typ:   plan.Type{Id: int32(retType.Oid), Width: retType.Width, Scale: retType.Scale},
		AuxId: 1,
	}

	// Allocate ZM and vec arrays (size = max AuxId + 1)
	zms := make([]index.ZM, 2)
	vecs := make([]*vector.Vector, 2)

	zm := GetExprZoneMap(ctx, proc, funcExpr, nil, nil, zms, vecs)
	require.True(t, zm.IsInited(), "result zone map should be initialized")

	// Clean up any vecs allocated during evaluation
	for _, v := range vecs {
		if v != nil {
			v.Free(proc.Mp())
		}
	}
	proc.Free()
}
