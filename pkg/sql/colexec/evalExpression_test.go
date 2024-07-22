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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestFixedExpressionExecutor(t *testing.T) {
	proc := testutil.NewProcess()

	// Expr_C
	con := makePlan2Int64ConstExprWithType(218311)
	conExprExecutor, err := NewExpressionExecutor(proc, con)
	require.NoError(t, err)

	emptyBatch := &batch.Batch{}
	emptyBatch.SetRowCount(10)
	vec, err := conExprExecutor.Eval(proc, []*batch.Batch{emptyBatch}, nil)
	require.NoError(t, err)
	curr1 := proc.Mp().CurrNB()
	{
		require.Equal(t, 10, vec.Length())
		require.Equal(t, types.T_int64.ToType(), *vec.GetType())
		require.Equal(t, int64(218311), vector.MustFixedCol[int64](vec)[0])
		require.Equal(t, false, vec.GetNulls().Contains(0))
	}
	_, err = conExprExecutor.Eval(proc, []*batch.Batch{emptyBatch}, nil)
	require.NoError(t, err)
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
	{
		require.Equal(t, 5, vec.Length())
		require.Equal(t, types.T_decimal128, vec.GetType().Oid)
		require.Equal(t, int32(30), vec.GetType().Width)
		require.Equal(t, int32(6), vec.GetType().Scale)
	}
	typExpressionExecutor.Free()
	require.Equal(t, curr2, proc.Mp().CurrNB())
}

func TestColumnExpressionExecutor(t *testing.T) {
	proc := testutil.NewProcess()

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

	bat := testutil.NewBatch(
		[]types.Type{types.T_int8.ToType(), types.T_int16.ToType(), types.T_int32.ToType(), types.T_int64.ToType()},
		true, 10, proc.Mp())
	curr := proc.Mp().CurrNB()
	vec, err := colExprExecutor.Eval(proc, []*batch.Batch{bat}, nil)
	require.NoError(t, err)
	{
		require.Equal(t, types.T_int32.ToType(), *vec.GetType())
		require.Equal(t, 10, vec.Length())
	}
	colExprExecutor.Free() // cannot free the vec of batch
	require.Equal(t, curr, proc.Mp().CurrNB())
}

func TestFunctionExpressionExecutor(t *testing.T) {
	{
		proc := testutil.NewProcess()

		bat := testutil.NewBatchWithVectors(
			[]*vector.Vector{
				testutil.NewVector(2, types.T_int64.ToType(), proc.Mp(), false, []int64{1, 2}),
			}, make([]int64, 2))

		currStart := proc.Mp().CurrNB()
		fExprExecutor := &FunctionExpressionExecutor{}
		err := fExprExecutor.Init(proc, 2, types.T_int64.ToType(),
			func(params []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *function.FunctionSelectList) error {
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
			}, nil)
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

		vec, err := fExprExecutor.Eval(proc, []*batch.Batch{bat}, nil)
		require.NoError(t, err)
		curr3 := proc.Mp().CurrNB()
		{
			require.Equal(t, 2, vec.Length())
			require.Equal(t, types.T_int64.ToType(), *vec.GetType())
			require.Equal(t, int64(101), vector.MustFixedCol[int64](vec)[0]) // 1+100
			require.Equal(t, int64(102), vector.MustFixedCol[int64](vec)[1]) // 2+100
		}
		_, err = fExprExecutor.Eval(proc, []*batch.Batch{bat}, nil)
		require.NoError(t, err)
		require.Equal(t, curr3, proc.Mp().CurrNB())
		fExprExecutor.Free()
		proc.FreeVectors()
		require.Equal(t, currStart, proc.Mp().CurrNB())
	}

	// test memory leak if constant fold happens
	{
		proc := testutil.NewProcess()

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
		_, ok := executor.(*FixedVectorExpressionExecutor)
		require.Equal(t, true, ok)
		executor.Free()
		proc.FreeVectors()
		require.Equal(t, currNb, proc.Mp().CurrNB())
	}
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
