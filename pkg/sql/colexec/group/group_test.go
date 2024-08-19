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

package group

import (
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"

	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"

	"github.com/matrixorigin/matrixone/pkg/sql/colexec/value_scan"
	"github.com/matrixorigin/matrixone/pkg/vm"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestGroupOperatorBehavior1(t *testing.T) {
	// This test is used to verify the behavior of the Group operator when pure agg.
	//    for the case of pure aggregation,
	//    the input data is a list of batches of vectors,
	//    and the output data is [agg result columns] or []+agg struct if no need to DoEval.
	//    return NULL if the input data is empty.
	{
		// Test Case 1: test the sum(1,2,3,4,5,6,7,8,9,10) = 55
		proc := testutil.NewProcess()

		OperatorArgument := &Group{
			NeedEval: true,
			Exprs:    nil,
			Aggs: []aggexec.AggFuncExecExpression{
				aggexec.MakeAggFunctionExpression(
					function.AggSumOverloadID, false,
					[]*plan.Expr{newColumnExpression(0, types.T_int64.ToType())}, nil),
			},
		}

		// (2 * [prepare + call + reset]) + free : result should be correct and memory should be released.
		for k := 2; k > 0; k-- {
			inputs := []*batch.Batch{
				{
					Cnt: 1,
					Vecs: []*vector.Vector{
						testutil.NewInt64Vector(10, types.T_int64.ToType(), proc.Mp(), false, []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
					},
				},
				nil,
			}
			inputs[0].SetRowCount(10)
			resetChildren(OperatorArgument, inputs)

			require.NoError(t, OperatorArgument.Prepare(proc))

			var outputBatch *batch.Batch
			outputTime := 0

			for {
				result, err := OperatorArgument.Call(proc)
				require.NoError(t, err)

				if result.Status == vm.ExecStop || result.Batch == nil {
					break
				}
				outputTime++
				outputBatch = result.Batch
			}
			require.True(t, outputTime == 1, "group operator should have and only 1 result out.")

			{
				// check the output batch
				if outputBatch != nil {
					require.Equal(t, 1, len(outputBatch.Vecs))
					require.Equal(t, 0, len(outputBatch.Aggs))
					require.Equal(t, int64(55), vector.MustFixedCol[int64](outputBatch.Vecs[0])[0])
				} else {
					require.Fail(t, "output batch should not be nil.")
				}
			}
			OperatorArgument.Reset(proc, false, nil)
			OperatorArgument.GetChildren(0).Free(proc, false, nil)
		}

		OperatorArgument.Free(proc, false, nil)
		OperatorArgument.GetChildren(0).Free(proc, false, nil)
		proc.Free()
		require.Equal(t, int64(0), proc.Mp().CurrNB())
	}

	{
		// Test Case 2: test the sum first 5 item of (1,2,3,4,5,6,7,8,9,10) = aggResult(15)
		proc := testutil.NewProcess()

		OperatorArgument := &Group{
			NeedEval: false,
			Exprs:    nil,
			Aggs: []aggexec.AggFuncExecExpression{
				aggexec.MakeAggFunctionExpression(
					function.AggSumOverloadID, false,
					[]*plan.Expr{newColumnExpression(0, types.T_int64.ToType())}, nil),
			},
		}

		for k := 2; k > 0; k-- {
			inputs := []*batch.Batch{
				{
					Cnt: 1,
					Vecs: []*vector.Vector{
						testutil.NewInt64Vector(10, types.T_int64.ToType(), proc.Mp(), false, []int64{1, 2, 3, 4, 5}),
					},
				},
				nil,
			}
			inputs[0].SetRowCount(5)
			resetChildren(OperatorArgument, inputs)

			require.NoError(t, OperatorArgument.Prepare(proc))

			var outputBatch *batch.Batch
			outputTime := 0

			for {
				result, err := OperatorArgument.Call(proc)
				require.NoError(t, err)

				if result.Status == vm.ExecStop || result.Batch == nil {
					break
				}
				outputTime++
				outputBatch = result.Batch
			}
			require.True(t, outputTime == 1, "group operator should have and only 1 result out.")

			{
				// check the output batch
				if outputBatch != nil {
					require.Equal(t, 0, len(outputBatch.Vecs))
					require.Equal(t, 1, len(outputBatch.Aggs))
					// just do a flush to check the result.
					aggResult, err := outputBatch.Aggs[0].Flush()
					require.NoError(t, err)
					require.Equal(t, int64(15), vector.MustFixedCol[int64](aggResult)[0])
					aggResult.Free(proc.Mp())
				} else {
					require.Fail(t, "output batch should not be nil.")
				}
			}

			OperatorArgument.Reset(proc, false, nil)
			OperatorArgument.GetChildren(0).Free(proc, false, nil)
		}

		OperatorArgument.Free(proc, false, nil)
		OperatorArgument.GetChildren(0).Free(proc, false, nil)
		proc.Free()
		require.Equal(t, int64(0), proc.Mp().CurrNB())
	}
}

func TestGroupOperatorBehavior2(t *testing.T) {
	// This test is used to verify the behavior of the Group operator when group by and agg.
	//    for the case of group by aggregation,
	//    the input data is a list of batches of vectors,
	//    and the output data is [group by columns, agg result columns] or [group by columns]+agg struct if no need to DoEval.
	//    return Nothing if the input data is empty.

	{
		// Test Case 1: test the sum(b) group by a, where [a,b] = {[1,1], [2, 3], [1,2]}
		proc := testutil.NewProcess()

		OperatorArgument := &Group{
			NeedEval: true,
			Exprs: []*plan.Expr{newColumnExpression(0, types.T_int64.ToType())},
			Aggs: []aggexec.AggFuncExecExpression{
				aggexec.MakeAggFunctionExpression(
					function.AggSumOverloadID, false,
					[]*plan.Expr{newColumnExpression(1, types.T_int64.ToType())}, nil),
			},
		}

		for k := 2; k > 0; k-- {
			inputs := []*batch.Batch{
				{
					Cnt: 1,
					Vecs: []*vector.Vector{
						testutil.NewInt64Vector(3, types.T_int64.ToType(), proc.Mp(), false, []int64{1, 2, 1}),
						testutil.NewInt64Vector(3, types.T_int64.ToType(), proc.Mp(), false, []int64{1, 3, 2}),
					},
				},
				nil,
			}
			inputs[0].SetRowCount(3)
			resetChildren(OperatorArgument, inputs)

			require.NoError(t, OperatorArgument.Prepare(proc))

			var outputBatch *batch.Batch
			outputTime := 0

			for {
				result, err := OperatorArgument.Call(proc)
				require.NoError(t, err)

				if result.Status == vm.ExecStop || result.Batch == nil {
					break
				}
				outputTime++
				outputBatch = result.Batch
			}
			require.True(t, outputTime == 1, "group operator should have and only 1 result out.")

			{
				// check the output batch
				if outputBatch != nil {
					require.Equal(t, 2, len(outputBatch.Vecs))
					require.Equal(t, 0, len(outputBatch.Aggs))

					vs0 := vector.MustFixedCol[int64](outputBatch.Vecs[0])
					vs1 := vector.MustFixedCol[int64](outputBatch.Vecs[1])
					require.Equal(t, int64(1), vs0[0])
					require.Equal(t, int64(1+2), vs1[0])
					require.Equal(t, int64(2), vs0[1])
					require.Equal(t, int64(3), vs1[1])

				} else {
					require.Fail(t, "output batch should not be nil.")
				}
			}

			OperatorArgument.Reset(proc, false, nil)
			OperatorArgument.GetChildren(0).Free(proc, false, nil)
		}

		OperatorArgument.Free(proc, false, nil)
		OperatorArgument.GetChildren(0).Free(proc, false, nil)
		proc.Free()
		require.Equal(t, int64(0), proc.Mp().CurrNB())
	}

	{
		// Test Case 2: test the sum(b) group by a, where [a,b] = first 2 rows of {[1,1], [2, 3], [1,2]}.
		proc := testutil.NewProcess()

		OperatorArgument := &Group{
			NeedEval: false,
			Exprs: []*plan.Expr{newColumnExpression(0, types.T_int64.ToType())},
			Aggs: []aggexec.AggFuncExecExpression{
				aggexec.MakeAggFunctionExpression(
					function.AggSumOverloadID, false,
					[]*plan.Expr{newColumnExpression(1, types.T_int64.ToType())}, nil),
			},
		}

		for k := 2; k > 0; k-- {
			inputs := []*batch.Batch{
				{
					Cnt: 1,
					Vecs: []*vector.Vector{
						testutil.NewInt64Vector(2, types.T_int64.ToType(), proc.Mp(), false, []int64{1, 2}),
						testutil.NewInt64Vector(2, types.T_int64.ToType(), proc.Mp(), false, []int64{1, 3}),
					},
				},
				nil,
			}
			inputs[0].SetRowCount(2)
			resetChildren(OperatorArgument, inputs)

			require.NoError(t, OperatorArgument.Prepare(proc))

			var outputBatch *batch.Batch
			outputTime := 0

			for {
				result, err := OperatorArgument.Call(proc)
				require.NoError(t, err)

				if result.Status == vm.ExecStop || result.Batch == nil {
					break
				}
				outputTime++
				outputBatch = result.Batch
			}
			require.True(t, outputTime == 1, "group operator should have and only 1 result out.")

			{
				// check the output batch
				if outputBatch != nil {
					require.Equal(t, 1, len(outputBatch.Vecs))
					require.Equal(t, 1, len(outputBatch.Aggs))

					vs0 := vector.MustFixedCol[int64](outputBatch.Vecs[0])
					// do a flush to check the result.
					aggResult, err := outputBatch.Aggs[0].Flush()
					require.NoError(t, err)
					vs1 := vector.MustFixedCol[int64](aggResult)
					require.Equal(t, int64(1), vs0[0])
					require.Equal(t, int64(1), vs1[0])
					require.Equal(t, int64(2), vs0[1])
					require.Equal(t, int64(3), vs1[1])
					aggResult.Free(proc.Mp())

				} else {
					require.Fail(t, "output batch should not be nil.")
				}
			}

			OperatorArgument.Reset(proc, false, nil)
			OperatorArgument.GetChildren(0).Free(proc, false, nil)
		}

		OperatorArgument.Free(proc, false, nil)
		OperatorArgument.GetChildren(0).Free(proc, false, nil)
		proc.Free()
		require.Equal(t, int64(0), proc.Mp().CurrNB())
	}
}

func newColumnExpression(pos int32, typ types.Type) *plan.Expr {
	return &plan.Expr{
		Typ: plan.Type{Id: int32(typ.Oid)},
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: 0,
				ColPos: pos,
			},
		},
	}
}

func resetChildren(arg *Group, bats []*batch.Batch) {
	valueScanArg := &value_scan.ValueScan{
		Batchs: bats,
	}
	_ = valueScanArg.Prepare(nil)
	if arg.NumChildren() == 0 {
		arg.AppendChild(valueScanArg)

	} else {
		arg.SetChildren(
			[]vm.Operator{
				valueScanArg,
			})
	}
	arg.ctr.state = vm.Build
}
