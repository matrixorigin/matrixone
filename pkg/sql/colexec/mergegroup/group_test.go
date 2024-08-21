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

package mergegroup

import (
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/value_scan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
	"testing"
)

// generateInputData return an input batch for mergeGroup operator.
//
// the input data for mergeGroup operator is always a batch with Agg field.
// batch's vectors is the group by columns.
// batch's Agg field is the aggregate middle result.
// and we use sum function to do the test.
//
// todo: only support one group by column now.
func generateInputData(proc *process.Process, groupValues []int64, sumResult []int64) *batch.Batch {
	if len(groupValues) == 0 && len(sumResult) != 1 {
		panic("bad input data")
	}

	bat := batch.NewWithSize(0)
	if len(groupValues) > 0 {
		bat.Vecs = make([]*vector.Vector, 1)
		bat.Vecs[0] = testutil.NewInt64Vector(
			len(groupValues), types.T_int64.ToType(), proc.Mp(), false, groupValues)
	}

	bat.Aggs = []aggexec.AggFuncExec{
		aggexec.MakeAgg(proc, function.AggSumOverloadID, false, types.T_int64.ToType()),
	}

	if err := bat.Aggs[0].GroupGrow(len(sumResult)); err != nil {
		panic(err)
	}

	vec := testutil.NewInt64Vector(len(sumResult), types.T_int64.ToType(), proc.Mp(), false, sumResult)
	input := []*vector.Vector{vec}
	for i := 0; i < len(sumResult); i++ {
		groupRow := i
		if len(groupValues) == 0 {
			groupRow = 0
		}

		if err := bat.Aggs[0].Fill(groupRow, i, input); err != nil {
			panic(err)
		}
	}
	vec.Free(proc.Mp())

	bat.SetRowCount(len(sumResult))
	return bat
}

// TestMergeGroupBehavior1 test the merge group operator should Evaluate the result immediately.
func TestMergeGroupBehavior1(t *testing.T) {
	// Test Case 1: merge from [3, sum = 6] and [3, sum = 5]
	// the result should be [3, 11]
	{
		proc := testutil.NewProcess()

		OperatorArgument := &MergeGroup{
			NeedEval: true,
		}
		OperatorArgument.ctr.hashKeyWidth = NeedCalculationForKeyWidth

		for k := 2; k > 0; k-- {
			inputs := []*batch.Batch{
				generateInputData(proc, []int64{3}, []int64{6}),
				generateInputData(proc, []int64{3}, []int64{5}),
				nil,
			}
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
			require.True(t, outputTime == 1, "merge group operator should have and only 1 result out.")

			{
				// check the output batch
				if outputBatch != nil {
					require.Equal(t, 2, len(outputBatch.Vecs))
					require.Equal(t, 0, len(outputBatch.Aggs))
					require.Equal(t, int64(3), vector.MustFixedCol[int64](outputBatch.Vecs[0])[0])
					require.Equal(t, int64(11), vector.MustFixedCol[int64](outputBatch.Vecs[1])[0])
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

	// Test Case 2: merge from [sum = 10] and [sum = 5]
	// the result should be [15]
	{
		proc := testutil.NewProcess()

		OperatorArgument := &MergeGroup{
			NeedEval: true,
		}
		OperatorArgument.ctr.hashKeyWidth = NeedCalculationForKeyWidth

		for k := 2; k > 0; k-- {
			inputs := []*batch.Batch{
				generateInputData(proc, nil, []int64{10}),
				generateInputData(proc, nil, []int64{5}),
				nil,
			}
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
			require.True(t, outputTime == 1, "merge group operator should have and only 1 result out.")

			{
				// check the output batch
				if outputBatch != nil {
					require.Equal(t, 1, len(outputBatch.Vecs))
					require.Equal(t, 0, len(outputBatch.Aggs))
					require.Equal(t, int64(15), vector.MustFixedCol[int64](outputBatch.Vecs[0])[0])
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

// TestMergeGroupBehavior2 test the merge group operator has no need to Evaluate the result immediately,
// but return the middle result.
func TestMergeGroupBehavior2(t *testing.T) {
	// Test Case 1: merge from [3, sum = 6] and [3, sum = 5]
	// the result should be [3, sum = 11]
	{
		proc := testutil.NewProcess()

		OperatorArgument := &MergeGroup{
			NeedEval: false,
		}
		OperatorArgument.ctr.hashKeyWidth = NeedCalculationForKeyWidth

		for k := 2; k > 0; k-- {
			inputs := []*batch.Batch{
				generateInputData(proc, []int64{3}, []int64{6}),
				generateInputData(proc, []int64{3}, []int64{5}),
				nil,
			}
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
			require.True(t, outputTime == 1, "merge group operator should have and only 1 result out.")

			{
				// check the output batch
				if outputBatch != nil {
					require.Equal(t, 1, len(outputBatch.Vecs))
					require.Equal(t, 1, len(outputBatch.Aggs))
					require.Equal(t, int64(3), vector.MustFixedCol[int64](outputBatch.Vecs[0])[0])

					// flush to check the result
					vec, err := outputBatch.Aggs[0].Flush()
					require.NoError(t, err)
					require.Equal(t, int64(11), vector.MustFixedCol[int64](vec)[0])
					vec.Free(proc.Mp())
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

	// Test Case 2: merge from [sum = 10] and [sum = 5]
	// the result should be [sum = 15]
	{
		proc := testutil.NewProcess()

		OperatorArgument := &MergeGroup{
			NeedEval: false,
		}
		OperatorArgument.ctr.hashKeyWidth = NeedCalculationForKeyWidth

		for k := 2; k > 0; k-- {
			inputs := []*batch.Batch{
				generateInputData(proc, nil, []int64{10}),
				generateInputData(proc, nil, []int64{5}),
				nil,
			}
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
			require.True(t, outputTime == 1, "merge group operator should have and only 1 result out.")

			{
				// check the output batch
				if outputBatch != nil {
					require.Equal(t, 0, len(outputBatch.Vecs))
					require.Equal(t, 1, len(outputBatch.Aggs))

					// flush to check the result
					vec, err := outputBatch.Aggs[0].Flush()
					require.NoError(t, err)
					require.Equal(t, int64(15), vector.MustFixedCol[int64](vec)[0])
					vec.Free(proc.Mp())
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

func resetChildren(arg *MergeGroup, bats []*batch.Batch) {
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
}
