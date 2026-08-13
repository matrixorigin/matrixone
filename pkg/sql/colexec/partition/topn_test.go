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

package partition

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestPartitionTopNMatchesPerGroupSort(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	input := makeTopNBatch(t, proc,
		[]int32{1, 2, 1, 2, 1, 2},
		[]int64{5, 4, 1, 9, 3, 4},
		[]int64{50, 40, 10, 90, 30, 41},
		nil)
	arg := newTopNArgument(2)
	child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{input})
	arg.AppendChild(child)

	require.NoError(t, arg.Prepare(proc))
	got := collectTopNRows(t, arg, proc)
	require.Equal(t, [][3]int64{
		{1, 1, 10}, {1, 3, 30},
		{2, 4, 41}, {2, 4, 40},
	}, got)
	require.LessOrEqual(t, arg.top.retained.RowCount(), len(arg.top.groups)*2)

	arg.Free(proc, false, nil)
	child.Free(proc, false, nil)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestPartitionTopNCompositeNullableKeyAndReset(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	arg := newTopNArgument(1)
	arg.PartitionByCount = 2
	arg.OrderBySpecs = []*plan.OrderBySpec{
		{Expr: topNCol(0, types.T_int32)},
		{Expr: topNCol(1, types.T_int32)},
		{Expr: topNCol(2, types.T_int64), Flag: plan.OrderBySpec_DESC | plan.OrderBySpec_NULLS_LAST},
	}

	for run := 0; run < 2; run++ {
		input := batch.NewWithSize(4)
		input.Vecs[0] = vector.NewVec(types.T_int32.ToType())
		require.NoError(t, vector.AppendFixedList(input.Vecs[0], []int32{1, 1, 1, 1}, nil, proc.Mp()))
		input.Vecs[1] = vector.NewVec(types.T_int32.ToType())
		require.NoError(t, vector.AppendFixedList(input.Vecs[1], []int32{0, 0, 2, 2}, []bool{true, true, false, false}, proc.Mp()))
		input.Vecs[2] = vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixedList(input.Vecs[2], []int64{7, 9, 0, 5}, []bool{false, false, true, false}, proc.Mp()))
		input.Vecs[3] = vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixedList(input.Vecs[3], []int64{70, 90, 0, 50}, nil, proc.Mp()))
		input.SetRowCount(4)

		child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{input})
		arg.Children = nil
		arg.AppendChild(child)
		require.NoError(t, arg.Prepare(proc))
		got := collectTopNRowsAt(t, arg, proc, 0, 2, 3)
		require.Equal(t, [][3]int64{{1, 9, 90}, {1, 5, 50}}, got)
		child.Free(proc, false, nil)
		arg.Reset(proc, false, nil)
	}

	arg.Free(proc, false, nil)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestPartitionTopNTwoStageMatchesSingleStage(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	left := makeTopNBatch(t, proc,
		[]int32{1, 1, 2, 2}, []int64{8, 2, 7, 1}, []int64{80, 20, 70, 10}, nil)
	right := makeTopNBatch(t, proc,
		[]int32{1, 1, 2, 2}, []int64{3, 5, 4, 6}, []int64{30, 50, 40, 60}, nil)

	localBatches := make([]*batch.Batch, 0, 4)
	for _, input := range []*batch.Batch{left, right} {
		local := newTopNArgument(2)
		child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{input})
		local.AppendChild(child)
		require.NoError(t, local.Prepare(proc))
		for {
			result, err := local.Call(proc)
			require.NoError(t, err)
			if result.Batch == nil || result.Status == vm.ExecStop {
				break
			}
			copyBatch, err := result.Batch.Dup(proc.Mp())
			require.NoError(t, err)
			localBatches = append(localBatches, copyBatch)
		}
		local.Free(proc, false, nil)
		child.Free(proc, false, nil)
	}

	global := newTopNArgument(2)
	globalChild := colexec.NewMockOperator().WithBatchs(localBatches)
	global.AppendChild(globalChild)
	require.NoError(t, global.Prepare(proc))
	require.Equal(t, [][3]int64{
		{1, 2, 20}, {1, 3, 30},
		{2, 1, 10}, {2, 4, 40},
	}, collectTopNRows(t, global, proc))

	global.Free(proc, false, nil)
	globalChild.Free(proc, false, nil)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestPartitionTopNPreReducePacksCandidateGroups(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	input := makeTopNBatch(t, proc,
		[]int32{1, 2, 1, 2}, []int64{8, 7, 2, 1}, []int64{80, 70, 20, 10}, nil)
	arg := newTopNArgument(1)
	arg.PreReduce = true
	child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{input})
	arg.AppendChild(child)
	require.NoError(t, arg.Prepare(proc))

	result, err := arg.Call(proc)
	require.NoError(t, err)
	require.NotNil(t, result.Batch)
	require.Equal(t, 2, result.Batch.RowCount(), "local candidates should share one dense batch")
	result, err = arg.Call(proc)
	require.NoError(t, err)
	require.Equal(t, vm.ExecStop, result.Status)

	arg.Free(proc, false, nil)
	child.Free(proc, false, nil)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestPartitionTopNVarlenReplacementMemoryBound(t *testing.T) {
	const (
		rows        = 2048
		payloadSize = 4096
	)
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	input := batch.NewWithSize(4)
	input.Vecs[0] = vector.NewVec(types.T_int32.ToType())
	input.Vecs[1] = vector.NewVec(types.T_int64.ToType())
	input.Vecs[2] = vector.NewVec(types.T_int64.ToType())
	input.Vecs[3] = vector.NewVec(types.T_varchar.ToType())
	payload := []byte(strings.Repeat("x", payloadSize))
	for i := 0; i < rows; i++ {
		require.NoError(t, vector.AppendFixed(input.Vecs[0], int32(1), false, proc.Mp()))
		require.NoError(t, vector.AppendFixed(input.Vecs[1], int64(i), false, proc.Mp()))
		require.NoError(t, vector.AppendFixed(input.Vecs[2], int64(i), false, proc.Mp()))
		require.NoError(t, vector.AppendBytes(input.Vecs[3], payload, false, proc.Mp()))
	}
	input.SetRowCount(rows)

	arg := newTopNArgument(1)
	arg.OrderBySpecs[1].Flag = plan.OrderBySpec_DESC
	child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{input})
	arg.AppendChild(child)
	require.NoError(t, arg.Prepare(proc))
	require.Len(t, collectTopNRows(t, arg, proc), 1)
	require.Equal(t, 1, arg.top.retained.RowCount())
	require.LessOrEqual(t, len(arg.top.retained.Vecs[3].GetArea()), minTopNVarlenCompactBytes+payloadSize)

	arg.Free(proc, false, nil)
	child.Free(proc, false, nil)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestPartitionTopNZeroAndCancellation(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	input := makeTopNBatch(t, proc, []int32{1}, []int64{1}, []int64{1}, nil)
	arg := newTopNArgument(0)
	child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{input})
	arg.AppendChild(child)
	require.NoError(t, arg.Prepare(proc))
	require.Empty(t, collectTopNRows(t, arg, proc))
	arg.Free(proc, false, nil)
	child.Free(proc, false, nil)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestPartitionTopNOutputHonorsCancellation(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	input := makeTopNBatch(t, proc, []int32{1, 2}, []int64{1, 2}, []int64{1, 2}, nil)
	arg := newTopNArgument(1)
	child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{input})
	arg.AppendChild(child)
	require.NoError(t, arg.Prepare(proc))

	// Build all groups, then cancel before the first output batch is materialized.
	result, err := arg.Call(proc)
	require.NoError(t, err)
	require.NotNil(t, result.Batch)
	ctx, cancel := context.WithCancel(proc.Ctx)
	proc.Ctx = ctx
	cancel()
	_, err = arg.Call(proc)
	require.ErrorIs(t, err, context.Canceled)

	arg.Free(proc, true, err)
	child.Free(proc, true, err)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func BenchmarkPartitionTopN(b *testing.B) {
	const (
		rows   = 1_000_000
		groups = 10_000
	)
	groupValues := make([]int32, rows)
	scores := make([]int64, rows)
	payloads := make([]int64, rows)
	for i := 0; i < rows; i++ {
		groupValues[i] = int32(i % groups)
		scores[i] = int64((i*7919 + 17) % rows)
		payloads[i] = int64(i)
	}

	for _, limit := range []uint64{1, 10} {
		b.Run(fmt.Sprintf("Top%d", limit), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				proc := testutil.NewProcessWithMPool(b, "", mpool.MustNewZero())
				input := batch.NewWithSize(3)
				input.Vecs[0] = vector.NewVec(types.T_int32.ToType())
				require.NoError(b, vector.AppendFixedList(input.Vecs[0], groupValues, nil, proc.Mp()))
				input.Vecs[1] = vector.NewVec(types.T_int64.ToType())
				require.NoError(b, vector.AppendFixedList(input.Vecs[1], scores, nil, proc.Mp()))
				input.Vecs[2] = vector.NewVec(types.T_int64.ToType())
				require.NoError(b, vector.AppendFixedList(input.Vecs[2], payloads, nil, proc.Mp()))
				input.SetRowCount(rows)
				arg := newTopNArgument(limit)
				child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{input})
				arg.AppendChild(child)
				require.NoError(b, arg.Prepare(proc))

				b.StartTimer()
				retained := 0
				for {
					result, err := arg.Call(proc)
					require.NoError(b, err)
					if result.Batch == nil || result.Status == vm.ExecStop {
						break
					}
				}
				retained = arg.top.retained.RowCount()
				b.StopTimer()

				b.ReportMetric(float64(retained), "retained_rows")
				arg.Free(proc, false, nil)
				child.Free(proc, false, nil)
				proc.Free()
				require.Zero(b, proc.Mp().CurrNB())
			}
		})
	}
}

func newTopNArgument(limit uint64) *Partition {
	return &Partition{
		OrderBySpecs: []*plan.OrderBySpec{
			{Expr: topNCol(0, types.T_int32)},
			{Expr: topNCol(1, types.T_int64)},
			{Expr: topNCol(2, types.T_int64), Flag: plan.OrderBySpec_DESC},
		},
		Limit: &plan.Expr{
			Typ: plan.Type{Id: int32(types.T_uint64), NotNullable: true},
			Expr: &plan.Expr_Lit{Lit: &plan.Literal{
				Value: &plan.Literal_U64Val{U64Val: limit},
			}},
		},
		PartitionByCount: 1,
	}
}

func topNCol(pos int32, typ types.T) *plan.Expr {
	return &plan.Expr{
		Typ:  plan.Type{Id: int32(typ)},
		Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: pos}},
	}
}

func makeTopNBatch(
	t *testing.T,
	proc *process.Process,
	groups []int32,
	scores []int64,
	payloads []int64,
	groupNulls []bool,
) *batch.Batch {
	t.Helper()
	require.Len(t, scores, len(groups))
	require.Len(t, payloads, len(groups))
	bat := batch.NewWithSize(3)
	bat.Vecs[0] = vector.NewVec(types.T_int32.ToType())
	require.NoError(t, vector.AppendFixedList(bat.Vecs[0], groups, groupNulls, proc.Mp()))
	bat.Vecs[1] = vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixedList(bat.Vecs[1], scores, nil, proc.Mp()))
	bat.Vecs[2] = vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixedList(bat.Vecs[2], payloads, nil, proc.Mp()))
	bat.SetRowCount(len(groups))
	return bat
}

func collectTopNRows(t *testing.T, arg *Partition, proc *process.Process) [][3]int64 {
	return collectTopNRowsAt(t, arg, proc, 0, 1, 2)
}

func collectTopNRowsAt(
	t *testing.T,
	arg *Partition,
	proc *process.Process,
	groupPos int,
	scorePos int,
	payloadPos int,
) [][3]int64 {
	t.Helper()
	rows := make([][3]int64, 0)
	for {
		result, err := arg.Call(proc)
		require.NoError(t, err)
		if result.Batch == nil || result.Status == vm.ExecStop {
			break
		}
		groups := vector.MustFixedColWithTypeCheck[int32](result.Batch.Vecs[groupPos])
		scores := vector.MustFixedColWithTypeCheck[int64](result.Batch.Vecs[scorePos])
		payloads := vector.MustFixedColWithTypeCheck[int64](result.Batch.Vecs[payloadPos])
		for i := 0; i < result.Batch.RowCount(); i++ {
			rows = append(rows, [3]int64{int64(groups[i]), scores[i], payloads[i]})
		}
	}
	return rows
}
