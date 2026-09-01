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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	orderop "github.com/matrixorigin/matrixone/pkg/sql/colexec/order"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestHashPartitionCompleteStableGroupsAndReset(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	arg := newHashPartitionArgument(1 << 30)

	for run := 0; run < 2; run++ {
		first := makeHashPartitionBatch(t, proc,
			[]int32{2, 1, 2}, []bool{false, false, false}, []int64{0, 1, 2})
		second := makeHashPartitionBatch(t, proc,
			[]int32{1, 0, 2, 0}, []bool{false, true, false, true}, []int64{3, 4, 5, 6})
		child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{first, second})
		arg.Children = nil
		arg.AppendChild(child)

		require.NoError(t, arg.Prepare(proc))
		groups := collectHashPartitionRows(t, arg, proc)
		require.Equal(t, [][]int64{{0, 2, 5}, {1, 3}, {4, 6}}, groups)
		require.False(t, arg.hash.fallbackToSort)

		arg.Reset(proc, false, nil)
		child.Free(proc, false, nil)
	}

	arg.Free(proc, false, nil)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestHashPartitionMemoryFallbackIsExact(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	arg := newHashPartitionArgument(1) // row-count threshold: force fallback.
	input := makeHashPartitionBatch(t, proc,
		[]int32{2, 1, 2, 1, 3}, nil, []int64{0, 1, 2, 3, 4})
	child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{input})
	arg.AppendChild(child)

	require.NoError(t, arg.Prepare(proc))
	groups := collectHashPartitionRows(t, arg, proc)
	require.True(t, arg.hash.fallbackToSort)
	require.Zero(t, cap(arg.hash.groupIDs), "hash-only row ids must be released before fallback output")
	require.Equal(t, [][]int64{{1, 3}, {0, 2}, {4}}, groups)

	arg.Free(proc, false, nil)
	child.Free(proc, false, nil)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestHashPartitionCompositeNullableKey(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	input := batch.New([]string{"k1", "k2", "v"})
	input.Vecs = []*vector.Vector{
		vector.NewVec(types.T_int32.ToType()),
		vector.NewVec(types.T_int32.ToType()),
		vector.NewVec(types.T_int64.ToType()),
	}
	require.NoError(t, vector.AppendFixedList(input.Vecs[0], []int32{1, 1, 1, 2, 1}, nil, proc.Mp()))
	require.NoError(t, vector.AppendFixedList(input.Vecs[1], []int32{0, 0, 1, 0, 0}, []bool{true, true, false, false, true}, proc.Mp()))
	require.NoError(t, vector.AppendFixedList(input.Vecs[2], []int64{0, 1, 2, 3, 4}, nil, proc.Mp()))
	input.SetRowCount(5)
	child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{input})
	arg := &Partition{
		Algorithm: plan.Node_PARTITION_ALGORITHM_HASH,
		SpillMem:  1 << 30,
		OrderBySpecs: []*plan.OrderBySpec{
			{Expr: newExpression(0, types.T_int32)},
			{Expr: newExpression(1, types.T_int32)},
		},
	}
	arg.AppendChild(child)

	require.NoError(t, arg.Prepare(proc))
	var groups [][]int64
	for {
		result, err := arg.Call(proc)
		require.NoError(t, err)
		if result.Status == vm.ExecStop {
			break
		}
		values := vector.MustFixedColWithTypeCheck[int64](result.Batch.Vecs[2])
		groups = append(groups, append([]int64(nil), values...))
	}
	require.Equal(t, [][]int64{{0, 1, 4}, {2}, {3}}, groups)

	arg.Free(proc, false, nil)
	child.Free(proc, false, nil)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestHashPartitionRejectsIncompatibleKey(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	arg := &Partition{
		Algorithm:    plan.Node_PARTITION_ALGORITHM_HASH,
		OrderBySpecs: []*plan.OrderBySpec{{Expr: newExpression(0, types.T_float64)}},
	}
	require.Error(t, arg.Prepare(proc))
	arg.Free(proc, true, nil)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestHashPartitionHonorsCancellationBeforeFinalize(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	arg := newHashPartitionArgument(1 << 30)
	input := makeHashPartitionBatch(t, proc, []int32{1, 2}, nil, []int64{10, 20})
	child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{input})
	arg.AppendChild(child)
	require.NoError(t, arg.Prepare(proc))

	ctx, cancel := context.WithCancel(proc.Ctx)
	proc.Ctx = ctx
	cancel()
	_, err := arg.Call(proc)
	require.ErrorIs(t, err, context.Canceled)

	arg.Free(proc, true, err)
	child.Free(proc, true, err)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func BenchmarkHashPartition(b *testing.B) {
	for _, rows := range []int{1 << 10, 1 << 16} {
		for _, ndv := range []int{1, 64, 1024} {
			if ndv > rows {
				continue
			}
			b.Run(fmt.Sprintf("rows=%d/ndv=%d", rows, ndv), func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					mp := mpool.MustNewZero()
					proc := testutil.NewProcessWithMPool(b, "", mp)
					keys := make([]int32, rows)
					payload := make([]int64, rows)
					for row := range keys {
						keys[row] = int32(row % ndv)
						payload[row] = int64(row)
					}
					input := makeHashPartitionBatch(b, proc, keys, nil, payload)
					child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{input})
					arg := newHashPartitionArgument(1 << 30)
					arg.AppendChild(child)
					require.NoError(b, arg.Prepare(proc))
					for {
						result, err := arg.Call(proc)
						require.NoError(b, err)
						if result.Status == vm.ExecStop {
							break
						}
					}
					arg.Free(proc, false, nil)
					child.Free(proc, false, nil)
					proc.Free()
					require.Zero(b, mp.CurrNB())
				}
			})
		}
	}
}

func BenchmarkWindowPartitionAlgorithms(b *testing.B) {
	const rows = 1 << 16
	for _, ndv := range []int{64, 1024, 16384, 1 << 16} {
		for _, algorithm := range []string{"sort", "hash"} {
			b.Run(fmt.Sprintf("%s/rows=%d/ndv=%d", algorithm, rows, ndv), func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					runWindowPartitionBenchmark(b, rows, ndv, algorithm == "hash")
				}
			})
		}
	}
}

func runWindowPartitionBenchmark(b *testing.B, rows, ndv int, useHash bool) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(b, "", mp)
	keys := make([]int32, rows)
	payload := make([]int64, rows)
	for row := range keys {
		keys[row] = int32((row * 7919) % ndv)
		payload[row] = int64(row)
	}
	input := makeHashPartitionBatch(b, proc, keys, nil, payload)
	child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{input})
	specs := []*plan.OrderBySpec{{Expr: newExpression(0, types.T_int32)}}
	arg := &Partition{OrderBySpecs: specs}
	if useHash {
		arg.Algorithm = plan.Node_PARTITION_ALGORITHM_HASH
		arg.SpillMem = 1 << 30
		arg.AppendChild(child)
	} else {
		order := orderop.NewArgument()
		order.OrderBySpec = specs
		order.AppendChild(child)
		require.NoError(b, order.Prepare(proc))
		arg.AppendChild(order)
	}
	require.NoError(b, arg.Prepare(proc))
	for {
		result, err := arg.Call(proc)
		require.NoError(b, err)
		if result.Status == vm.ExecStop {
			break
		}
	}
	arg.Free(proc, false, nil)
	if !useHash {
		arg.GetChildren(0).Free(proc, false, nil)
	}
	child.Free(proc, false, nil)
	proc.Free()
	require.Zero(b, mp.CurrNB())
}

func newHashPartitionArgument(spillMem int64) *Partition {
	return &Partition{
		Algorithm: plan.Node_PARTITION_ALGORITHM_HASH,
		SpillMem:  spillMem,
		OrderBySpecs: []*plan.OrderBySpec{{
			Expr: newExpression(0, types.T_int32),
		}},
	}
}

func makeHashPartitionBatch(
	t testing.TB,
	proc *process.Process,
	keys []int32,
	nulls []bool,
	payload []int64,
) *batch.Batch {
	t.Helper()
	bat := batch.New([]string{"k", "v"})
	bat.Vecs = []*vector.Vector{vector.NewVec(types.T_int32.ToType()), vector.NewVec(types.T_int64.ToType())}
	require.NoError(t, vector.AppendFixedList(bat.Vecs[0], keys, nulls, proc.Mp()))
	require.NoError(t, vector.AppendFixedList(bat.Vecs[1], payload, nil, proc.Mp()))
	bat.SetRowCount(len(keys))
	return bat
}

func collectHashPartitionRows(t testing.TB, arg *Partition, proc *process.Process) [][]int64 {
	t.Helper()
	var groups [][]int64
	for {
		result, err := arg.Call(proc)
		require.NoError(t, err)
		if result.Status == vm.ExecStop {
			return groups
		}
		require.NotNil(t, result.Batch)
		values := vector.MustFixedColWithTypeCheck[int64](result.Batch.Vecs[1])
		groups = append(groups, append([]int64(nil), values...))
	}
}
