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
	"strconv"
	"strings"
	"testing"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
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

func TestHashPartitionMemoryFallbackIncludesRetainedBatch(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	input := batch.New([]string{"k", "payload", "id"})
	input.Vecs = []*vector.Vector{
		vector.NewVec(types.T_int32.ToType()),
		vector.NewVec(types.T_varchar.ToType()),
		vector.NewVec(types.T_int64.ToType()),
	}
	keys := []int32{2, 1, 2, 1, 3, 2, 3, 1}
	ids := []int64{0, 1, 2, 3, 4, 5, 6, 7}
	payloads := make([][]byte, len(keys))
	for i := range payloads {
		payloads[i] = []byte(strings.Repeat(string(rune('a'+i)), 32<<10))
	}
	require.NoError(t, vector.AppendFixedList(input.Vecs[0], keys, nil, proc.Mp()))
	require.NoError(t, vector.AppendBytesList(input.Vecs[1], payloads, nil, proc.Mp()))
	require.NoError(t, vector.AppendFixedList(input.Vecs[2], ids, nil, proc.Mp()))
	input.SetRowCount(len(keys))
	child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{input})
	arg := newHashPartitionArgument(128 << 10)
	arg.AppendChild(child)

	require.NoError(t, arg.Prepare(proc))
	var groups [][]int64
	for {
		result, err := arg.Call(proc)
		require.NoError(t, err)
		if result.Status == vm.ExecStop {
			break
		}
		groups = append(groups, append([]int64(nil),
			vector.MustFixedColWithTypeCheck[int64](result.Batch.Vecs[2])...))
	}
	require.True(t, arg.hash.fallbackToSort)
	require.Greater(t, int64(arg.hash.retained.Size()), int64(128<<10))
	require.Equal(t, [][]int64{{1, 3, 7}, {0, 2, 5}, {4, 6}}, groups)

	arg.Free(proc, false, nil)
	child.Free(proc, false, nil)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestHashPartitionFallbackRejectsAdditionalOverBudgetInput(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	inputs := make([]*batch.Batch, 4)
	for i := range inputs {
		inputs[i] = makeWideHashPartitionBatch(t, proc, i*8)
	}
	child := colexec.NewMockOperator().WithBatchs(inputs)
	arg := newHashPartitionArgument(128 << 10)
	arg.AppendChild(child)

	require.NoError(t, arg.Prepare(proc))
	_, err := arg.Call(proc)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrOOM), err)
	require.True(t, arg.hash.fallbackToSort)
	require.Equal(t, 8, arg.hash.retained.RowCount(), "only the triggering batch may be retained")

	arg.Free(proc, true, err)
	child.Free(proc, true, err)
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

func TestHashPartitionTreatsGroupingSentinelAsNull(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	input := makeHashPartitionBatch(t, proc,
		[]int32{0, 0, 1}, []bool{true, false, false}, []int64{0, 1, 2})
	input.Vecs[0].GetGrouping().Add(1)
	child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{input})
	arg := newHashPartitionArgument(1 << 30)
	arg.AppendChild(child)

	require.NoError(t, arg.Prepare(proc))
	groups := collectHashPartitionRows(t, arg, proc)
	require.Equal(t, [][]int64{{0, 1}, {2}}, groups)
	require.True(t, input.Vecs[0].GetNulls().Contains(0))
	require.False(t, input.Vecs[0].GetNulls().Contains(1))
	require.True(t, input.Vecs[0].GetGrouping().Contains(1),
		"hash-key normalization must not mutate borrowed input vectors")

	arg.Free(proc, false, nil)
	child.Free(proc, false, nil)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestHashPartitionTreatsGroupingSentinelAsNullStringHash(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	input := batch.New([]string{"k", "v"})
	input.Vecs = []*vector.Vector{
		vector.NewVec(types.T_varchar.ToType()),
		vector.NewVec(types.T_int64.ToType()),
	}
	require.NoError(t, vector.AppendBytesList(
		input.Vecs[0], [][]byte{[]byte(""), []byte(""), []byte("x")},
		[]bool{true, false, false}, proc.Mp(),
	))
	require.NoError(t, vector.AppendFixedList(
		input.Vecs[1], []int64{0, 1, 2}, nil, proc.Mp(),
	))
	input.Vecs[0].GetGrouping().Add(1)
	input.SetRowCount(3)
	child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{input})
	arg := &Partition{
		Algorithm: plan.Node_PARTITION_ALGORITHM_HASH,
		SpillMem:  1 << 30,
		OrderBySpecs: []*plan.OrderBySpec{{
			Expr: newExpression(0, types.T_varchar),
		}},
	}
	arg.AppendChild(child)

	require.NoError(t, arg.Prepare(proc))
	groups := collectHashPartitionRows(t, arg, proc)
	require.Equal(t, [][]int64{{0, 1}, {2}}, groups)
	require.False(t, input.Vecs[0].GetNulls().Contains(1))
	require.True(t, input.Vecs[0].GetGrouping().Contains(1))

	arg.Free(proc, false, nil)
	child.Free(proc, false, nil)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestHashPartitionGroupingOnNonNullableKeyFallsBackToSort(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	input := makeHashPartitionBatch(t, proc,
		[]int32{0, 99, 0, 1}, nil, []int64{0, 1, 2, 3})
	input.Vecs[0].GetGrouping().Add(0, 1)
	child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{input})
	arg := newHashPartitionArgument(1 << 30)
	arg.OrderBySpecs[0].Expr.Typ.NotNullable = true
	arg.AppendChild(child)

	require.NoError(t, arg.Prepare(proc))
	groups := collectHashPartitionRows(t, arg, proc)
	require.True(t, arg.hash.fallbackToSort)
	require.Equal(t, [][]int64{{0, 1}, {2}, {3}}, groups)

	arg.Free(proc, false, nil)
	child.Free(proc, false, nil)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestHashPartitionRejectsIncompatibleKey(t *testing.T) {
	for _, typ := range []types.T{types.T_float64, types.T_char} {
		t.Run(typ.String(), func(t *testing.T) {
			proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
			arg := &Partition{
				Algorithm:    plan.Node_PARTITION_ALGORITHM_HASH,
				OrderBySpecs: []*plan.OrderBySpec{{Expr: newExpression(0, typ)}},
			}
			require.Error(t, arg.Prepare(proc))
			arg.Free(proc, true, nil)
			proc.Free()
			require.Zero(t, proc.Mp().CurrNB())
		})
	}
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

func TestHashPartitionSortFallbackHonorsCancellationDuringFinalize(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	keys := make([]int32, 64)
	values := make([]int64, len(keys))
	for i := range keys {
		keys[i] = int32(len(keys) - i)
		values[i] = int64(i)
	}
	input := makeHashPartitionBatch(t, proc, keys, nil, values)
	child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{input})
	arg := newHashPartitionArgument(1) // Force the pre-output sort fallback.
	arg.AppendChild(child)
	require.NoError(t, arg.Prepare(proc))

	// The input chunk and EOF consume two checks. The fallback sorter then
	// reaches its bounded merge checkpoint, where this context cancels.
	proc.Ctx = newCancelAfterDoneChecksContext(4)
	_, err := arg.Call(proc)
	require.ErrorIs(t, err, context.Canceled)
	require.True(t, arg.hash.fallbackToSort)

	arg.Free(proc, true, err)
	child.Free(proc, true, err)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestHashPartitionHonorsCancellationDuringFinalMaterialization(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	rows := 2 * cancellationCheckInterval
	keys := make([]int32, rows)
	values := make([]int64, rows)
	for i := range keys {
		keys[i] = int32(i % 2)
		values[i] = int64(i)
	}
	input := makeHashPartitionBatch(t, proc, keys, nil, values)
	child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{input})
	arg := newHashPartitionArgument(1 << 30)
	arg.AppendChild(child)
	require.NoError(t, arg.Prepare(proc))

	// Receive, EOF, count, and scatter each poll at the unit boundary. The
	// ninth poll is between the first and second copy units, proving that a
	// cancel after grouping is complete interrupts final materialization.
	proc.Ctx = newCancelAfterDoneChecksContext(9)
	_, err := arg.Call(proc)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, arg.hash.materializing)

	arg.Free(proc, true, err)
	child.Free(proc, true, err)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestCopyPartitionSelectionsHonorsCancellationDuringTailCopy(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	src := make([]int64, 2*cancellationCheckInterval)
	dst := make([]int64, len(src))
	for i := range src {
		src[i] = int64(i)
	}
	proc.Ctx = newCancelAfterDoneChecksContext(2)

	copied, err := copyPartitionSelections(proc, dst, src, 0)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, cancellationCheckInterval, copied)
	require.Equal(t, src[:copied], dst[:copied])

	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestHashPartitionAccountsHashAndRowIndexMemory(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	arg := newHashPartitionArgument(1 << 30)
	input := makeHashPartitionBatch(t, proc,
		[]int32{2, 1, 2, 1}, nil, []int64{0, 1, 2, 3})
	child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{input})
	arg.AppendChild(child)
	require.NoError(t, arg.Prepare(proc))

	_, err := arg.Call(proc)
	require.NoError(t, err)
	rowIndexBytes := int64(arg.hash.retained.RowCount()+2*arg.hash.retained.RowCount()+len(arg.hash.groupBoundaries)) * int64(unsafe.Sizeof(int64(0)))
	require.GreaterOrEqual(t, arg.OpAnalyzer.GetOpStats().MemorySize, int64(arg.hash.retained.Size())+rowIndexBytes,
		"operator statistics must include hash and every finalization row-index workspace")

	arg.Free(proc, false, nil)
	child.Free(proc, false, nil)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

type cancelAfterDoneChecksContext struct {
	context.Context
	remaining int
	done      chan struct{}
}

func newCancelAfterDoneChecksContext(checks int) *cancelAfterDoneChecksContext {
	return &cancelAfterDoneChecksContext{
		Context:   context.Background(),
		remaining: checks,
		done:      make(chan struct{}),
	}
}

func (ctx *cancelAfterDoneChecksContext) Done() <-chan struct{} {
	if ctx.remaining > 0 {
		ctx.remaining--
		if ctx.remaining == 0 {
			close(ctx.done)
		}
	}
	return ctx.done
}

func (ctx *cancelAfterDoneChecksContext) Err() error {
	select {
	case <-ctx.done:
		return context.Canceled
	default:
		return nil
	}
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
	// This measures the Partition prerequisite itself. The SQL BVT covers the
	// downstream Window contract independently; it would be misleading to claim
	// that this operator microbenchmark measures full query latency.
	for _, rows := range []int{1 << 10, 1 << 16, 1 << 20} {
		for _, ndv := range []int{1, max(1, rows/100), rows} {
			for _, keyCount := range []int{1, 3} {
				for _, varlen := range []bool{false, true} {
					for _, algorithm := range []string{"sort", "hash"} {
						name := fmt.Sprintf("%s/rows=%d/ndv=%d/keys=%d/%s",
							algorithm, rows, ndv, keyCount, map[bool]string{false: "fixed", true: "varlen"}[varlen])
						b.Run(name, func(b *testing.B) {
							var peak int64
							for i := 0; i < b.N; i++ {
								peak = max(peak, runWindowPartitionBenchmark(b, rows, ndv, keyCount, varlen, algorithm == "hash"))
							}
							b.ReportMetric(float64(peak), "peak-mpool-B")
						})
					}
				}
			}
		}
	}
}

func runWindowPartitionBenchmark(b *testing.B, rows, ndv, keyCount int, varlen, useHash bool) int64 {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(b, "", mp)
	input, specs := makeWindowPartitionBenchmarkInput(b, proc, rows, ndv, keyCount, varlen)
	child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{input})
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
	peak := mp.CurrNB()
	for {
		result, err := arg.Call(proc)
		require.NoError(b, err)
		peak = max(peak, mp.CurrNB())
		if useHash {
			// finalize frees its scratch before Call returns, so the current
			// mpool value alone would under-report the peak hash working set.
			peak = max(peak, arg.hash.observedMemory)
			if arg.hash.retained != nil {
				// At the maximum finalization point, the retained/hash accounting
				// overlaps the stable-selection array and per-group positions.
				scratch := int64(arg.hash.retained.RowCount()+len(arg.hash.groupBoundaries)) * int64(unsafe.Sizeof(int64(0)))
				peak = max(peak, arg.hash.observedMemory+scratch)
			}
		}
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
	return peak
}

func makeWindowPartitionBenchmarkInput(
	t testing.TB,
	proc *process.Process,
	rows, ndv, keyCount int,
	varlen bool,
) (*batch.Batch, []*plan.OrderBySpec) {
	t.Helper()
	bat := batch.New(nil)
	bat.Attrs = make([]string, 0, keyCount+1)
	bat.Vecs = make([]*vector.Vector, 0, keyCount+1)
	specs := make([]*plan.OrderBySpec, 0, keyCount)
	for key := 0; key < keyCount; key++ {
		bat.Attrs = append(bat.Attrs, fmt.Sprintf("k%d", key))
		if varlen {
			vec := vector.NewVec(types.T_varchar.ToType())
			values := make([][]byte, rows)
			for row := range values {
				values[row] = strconv.AppendInt(nil, int64((row*7919+key*104729)%ndv), 10)
			}
			require.NoError(t, vector.AppendBytesList(vec, values, nil, proc.Mp()))
			bat.Vecs = append(bat.Vecs, vec)
			specs = append(specs, &plan.OrderBySpec{Expr: newExpression(int32(key), types.T_varchar)})
			continue
		}
		vec := vector.NewVec(types.T_int32.ToType())
		values := make([]int32, rows)
		for row := range values {
			values[row] = int32((row*7919 + key*104729) % ndv)
		}
		require.NoError(t, vector.AppendFixedList(vec, values, nil, proc.Mp()))
		bat.Vecs = append(bat.Vecs, vec)
		specs = append(specs, &plan.OrderBySpec{Expr: newExpression(int32(key), types.T_int32)})
	}
	bat.Attrs = append(bat.Attrs, "v")
	payload := make([]int64, rows)
	for row := range payload {
		payload[row] = int64(row)
	}
	payloadVec := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixedList(payloadVec, payload, nil, proc.Mp()))
	bat.Vecs = append(bat.Vecs, payloadVec)
	bat.SetRowCount(rows)
	return bat, specs
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

func makeWideHashPartitionBatch(t testing.TB, proc *process.Process, start int) *batch.Batch {
	t.Helper()
	bat := batch.New([]string{"k", "payload", "id"})
	bat.Vecs = []*vector.Vector{
		vector.NewVec(types.T_int32.ToType()),
		vector.NewVec(types.T_varchar.ToType()),
		vector.NewVec(types.T_int64.ToType()),
	}
	keys := make([]int32, 8)
	ids := make([]int64, 8)
	payloads := make([][]byte, 8)
	for i := range keys {
		keys[i] = int32(i % 3)
		ids[i] = int64(start + i)
		payloads[i] = []byte(strings.Repeat(string(rune('a'+i)), 32<<10))
	}
	require.NoError(t, vector.AppendFixedList(bat.Vecs[0], keys, nil, proc.Mp()))
	require.NoError(t, vector.AppendBytesList(bat.Vecs[1], payloads, nil, proc.Mp()))
	require.NoError(t, vector.AppendFixedList(bat.Vecs[2], ids, nil, proc.Mp()))
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
