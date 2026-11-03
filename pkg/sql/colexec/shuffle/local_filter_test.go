// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package shuffle

import (
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/runtimefilter"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestLocalShuffleFilterPreservesProbeStreaming(t *testing.T) {
	proc := testutil.NewProcess(t)
	t.Cleanup(proc.Free)
	ctx, cancel := context.WithTimeoutCause(proc.Ctx, 5*time.Second, moerr.NewInternalErrorNoCtx("probe streaming test deadline"))
	t.Cleanup(cancel)
	proc.Ctx = ctx
	proc.SetMessageBoard(message.NewMessageBoard())
	budget := process.MustNewExecutionResourceBudget(16<<20, 16<<20)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	t.Cleanup(generation.Close)
	require.NoError(t, proc.SetExecutionResourceBudgetForTesting(generation))
	filter, _ := runtimefilter.NewLocalShuffleFilter(82, types.T_int64, 2, 1)
	t.Cleanup(filter.Close)
	producer, err := filter.Begin(proc, 0, 1)
	require.NoError(t, err)
	input := batch.NewWithSize(1)
	t.Cleanup(func() { input.Clean(proc.Mp()) })
	input.Vecs[0] = testutil.MakeInt64Vector([]int64{1, 2, 3}, nil, proc.Mp())
	input.SetRowCount(3)
	producer.AddVector(input.Vecs[0])
	source := colexec.NewMockOperator().WithBatchs([]*batch.Batch{input})
	arg := NewArgument()
	arg.BucketNum = 1
	arg.ShuffleType = int32(plan.ShuffleType_Hash)
	arg.RuntimeFilterSpec = &plan.RuntimeFilterSpec{Tag: 82}
	arg.LocalFilter = filter
	arg.SetShufflePool(NewShufflePool(1, 1, false))
	arg.AppendChild(source)
	t.Cleanup(func() { arg.Reset(proc, true, nil); arg.Free(proc, false, nil); arg.Release(); source.ResetBatchs() })
	require.NoError(t, arg.Prepare(proc))
	arg.startLocalProducer(proc)
	select {
	case <-arg.ctr.producerDone:
		require.False(t, filter.Ready())
		require.NoError(t, producer.Complete(false))
	case <-ctx.Done():
		t.Fatal("the optional filter blocked upstream probe work", context.Cause(ctx))
	}
	result, err := vm.Exec(arg, proc)
	require.NoError(t, err)
	require.NotNil(t, result.Batch)
	require.Equal(t, []int64{1, 2, 3}, vector.MustFixedColNoTypeCheck[int64](result.Batch.Vecs[0]))
	require.Zero(t, arg.ctr.filterTested, "incomplete membership cannot reject probe rows")
}

func TestLocalShuffleFilterConsumer(t *testing.T) {
	proc := testutil.NewProcess(t)
	t.Cleanup(proc.Free)
	proc.SetMessageBoard(message.NewMessageBoard())
	budget := process.MustNewExecutionResourceBudget(16<<20, 16<<20)
	g, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	t.Cleanup(g.Close)
	require.NoError(t, proc.SetExecutionResourceBudgetForTesting(g))
	filter, _ := runtimefilter.NewLocalShuffleFilter(81, types.T_int64, 2, 1)
	t.Cleanup(filter.Close)
	keys := testutil.MakeInt64Vector([]int64{1, 7}, nil, proc.Mp())
	t.Cleanup(func() { keys.Free(proc.Mp()) })
	producer, err := filter.Begin(proc, 0, 1)
	require.NoError(t, err)
	producer.AddVector(keys)
	require.NoError(t, producer.Complete(false))
	bloom, err := filter.Wait(proc.Ctx, nil)
	require.NoError(t, err)
	input := batch.NewWithSize(2)
	t.Cleanup(func() { input.Clean(proc.Mp()) })
	values := make([]int64, 32)
	labels := make([]int32, 32)
	for i := range values {
		values[i] = int64(i)
		labels[i] = int32(i * 10)
	}
	input.Vecs[0] = testutil.MakeInt64Vector(values, []uint64{0}, proc.Mp())
	input.Vecs[1] = testutil.MakeInt32Vector(labels, nil, proc.Mp())
	input.SetRowCount(len(values))
	// Bloom false positives are permitted; the independent consumer oracle is
	// that every positive bit routes the original row with its payload intact.
	mask := bloom.TestVector(input.Vecs[0], nil)
	expected := make(map[int64]int32)
	for row, present := range mask {
		if present != 0 && row != 0 {
			expected[values[row]] = labels[row]
		}
	}
	source := colexec.NewMockOperator().WithBatchs([]*batch.Batch{input})
	arg := NewArgument()
	arg.BucketNum = 1
	arg.ShuffleType = int32(plan.ShuffleType_Hash)
	arg.RuntimeFilterSpec = &plan.RuntimeFilterSpec{Tag: 81}
	arg.LocalFilter = filter
	arg.SetShufflePool(NewShufflePool(1, 1, false))
	arg.AppendChild(source)
	t.Cleanup(func() { arg.Reset(proc, true, nil); arg.Free(proc, false, nil); arg.Release(); source.ResetBatchs() })
	require.NoError(t, arg.Prepare(proc))
	got := make(map[int64]int32)
	for {
		result, err := vm.Exec(arg, proc)
		require.NoError(t, err)
		if result.Batch == nil {
			break
		}
		b := result.Batch
		for row, key := range vector.MustFixedColNoTypeCheck[int64](b.Vecs[0]) {
			got[key] = vector.GetFixedAtNoTypeCheck[int32](b.Vecs[1], row)
		}
	}
	require.Equal(t, expected, got)
	require.Contains(t, got, int64(1))
	require.Contains(t, got, int64(7))
	require.Equal(t, int64(32), arg.ctr.filterTested)
	require.Equal(t, values, vector.MustFixedColNoTypeCheck[int64](input.Vecs[0]))
}
