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

package merge_test

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dispatch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/merge"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergeorder"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/projection"
	"github.com/matrixorigin/matrixone/pkg/sql/internal/materialized"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
)

func TestMaterializedSinkScanConsumersAdvanceIndependently(t *testing.T) {
	proc := testutil.NewProcess(t)
	source := materialized.NewSource(2)
	require.NoError(t, source.Begin(proc.Mp()))

	inputs := make([]*batch.Batch, 4)
	for i := range inputs {
		inputs[i] = batch.NewWithSize(1)
		inputs[i].Vecs[0] = vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed(inputs[i].Vecs[0], int64(i), false, proc.Mp()))
		inputs[i].SetRowCount(1)
	}
	inputMemory := proc.Mp().CurrNB()

	producer := dispatch.NewArgument()
	defer producer.Release()
	producer.FuncId = dispatch.SendToAllLocalFunc
	producer.MaterializedSource = source
	producer.AppendChild(colexec.NewMockOperator().WithBatchs(inputs))
	require.NoError(t, producer.Prepare(proc))
	for {
		result, err := producer.Call(proc)
		require.NoError(t, err)
		if result.Status == vm.ExecStop {
			break
		}
	}
	require.Equal(t, source.CurrentBytes(), producer.GetOperatorBase().OpAnalyzer.GetOpStats().MemorySize)
	producer.Reset(proc, false, nil)

	readers := []*merge.Merge{merge.NewArgument(), merge.NewArgument()}
	defer readers[0].Release()
	defer readers[1].Release()
	for i, reader := range readers {
		reader.SinkScan = true
		reader.MaterializedSource = source
		reader.MaterializedReaderID = i
		require.NoError(t, reader.Prepare(proc))
	}

	// The second consumer completes before the first consumer starts. Streaming
	// fan-out cannot make progress in this topology once its bounded spool fills.
	for _, reader := range []*merge.Merge{readers[1], readers[0]} {
		var previous *batch.Batch
		for i := range inputs {
			result, err := reader.Call(proc)
			require.NoError(t, err)
			if previous != nil {
				require.Nil(t, previous.Vecs, "previous batch must be reclaimed before advancing")
			}
			require.Equal(t, int64(i), vector.GetFixedAtNoTypeCheck[int64](result.Batch.Vecs[0], 0))
			previous = result.Batch
		}
		result, err := reader.Call(proc)
		require.NoError(t, err)
		require.Equal(t, vm.ExecStop, result.Status)
		require.Nil(t, previous.Vecs, "EOF must reclaim the last batch")
		reader.Reset(proc, false, nil)
	}

	require.Equal(t, inputMemory, proc.Mp().CurrNB())
	for _, bat := range inputs {
		bat.Clean(proc.Mp())
	}
}

func TestMaterializedSinkScanConcurrentProductionReaders(t *testing.T) {
	proc := testutil.NewProcess(t)
	source := materialized.NewSource(2)
	require.NoError(t, source.Begin(proc.Mp()))

	inputs := make([]*batch.Batch, 32)
	for i := range inputs {
		inputs[i] = batch.NewWithSize(1)
		inputs[i].Vecs[0] = vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed(inputs[i].Vecs[0], int64(i), false, proc.Mp()))
		inputs[i].SetRowCount(1)
	}
	inputMemory := proc.Mp().CurrNB()

	producer := dispatch.NewArgument()
	defer producer.Release()
	producer.FuncId = dispatch.SendToAllLocalFunc
	producer.MaterializedSource = source
	producer.AppendChild(colexec.NewMockOperator().WithBatchs(inputs))
	require.NoError(t, producer.Prepare(proc))

	readers := []*merge.Merge{merge.NewArgument(), merge.NewArgument()}
	defer readers[0].Release()
	defer readers[1].Release()
	for i, reader := range readers {
		reader.SinkScan = true
		reader.MaterializedSource = source
		reader.MaterializedReaderID = i
		require.NoError(t, reader.Prepare(proc))
	}

	errCh := make(chan error, len(readers))
	var wg sync.WaitGroup
	for _, reader := range readers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range inputs {
				result, err := reader.Call(proc)
				if err != nil {
					errCh <- err
					return
				}
				got := vector.GetFixedAtNoTypeCheck[int64](result.Batch.Vecs[0], 0)
				if got != int64(i) {
					errCh <- moerr.NewInternalErrorNoCtxf("unexpected value %d at %d", got, i)
					return
				}
			}
			result, err := reader.Call(proc)
			if err != nil {
				errCh <- err
				return
			}
			if result.Status != vm.ExecStop {
				errCh <- moerr.NewInternalErrorNoCtx("materialized reader did not stop")
			}
		}()
	}

	for {
		result, err := producer.Call(proc)
		require.NoError(t, err)
		if result.Status == vm.ExecStop {
			break
		}
	}
	producer.Reset(proc, false, nil)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		require.NoError(t, err)
	}
	for _, reader := range readers {
		reader.Reset(proc, false, nil)
	}

	require.Equal(t, inputMemory, proc.Mp().CurrNB())
	for _, bat := range inputs {
		bat.Clean(proc.Mp())
	}
}

func TestMaterializedSinkScanReadersObserveProducerErrorAfterBufferedData(t *testing.T) {
	proc := testutil.NewProcess(t)
	source := materialized.NewSource(2)
	require.NoError(t, source.Begin(proc.Mp()))

	input := batch.NewWithSize(1)
	input.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(input.Vecs[0], int64(42), false, proc.Mp()))
	input.SetRowCount(1)
	inputMemory := proc.Mp().CurrNB()

	producer := dispatch.NewArgument()
	defer producer.Release()
	producer.FuncId = dispatch.SendToAllLocalFunc
	producer.MaterializedSource = source
	producer.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{input}))
	require.NoError(t, producer.Prepare(proc))
	result, err := producer.Call(proc)
	require.NoError(t, err)
	require.NotNil(t, result.Batch)

	want := moerr.NewInternalErrorNoCtx("materialized producer failed")
	producer.Reset(proc, true, want)

	for readerID := 0; readerID < 2; readerID++ {
		reader := merge.NewArgument()
		reader.SinkScan = true
		reader.MaterializedSource = source
		reader.MaterializedReaderID = readerID
		require.NoError(t, reader.Prepare(proc))

		result, err = reader.Call(proc)
		require.NoError(t, err)
		require.Equal(t, int64(42), vector.GetFixedAtNoTypeCheck[int64](result.Batch.Vecs[0], 0))
		returned := result.Batch
		result, err = reader.Call(proc)
		require.ErrorIs(t, err, want)
		require.Nil(t, result.Batch)
		require.Nil(t, returned.Vecs, "producer error must reclaim the previous batch")

		reader.Reset(proc, true, err)
		reader.Release()
	}

	require.Equal(t, inputMemory, proc.Mp().CurrNB())
	input.Clean(proc.Mp())
}

func TestMaterializedSinkScanBatchLifecycle(t *testing.T) {
	for _, terminal := range []string{"reset", "free", "reset_then_free", "free_then_reset", "prepare", "consumer_clean", "cancel"} {
		t.Run(terminal, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			source := materialized.NewSource(1)
			t.Cleanup(source.Close)
			input := batch.NewWithSize(1)
			t.Cleanup(func() { input.Clean(proc.Mp()) })
			input.Vecs[0] = vector.NewVec(types.T_int64.ToType())
			require.NoError(t, vector.AppendFixed(input.Vecs[0], int64(42), false, proc.Mp()))
			input.SetRowCount(1)
			inputMemory := proc.Mp().CurrNB()

			reader := merge.NewArgument()
			reader.MaterializedSource = source
			t.Cleanup(func() {
				reader.Free(proc, false, nil)
				reader.Release()
			})
			// Reuse the same source and operator to prove reader release and
			// position/current-batch state do not leak into the next generation.
			for generation := 0; generation < 2; generation++ {
				require.NoError(t, source.Begin(proc.Mp()))
				require.NoError(t, source.Append(input))
				if terminal != "cancel" {
					source.Finish(nil)
				}
				require.NoError(t, reader.Prepare(proc))
				result, err := reader.Call(proc)
				require.NoError(t, err)
				require.Equal(t, int64(42), vector.GetFixedAtNoTypeCheck[int64](result.Batch.Vecs[0], 0))
				returned := result.Batch

				switch terminal {
				case "reset":
					reader.Reset(proc, true, moerr.NewInternalErrorNoCtx("downstream failed"))
				case "free":
					reader.Free(proc, false, nil)
				case "reset_then_free":
					reader.Reset(proc, false, nil)
					reader.Reset(proc, false, nil)
					reader.Free(proc, false, nil)
				case "free_then_reset":
					reader.Free(proc, false, nil)
					reader.Free(proc, false, nil)
					reader.Reset(proc, false, nil)
				case "prepare":
					require.NoError(t, reader.Prepare(proc))
					require.Nil(t, returned.Vecs)
					result, err = reader.Call(proc)
					require.NoError(t, err)
					require.Equal(t, int64(42), vector.GetFixedAtNoTypeCheck[int64](result.Batch.Vecs[0], 0))
					reader.Reset(proc, false, nil)
				case "consumer_clean":
					returned.Clean(proc.Mp())
					reader.Free(proc, false, nil)
				case "cancel":
					originalCtx := proc.Ctx
					ctx, cancel := context.WithCancelCause(originalCtx)
					want := moerr.NewInternalErrorNoCtx("materialized reader cancelled")
					cancel(want)
					proc.Ctx = ctx
					result, err = reader.Call(proc)
					proc.Ctx = originalCtx
					require.ErrorIs(t, err, want)
					require.Nil(t, result.Batch)
					require.Nil(t, returned.Vecs, "cancellation must reclaim the previous batch")
					source.Finish(want)
					reader.Reset(proc, true, err)
				}
				require.Nil(t, returned.Vecs)
				require.Equal(t, inputMemory, proc.Mp().CurrNB())
			}
		})
	}
}

func TestMaterializedSinkScanProjectionCompletesAllocationAccount(t *testing.T) {
	registry, err := mpool.NewAllocationAccountRegistry(2, 32)
	require.NoError(t, err)
	account, err := registry.Open(1 << 20)
	require.NoError(t, err)
	// Fixture-local owner/site IDs test preserved allocation ownership through
	// Source.Dup; physical-operator attribution is covered by the SQL witness.
	selection, err := vector.NewAllocationAccountSelection(account, 1, 102, 103, 104, 105)
	require.NoError(t, err)
	proc := testutil.NewProcess(t)
	source := materialized.NewSource(2)
	t.Cleanup(source.Close)
	require.NoError(t, source.Begin(proc.Mp()))
	input := batch.NewOffHeapWithSize(1)
	t.Cleanup(func() { input.Clean(proc.Mp()) })
	input.Vecs[0], err = vector.NewOffHeapVecWithTypeAndAllocation(types.T_int64.ToType(), selection)
	require.NoError(t, err)
	for _, value := range []int64{11, 22} {
		require.NoError(t, vector.AppendFixed(input.Vecs[0], value, false, proc.Mp()))
	}
	input.SetRowCount(2)
	require.NoError(t, source.Append(input))
	input.Clean(proc.Mp())
	source.Finish(nil)

	for readerID := 0; readerID < 2; readerID++ {
		reader := merge.NewArgument()
		reader.MaterializedSource = source
		reader.MaterializedReaderID = readerID
		project := projection.NewArgument()
		t.Cleanup(func() {
			project.Free(proc, false, nil)
			reader.Free(proc, false, nil)
			project.Release()
			reader.Release()
		})
		require.NoError(t, reader.Prepare(proc))
		project.ProjectList = []*plan.Expr{{
			Typ:  plan.Type{Id: int32(types.T_int64)},
			Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}},
		}}
		project.AppendChild(reader)
		require.NoError(t, project.Prepare(proc))
		result, err := project.Call(proc)
		require.NoError(t, err)
		require.Equal(t, 2, result.Batch.RowCount())
		require.Equal(t, []int64{11, 22}, vector.MustFixedColWithTypeCheck[int64](result.Batch.Vecs[0]))
		// Projection borrows the source vector; its own cleanup does not own
		// this allocation. EOF must reclaim the Merge-owned input copy.
		result, err = project.Call(proc)
		require.NoError(t, err)
		require.Nil(t, result.Batch)
		require.Equal(t, vm.ExecStop, result.Status)
		project.Reset(proc, false, nil)
		reader.Reset(proc, false, nil)
		project.Free(proc, false, nil)
		reader.Free(proc, false, nil)
	}

	snapshot, first, err := registry.CompleteTerminal(account)
	require.NoError(t, err)
	require.True(t, first)
	require.Zero(t, snapshot.Used)
	require.Zero(t, snapshot.LiveAllocations)
	require.False(t, registry.AdmissionSuspended())
	next, err := registry.Open(1 << 20)
	require.NoError(t, err)
	_, _, err = registry.CompleteTerminal(next)
	require.NoError(t, err)
}

func TestMaterializedSinkScanOwnsBatchesThroughMergeOrderSpill(t *testing.T) {
	proc := testutil.NewProcess(t)
	source := materialized.NewSource(2)
	require.NoError(t, source.Begin(proc.Mp()))

	inputs := make([]*batch.Batch, 3)
	for i, value := range []int64{3, 1, 2} {
		inputs[i] = batch.NewWithSize(1)
		inputs[i].Vecs[0] = vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed(inputs[i].Vecs[0], value, false, proc.Mp()))
		inputs[i].SetRowCount(1)
	}
	inputMemory := proc.Mp().CurrNB()
	for _, input := range inputs {
		require.NoError(t, source.Append(input))
	}
	source.Finish(nil)

	firstReader := merge.NewArgument()
	defer firstReader.Release()
	firstReader.SinkScan = true
	firstReader.MaterializedSource = source
	firstReader.MaterializedReaderID = 0
	require.NoError(t, firstReader.Prepare(proc))
	secondReader := merge.NewArgument()
	defer secondReader.Release()
	secondReader.SinkScan = true
	secondReader.MaterializedSource = source
	secondReader.MaterializedReaderID = 1
	require.NoError(t, secondReader.Prepare(proc))

	// Keep reader 1's first batch live while MergeOrder consumes and spills the
	// same logical producer output through reader 0. A source-owned shared batch
	// would be cleaned by MergeOrder underneath reader 1 here.
	retained, err := secondReader.Call(proc)
	require.NoError(t, err)
	require.Equal(t, int64(3), vector.GetFixedAtNoTypeCheck[int64](retained.Batch.Vecs[0], 0))

	order := mergeorder.NewArgument()
	defer order.Release()
	order.SpillThreshold = 1
	order.OrderBySpecs = []*plan.OrderBySpec{{
		Expr: &plan.Expr{
			Typ:  plan.Type{Id: int32(types.T_int64)},
			Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 0, ColPos: 0}},
		},
	}}
	order.AppendChild(firstReader)
	require.NoError(t, order.Prepare(proc))

	var sorted []int64
	for {
		result, err := vm.Exec(order, proc)
		require.NoError(t, err)
		if result.Batch != nil {
			sorted = append(sorted, vector.MustFixedColWithTypeCheck[int64](result.Batch.Vecs[0])...)
		}
		if result.Status == vm.ExecStop {
			break
		}
	}
	require.Equal(t, []int64{1, 2, 3}, sorted)
	require.Positive(t, order.GetOperatorBase().OpAnalyzer.GetOpStats().SpillSize)
	order.Free(proc, false, nil)
	firstReader.Reset(proc, false, nil)

	// MergeOrder's spill path cleaned every batch from reader 0. Reader 1's
	// retained batch and remaining reads must still own independent copies.
	require.Equal(t, int64(3), vector.GetFixedAtNoTypeCheck[int64](retained.Batch.Vecs[0], 0))
	retained.Batch.Clean(proc.Mp())
	for i, value := range []int64{1, 2} {
		result, err := secondReader.Call(proc)
		require.NoError(t, err)
		require.Equal(t, value, vector.GetFixedAtNoTypeCheck[int64](result.Batch.Vecs[0], 0), "remaining position %d", i)
		result.Batch.Clean(proc.Mp())
	}
	result, err := secondReader.Call(proc)
	require.NoError(t, err)
	require.Equal(t, vm.ExecStop, result.Status)
	secondReader.Reset(proc, false, nil)

	require.Equal(t, inputMemory, proc.Mp().CurrNB())
	for _, bat := range inputs {
		bat.Clean(proc.Mp())
	}
}
