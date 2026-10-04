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

package dispatch

import (
	"math"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/internal/materialized"
	"github.com/matrixorigin/matrixone/pkg/testutil"
)

func TestMaterializedDispatchAttributesSourceSpill(t *testing.T) {
	proc := testutil.NewProcess(t)
	t.Cleanup(func() { proc.SetStmtProfile(nil) })
	proc.Base.Lim.SpillSize = 1 << 20
	budget, err := proc.GetExecutionResourceBudget()
	require.NoError(t, err)
	registry, err := mpool.NewAllocationAccountRegistry(1, math.MaxUint64)
	require.NoError(t, err)
	account, err := registry.Open(math.MaxInt64)
	require.NoError(t, err)

	spillDir := t.TempDir()
	source := materialized.NewSource(1)
	t.Cleanup(source.Close)
	require.NoError(t, source.Begin(proc.Mp(), materialized.SpillConfig{
		AllocationAccount: account,
		FileFactory: func(name string) (*os.File, error) {
			file, err := os.CreateTemp(spillDir, name)
			if err == nil {
				err = os.Remove(file.Name())
			}
			return file, err
		},
		Budget: materialized.SpillBudget{
			ReserveMemory: func(size uint64) (materialized.Reservation, error) {
				return proc.GetCTEMemoryBudget().Reserve(proc.Ctx, size)
			},
			ReserveDisk: func(size uint64) (materialized.GrowingReservation, error) {
				return budget.ReserveSpillDisk(size)
			},
			ReserveFD: func(size uint64) (materialized.Reservation, error) {
				return budget.ReserveSpillFD(size)
			},
		},
	}))

	input := batch.NewWithSize(1)
	input.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(input.Vecs[0], int64(42), false, proc.Mp()))
	input.SetRowCount(1)
	t.Cleanup(func() { input.Clean(proc.Mp()) })

	// Reach the source's bounded in-memory batch count without allocating a
	// large fixture; the producer's next append must take the spill path.
	for range 4096 {
		require.NoError(t, source.Append(input))
	}

	producer := NewArgument()
	defer producer.Release()
	producer.FuncId = SendToAllLocalFunc
	producer.MaterializedSource = source
	producer.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{input}))
	require.NoError(t, producer.Prepare(proc))

	result, err := producer.Call(proc)
	require.NoError(t, err)
	require.Same(t, input, result.Batch)
	stats := producer.GetOperatorBase().OpAnalyzer.GetOpStats()
	require.Positive(t, stats.SpillSize)
	require.Equal(t, int64(input.RowCount()), stats.SpillRows)

	producer.Reset(proc, false, nil)
	source.ReleaseReader(0)
}

func TestMaterializedDispatchDoesNotPersistLastBatch(t *testing.T) {
	proc := testutil.NewProcess(t)
	source := materialized.NewSource(1)
	t.Cleanup(source.Close)
	require.NoError(t, source.Begin(proc.Mp()))

	input := batch.NewWithSize(1)
	input.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(input.Vecs[0], int64(42), false, proc.Mp()))
	input.SetRowCount(1)
	last := batch.NewWithSize(1)
	last.Vecs[0] = vector.NewConstNull(types.T_int64.ToType(), 1, proc.Mp())
	last.SetRowCount(1)
	last.SetLast()
	t.Cleanup(func() {
		input.Clean(proc.Mp())
		last.Clean(proc.Mp())
	})

	producer := NewArgument()
	defer producer.Release()
	producer.FuncId = SendToAllLocalFunc
	producer.MaterializedSource = source
	producer.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{input, last}))
	require.NoError(t, producer.Prepare(proc))

	result, err := producer.Call(proc)
	require.NoError(t, err)
	require.Same(t, input, result.Batch)
	result, err = producer.Call(proc)
	require.NoError(t, err)
	require.Same(t, last, result.Batch)
	_, err = producer.Call(proc)
	require.NoError(t, err)
	producer.Reset(proc, false, nil)

	stored, end, err := source.Next(proc.Ctx, 0, 0)
	require.NoError(t, err)
	require.False(t, end)
	require.Equal(t, "42", stored.Vecs[0].String())
	stored.Clean(proc.Mp())
	stored, end, err = source.Next(proc.Ctx, 0, 1)
	require.NoError(t, err)
	require.True(t, end)
	require.Nil(t, stored)
	source.ReleaseReader(0)
}
