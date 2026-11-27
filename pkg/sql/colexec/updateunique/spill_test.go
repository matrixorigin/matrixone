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

package updateunique

import (
	"context"
	"os"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/internal/materialized"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

const admissionInMemoryBatchLimit = 4096

type admissionSpillInput struct {
	*colexec.MockOperator
	t       *testing.T
	cancel  context.CancelFunc
	batches int
	count   int
	last    *batch.Batch
}

func (child *admissionSpillInput) Call(proc *process.Process) (vm.CallResult, error) {
	if child.last != nil {
		child.last.Clean(proc.Mp())
		child.last = nil
	}
	if child.count == child.batches {
		if child.cancel != nil {
			child.cancel()
			return vm.CancelResult, context.Canceled
		}
		return vm.CallResult{Status: vm.ExecStop}, nil
	}
	child.count++
	id := uint32(child.count)
	key := int32(child.count)
	child.last = admissionBatch(child.t, proc.Mp(), admissionRow{
		id: id, active: true, oldU: key, newU: key + 10000, oldV: key + 20000, newV: key + 30000,
	})
	return vm.CallResult{Batch: child.last, Status: vm.ExecNext}, nil
}

func configureAdmissionSpill(t *testing.T, proc *process.Process, op *UpdateUnique) (*process.HashBuildBudgetGeneration, func() *os.File) {
	t.Helper()
	proc.Base.Lim.SpillSize = 64 << 20
	budget, err := proc.GetHashBuildBudget()
	require.NoError(t, err)
	var spillFile *os.File
	dir := t.TempDir()
	op.SpillConfig = materialized.SpillConfig{
		FileFactory: func(_ context.Context, name string) (*os.File, error) {
			file, err := os.CreateTemp(dir, name)
			if err != nil {
				return nil, err
			}
			if err = os.Remove(file.Name()); err != nil {
				file.Close()
				return nil, err
			}
			spillFile = file
			return file, nil
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
	}
	return budget, func() *os.File { return spillFile }
}

func TestAdmissionCancellationAfterSpillReleasesResources(t *testing.T) {
	proc := testutil.NewProcess(t)
	ctx, cancel := context.WithCancel(proc.Ctx)
	proc.Ctx = ctx
	t.Cleanup(cancel)
	op, _, account := admissionOperator(t, proc)
	budget, spilledFile := configureAdmissionSpill(t, proc, op)
	child := &admissionSpillInput{
		MockOperator: colexec.NewMockOperator(), t: t, cancel: cancel, batches: admissionInMemoryBatchLimit + 1,
	}
	op.AppendChild(child)
	require.NoError(t, op.Prepare(proc))
	result, err := op.Call(proc)
	require.Nil(t, result.Batch)
	require.ErrorIs(t, err, context.Canceled)
	spillFile := spilledFile()
	require.NotNil(t, spillFile)
	_, err = spillFile.Stat()
	require.ErrorIs(t, err, os.ErrClosed)
	require.Zero(t, budget.SpillDiskUsed())
	require.Zero(t, budget.SpillFDUsed())
	require.Zero(t, account.Snapshot().Used)
	proc.SetStmtProfile(nil)
}

func TestAdmissionSpillReplayOwnsOutputReservation(t *testing.T) {
	proc := testutil.NewProcess(t)
	op, _, account := admissionOperator(t, proc)
	budget, spilledFile := configureAdmissionSpill(t, proc, op)
	op.Values = []Value{{Current: 3, Proposed: 4}, {Current: 7, Proposed: 8}}
	const batches = admissionInMemoryBatchLimit + 2
	op.AppendChild(&admissionSpillInput{MockOperator: colexec.NewMockOperator(), t: t, batches: batches})
	require.NoError(t, op.Prepare(proc))

	// With the production configuration, only decoded spill frames reserve
	// replay memory. The final two batches have the same serialized shape.
	memory := proc.GetCTEMemoryBudget()
	var firstFrameBytes uint64
	for i := 1; i <= batches; i++ {
		result, err := op.Call(proc)
		require.NoError(t, err)
		require.Equal(t, vm.ExecNext, result.Status)
		require.NotNil(t, result.Batch)
		require.Equal(t, 1, result.Batch.RowCount())
		vecs := result.Batch.Vecs
		require.Equal(t, updateLedgerRow(uint32(i)), vector.GetFixedAtNoTypeCheck[types.Rowid](vecs[0], 0))
		require.True(t, vector.GetFixedAtNoTypeCheck[bool](vecs[10], 0))
		require.Equal(t, int32(i+10000), vector.GetFixedAtNoTypeCheck[int32](vecs[11], 0))
		require.Equal(t, int32(i+30000), vector.GetFixedAtNoTypeCheck[int32](vecs[12], 0))
		_, used, _ := memory.Snapshot()
		switch i {
		case admissionInMemoryBatchLimit + 1:
			require.NotNil(t, spilledFile())
			require.Positive(t, budget.SpillDiskUsed())
			require.Positive(t, budget.SpillFDUsed())
			require.Positive(t, used, "the returned spill frame must remain reserved")
			firstFrameBytes = used
		case batches:
			require.Equal(t, firstFrameBytes, used, "advancing must release the preceding spill frame")
		default:
			require.Zero(t, used, "decision replay must release spill frames before output")
		}
	}

	op.Reset(proc, false, nil)
	_, used, _ := memory.Snapshot()
	require.Zero(t, used, "reset must release the last returned spill frame")
	require.Zero(t, budget.SpillDiskUsed())
	require.Zero(t, budget.SpillFDUsed())
	require.Zero(t, account.Snapshot().Used)
	_, err := spilledFile().Stat()
	require.ErrorIs(t, err, os.ErrClosed)
	proc.SetStmtProfile(nil)
}
