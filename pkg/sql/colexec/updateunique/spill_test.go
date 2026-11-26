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
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/internal/materialized"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type spillThenCancel struct {
	*colexec.MockOperator
	t      *testing.T
	cancel context.CancelFunc
	count  int
	last   *batch.Batch
	file   **os.File
}

func (child *spillThenCancel) Call(proc *process.Process) (vm.CallResult, error) {
	if child.last != nil {
		child.last.Clean(proc.Mp())
		child.last = nil
	}
	// Exceed the materialized source's in-memory batch limit without creating
	// an oversized batch. The factory assertion proves that spill occurred.
	if child.count == 4097 {
		require.NotNil(child.t, *child.file)
		child.cancel()
		return vm.CancelResult, context.Canceled
	}
	child.count++
	id := uint32(child.count)
	key := int32(child.count)
	child.last = admissionBatch(child.t, proc.Mp(), admissionRow{
		id: id, active: true, oldU: key, newU: key, oldV: key + 10000, newV: key + 10000,
	})
	return vm.CallResult{Batch: child.last, Status: vm.ExecNext}, nil
}

func TestAdmissionCancellationAfterSpillReleasesResources(t *testing.T) {
	proc := testutil.NewProcess(t)
	proc.Base.Lim.SpillSize = 64 << 20
	ctx, cancel := context.WithCancel(proc.Ctx)
	proc.Ctx = ctx
	t.Cleanup(cancel)
	budget, err := proc.GetHashBuildBudget()
	require.NoError(t, err)
	op, _, account := admissionOperator(t, proc)
	var spillFile *os.File
	dir := t.TempDir()
	op.SpillConfig = materialized.SpillConfig{
		FileFactory: func(name string) (*os.File, error) {
			file, err := os.CreateTemp(dir, name)
			if err == nil {
				err = os.Remove(file.Name())
			}
			spillFile = file
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
	}
	child := &spillThenCancel{MockOperator: colexec.NewMockOperator(), t: t, cancel: cancel, file: &spillFile}
	op.AppendChild(child)
	require.NoError(t, op.Prepare(proc))
	result, err := op.Call(proc)
	require.Nil(t, result.Batch)
	require.ErrorIs(t, err, context.Canceled)
	require.NotNil(t, spillFile)
	_, err = spillFile.Stat()
	require.ErrorIs(t, err, os.ErrClosed)
	require.Zero(t, budget.SpillDiskUsed())
	require.Zero(t, budget.SpillFDUsed())
	require.Zero(t, account.Snapshot().Used)
	proc.SetStmtProfile(nil)
}
