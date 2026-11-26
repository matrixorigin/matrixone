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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
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

type admissionRow struct {
	id                     uint32
	active                 bool
	oldU, newU, oldV, newV int32
	hitU, hitV             bool
}

func admissionBatch(t *testing.T, mp *mpool.MPool, rows ...admissionRow) *batch.Batch {
	t.Helper()
	bat := batch.NewWithSize(10)
	bat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_bool.ToType())
	for _, i := range []int{2, 3, 4, 6, 7, 8} {
		bat.Vecs[i] = vector.NewVec(types.T_int32.ToType())
	}
	bat.Vecs[5], bat.Vecs[9] = vector.NewVec(types.T_bool.ToType()), vector.NewVec(types.T_bool.ToType())
	for _, row := range rows {
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], updateLedgerRow(row.id), false, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[1], row.active, false, mp))
		for _, pair := range []struct {
			pos   int
			value int32
		}{
			{2, row.oldU}, {3, row.oldU}, {4, row.newU},
			{6, row.oldV}, {7, row.oldV}, {8, row.newV},
		} {
			require.NoError(t, vector.AppendFixed(bat.Vecs[pair.pos], pair.value, false, mp))
		}
		require.NoError(t, vector.AppendFixed(bat.Vecs[5], row.hitU, false, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[9], row.hitV, false, mp))
	}
	bat.SetRowCount(len(rows))
	return bat
}

func admissionOperator(t *testing.T, proc *process.Process) (*UpdateUnique, *mpool.AllocationAccountRegistry, *mpool.AllocationAccount) {
	t.Helper()
	registry, err := mpool.NewAllocationAccountRegistry(1, 1024)
	require.NoError(t, err)
	account, err := registry.Open(1 << 30)
	require.NoError(t, err)
	op := &UpdateUnique{RowID: 0, Active: 1, Keys: []Key{{2, 3, 4, 5}, {6, 7, 8, 9}}}
	require.NoError(t, op.SetAllocationAccount(account))
	t.Cleanup(func() {
		op.Free(proc, false, nil)
		require.NoError(t, op.ClearAllocationAccount(account))
		require.Zero(t, account.Snapshot().Used)
		_, _, err := registry.CompleteTerminal(account)
		require.NoError(t, err)
	})
	return op, registry, account
}

func TestAdmissionDrainsAndChecksAllKeys(t *testing.T) {
	proc := testutil.NewProcess(t)
	op, _, _ := admissionOperator(t, proc)
	budget := proc.GetCTEMemoryBudget()
	op.SpillConfig.Allocation = op.alloc.vector
	op.SpillConfig.Budget.ReserveMemory = func(size uint64) (materialized.Reservation, error) {
		return budget.Reserve(proc.Ctx, size)
	}
	first := admissionBatch(t, proc.Mp(), admissionRow{id: 1, active: true, oldU: 10, newU: 500, oldV: 100, newV: 300, hitV: true})
	second := admissionBatch(t, proc.Mp(), admissionRow{id: 2, active: true, oldU: 20, newU: 500, oldV: 200, newV: 400})
	child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{first, second})
	t.Cleanup(func() { child.Free(proc, false, nil) })
	drained := false
	child.WithEndOfDataCallback(func() { drained = true })
	op.AppendChild(child)
	require.NoError(t, op.Prepare(proc))
	for _, expected := range []bool{false, true} {
		result, err := op.Call(proc)
		require.NoError(t, err)
		require.NotNil(t, op.ctr.outputReservation)
		require.True(t, drained, "no decisions may be published before the child finishes")
		require.Equal(t, 1, result.Batch.RowCount())
		require.Equal(t, expected, vector.GetFixedAtNoTypeCheck[bool](result.Batch.Vecs[10], 0))
	}
	result, err := op.Call(proc)
	require.NoError(t, err)
	require.Nil(t, result.Batch)
	require.Equal(t, vm.ExecStop, result.Status)
	_, used, _ := budget.Snapshot()
	require.Zero(t, used, "decision and output replays must release their batch reservations")
}

func TestAdmissionIncludesInactiveLaterParticipant(t *testing.T) {
	proc := testutil.NewProcess(t)
	op, _, _ := admissionOperator(t, proc)
	first := admissionBatch(t, proc.Mp(), admissionRow{id: 1, active: true, oldU: 10, newU: 20, oldV: 100, newV: 100, hitU: true, hitV: true})
	second := admissionBatch(t, proc.Mp(), admissionRow{id: 2, oldU: 20, newU: 20, oldV: 200, newV: 200})
	child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{first, second})
	t.Cleanup(func() { child.Free(proc, false, nil) })
	op.AppendChild(child)
	require.NoError(t, op.Prepare(proc))
	for range 2 {
		result, err := op.Call(proc)
		require.NoError(t, err)
		require.False(t, vector.GetFixedAtNoTypeCheck[bool](result.Batch.Vecs[10], 0))
	}
}

func TestAdmissionChecksOnlyAcceptedRowsBeforePublishing(t *testing.T) {
	for _, tc := range []struct {
		name        string
		rejectFirst bool
		nullSecond  bool
		wantError   bool
	}{
		{name: "rejected role skips invalid value", rejectFirst: true},
		{name: "later accepted invalid value aborts", nullSecond: true, wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			op, _, account := admissionOperator(t, proc)
			op.Checks = []Check{{Column: 10, Name: "v", NotNull: true}}
			first := admissionBatch(t, proc.Mp(), admissionRow{id: 1, active: true, oldU: 10, newU: 11, oldV: 100, newV: 300, hitV: tc.rejectFirst})
			second := admissionBatch(t, proc.Mp(), admissionRow{id: 2, active: true, oldU: 20, newU: 21, oldV: 200, newV: 400})
			for i, input := range []*batch.Batch{first, second} {
				value := vector.NewVec(types.T_int32.ToType())
				isNull := i == 0 && tc.rejectFirst || i == 1 && tc.nullSecond
				require.NoError(t, vector.AppendFixed(value, int32(1), isNull, proc.Mp()))
				input.Vecs = append(input.Vecs, value)
			}
			child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{first, second})
			t.Cleanup(func() { child.Free(proc, false, nil) })
			op.AppendChild(child)
			require.NoError(t, op.Prepare(proc))
			result, err := op.Call(proc)
			if tc.wantError {
				require.Nil(t, result.Batch)
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrConstraintViolation), err)
				require.Zero(t, account.Snapshot().Used)
				return
			}
			require.NoError(t, err)
			require.False(t, vector.GetFixedAtNoTypeCheck[bool](result.Batch.Vecs[11], 0))
			result, err = op.Call(proc)
			require.NoError(t, err)
			require.True(t, vector.GetFixedAtNoTypeCheck[bool](result.Batch.Vecs[11], 0))
		})
	}
}

func TestAdmissionSelectsTypedValuesIncludingNull(t *testing.T) {
	proc := testutil.NewProcess(t)
	op, _, _ := admissionOperator(t, proc)
	op.Values = []Value{{Current: 10, Proposed: 11}}
	first := admissionBatch(t, proc.Mp(), admissionRow{id: 1, active: true, oldU: 10, newU: 500, oldV: 100, newV: 300, hitV: true})
	second := admissionBatch(t, proc.Mp(), admissionRow{id: 2, active: true, oldU: 20, newU: 500, oldV: 200, newV: 400})
	for _, input := range []*batch.Batch{first, second} {
		current, proposed := vector.NewVec(types.T_geometry32.ToType()), vector.NewVec(types.T_geometry32.ToType())
		require.NoError(t, vector.AppendBytes(current, []byte("old geometry"), false, proc.Mp()))
		require.NoError(t, vector.AppendBytes(proposed, nil, true, proc.Mp()))
		input.Vecs = append(input.Vecs, current, proposed)
	}
	child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{first, second})
	t.Cleanup(func() { child.Free(proc, false, nil) })
	op.AppendChild(child)
	require.NoError(t, op.Prepare(proc))
	result, err := op.Call(proc)
	require.NoError(t, err)
	require.Equal(t, types.T_geometry32, result.Batch.Vecs[13].GetType().Oid)
	require.Equal(t, []byte("old geometry"), result.Batch.Vecs[13].GetRawBytesAt(0))
	result, err = op.Call(proc)
	require.NoError(t, err)
	require.True(t, result.Batch.Vecs[13].IsNull(0), "an accepted NULL must remain NULL")
}

func TestAdmissionWithoutKeysStreams(t *testing.T) {
	proc := testutil.NewProcess(t)
	op, _, _ := admissionOperator(t, proc)
	op.Keys = nil
	op.Values = []Value{{Current: 2, Proposed: 4}}
	input := admissionBatch(t, proc.Mp(),
		admissionRow{id: 1, active: true, oldU: 10, newU: 11},
		admissionRow{id: 2, active: false, oldU: 20, newU: 21},
	)
	drained := false
	child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{input}).WithEndOfDataCallback(func() { drained = true })
	t.Cleanup(func() { child.Free(proc, false, nil) })
	op.AppendChild(child)
	require.NoError(t, op.Prepare(proc))
	result, err := op.Call(proc)
	require.NoError(t, err)
	require.False(t, drained, "an unkeyed role should not buffer all input")
	require.True(t, vector.GetFixedAtNoTypeCheck[bool](result.Batch.Vecs[10], 0))
	require.False(t, vector.GetFixedAtNoTypeCheck[bool](result.Batch.Vecs[10], 1))
	require.Equal(t, int32(11), vector.GetFixedAtNoTypeCheck[int32](result.Batch.Vecs[11], 0))
	require.Equal(t, int32(20), vector.GetFixedAtNoTypeCheck[int32](result.Batch.Vecs[11], 1))
}

type lateFailure struct {
	*colexec.MockOperator
	err error
}

func (child *lateFailure) Call(proc *process.Process) (vm.CallResult, error) {
	result, err := child.MockOperator.Call(proc)
	if err == nil && result.Batch == nil {
		return vm.CancelResult, child.err
	}
	return result, err
}

func TestAdmissionLateErrorAndReuse(t *testing.T) {
	proc := testutil.NewProcess(t)
	op, _, account := admissionOperator(t, proc)
	input := admissionBatch(t, proc.Mp(), admissionRow{id: 1, active: true, oldU: 10, newU: 11, oldV: 100, newV: 101})
	child := &lateFailure{MockOperator: colexec.NewMockOperator().WithBatchs([]*batch.Batch{input}), err: moerr.NewTxnNeedRetry(proc.Ctx)}
	t.Cleanup(func() { child.Free(proc, false, nil) })
	op.AppendChild(child)
	require.NoError(t, op.Prepare(proc))
	result, err := op.Call(proc)
	require.Nil(t, result.Batch)
	require.Same(t, child.err, err)
	require.Zero(t, account.Snapshot().Used, "failed collection must release its state")
	op.Reset(proc, true, err)
	child.Reset(proc, true, err)
	// Reset cleared the input batches; rebuild a fresh execution on the same operator.
	input.Clean(proc.Mp())
	child.ResetBatchs()
	child.WithBatchs([]*batch.Batch{admissionBatch(t, proc.Mp(), admissionRow{id: 2, active: true, oldU: 20, newU: 21, oldV: 200, newV: 201})})
	child.err = nil
	require.NoError(t, op.Prepare(proc))
	result, err = op.Call(proc)
	require.NoError(t, err)
	require.True(t, vector.GetFixedAtNoTypeCheck[bool](result.Batch.Vecs[10], 0))
}

func TestAdmissionCancelledBeforeInput(t *testing.T) {
	proc := testutil.NewProcess(t)
	ctx, cancel := context.WithCancel(proc.Ctx)
	proc.Ctx = ctx
	op, _, account := admissionOperator(t, proc)
	child := colexec.NewMockOperator()
	op.AppendChild(child)
	require.NoError(t, op.Prepare(proc))
	cancel()
	result, err := op.Call(proc)
	require.Nil(t, result.Batch)
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, account.Snapshot().Used)
}
