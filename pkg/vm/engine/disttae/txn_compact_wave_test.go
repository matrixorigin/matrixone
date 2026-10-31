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

package disttae

import (
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/require"
)

// TestRunCompactWaveCollectsWorkerPanic pins the r26 review F1 recovery
// boundary: a panicking compaction worker must surface as a collected wave
// error (failing the enclosing boundary) instead of being swallowed by the
// ants pool's default recovery, and the collection must be complete before
// runCompactWave returns — the error append happens BEFORE Done releases
// the parent wait on the panic path too (the pool PanicHandler runs
// synchronously in the aborted worker's goroutine).
func TestRunCompactWaveCollectsWorkerPanic(t *testing.T) {
	txn := &Transaction{}
	const workers = 3
	slots := make([]compactSlot, workers)
	errs := txn.runCompactWave(slots, func(slot *compactSlot) {
		panic(fmt.Sprintf("boom-%p", slot))
	})
	require.Len(t, errs, workers,
		"every panicking worker must be collected exactly once")
	for _, err := range errs {
		require.Contains(t, err.Error(), "compaction worker aborted")
		require.Contains(t, err.Error(), "boom-")
	}
	// The helper created and released its pool.
	require.Nil(t, txn.compactWorker)
}

// TestRunCompactWaveCollectsOnlyPanickingWorkers drives a mixed wave:
// successful workers mutate their slot and return normally while others
// panic. Only the panics become wave errors, slot mutations from the
// successful workers survive untouched, and every submission completes
// exactly once.
func TestRunCompactWaveCollectsOnlyPanickingWorkers(t *testing.T) {
	txn := &Transaction{}
	const total = 8
	const panicking = 3
	slots := make([]compactSlot, total)
	var tickets atomic.Int64
	var completed atomic.Int64
	errs := txn.runCompactWave(slots, func(slot *compactSlot) {
		defer completed.Add(1)
		if tickets.Add(1) <= panicking {
			slot.writeName = "aborted"
			panic("injected worker abort")
		}
		slot.err = moerr.NewInternalErrorNoCtx("slot-local failure")
	})
	require.EqualValues(t, total, completed.Load(),
		"every submitted worker must run to completion or abort")
	require.Len(t, errs, panicking,
		"only panicking workers contribute wave errors")
	for _, err := range errs {
		require.Contains(t, err.Error(), "compaction worker aborted")
	}
	// Slot-local errors stay on the slots; the wave layer does not join
	// them (the boundary joins slot errors separately).
	aborted := 0
	for i := range slots {
		if slots[i].writeName == "aborted" {
			aborted++
			require.Nil(t, slots[i].err)
		} else {
			require.NotNil(t, slots[i].err)
			require.Contains(t, slots[i].err.Error(), "slot-local failure")
		}
	}
	require.Equal(t, panicking, aborted,
		"exactly the ticketed workers panicked")
}

// TestRunCompactWaveSubmitFailure: a submission rejected by a closed pool
// must also fail the wave with a collected error instead of hanging the
// parent wait (every waiter.Add pairs with exactly one Done).
func TestRunCompactWaveSubmitFailure(t *testing.T) {
	pool, err := ants.NewPool(1)
	require.NoError(t, err)
	pool.Release()
	txn := &Transaction{compactWorker: pool}

	slots := make([]compactSlot, 2)
	errs := txn.runCompactWave(slots, func(slot *compactSlot) {})
	require.Len(t, errs, 2,
		"every rejected submission must be collected")
	for _, err := range errs {
		require.Contains(t, err.Error(), "compaction worker submit failed")
	}
	// A pre-existing (foreign) pool is reused as-is and not released by
	// the helper itself.
	require.NotNil(t, txn.compactWorker)
}

// TestRunCompactWaveEmptyWave: an empty wave must return no errors and
// must not hang waiting for workers that were never submitted.
func TestRunCompactWaveEmptyWave(t *testing.T) {
	txn := &Transaction{}
	errs := txn.runCompactWave(nil, func(slot *compactSlot) {
		panic("no worker may run for an empty wave")
	})
	require.Empty(t, errs)
}
