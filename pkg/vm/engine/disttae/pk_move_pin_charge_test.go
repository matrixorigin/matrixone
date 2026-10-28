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

// Era-15 (r21 F4) witnesses: the pin-retained workspace measure is derived
// from the live workspace at every budget check, never cached, so a
// statement rollback that releases the batches drops the charge with them
// instead of leaving a stale charge behind that spuriously fails the
// transaction's later small updates.

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

// A rejected dump leaves the pin-retained measure observing exactly the
// workspace the rejected statement still holds; once the statement's
// rollback releases those batches (RollbackLastStatement truncates the
// writes tail), the charge dies with them and the retried small update
// stages at the same budget that rejected the transaction mid-flight.
func TestPkMovePinChargeDiesWithItsWorkspace(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()
	const userTable = uint64(42)

	wP := pkMoveWorkspaceRowid(7)
	txn := &Transaction{proc: proc}
	txn.appendWorkspaceEntryLocked(pkMoveStagedInsertEntry(
		t, mp, 7, userTable, []int64{9}, nil, []types.Rowid{wP}))
	txn.appendWorkspaceEntryLocked(pkMovePruneLinkEntry(t, mp, 7,
		[]uint64{userTable}, []*types.Rowid{nil}, []*types.Rowid{&wP},
		[]uint64{700}))

	pinned := txn.pkMoveStagedLinkPinnedRowIDsLocked()
	pinBytes := txn.pkMovePinnedRetainedBytesLocked(pinned)
	require.Positive(t, pinBytes)
	retained := txn.pkMoveLinkStagedBytes + txn.pkMoveBookkeepBytesLocked() + pinBytes

	prevBudget := pkMoveLinkCoordinatorBudget
	t.Cleanup(func() { pkMoveLinkCoordinatorBudget = prevBudget })

	pkMoveLinkCoordinatorBudget = retained - 1
	require.Error(t, txn.checkPkMoveBookkeepBudgetLocked(),
		"the live workspace holds the pinned payload over the bound")
	require.Contains(t, txn.checkPkMoveBookkeepBudgetLocked().Error(),
		"pin-retained workspace bytes")

	// Statement rollback releases the workspace exactly like
	// RollbackLastStatement's truncation of the writes tail.
	for i := range txn.writes {
		txn.releaseWorkspaceEntryBatchLocked(i)
	}
	txn.writes = txn.writes[:0]

	require.NoError(t, txn.checkPkMoveBookkeepBudgetLocked(),
		"the released payload no longer crosses the same bound")
	require.Zero(t, txn.pkMovePinnedRetainedBytesLocked(
		txn.pkMoveStagedLinkPinnedRowIDsLocked()),
		"the live pin measure reads an empty workspace as zero")

	// The retried statement stages its link batch at the tightest budget
	// that fits the batch itself; no residue from the rolled-back
	// statement inflates the charge.
	retry := pkMovePruneLinkEntry(t, mp, 7,
		[]uint64{userTable}, []*types.Rowid{nil}, []*types.Rowid{nil},
		[]uint64{701})
	defer retry.bat.Clean(mp)
	pkMoveLinkCoordinatorBudget = int64(retry.bat.Size())
	require.NoError(t, txn.admitPkMoveLinkBatchLocked(retry.bat),
		"payload released by the rollback must not reject the retry")
}

// The dump gate's own KEEP decision and the budget check share one pin
// measure: batches a successful dump released drop out of the measure
// because the keep predicate no longer holds them, while batches it kept
// stay charged until a later dump or the final prune lets them go.
func TestPkMovePinChargeTracksDumpKeep(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()
	const userTable = uint64(42)

	wKeep := pkMoveWorkspaceRowid(3)
	wFree := pkMoveWorkspaceRowid(4)
	txn := &Transaction{proc: proc}
	txn.appendWorkspaceEntryLocked(pkMoveStagedInsertEntry(
		t, mp, 7, userTable, []int64{9}, nil, []types.Rowid{wKeep}))
	txn.appendWorkspaceEntryLocked(pkMoveStagedInsertEntry(
		t, mp, 7, userTable, []int64{10}, nil, []types.Rowid{wFree}))
	txn.appendWorkspaceEntryLocked(pkMovePruneLinkEntry(t, mp, 7,
		[]uint64{userTable}, []*types.Rowid{nil}, []*types.Rowid{&wKeep},
		[]uint64{700}))

	pinned := txn.pkMoveStagedLinkPinnedRowIDsLocked()
	require.Len(t, pinned, 1, "only the linked inhabitant is pinned")

	withBoth := txn.pkMovePinnedRetainedBytesLocked(pinned)
	require.Positive(t, withBoth)

	// The pin is row-grained while retention is batch-grained: releasing
	// the pinned row's own batch drops the whole charge even though the
	// unpinned sibling is still staged.
	txn.releaseWorkspaceEntryBatchLocked(0)
	txn.writes = append(txn.writes[:0], txn.writes[1:]...)
	require.Zero(t, txn.pkMovePinnedRetainedBytesLocked(pinned),
		"releasing the pinned batch drops its charge immediately")

	for i := range txn.writes {
		txn.releaseWorkspaceEntryBatchLocked(i)
	}
	txn.writes = txn.writes[:0]
}
