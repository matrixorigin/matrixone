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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

// TestPkMoveRetireAddressesTransitions proves the D167 RETIRE transition of
// the genesis epoch registry: a retired address leaves the map in the same
// journaled statement segment as the paired deletes, the journaled
// pre-transition epoch restores byte-exactly on statement rollback, and
// untracked contexts (background reaps, ordinary drops) hit the uniform
// map-miss no-op.
func TestPkMoveRetireAddressesTransitions(t *testing.T) {
	proc := testutil.NewProcess(t)
	addr := pkMoveAcctTbl{account: 7, table: 42}

	t.Run("retire drops the epoch and journals the pre-transition value", func(t *testing.T) {
		txn := &Transaction{proc: proc}
		txn.pkMoveGenesisStates = map[pkMoveAcctTbl]*pkMoveGenesisEpoch{
			addr: {state: pkMoveGenesisOwned, committedLinks: true},
		}
		txn.statementID = 3
		txn.PkMoveRetireAddresses(7, []uint64{42})
		require.NotContains(t, txn.pkMoveGenesisStates, addr,
			"the retired address leaves the epoch map")
		require.Len(t, txn.pkMoveGenesisJournal, 1)
		entry := txn.pkMoveGenesisJournal[0]
		require.Equal(t, pkMoveGenesisJournalRetire, entry.kind)
		require.Equal(t, 3, entry.seq)
		require.True(t, entry.hasPrev)
		require.Equal(t, pkMoveGenesisOwned, entry.prev.state)
		require.True(t, entry.prev.committedLinks)
	})

	t.Run("absent addresses, empty maps, and empty sets are uniform no-ops", func(t *testing.T) {
		txn := &Transaction{proc: proc}
		txn.statementID = 1
		txn.PkMoveRetireAddresses(7, []uint64{42}) // nil map: background reap shape
		require.Empty(t, txn.pkMoveGenesisJournal)
		txn.pkMoveGenesisStates = map[pkMoveAcctTbl]*pkMoveGenesisEpoch{}
		txn.PkMoveRetireAddresses(7, []uint64{42}) // map live, address absent
		require.Empty(t, txn.pkMoveGenesisJournal)
		txn.PkMoveRetireAddresses(7, nil) // nothing asked
		require.Empty(t, txn.pkMoveGenesisJournal)
	})

	t.Run("statement rollback restores the retired epoch byte-exactly", func(t *testing.T) {
		txn := &Transaction{proc: proc}
		txn.pkMoveGenesisStates = map[pkMoveAcctTbl]*pkMoveGenesisEpoch{
			addr: {state: pkMoveGenesisExternal, committedLinks: true},
		}
		txn.statementID = 1
		txn.PkMoveRetireAddresses(7, []uint64{42})
		require.NotContains(t, txn.pkMoveGenesisStates, addr)

		txn.statementID = 0
		require.NoError(t, txn.rollbackPkMoveStmtStateLocked())
		epoch, ok := txn.pkMoveGenesisStates[addr]
		require.True(t, ok, "the pre-statement epoch is restored")
		require.Equal(t, pkMoveGenesisExternal, epoch.state)
		require.True(t, epoch.committedLinks)
		require.Empty(t, txn.pkMoveGenesisJournal)
	})

	t.Run("register then retire in one statement unwinds to ABSENT in reverse order", func(t *testing.T) {
		txn := &Transaction{proc: proc}
		txn.statementID = 5
		txn.pkMoveGenesisStates = map[pkMoveAcctTbl]*pkMoveGenesisEpoch{
			addr: {state: pkMoveGenesisOwned},
		}
		txn.pkMoveGenesisJournal = append(txn.pkMoveGenesisJournal,
			pkMoveGenesisJournalEntry{seq: 5, addr: addr, kind: pkMoveGenesisJournalRegister})
		txn.PkMoveRetireAddresses(7, []uint64{42})
		require.NotContains(t, txn.pkMoveGenesisStates, addr)
		require.Len(t, txn.pkMoveGenesisJournal, 2)

		txn.statementID = 4
		require.NoError(t, txn.rollbackPkMoveStmtStateLocked())
		require.NotContains(t, txn.pkMoveGenesisStates, addr,
			"a forward pass would wrongly restore the statement-staged epoch the retire journaled; reverse order ends at the pre-statement ABSENT")
		require.Empty(t, txn.pkMoveGenesisJournal)
	})

	t.Run("earlier statements' journal entries survive the unwind", func(t *testing.T) {
		txn := &Transaction{proc: proc}
		settled := pkMoveAcctTbl{account: 7, table: 40}
		txn.pkMoveGenesisStates = map[pkMoveAcctTbl]*pkMoveGenesisEpoch{
			settled: {state: pkMoveGenesisOwned},
			addr:    {state: pkMoveGenesisOwned},
		}
		txn.pkMoveGenesisJournal = append(txn.pkMoveGenesisJournal,
			pkMoveGenesisJournalEntry{seq: 0, addr: settled, kind: pkMoveGenesisJournalRegister})
		txn.statementID = 1
		txn.PkMoveRetireAddresses(7, []uint64{42})

		txn.statementID = 0
		require.NoError(t, txn.rollbackPkMoveStmtStateLocked())
		require.Contains(t, txn.pkMoveGenesisStates, addr,
			"the current statement's retire is journaled above seq 0, so the unwind restores the retired epoch")
		require.Equal(t, pkMoveGenesisOwned, txn.pkMoveGenesisStates[addr].state)
		require.Contains(t, txn.pkMoveGenesisStates, settled,
			"the settled statement's registration is beyond statement-rollback reach")
		require.Len(t, txn.pkMoveGenesisJournal, 1)
		require.Equal(t, 0, txn.pkMoveGenesisJournal[0].seq)
	})
}

// TestPkMoveNoteGenesisRefreshTransitions proves the D169 REFRESH ownership
// transfer: a TRACKED EXTERNAL epoch promotes to OWNED with the first-touch
// committed-links memo carried across, OWNED and untracked addresses journal
// nothing, and statement rollback restores the pre-transition EXTERNAL memo.
func TestPkMoveNoteGenesisRefreshTransitions(t *testing.T) {
	proc := testutil.NewProcess(t)
	addr := pkMoveAcctTbl{account: 7, table: 42}

	t.Run("EXTERNAL promotes to OWNED keeping the committed-links memo", func(t *testing.T) {
		txn := &Transaction{proc: proc}
		txn.pkMoveGenesisStates = map[pkMoveAcctTbl]*pkMoveGenesisEpoch{
			addr: {state: pkMoveGenesisExternal, committedLinks: true},
		}
		txn.statementID = 2
		txn.PkMoveNoteGenesisRefresh(7, []uint64{42})
		epoch := txn.pkMoveGenesisStates[addr]
		require.Equal(t, pkMoveGenesisOwned, epoch.state,
			"the refreshing transaction now owns the live row and cancel authority")
		require.True(t, epoch.committedLinks,
			"the first-touch memo rides the promotion: committed links still discipline the prune cancellation")
		require.Len(t, txn.pkMoveGenesisJournal, 1)
		entry := txn.pkMoveGenesisJournal[0]
		require.Equal(t, pkMoveGenesisJournalRefresh, entry.kind)
		require.Equal(t, 2, entry.seq)
		require.True(t, entry.hasPrev)
		require.Equal(t, pkMoveGenesisExternal, entry.prev.state)
		require.True(t, entry.prev.committedLinks)
	})

	t.Run("statement rollback restores the EXTERNAL memo", func(t *testing.T) {
		txn := &Transaction{proc: proc}
		txn.pkMoveGenesisStates = map[pkMoveAcctTbl]*pkMoveGenesisEpoch{
			addr: {state: pkMoveGenesisExternal, committedLinks: true},
		}
		txn.statementID = 1
		txn.PkMoveNoteGenesisRefresh(7, []uint64{42})

		txn.statementID = 0
		require.NoError(t, txn.rollbackPkMoveStmtStateLocked())
		epoch := txn.pkMoveGenesisStates[addr]
		require.Equal(t, pkMoveGenesisExternal, epoch.state)
		require.True(t, epoch.committedLinks)
		require.Empty(t, txn.pkMoveGenesisJournal)
	})

	t.Run("OWNED and untracked addresses journal nothing", func(t *testing.T) {
		txn := &Transaction{proc: proc}
		txn.pkMoveGenesisStates = map[pkMoveAcctTbl]*pkMoveGenesisEpoch{
			addr: {state: pkMoveGenesisOwned, committedLinks: false},
		}
		txn.statementID = 1
		txn.PkMoveNoteGenesisRefresh(7, []uint64{42, 99})
		require.Equal(t, pkMoveGenesisOwned, txn.pkMoveGenesisStates[addr].state,
			"OWNED stays OWNED - the rebound needs no map change")
		require.Empty(t, txn.pkMoveGenesisJournal,
			"no map change journals nothing")

		untracked := &Transaction{proc: proc}
		untracked.PkMoveNoteGenesisRefresh(7, []uint64{42})
		require.Empty(t, untracked.pkMoveGenesisStates)
		require.Empty(t, untracked.pkMoveGenesisJournal,
			"untracked refreshes stay plain unowned funnel upserts")
	})

	t.Run("refresh then retire in one statement unwinds to the pre-statement EXTERNAL", func(t *testing.T) {
		txn := &Transaction{proc: proc}
		txn.pkMoveGenesisStates = map[pkMoveAcctTbl]*pkMoveGenesisEpoch{
			addr: {state: pkMoveGenesisExternal, committedLinks: true},
		}
		txn.statementID = 7
		txn.PkMoveNoteGenesisRefresh(7, []uint64{42})
		txn.PkMoveRetireAddresses(7, []uint64{42})
		require.NotContains(t, txn.pkMoveGenesisStates, addr)
		require.Len(t, txn.pkMoveGenesisJournal, 2)

		txn.statementID = 6
		require.NoError(t, txn.rollbackPkMoveStmtStateLocked())
		epoch, ok := txn.pkMoveGenesisStates[addr]
		require.True(t, ok,
			"reverse-order undo restores the retire's journaled OWNED first, then the refresh's journaled EXTERNAL")
		require.Equal(t, pkMoveGenesisExternal, epoch.state)
		require.True(t, epoch.committedLinks)
		require.Empty(t, txn.pkMoveGenesisJournal)
	})
}
