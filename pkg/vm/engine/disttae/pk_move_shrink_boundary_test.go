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

// Era-15 (r21) review finding F1: the batchSelectList shrink boundary
// renumbers the surviving workspace row ids (shrinkBatchWithRowids) without
// carrying the pk-move provenance that names them. Two proven moves whose
// replacements share one in-memory INSERT batch, followed by a delete of the
// batch's first row, leave the survivor's recorded replacement identity
// stale: the next prune sees no staged row at the old identity and retires a
// perfectly live link, and the apply side replays the move as DELETE+INSERT,
// letting declared ON DELETE actions destroy dependents of a row that moved.

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

// pkMoveShrinkLinkNewRowID reads the new_row_id cell of row r in a staged
// link entry, or false when the cell is NULL.
func pkMoveShrinkLinkNewRowID(
	t *testing.T, e *Entry, r int,
) (rid types.Rowid, ok bool) {
	t.Helper()
	idx := -1
	for j, attr := range e.bat.Attrs {
		if attr == catalog.DataBranchUpdateLinkColNewRow {
			idx = j
			break
		}
	}
	require.GreaterOrEqual(t, idx, 0)
	vec := e.bat.Vecs[idx]
	if vec.IsNull(uint64(r)) {
		return types.Rowid{}, false
	}
	raw := vec.GetBytesAt(r)
	if len(raw) != types.RowidSize {
		return types.Rowid{}, false
	}
	return types.DecodeFixed[types.Rowid](raw), true
}

func TestPkMoveShrinkRemapsProvenance(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()

	const userTable = uint64(42)

	w0 := pkMoveWorkspaceRowid(0) // first batch row: moved, then deleted
	w1 := pkMoveWorkspaceRowid(1) // second batch row: moved, survives
	dur0 := types.BuildTestRowid(6, 1)
	dur1 := types.BuildTestRowid(6, 2)

	newShrinkTxn := func(t *testing.T) (*Transaction, types.Rowid) {
		txn := &Transaction{proc: proc}
		txn.appendWorkspaceEntryLocked(pkMoveStagedInsertEntry(
			t, mp, 7, userTable, []int64{3, 4}, nil, []types.Rowid{w0, w1}))
		linkEntry := pkMovePruneLinkEntry(t, mp, 7,
			[]uint64{userTable, userTable},
			[]*types.Rowid{&dur0, &dur1},
			[]*types.Rowid{&w0, &w1},
			[]uint64{100, 101})
		txn.appendWorkspaceEntryLocked(linkEntry)

		// Origin and heal bookkeeping that names the survivor's identity.
		txn.pkMoveRowidOrigins = map[types.Rowid]types.Rowid{w1: dur1}
		txn.pkMoveNullHeals = map[string]pkMoveNullHeal{"H1": {rid: w1, stmt: 1}}

		// The swallowed-delete ledger the D82 boundary records for the
		// first row's death at statement 2 (both links staged at 1).
		acct := pkMoveAcctTbl{account: 7, table: userTable}
		txn.pkMoveLinkBirth = map[string]int{"\x00": 1, "\x01": 1}
		txn.pkMoveDeadPks = map[pkMoveAcctTbl]map[string]map[int]struct{}{
			acct: {"\x00": {2: {}}},
		}
		txn.batchSelectList = map[*batch.Batch][]int64{
			txn.writes[0].bat: {0},
		}
		return txn, w1
	}

	t.Run("survivor remap keeps the replacement identity valid", func(t *testing.T) {
		txn, survivor := newShrinkTxn(t)
		genBefore := txn.pkMoveHarvestGen

		txn.shrinkWorkspaceEntryBatchLocked(0, []int64{0})

		// The survivor renumbered onto offset 0 of the same block.
		var shrunk []types.Rowid
		{
			vec := txn.writes[0].bat.Vecs[0]
			shrunk = vector.MustFixedColNoTypeCheck[types.Rowid](vec)
		}
		require.Len(t, shrunk, 1)
		require.Equal(t, pkMoveWorkspaceRowid(0), shrunk[0],
			"the engine renumbers the survivor inside the same block")

		got, ok := pkMoveShrinkLinkNewRowID(t, &txn.writes[1], 1)
		require.True(t, ok, "link B keeps its non-NULL replacement identity")
		require.Equal(t, shrunk[0], got,
			"the shrink carried link B's recorded identity onto the survivor")
		require.Equal(t, dur1, txn.pkMoveRowidOrigins[shrunk[0]],
			"the origin ledger followed the survivor")
		_, stale := txn.pkMoveRowidOrigins[survivor]
		require.False(t, stale, "no provenance names the pre-shrink identity")
		require.Equal(t, shrunk[0], txn.pkMoveNullHeals["H1"].rid,
			"the pending heal binding followed the survivor")
		require.Greater(t, txn.pkMoveHarvestGen, genBefore,
			"folded harvest caches built on the old identities must rebuild")

		for i := range txn.writes {
			txn.releaseWorkspaceEntryBatchLocked(i)
		}
	})

	t.Run("the post-shrink prune keeps the valid link and retires the murdered one", func(t *testing.T) {
		txn, _ := newShrinkTxn(t)
		txn.shrinkWorkspaceEntryBatchLocked(0, []int64{0})

		txn.pruneDeadPkMoveLinkRowsLocked()
		require.Equal(t, []uint64{101}, pkMoveLinkWriteTSes(t, &txn.writes[1]),
			"link B's replacement provably survives; link A's dates from a "+
				"generation the swallowed-delete ledger proves dead")

		for i := range txn.writes {
			txn.releaseWorkspaceEntryBatchLocked(i)
		}
	})
}

// TestPkMoveShrinkRemapRepelsOverlappingLineage is the era-16 (r22) review
// finding F1: shrinking offset 0 out of a multi-row batch chains the remap
// onto itself — survivors w1,w2,w3 renumber onto the very rowid values
// w0,w1,w2 held, so the origin/heal rewrite sees mappings whose targets
// still name other entries' sources. Mutating pkMoveRowidOrigins in place
// while ranging it then lets Go's random iteration order clobber a lineage
// written earlier in the same pass, or re-remap one twice, and the deleted
// victim's origin grafts onto the survivor that inherits its rowid. The
// remap must therefore publish a fresh map built from the untouched source
// and retire every entry naming a removed row. The loop re-seeds fresh maps
// each pass, so iteration orders vary across passes: one order alone
// passing proves nothing.
func TestPkMoveShrinkRemapRepelsOverlappingLineage(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()

	const userTable = uint64(42)

	w0 := pkMoveWorkspaceRowid(0) // deleted by the shrink
	w1 := pkMoveWorkspaceRowid(1)
	w2 := pkMoveWorkspaceRowid(2)
	w3 := pkMoveWorkspaceRowid(3)
	d0 := types.BuildTestRowid(6, 0)
	d1 := types.BuildTestRowid(6, 1)
	d2 := types.BuildTestRowid(6, 2)
	durableOutsider := types.BuildTestRowid(9, 9)

	for pass := 0; pass < 300; pass++ {
		txn := &Transaction{proc: proc}
		txn.appendWorkspaceEntryLocked(pkMoveStagedInsertEntry(
			t, mp, 7, userTable, []int64{3, 4, 5, 6}, nil,
			[]types.Rowid{w0, w1, w2, w3}))
		txn.pkMoveRowidOrigins = map[types.Rowid]types.Rowid{
			w0:              d0, // victim's lineage: retire
			w1:              d1, // kok-only survivor
			w2:              d2, // kok-only survivor
			w3:              w2, // kok && vok survivor (chained composition)
			durableOutsider: w2, // vok-only: durable key, workspace value
		}
		txn.pkMoveNullHeals = map[string]pkMoveNullHeal{
			"H1": {rid: w1, stmt: 1},
			"H2": {rid: w2, stmt: 1},
			"Hd": {rid: w0, stmt: 1}, // victim's binding: retire
		}

		require.NoError(t, txn.shrinkWorkspaceEntryBatchLocked(0, []int64{0}),
			"pass %d", pass)

		// Survivors renumber onto offsets 0,1,2 of the same block — the
		// values w0,w1,w2 held before the shrink.
		require.Equal(t, map[types.Rowid]types.Rowid{
			w0:              d1,
			w1:              d2,
			w2:              w1,
			durableOutsider: w1,
		}, txn.pkMoveRowidOrigins,
			"pass %d: each survivor's lineage rides exactly to its new "+
				"identity, the victim's lineage retires", pass)
		require.Equal(t, map[string]pkMoveNullHeal{
			"H1": {rid: w0, stmt: 1},
			"H2": {rid: w1, stmt: 1},
		}, txn.pkMoveNullHeals,
			"pass %d: heal bindings ride the same remap; the victim's "+
				"binding must not graft onto the inheriting survivor", pass)

		for i := range txn.writes {
			txn.releaseWorkspaceEntryBatchLocked(i)
		}
	}
}
