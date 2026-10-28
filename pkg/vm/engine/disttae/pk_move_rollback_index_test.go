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

// Era-15 (r21 F6) witnesses: statement rollback restores N retargeted
// staged link rows through ONE staged-link index build (a single linear
// pass) instead of re-scanning every staged link batch per undo record.

import (
	"encoding/binary"
	"errors"
	"strconv"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// pkMoveWideLinkEntry builds one staged INSERT for the link catalog table
// with n rows, distinct 16-byte link ids, per-row old/new row ids and write
// timestamps. newRows[i]==nil stages a NULL new_row_id cell.
func pkMoveWideLinkEntry(
	t testing.TB,
	proc *process.Process,
	accountID uint32,
	n int,
	tableIDs []uint64,
	oldRows, newRows []*types.Rowid,
	nullNew map[int]bool,
) Entry {
	t.Helper()
	mp := proc.Mp()
	bat := batch.NewWithSize(11)
	bat.SetAttributes([]string{
		catalog.Row_ID,
		catalog.DataBranchUpdateLinkColLinkID,
		catalog.DataBranchUpdateLinkColTableID,
		catalog.DataBranchUpdateLinkColOldPKSha,
		catalog.DataBranchUpdateLinkColOldRow,
		catalog.DataBranchUpdateLinkColOldPK,
		catalog.DataBranchUpdateLinkColNewPKSha,
		catalog.DataBranchUpdateLinkColNewPK,
		catalog.DataBranchUpdateLinkColNewRow,
		catalog.DataBranchUpdateLinkColWriteTS,
		catalog.DataBranchUpdateLinkColTxnID,
	})
	bat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_varbinary.ToType())
	bat.Vecs[2] = vector.NewVec(types.T_uint64.ToType())
	bat.Vecs[3] = vector.NewVec(types.T_varbinary.ToType())
	bat.Vecs[4] = vector.NewVec(types.T_varbinary.ToType())
	bat.Vecs[5] = vector.NewVec(types.T_varbinary.ToType())
	bat.Vecs[6] = vector.NewVec(types.T_varbinary.ToType())
	bat.Vecs[7] = vector.NewVec(types.T_varbinary.ToType())
	bat.Vecs[8] = vector.NewVec(types.T_binary.ToType())
	bat.Vecs[9] = vector.NewVec(types.T_uint64.ToType())
	bat.Vecs[10] = vector.NewVec(types.T_binary.ToType())
	for i := 0; i < n; i++ {
		id := make([]byte, 16)
		binary.BigEndian.PutUint64(id[8:], uint64(i))
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], types.BuildTestRowid(9, int64(i+1)), false, mp))
		require.NoError(t, vector.AppendBytes(bat.Vecs[1], id, false, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[2], tableIDs[i], false, mp))
		require.NoError(t, vector.AppendBytes(bat.Vecs[3], id[:8], false, mp))
		if oldRows[i] == nil {
			require.NoError(t, vector.AppendBytes(bat.Vecs[4], nil, true, mp))
		} else {
			require.NoError(t, vector.AppendBytes(bat.Vecs[4], oldRows[i][:], false, mp))
		}
		require.NoError(t, vector.AppendBytes(bat.Vecs[5], id[:8], false, mp))
		require.NoError(t, vector.AppendBytes(bat.Vecs[6], id[:8], false, mp))
		require.NoError(t, vector.AppendBytes(bat.Vecs[7], id[:8], false, mp))
		if nullNew[i] || newRows[i] == nil {
			require.NoError(t, vector.AppendBytes(bat.Vecs[8], nil, true, mp))
		} else {
			require.NoError(t, vector.AppendBytes(bat.Vecs[8], newRows[i][:], false, mp))
		}
		require.NoError(t, vector.AppendFixed(bat.Vecs[9], uint64(i), false, mp))
		require.NoError(t, vector.AppendBytes(bat.Vecs[10], testDisttaePkMoveTxnID(byte(i+1)), false, mp))
	}
	bat.SetRowCount(n)
	return Entry{
		typ:          INSERT,
		accountId:    accountID,
		databaseName: catalog.MO_CATALOG,
		tableName:    catalog.MO_DATA_BRANCH_UPDATE_LINK,
		bat:          bat,
	}
}

// pkMoveRollbackUndoTxn stages n null-new-row link rows plus one non-heal
// retarget undo record per row (prev = BuildTestRowid(8, i)), mirroring a
// statement that nullified every cell the rollback must restore.
func pkMoveRollbackUndoTxn(t testing.TB, proc *process.Process, n int) *Transaction {
	t.Helper()
	const userTable = uint64(42)
	tableIDs := make([]uint64, n)
	oldRows := make([]*types.Rowid, n)
	newRows := make([]*types.Rowid, n)
	for i := range tableIDs {
		tableIDs[i] = userTable
	}
	txn := &Transaction{proc: proc}
	txn.appendWorkspaceEntryLocked(pkMoveWideLinkEntry(
		t, proc, 7, n, tableIDs, oldRows, newRows, nil))
	for i := 0; i < n; i++ {
		id := make([]byte, 16)
		binary.BigEndian.PutUint64(id[8:], uint64(i))
		prev := types.BuildTestRowid(8, int64(i))
		txn.pkMoveRetargetUndo = append(txn.pkMoveRetargetUndo,
			pkMoveRetargetUndoRec{linkID: string(id), prev: prev})
	}
	return txn
}

// pkMoveLinkNewRowCell decodes the new_row_id cell of staged link row r.
func pkMoveLinkNewRowCell(t testing.TB, txn *Transaction, r int) (types.Rowid, bool) {
	t.Helper()
	for i := range txn.writes {
		we := &txn.writes[i]
		if !isPkMoveLinkTableInsert(we.databaseName, we.tableName) ||
			we.typ != INSERT || we.bat == nil {
			continue
		}
		for j, attr := range we.bat.Attrs {
			if attr != catalog.DataBranchUpdateLinkColNewRow {
				continue
			}
			vec := we.bat.Vecs[j]
			if vec.IsNull(uint64(r)) {
				return types.Rowid{}, true
			}
			raw := vec.GetBytesAt(r)
			if len(raw) != types.RowidSize {
				// A faulted rewrite leaves the null bit cleared over an
				// empty cell (SetBytesAt failed after the null clear);
				// that is still "not restored".
				return types.Rowid{}, true
			}
			return types.DecodeFixed[types.Rowid](raw), false
		}
	}
	t.Fatalf("no staged link batch in the workspace")
	return types.Rowid{}, false
}

func TestPkMoveRollbackRestoresRetargetsWithOneScan(t *testing.T) {
	proc := testutil.NewProcess(t)
	const n = 64

	t.Run("one linear pass restores every row", func(t *testing.T) {
		txn := pkMoveRollbackUndoTxn(t, proc, n)
		visits := 0
		prevHook := pkMoveRestoreScanVisitHook
		pkMoveRestoreScanVisitHook = func() { visits++ }
		t.Cleanup(func() { pkMoveRestoreScanVisitHook = prevHook })

		txn.Lock()
		err := txn.rollbackPkMoveStmtStateLocked()
		txn.Unlock()
		require.NoError(t, err)
		require.Equal(t, n, visits,
			"restoring %d retargets costs exactly one pass over the staged rows", n)
		for r := 0; r < n; r++ {
			got, isNull := pkMoveLinkNewRowCell(t, txn, r)
			want := types.BuildTestRowid(8, int64(r))
			require.False(t, isNull, "row %d's null bit must be cleared", r)
			require.Equal(t, want, got, "row %d restored to its pre-statement identity", r)
		}
		require.Empty(t, txn.pkMoveRetargetUndo)
		for i := range txn.writes {
			txn.releaseWorkspaceEntryBatchLocked(i)
		}
		txn.writes = txn.writes[:0]
	})

	t.Run("a faulting cell joins while the rest restore", func(t *testing.T) {
		txn := pkMoveRollbackUndoTxn(t, proc, n)
		sentinel := errors.New("injected new_row rewrite failure")
		calls := 0
		prevSet := pkMoveVecSetBytesAt
		pkMoveVecSetBytesAt = func(vec *vector.Vector, row int, data []byte, mp *mpool.MPool) error {
			calls++
			if calls == 1 {
				return sentinel
			}
			return prevSet(vec, row, data, mp)
		}
		t.Cleanup(func() { pkMoveVecSetBytesAt = prevSet })

		txn.Lock()
		err := txn.rollbackPkMoveStmtStateLocked()
		txn.Unlock()
		require.ErrorIs(t, err, sentinel,
			"the injected failure surfaces through the rollback")
		require.Equal(t, n, calls,
			"one faulting cell must not skip the rest of the undo list")
		// Undo records restore in reverse order: the first rewrite targets
		// row n-1, which therefore keeps its nullified cell.
		got, isNull := pkMoveLinkNewRowCell(t, txn, n-1)
		require.True(t, isNull, "the faulting row keeps its rolled-back cell")
		require.Equal(t, types.Rowid{}, got)
		for r := 0; r < n-1; r++ {
			got, isNull := pkMoveLinkNewRowCell(t, txn, r)
			require.False(t, isNull)
			require.Equal(t, types.BuildTestRowid(8, int64(r)), got,
				"row %d restored despite the sibling failure", r)
		}
		for i := range txn.writes {
			txn.releaseWorkspaceEntryBatchLocked(i)
		}
		txn.writes = txn.writes[:0]
	})
}

// Linear-work scaling witness: the pre-fix shape re-scanned every staged
// link batch per undo record and grew ~4x per doubling of N; the indexed
// restore builds once and stays near-flat. The archived pre/post pair
// lives in /tmp/e15_red_bench_f6.log and /tmp/e15_green_bench_f6.log.
func BenchmarkPkMoveRollbackRetargetUndo(b *testing.B) {
	proc := testutil.NewProcess(b)
	for _, n := range []int{64, 128, 256, 512} {
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				txn := pkMoveRollbackUndoTxn(b, proc, n)
				b.StartTimer()
				txn.Lock()
				_ = txn.rollbackPkMoveStmtStateLocked()
				txn.Unlock()
			}
		})
	}
}
