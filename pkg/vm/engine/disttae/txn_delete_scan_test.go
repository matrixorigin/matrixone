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

package disttae

import (
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func deleteScanRowIDBatch(t testing.TB, proc *process.Process, ids ...types.Rowid) *batch.Batch {
	t.Helper()
	bat := batch.NewWithSize(1)
	bat.Attrs = []string{objectio.PhysicalAddr_Attr}
	bat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	require.NoError(t, vector.AppendFixedList(bat.Vecs[0], ids, nil, proc.Mp()))
	bat.SetRowCount(len(ids))
	return bat
}

func TestDeleteBatchPreservesMixedRowOwnership(t *testing.T) {
	proc := testutil.NewProc(t)
	rt := runtime.ServiceRuntime("")
	previous, existed := rt.GetGlobalVariables(runtime.ColexecServer)
	server := colexec.NewServer("")
	rt.SetGlobalVariables(runtime.ColexecServer, server)
	t.Cleanup(func() {
		if existed {
			rt.SetGlobalVariables(runtime.ColexecServer, previous)
		} else {
			rt.CompareAndDeleteGlobalVariables(runtime.ColexecServer, server)
		}
	})

	txn := &Transaction{proc: proc, engine: &Engine{}, workspace: newTxnWorkspace()}
	txn.op = newTxnOperatorForTestWithWorkspace(t, txn)
	t.Cleanup(func() { closeWorkspaceForTest(t, txn) })

	var memory, committed, spilled types.Rowid
	memory.SetSegment(colexec.TxnWorkspaceSegment)
	memory.SetRowOffset(1)
	segment := colexec.TxnWorkspaceSegment
	segment[0] = 1
	committed.SetSegment(segment)
	segment[0] = 2
	spilled.SetSegment(segment)
	spilled.SetRowOffset(3)
	server.PutCnSegment(txn.op.Txn().ID, 42, spilled.BorrowSegmentID(), colexec.TxnWorkspaceUnCommitType)
	t.Cleanup(func() { server.DeleteTxnSegmentIds(txn.op.Txn().ID) })

	insertID := appendWorkspaceEntryForTest(txn, Entry{
		typ: INSERT, databaseId: 7, tableId: 42,
		bat: deleteScanRowIDBatch(t, proc, memory),
	})
	deleting := deleteScanRowIDBatch(t, proc, committed, spilled, memory)
	defer deleting.Clean(proc.Mp())
	out := txn.deleteBatch(deleting, 0, 7, 42)
	require.Equal(t, []types.Rowid{committed}, vector.MustFixedColWithTypeCheck[types.Rowid](out.Vecs[0]))
	require.Equal(t, []int64{0}, mustMutationSelections(t, txn.workspace, insertID))
	hasTombstone, err := txn.workspace.hasTableTombstones(txn.workspace.currentReadView(), 0, 7, 42)
	require.NoError(t, err)
	require.True(t, hasTombstone)
}

func TestDeleteTableWritesOnlySelectsMatchingWorkspaceRows(t *testing.T) {
	proc := testutil.NewProc(t)
	txn := &Transaction{proc: proc, workspace: newTxnWorkspace()}
	t.Cleanup(func() { closeWorkspaceForTest(t, txn) })
	var hit, miss types.Rowid
	hit.SetSegment(colexec.TxnWorkspaceSegment)
	hit.SetRowOffset(1)
	miss = hit
	miss.SetRowOffset(9)

	target := appendWorkspaceEntryForTest(txn, Entry{
		typ: INSERT, accountId: 1, databaseId: 7, tableId: 42,
		bat: deleteScanRowIDBatch(t, proc, hit),
	})
	other := appendWorkspaceEntryForTest(txn, Entry{
		typ: INSERT, accountId: 2, databaseId: 7, tableId: 42,
		bat: deleteScanRowIDBatch(t, proc, miss),
	})
	rows := map[types.Rowid]uint8{hit: 0, miss: 0}
	txn.deleteTableWrites(1, 7, 42, nil, rows)
	require.Equal(t, uint8(1), rows[hit])
	require.Zero(t, rows[miss])
	require.Equal(t, []int64{0}, mustMutationSelections(t, txn.workspace, target))
	require.Empty(t, mustMutationSelections(t, txn.workspace, other))
}

func BenchmarkWorkspaceDeleteRowLookup(b *testing.B) {
	proc := testutil.NewProcess(b)
	for _, unrelated := range []int{0, 1024} {
		b.Run(fmt.Sprintf("unrelated=%d", unrelated), func(b *testing.B) {
			workspace := newTxnWorkspace()
			b.Cleanup(func() { require.NoError(b, workspace.close(proc.Mp())) })
			var rowID types.Rowid
			rowID.SetSegment(colexec.TxnWorkspaceSegment)
			for i := 0; i < unrelated; i++ {
				rowID.SetRowOffset(uint32(i))
				workspace.append(Entry{
					typ: INSERT, accountId: 1, databaseId: 7, tableId: uint64(i + 100),
					bat: deleteScanRowIDBatch(b, proc, rowID),
				})
			}
			rowID.SetRowOffset(uint32(unrelated + 1))
			workspace.append(Entry{
				typ: INSERT, accountId: 1, databaseId: 7, tableId: 42,
				bat: deleteScanRowIDBatch(b, proc, rowID),
			})
			view := workspace.currentReadView()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				entries, indexed, err := workspace.tablePointInsertEntriesByRowIDs(
					view, 1, 7, 42, []types.Rowid{rowID},
				)
				if err != nil || !indexed || len(entries.entries) != 1 {
					b.Fatalf("indexed lookup failed: entries=%v indexed=%t err=%v", entries, indexed, err)
				}
				entries.Close()
			}
		})
	}
}
