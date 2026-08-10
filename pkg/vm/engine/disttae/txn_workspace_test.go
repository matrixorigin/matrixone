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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func TestTxnWorkspaceAccessPathsRespectVisibility(t *testing.T) {
	proc := testutil.NewProc(t)
	block1 := types.BuildTestBlockid(1, 1)
	block2 := types.BuildTestBlockid(1, 2)
	row1 := types.NewRowid(&block1, 1)
	row2 := types.NewRowid(&block1, 2)
	row3 := types.NewRowid(&block2, 1)

	var workspace txnWorkspace
	batches := []*batch.Batch{
		newWorkspaceInsertBatch(t, proc, row1, row2),
		newWorkspaceInsertBatch(t, proc, row3),
		newWorkspaceInsertBatch(t, proc, row1),
	}
	defer func() {
		for _, bat := range batches {
			bat.Clean(proc.Mp())
		}
	}()

	workspace.append(Entry{typ: INSERT, databaseId: 7, tableId: 42, bat: batches[0]})
	workspace.append(Entry{typ: INSERT, databaseId: 7, tableId: 99, bat: batches[1]})
	workspace.append(Entry{typ: INSERT, databaseId: 7, tableId: 42, bat: batches[2]})

	require.Equal(t, []int{0}, workspace.visiblePrefix(2).tableEntryIndexes(7, 42))
	require.Equal(t, []int{0, 2}, workspace.visiblePrefix(3).tableEntryIndexes(7, 42))
	require.Equal(t, []int{2}, workspace.view(1, 3).tableEntryIndexes(7, 42))
	require.Nil(t, workspace.visiblePrefix(3).tableEntryIndexes(7, 43))

	require.Equal(t, []int{0}, workspace.visiblePrefix(2).rawInsertEntryIndexes(
		7, 42, map[types.Blockid]bool{block1: true}))
	require.Equal(t, []int{0, 2}, workspace.visiblePrefix(3).rawInsertEntryIndexes(
		7, 42, map[types.Blockid]bool{block1: true}))
	require.Nil(t, workspace.visiblePrefix(3).rawInsertEntryIndexes(
		7, 42, map[types.Blockid]bool{block2: true}))

	workspace.truncate(2)
	require.Equal(t, []int{0}, workspace.visiblePrefix(3).tableEntryIndexes(7, 42))
	require.Equal(t, []int{0}, workspace.visiblePrefix(3).rawInsertEntryIndexes(
		7, 42, map[types.Blockid]bool{block1: true}))
}

func TestTxnWorkspaceMaintainsDerivedIndexes(t *testing.T) {
	proc := testutil.NewProc(t)
	block1 := types.BuildTestBlockid(1, 1)
	block2 := types.BuildTestBlockid(1, 2)
	row1 := types.NewRowid(&block1, 1)
	row2 := types.NewRowid(&block2, 1)

	var workspace txnWorkspace
	batches := []*batch.Batch{
		newWorkspaceInsertBatch(t, proc, row1),
		newWorkspaceInsertBatch(t, proc, row2),
		newWorkspaceInsertBatch(t, proc, row1),
	}
	defer func() {
		for _, bat := range batches {
			bat.Clean(proc.Mp())
		}
	}()

	workspace.append(Entry{typ: INSERT, databaseId: 7, tableId: 42, bat: batches[0]})
	require.Equal(t, []int{0}, workspace.currentView().rawInsertEntryIndexes(
		7, 42, map[types.Blockid]bool{block1: true}))
	index := workspace.index

	// Appending to an indexed workspace extends the access paths in place.
	workspace.append(Entry{typ: INSERT, databaseId: 7, tableId: 42, bat: batches[1]})
	require.Same(t, index, workspace.index)
	require.Equal(t, 2, workspace.index.indexedCount)
	require.Equal(t, []int{0, 1}, workspace.currentView().rawInsertEntryIndexes(
		7, 42, map[types.Blockid]bool{block1: true, block2: true}))

	// Reordering is structural: the old index is discarded and rebuilt lazily.
	workspace.stableSortFrom(0, func(a, b Entry) int {
		return int(b.tableId) - int(a.tableId)
	})
	require.Nil(t, workspace.index)
	require.Equal(t, []int{0, 1}, workspace.currentView().tableEntryIndexes(7, 42))
	require.NotSame(t, index, workspace.index)

	// Replacement and truncation also discard derived state and clamp stale
	// visibility boundaries to the physical log.
	workspace.replace([]Entry{
		{typ: INSERT, databaseId: 7, tableId: 99, bat: batches[2]},
		{typ: INSERT, databaseId: 7, tableId: 42, bat: batches[0]},
	})
	require.Nil(t, workspace.index)
	require.Equal(t, []int{1}, workspace.visiblePrefix(10).tableEntryIndexes(7, 42))
	workspace.truncate(1)
	require.Nil(t, workspace.visiblePrefix(10).tableEntryIndexes(7, 42))
}

func TestDeleteTableWritesUsesWorkspaceRowAccessPath(t *testing.T) {
	proc := testutil.NewProc(t)
	targetBlock := types.BuildTestBlockid(7, 1)
	otherBlock := types.BuildTestBlockid(7, 2)
	target1 := types.NewRowid(&targetBlock, 1)
	target2 := types.NewRowid(&targetBlock, 2)
	other := types.NewRowid(&otherBlock, 1)

	targetBatch := newWorkspaceInsertBatch(t, proc, target1, target2)
	otherBatch := newWorkspaceInsertBatch(t, proc, other)
	txn := &Transaction{proc: proc, batchSelectList: make(map[*batch.Batch][]int64)}
	txn.appendWorkspaceEntryLocked(Entry{typ: INSERT, databaseId: 7, tableId: 42, bat: targetBatch})
	txn.appendWorkspaceEntryLocked(Entry{typ: INSERT, databaseId: 7, tableId: 99, bat: otherBatch})
	defer func() {
		for i := range txn.workspace.entries {
			txn.releaseWorkspaceEntryBatchLocked(i)
		}
	}()

	deleted := map[types.Rowid]uint8{target2: 0, other: 0}
	txn.deleteTableWrites(
		7,
		42,
		nil,
		map[types.Blockid]bool{targetBlock: true, otherBlock: true},
		0,
		10,
		deleted)

	require.Equal(t, []int64{1}, txn.batchSelectList[targetBatch])
	require.NotContains(t, txn.batchSelectList, otherBatch)
	require.Equal(t, uint8(1), deleted[target2])
	require.Zero(t, deleted[other])
}

func BenchmarkTxnWorkspaceRawInsertLookup(b *testing.B) {
	for _, entryCount := range []int{1_000, 10_000} {
		b.Run(fmt.Sprintf("entries=%d", entryCount), func(b *testing.B) {
			proc := testutil.NewProc(b)
			workspace, target := buildBenchmarkWorkspace(b, proc, entryCount)
			defer cleanWorkspaceBatches(workspace.entries, proc)

			blocks := map[types.Blockid]bool{target.CloneBlockID(): true}
			// Build the derived access paths outside the timed section. Production
			// pays this once after a structural rewrite and extends them on append.
			require.Len(b, workspace.currentView().rawInsertEntryIndexes(7, 42, blocks), 1)

			b.Run("indexed", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					if len(workspace.currentView().rawInsertEntryIndexes(7, 42, blocks)) != 1 {
						b.Fatal("target row not found")
					}
				}
			})

			b.Run("linear", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					if linearWorkspaceRowLookup(workspace.entries, 7, 42, target) != 1 {
						b.Fatal("target row not found")
					}
				}
			})
		})
	}
}

func newWorkspaceInsertBatch(t testing.TB, proc *process.Process, rows ...types.Rowid) *batch.Batch {
	t.Helper()
	bat := batch.NewWithSize(1)
	bat.SetAttributes([]string{catalog.Row_ID})
	bat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	require.NoError(t, vector.AppendFixedList(bat.Vecs[0], rows, nil, proc.Mp()))
	bat.SetRowCount(len(rows))
	return bat
}

func buildBenchmarkWorkspace(b *testing.B, proc *process.Process, entryCount int) (txnWorkspace, types.Rowid) {
	b.Helper()
	var workspace txnWorkspace
	var target types.Rowid
	for i := 0; i < entryCount; i++ {
		block := types.BuildTestBlockid(1, int64(i+1))
		row := types.NewRowid(&block, 0)
		if i == entryCount-1 {
			target = row
		}
		workspace.append(Entry{
			typ:        INSERT,
			databaseId: 7,
			tableId:    42,
			bat:        newWorkspaceInsertBatch(b, proc, row),
		})
	}
	return workspace, target
}

func linearWorkspaceRowLookup(entries []Entry, databaseID, tableID uint64, target types.Rowid) int {
	found := 0
	for idx := range entries {
		entry := &entries[idx]
		if entry.databaseId != databaseID || entry.tableId != tableID || !isIndexedWorkspaceInsert(entry) {
			continue
		}
		for _, row := range vector.MustFixedColNoTypeCheck[types.Rowid](entry.bat.Vecs[0]) {
			if row == target {
				found++
			}
		}
	}
	return found
}

func cleanWorkspaceBatches(entries []Entry, proc *process.Process) {
	for idx := range entries {
		if entries[idx].bat != nil {
			entries[idx].bat.Clean(proc.Mp())
		}
	}
}
