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
	"errors"
	"slices"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func retireWorkspaceMutationForTest(t *testing.T, txn *Transaction, idx int) {
	t.Helper()
	entries := workspaceEntriesForTest(t, txn)
	require.Less(t, idx, len(entries))
	entry := entries[idx]
	if entry.bat == nil {
		return
	}
	id := entry.workspaceMutationID
	require.NoError(t, txn.workspace.retireMutations([]workspaceMutationID{id}))
}

func workspaceEntriesForTest(t *testing.T, txn *Transaction) []Entry {
	t.Helper()
	entries, err := txn.workspace.commitEntries()
	require.NoError(t, err)
	defer entries.Close()
	result := make([]Entry, len(entries.entries))
	for idx := range entries.entries {
		result[idx] = entries.entries[idx].Entry
	}
	return result
}

func appendWorkspaceEntryForTest(txn *Transaction, entry Entry) workspaceMutationID {
	return txn.appendWorkspaceEntryLocked(entry)
}

func mustMutationSelections(
	t *testing.T,
	workspace *txnWorkspace,
	id workspaceMutationID,
) []int64 {
	t.Helper()
	selections, err := workspace.mutationSelections(id)
	require.NoError(t, err)
	return selections
}

func closeWorkspaceForTest(t *testing.T, txn *Transaction) {
	t.Helper()
	require.NoError(t, txn.workspace.close(txn.proc.Mp()))
}

func TestWorkspaceMutationOwnersInlineAndPromotion(t *testing.T) {
	var owners workspaceMutationOwners
	first := workspaceMutationID(11)
	second := workspaceMutationID(22)

	require.Zero(t, owners.len())
	owners.add(first)
	owners.add(first)
	require.Equal(t, 1, owners.len())
	require.Nil(t, owners.multiple)
	require.Equal(t, first, owners.singleID)
	require.Equal(t, uint32(2), owners.singleCount)

	owners.add(second)
	require.Equal(t, 2, owners.len())
	require.Equal(t, map[workspaceMutationID]uint32{
		first:  2,
		second: 1,
	}, owners.multiple)

	require.True(t, owners.remove(first))
	require.Equal(t, map[workspaceMutationID]uint32{
		first:  1,
		second: 1,
	}, owners.multiple)
	require.True(t, owners.remove(first))
	require.Nil(t, owners.multiple)
	require.Equal(t, second, owners.singleID)
	require.Equal(t, uint32(1), owners.singleCount)

	require.False(t, owners.remove(first))
	require.True(t, owners.remove(second))
	require.Zero(t, owners.len())
	require.Zero(t, owners.singleID)
	require.Zero(t, owners.singleCount)
}

func encodedWorkspaceKeysForTest(
	t *testing.T,
	vec *vector.Vector,
) [][]byte {
	t.Helper()
	packer := types.NewPacker()
	defer packer.Close()
	keys := readutil.EncodePrimaryKeyVector(vec, packer)
	cloned := make([][]byte, len(keys))
	for idx := range keys {
		cloned[idx] = slices.Clone(keys[idx])
	}
	return cloned
}

func newWorkspaceDeleteBatchForTest(
	t *testing.T,
	proc *process.Process,
	rows []types.Rowid,
) *batch.Batch {
	t.Helper()
	bat := batch.NewWithSize(1)
	bat.SetAttributes([]string{catalog.Row_ID})
	bat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	require.NoError(t, vector.AppendFixedList(bat.Vecs[0], rows, nil, proc.Mp()))
	bat.SetRowCount(len(rows))
	return bat
}

func newWorkspaceObjectBatchForTest(
	t testing.TB,
	proc *process.Process,
	objectID types.Objectid,
) *batch.Batch {
	t.Helper()
	stats := objectio.NewObjectStatsWithObjectID(
		&objectID, false, false, false)
	require.NoError(t, objectio.SetObjectStatsBlkCnt(stats, 1))

	bat := batch.New([]string{catalog.ObjectMeta_ObjectStats})
	bat.SetVector(0, vector.NewVec(types.T_varchar.ToType()))
	require.NoError(t, vector.AppendBytes(
		bat.Vecs[0], stats.Marshal(), false, proc.Mp()))
	bat.SetRowCount(1)
	return bat
}

func TestWorkspaceEntryViewObjectStatsUsesColumnCardinality(t *testing.T) {
	proc := testutil.NewProc(t)
	objectID := types.Objectid{1}
	stats := objectio.NewObjectStatsWithObjectID(
		&objectID, false, false, false)
	require.NoError(t, objectio.SetObjectStatsBlkCnt(stats, 2))

	bat := batch.New([]string{
		catalog.BlockMeta_BlockInfo,
		catalog.ObjectMeta_ObjectStats,
	})
	bat.SetVector(0, vector.NewVec(types.T_varchar.ToType()))
	bat.SetVector(1, vector.NewVec(types.T_varchar.ToType()))
	for range 2 {
		require.NoError(t, vector.AppendBytes(
			bat.Vecs[0], []byte("block-info"), false, proc.Mp()))
	}
	require.NoError(t, vector.AppendBytes(
		bat.Vecs[1], stats.Marshal(), false, proc.Mp()))
	bat.SetRowCount(2)
	defer bat.Clean(proc.Mp())

	view := workspaceEntryView{Entry: Entry{bat: bat}}
	var actual []objectio.ObjectStats
	require.True(t, view.forEachVisibleObjectStats(func(stats objectio.ObjectStats) {
		actual = append(actual, stats)
	}))
	require.Equal(t, []objectio.ObjectStats{*stats}, actual)
}

func TestTxnWorkspaceDumpScopeUsesStatementAttemptOwnership(t *testing.T) {
	proc := testutil.NewProc(t)
	workspace := newTxnWorkspace()
	workspace.append(Entry{
		accountId: 1, databaseId: 2, tableId: 3,
		bat: newInt64BatchForTest(t, proc, []string{"pk"}, []int64{11}),
	})
	workspace.advanceStatement()
	workspace.append(Entry{
		accountId: 1, databaseId: 2, tableId: 3,
		bat: newInt64BatchForTest(t, proc, []string{"pk"}, []int64{22}),
	})

	current, err := workspace.entriesForDumpScope(workspaceDumpCurrentAttempt())
	require.NoError(t, err)
	require.Len(t, current.entries, 1)
	require.Equal(t, workspaceMutationID(2), current.entries[0].workspaceMutationID)
	current.Close()

	all, err := workspace.entriesForDumpScope(workspaceDumpAll(false))
	require.NoError(t, err)
	require.Len(t, all.entries, 2)
	all.Close()
	require.NoError(t, workspace.close(proc.Mp()))
}

func TestTxnWorkspaceDumpScopeRejectsInvalidKind(t *testing.T) {
	workspace := newTxnWorkspace()
	_, err := workspace.entriesForDumpScope(workspaceDumpScope{})
	require.ErrorContains(t, err, "scope is invalid")
}

func TestTxnWorkspaceDumpScopeUsesIncrementalInsertUsage(t *testing.T) {
	proc := testutil.NewProc(t)
	workspace := newTxnWorkspace()

	first := newInt64BatchForTest(
		t, proc, []string{"pk"}, []int64{11, 12})
	workspace.append(Entry{
		typ: INSERT, accountId: 1, databaseId: 2, tableId: 100,
		bat: first,
	})
	_, err := workspace.advanceStatement()
	require.NoError(t, err)

	second := newInt64BatchForTest(
		t, proc, []string{"pk"}, []int64{21})
	workspace.append(Entry{
		typ: INSERT, accountId: 1, databaseId: 2, tableId: 100,
		bat: second,
	})
	// Catalog entries are retained in total usage but are not eligible for an
	// INSERT spill. File-backed entries have already left memory and are also
	// excluded from this counter.
	workspace.append(Entry{
		typ: INSERT, accountId: 1, databaseId: 2,
		tableId: catalog.MO_TABLES_ID,
		bat:     newInt64BatchForTest(t, proc, []string{"pk"}, []int64{31}),
	})
	workspace.append(Entry{
		typ: INSERT, accountId: 1, databaseId: 2, tableId: 100,
		fileName: "already-spilled", bat: newInt64BatchForTest(
			t, proc, []string{"pk"}, []int64{41}),
	})

	all, err := workspace.inMemoryInsertBytesForDumpScope(
		workspaceDumpAll(false))
	require.NoError(t, err)
	require.Equal(t, uint64(first.Size()+second.Size()), all)

	current, err := workspace.inMemoryInsertBytesForDumpScope(
		workspaceDumpCurrentAttempt())
	require.NoError(t, err)
	require.Equal(t, uint64(second.Size()), current)

	_, err = workspace.inMemoryInsertBytesForDumpScope(workspaceDumpScope{})
	require.ErrorContains(t, err, "scope is invalid")
	require.NoError(t, workspace.validateUsage())
	require.NoError(t, workspace.close(proc.Mp()))
}

func TestTxnWorkspaceCommitOrderCoversEntireStatement(t *testing.T) {
	proc := testutil.NewProc(t)
	workspace := newTxnWorkspace()
	workspace.publishReadView()
	firstScope := workspace.beginWriteAttempt()
	workspace.append(Entry{
		typ: INSERT, accountId: 1, databaseId: 2, tableId: 100,
		bat: newInt64BatchForTest(t, proc, []string{"pk"}, []int64{1}),
	})
	require.NoError(t, workspace.adjustAttempt(firstScope))

	// Catalog metadata can be produced by an internal SQL write scope after
	// the physical user-table mutation. It must nevertheless precede that
	// mutation in the TN precommit stream for the same statement.
	secondScope := workspace.beginWriteAttempt()
	workspace.append(Entry{
		typ: INSERT, accountId: 1, databaseId: catalog.MO_CATALOG_ID,
		tableId: catalog.MO_TABLES_ID,
		bat:     newInt64BatchForTest(t, proc, []string{"pk"}, []int64{2}),
	})
	workspace.append(Entry{
		typ: DELETE, accountId: 1, databaseId: 2, tableId: 100,
		bat: newInt64BatchForTest(t, proc, []string{"pk"}, []int64{3}),
	})

	require.NoError(t, workspace.adjustAttempt(secondScope))
	entries, err := workspace.commitEntries()
	require.NoError(t, err)
	require.Len(t, entries.entries, 3)
	require.Equal(t, uint64(catalog.MO_CATALOG_ID), entries.entries[0].databaseId)
	require.Equal(t, DELETE, entries.entries[1].typ)
	require.Equal(t, INSERT, entries.entries[2].typ)
	entries.Close()
	require.NoError(t, workspace.close(proc.Mp()))
}

func TestTxnWorkspaceCommitOrderDoesNotCrossStatementBoundary(t *testing.T) {
	proc := testutil.NewProc(t)
	workspace := newTxnWorkspace()
	workspace.publishReadView()
	firstScope := workspace.beginWriteAttempt()
	firstID := workspace.append(Entry{
		typ: INSERT, accountId: 1, databaseId: 2, tableId: 100,
		bat: newInt64BatchForTest(t, proc, []string{"pk"}, []int64{1}),
	})
	require.NoError(t, workspace.adjustAttempt(firstScope))
	// A new ordinary Compile advances the protocol-order epoch. The second
	// Compile's catalog mutation must not move before the first Compile's user
	// mutation even if both belong to the same transaction.
	workspace.publishReadView()

	secondScope := workspace.beginWriteAttempt()
	secondID := workspace.append(Entry{
		typ: DELETE, accountId: 1, databaseId: catalog.MO_CATALOG_ID,
		tableId: catalog.MO_TABLES_ID,
		bat:     newInt64BatchForTest(t, proc, []string{"pk"}, []int64{2}),
	})
	require.NoError(t, workspace.adjustAttempt(secondScope))

	entries, err := workspace.commitEntries()
	require.NoError(t, err)
	require.Len(t, entries.entries, 2)
	require.Equal(t, firstID, entries.entries[0].workspaceMutationID)
	require.Equal(t, secondID, entries.entries[1].workspaceMutationID)
	entries.Close()
	require.NoError(t, workspace.close(proc.Mp()))
}

func TestTxnWorkspaceLaterStatementRewriteUsesCurrentCommitOrder(t *testing.T) {
	proc := testutil.NewProc(t)
	workspace := newTxnWorkspace()

	workspace.publishReadView()
	original := newInt64BatchForTest(
		t, proc, []string{"old_pk"}, []int64{1})
	sourceID := workspace.append(Entry{
		typ: INSERT, accountId: 1, databaseId: 2, tableId: 100,
		tableName: "old", bat: original,
	})
	_, err := workspace.advanceStatement()
	require.NoError(t, err)

	// ALTER of a table created earlier in the transaction recreates catalog
	// metadata and rewrites the old table data in one later Compile. The
	// replacement must belong to this Compile's commit epoch; retaining the
	// source epoch would send table data to TN before the recreated relation.
	workspace.publishReadView()
	catalogID := workspace.append(Entry{
		typ: INSERT, accountId: 1, databaseId: catalog.MO_CATALOG_ID,
		tableId: catalog.MO_TABLES_ID,
		bat:     newInt64BatchForTest(t, proc, []string{"pk"}, []int64{2}),
	})
	replacement := newInt64BatchForTest(
		t, proc, []string{"new_pk"}, []int64{1})
	results, err := workspace.rewriteMutations([]workspaceMutationRewrite{{
		mutationID: sourceID,
		oldBat:     original,
		entry: Entry{
			typ: INSERT, accountId: 1, databaseId: 2, tableId: 100,
			tableName: "new", bat: replacement,
		},
	}})
	require.NoError(t, err)
	require.Len(t, results, 1)

	workspace.mu.RLock()
	require.Equal(t, workspace.commitEpoch,
		workspace.mutations[results[0].targetID].commitOrder[0])
	require.NotEqual(t,
		workspace.mutations[sourceID].commitOrder,
		workspace.mutations[results[0].targetID].commitOrder)
	workspace.mu.RUnlock()

	entries, err := workspace.commitEntries()
	require.NoError(t, err)
	require.Len(t, entries.entries, 2)
	require.Equal(t, catalogID, entries.entries[0].workspaceMutationID)
	require.Equal(t, results[0].targetID,
		entries.entries[1].workspaceMutationID)
	entries.Close()
	require.NoError(t, workspace.close(proc.Mp()))
}

func TestTxnWorkspaceSameStatementRewriteRetainsCommitOrder(t *testing.T) {
	proc := testutil.NewProc(t)
	workspace := newTxnWorkspace()
	workspace.publishReadView()

	original := newInt64BatchForTest(
		t, proc, []string{"old_pk"}, []int64{1})
	sourceID := workspace.append(Entry{
		typ: INSERT, accountId: 1, databaseId: 2, tableId: 100,
		tableName: "old", bat: original,
	})
	workspace.mu.RLock()
	sourceOrder := slices.Clone(workspace.mutations[sourceID].commitOrder)
	workspace.mu.RUnlock()

	replacement := newInt64BatchForTest(
		t, proc, []string{"new_pk"}, []int64{1})
	results, err := workspace.rewriteMutations([]workspaceMutationRewrite{{
		mutationID: sourceID,
		oldBat:     original,
		entry: Entry{
			typ: INSERT, accountId: 1, databaseId: 2, tableId: 100,
			tableName: "new", bat: replacement,
		},
	}})
	require.NoError(t, err)
	require.Len(t, results, 1)
	workspace.mu.RLock()
	require.Equal(t, sourceOrder,
		workspace.mutations[results[0].targetID].commitOrder)
	workspace.mu.RUnlock()
	require.NoError(t, workspace.close(proc.Mp()))
}

func TestTxnWorkspaceNestedWriteAttemptsShareStatementCommitOrder(t *testing.T) {
	proc := testutil.NewProc(t)
	workspace := newTxnWorkspace()
	workspace.publishReadView()
	outer := workspace.beginWriteAttempt()
	outerInsertID := workspace.append(Entry{
		typ: INSERT, accountId: 1, databaseId: 2, tableId: 100,
		bat: newInt64BatchForTest(t, proc, []string{"pk"}, []int64{1}),
	})
	inner := workspace.beginWriteAttempt()
	innerDeleteID := workspace.append(Entry{
		typ: DELETE, accountId: 1, databaseId: 2, tableId: 100,
		bat: newInt64BatchForTest(t, proc, []string{"pk"}, []int64{2}),
	})
	require.NoError(t, workspace.adjustAttempt(inner))
	outerDeleteID := workspace.append(Entry{
		typ: DELETE, accountId: 1, databaseId: 2, tableId: 100,
		bat: newInt64BatchForTest(t, proc, []string{"pk"}, []int64{3}),
	})
	require.NoError(t, workspace.adjustAttempt(outer))

	entries, err := workspace.commitEntries()
	require.NoError(t, err)
	require.Len(t, entries.entries, 3)
	require.Equal(t, innerDeleteID, entries.entries[0].workspaceMutationID)
	require.Equal(t, outerDeleteID, entries.entries[1].workspaceMutationID)
	require.Equal(t, outerInsertID, entries.entries[2].workspaceMutationID)
	entries.Close()
	require.NoError(t, workspace.close(proc.Mp()))
}

func TestTxnWorkspaceInternalReadViewKeepsCompileCommitEpoch(t *testing.T) {
	proc := testutil.NewProc(t)
	workspace := newTxnWorkspace()
	workspace.publishReadView()
	outerID := workspace.append(Entry{
		typ: INSERT, accountId: 1, databaseId: 2, tableId: 100,
		bat: newInt64BatchForTest(t, proc, []string{"pk"}, []int64{1}),
	})

	// Internal SQL captures the current view rather than publishing a new one.
	// Its later catalog write therefore shares the outer Compile's epoch.
	workspace.currentReadView()
	internalID := workspace.append(Entry{
		typ: INSERT, accountId: 1, databaseId: catalog.MO_CATALOG_ID,
		tableId: catalog.MO_TABLES_ID,
		bat:     newInt64BatchForTest(t, proc, []string{"pk"}, []int64{2}),
	})

	entries, err := workspace.commitEntries()
	require.NoError(t, err)
	require.Len(t, entries.entries, 2)
	require.Equal(t, internalID, entries.entries[0].workspaceMutationID)
	require.Equal(t, outerID, entries.entries[1].workspaceMutationID)
	entries.Close()
	require.NoError(t, workspace.close(proc.Mp()))
}

func TestTxnWorkspaceAdjustAttemptAllowsOutOfOrderCompletion(t *testing.T) {
	workspace := newTxnWorkspace()
	first := workspace.beginWriteAttempt()
	second := workspace.beginWriteAttempt()
	require.NoError(t, workspace.adjustAttempt(first))
	require.NoError(t, workspace.adjustAttempt(second))
	require.ErrorContains(t, workspace.adjustAttempt(first),
		"write scope is not active")
}

func TestTxnWorkspaceStatementBoundaryRejectsUnfinishedWriteScope(t *testing.T) {
	workspace := newTxnWorkspace()
	mark := workspace.beginWriteAttempt()

	_, err := workspace.advanceStatement()
	require.ErrorContains(t, err, "unfinished write scopes")
	require.Equal(t, uint64(0), workspace.journal.current.statementID)
	require.Equal(t, uint64(1), workspace.journal.current.attemptID)
	require.Equal(t, statementAttemptOpen, workspace.journal.current.state)

	require.NoError(t, workspace.adjustAttempt(mark))
	next, err := workspace.advanceStatement()
	require.NoError(t, err)
	require.Equal(t, uint64(1), next.statementID)
	require.Equal(t, uint64(1), next.attemptID)
}

func TestTxnWorkspaceRetryDiscardsUnfinishedWriteScopes(t *testing.T) {
	workspace := newTxnWorkspace()
	workspace.beginWriteAttempt()

	rollback, err := workspace.rollbackCurrentAttempt()
	require.NoError(t, err)
	rollback.Close()

	retry, err := workspace.advanceStatement()
	require.NoError(t, err)
	require.Equal(t, uint64(0), retry.statementID)
	require.Equal(t, uint64(2), retry.attemptID)
	require.Empty(t, workspace.journal.current.activeWriteScopes)
}

func TestTxnWorkspaceAdjustAttemptDoesNotRewritePublishedCommitOrder(t *testing.T) {
	proc := testutil.NewProc(t)
	workspace := newTxnWorkspace()
	mark := workspace.beginWriteAttempt()
	insertID := workspace.append(Entry{
		typ: INSERT, accountId: 1, databaseId: 2, tableId: 3,
		bat: newInt64BatchForTest(t, proc, []string{"pk"}, []int64{1}),
	})
	deleteID := workspace.append(Entry{
		typ: DELETE, accountId: 1, databaseId: 2, tableId: 3,
		bat: newInt64BatchForTest(t, proc, []string{"pk"}, []int64{2}),
	})
	workspace.mu.RLock()
	insertOrder := slices.Clone(workspace.mutations[insertID].commitOrder)
	deleteOrder := slices.Clone(workspace.mutations[deleteID].commitOrder)
	workspace.mu.RUnlock()

	require.NoError(t, workspace.adjustAttempt(mark))
	workspace.mu.RLock()
	require.Equal(t, insertOrder, workspace.mutations[insertID].commitOrder)
	require.Equal(t, deleteOrder, workspace.mutations[deleteID].commitOrder)
	workspace.mu.RUnlock()
	entries, err := workspace.commitEntries()
	require.NoError(t, err)
	require.Len(t, entries.entries, 2)
	require.Equal(t, DELETE, entries.entries[0].typ)
	require.Equal(t, INSERT, entries.entries[1].typ)
	entries.Close()
	require.NoError(t, workspace.close(proc.Mp()))
}

func TestTxnWorkspaceTableOverlayUsesExactIdentity(t *testing.T) {
	proc := testutil.NewProc(t)
	workspace := newTxnWorkspace()
	workspace.append(Entry{
		accountId:  1,
		databaseId: 2,
		tableId:    3,
		bat:        newInt64BatchForTest(t, proc, []string{"pk"}, []int64{11}),
	})
	workspace.append(Entry{
		accountId:  4,
		databaseId: 2,
		tableId:    3,
		bat:        newInt64BatchForTest(t, proc, []string{"pk"}, []int64{22}),
	})
	workspace.append(Entry{
		accountId:  1,
		databaseId: 5,
		tableId:    3,
		bat:        newInt64BatchForTest(t, proc, []string{"pk"}, []int64{33}),
	})

	entries, err := workspace.tableEntries(workspace.currentReadView(), 1, 2, 3)
	require.NoError(t, err)
	require.Len(t, entries.entries, 1)
	require.Equal(t, uint32(1), entries.entries[0].accountId)
	require.Equal(t, uint64(2), entries.entries[0].databaseId)
	require.Equal(t, uint64(3), entries.entries[0].tableId)
	entries.Close()
	require.NoError(t, workspace.close(proc.Mp()))
}

func TestTxnWorkspacePKCandidateIndexSelectsOnlyRelevantMutations(t *testing.T) {
	proc := testutil.NewProc(t)
	workspace := newTxnWorkspace()
	firstID := workspace.append(Entry{
		typ: INSERT, accountId: 1, databaseId: 2, tableId: 10,
		bat:     newInt64BatchForTest(t, proc, []string{"pk"}, []int64{11}),
		pkCheck: workspacePKCheck{vectorPos: 0, enabled: true},
	})
	firstView := workspace.currentReadView()
	workspace.append(Entry{
		typ: INSERT, accountId: 1, databaseId: 2, tableId: 11,
		fileName: "object", bat: newInt64BatchForTest(
			t, proc, []string{"pk"}, []int64{22}),
	})
	workspace.append(Entry{
		typ: INSERT, accountId: 1, databaseId: catalog.MO_CATALOG_ID,
		tableId: catalog.MO_DATABASE_ID,
		bat:     newInt64BatchForTest(t, proc, []string{"pk"}, []int64{33}),
	})
	workspace.append(Entry{
		typ: ALTER, accountId: 1, databaseId: 2, tableId: 12,
		bat: newInt64BatchForTest(t, proc, []string{"pk"}, []int64{44}),
	})
	secondID := workspace.append(Entry{
		typ: DELETE, accountId: 4, databaseId: 5, tableId: 13,
		bat:     newInt64BatchForTest(t, proc, []string{"pk"}, []int64{55}),
		pkCheck: workspacePKCheck{vectorPos: 0, enabled: true},
	})

	entries, err := workspace.pkCandidateEntries(workspace.currentReadView())
	require.NoError(t, err)
	require.Len(t, entries.entries, 2)
	// Candidate indexes expose the same immutable logical order as every
	// workspace reader. DELETE precedes INSERT inside this write scope; the
	// index must not leak physical mutation-ID order.
	require.Equal(t, []workspaceMutationID{secondID, firstID}, []workspaceMutationID{
		entries.entries[0].workspaceMutationID,
		entries.entries[1].workspaceMutationID,
	})
	entries.Close()

	entries, err = workspace.pkCandidateEntries(firstView)
	require.NoError(t, err)
	require.Len(t, entries.entries, 1)
	require.Equal(t, firstID, entries.entries[0].workspaceMutationID)
	entries.Close()

	require.NoError(t, workspace.retireMutations([]workspaceMutationID{firstID}))
	workspace.mu.RLock()
	firstOverlay := workspace.overlays[workspaceOverlayKey{
		accountID: 1, databaseID: 2, tableID: 10,
	}]
	require.NotNil(t, firstOverlay)
	require.False(t, workspace.activePKCandidates.contains(firstID))
	require.Contains(t, firstOverlay.retiredPKCandidateMutations, firstID)
	workspace.mu.RUnlock()
	entries, err = workspace.pkCandidateEntries(workspace.currentReadView())
	require.NoError(t, err)
	require.Len(t, entries.entries, 1)
	require.Equal(t, secondID, entries.entries[0].workspaceMutationID)
	entries.Close()

	entries, err = workspace.pkCandidateEntries(firstView)
	require.NoError(t, err)
	require.Len(t, entries.entries, 1)
	require.Equal(t, firstID, entries.entries[0].workspaceMutationID)
	entries.Close()
	require.NoError(t, workspace.close(proc.Mp()))
}

func TestTxnWorkspacePointInsertIndexTracksCurrentPayload(t *testing.T) {
	proc := testutil.NewProc(t)
	workspace := newTxnWorkspace()
	first := newInt64BatchForTest(t, proc, []string{"pk"}, []int64{11, 22})
	id := workspace.append(Entry{
		typ: INSERT, accountId: 1, databaseId: 2, tableId: 10,
		bat: first, pkCheck: workspacePKCheck{vectorPos: 0, enabled: true},
		pkIndex: workspacePKIndex{vectorPos: 0, enabled: true},
	})
	keys := encodedWorkspaceKeysForTest(t, first.Vecs[0])
	historical := workspace.currentReadView()

	entries, indexed, err := workspace.tablePointInsertEntries(
		workspace.currentReadView(), 1, 2, 10, keys[0])
	require.NoError(t, err)
	require.True(t, indexed)
	require.Len(t, entries.entries, 1)
	require.Equal(t, id, entries.entries[0].workspaceMutationID)
	entries.Close()
	entries, indexed, err = workspace.tablePointInsertEntries(
		workspace.currentReadView(), 1, 2, 10, keys...)
	require.NoError(t, err)
	require.True(t, indexed)
	// Both keys belong to one mutation; a multi-key lookup must pin and return
	// that payload exactly once.
	require.Len(t, entries.entries, 1)
	require.Equal(t, id, entries.entries[0].workspaceMutationID)
	entries.Close()

	// A logical delete must remove only the selected row from the access index.
	require.NoError(t, workspace.replaceSelections(id, []int64{0}))
	entries, indexed, err = workspace.tablePointInsertEntries(
		workspace.currentReadView(), 1, 2, 10, keys[0])
	require.NoError(t, err)
	require.True(t, indexed)
	require.Empty(t, entries.entries)
	entries.Close()
	entries, indexed, err = workspace.tablePointInsertEntries(
		workspace.currentReadView(), 1, 2, 10, keys[1])
	require.NoError(t, err)
	require.True(t, indexed)
	require.Len(t, entries.entries, 1)
	entries.Close()

	// The point index retains the access facts needed to find the immutable
	// payload generation visible to a statement ReadView.
	entries, indexed, err = workspace.tablePointInsertEntries(
		historical, 1, 2, 10, keys[0])
	require.NoError(t, err)
	require.True(t, indexed)
	require.Len(t, entries.entries, 1)
	require.Equal(t, id, entries.entries[0].workspaceMutationID)
	require.Empty(t, entries.entries[0].selections)
	entries.Close()

	replacement := newInt64BatchForTest(t, proc, []string{"pk"}, []int64{33})
	replacementKey := encodedWorkspaceKeysForTest(t, replacement.Vecs[0])[0]
	require.NoError(t, workspace.replaceMemory(id, first, replacement, nil))
	entries, indexed, err = workspace.tablePointInsertEntries(
		workspace.currentReadView(), 1, 2, 10, keys[1])
	require.NoError(t, err)
	require.True(t, indexed)
	require.Empty(t, entries.entries)
	entries.Close()
	entries, indexed, err = workspace.tablePointInsertEntries(
		workspace.currentReadView(), 1, 2, 10, replacementKey)
	require.NoError(t, err)
	require.True(t, indexed)
	require.Len(t, entries.entries, 1)
	entries.Close()
	// A key introduced after the statement view must not resolve merely because
	// the owning mutation already existed in that view.
	entries, indexed, err = workspace.tablePointInsertEntries(
		historical, 1, 2, 10, replacementKey)
	require.NoError(t, err)
	require.True(t, indexed)
	require.Empty(t, entries.entries)
	entries.Close()

	require.NoError(t, workspace.retireMutations([]workspaceMutationID{id}))
	entries, indexed, err = workspace.tablePointInsertEntries(
		workspace.currentReadView(), 1, 2, 10, replacementKey)
	require.NoError(t, err)
	require.True(t, indexed)
	require.Empty(t, entries.entries)
	entries.Close()
	// Retirement affects later readers only. The still-live statement view
	// continues to resolve the payload generation it published against.
	entries, indexed, err = workspace.tablePointInsertEntries(
		historical, 1, 2, 10, keys[0])
	require.NoError(t, err)
	require.True(t, indexed)
	require.Len(t, entries.entries, 1)
	entries.Close()
	require.NoError(t, workspace.validateUsage())
	require.NoError(t, workspace.close(proc.Mp()))
}

func TestTxnWorkspacePointInsertIndexRejectsIncompleteCoverage(t *testing.T) {
	proc := testutil.NewProc(t)
	workspace := newTxnWorkspace()
	indexed := newInt64BatchForTest(t, proc, []string{"pk"}, []int64{11})
	workspace.append(Entry{
		typ: INSERT, accountId: 1, databaseId: 2, tableId: 10,
		bat: indexed, pkIndex: workspacePKIndex{vectorPos: 0, enabled: true},
	})
	workspace.append(Entry{
		typ: INSERT, accountId: 1, databaseId: 2, tableId: 10,
		bat: newInt64BatchForTest(t, proc, []string{"other"}, []int64{22}),
	})

	key := encodedWorkspaceKeysForTest(t, indexed.Vecs[0])[0]
	entries, complete, err := workspace.tablePointInsertEntries(
		workspace.currentReadView(), 1, 2, 10, key)
	require.NoError(t, err)
	require.False(t, complete)
	require.Nil(t, entries)
	require.NoError(t, workspace.validateUsage())
	require.NoError(t, workspace.close(proc.Mp()))
}

func TestTxnWorkspaceInsertRowIDIndexTracksCurrentPayload(t *testing.T) {
	proc := testutil.NewProc(t)
	workspace := newTxnWorkspace()
	first := newInsertBatchWithRowIDForTest(t, proc, []int64{11, 22})
	firstRowIDs := slices.Clone(
		vector.MustFixedColWithTypeCheck[types.Rowid](first.Vecs[0]))
	id := workspace.append(Entry{
		typ: INSERT, accountId: 1, databaseId: 2, tableId: 10, bat: first,
	})
	historical := workspace.currentReadView()

	entries, indexed, err := workspace.tablePointInsertEntriesByRowIDs(
		workspace.currentReadView(), 1, 2, 10, firstRowIDs[:1])
	require.NoError(t, err)
	require.True(t, indexed)
	require.Len(t, entries.entries, 1)
	require.Equal(t, id, entries.entries[0].workspaceMutationID)
	entries.Close()

	// Selection publication and the RowID index are one workspace transition:
	// the removed row must stop resolving while its sibling remains visible.
	require.NoError(t, workspace.addMutationSelections(id, []int64{0}))
	entries, indexed, err = workspace.tablePointInsertEntriesByRowIDs(
		workspace.currentReadView(), 1, 2, 10, firstRowIDs[:1])
	require.NoError(t, err)
	require.True(t, indexed)
	require.Empty(t, entries.entries)
	entries.Close()
	entries, indexed, err = workspace.tablePointInsertEntriesByRowIDs(
		workspace.currentReadView(), 1, 2, 10, firstRowIDs[1:])
	require.NoError(t, err)
	require.True(t, indexed)
	require.Len(t, entries.entries, 1)
	entries.Close()

	// Historical RowID access resolves the payload generation that existed when
	// the statement ReadView was published.
	entries, indexed, err = workspace.tablePointInsertEntriesByRowIDs(
		historical, 1, 2, 10, firstRowIDs[:1])
	require.NoError(t, err)
	require.True(t, indexed)
	require.Len(t, entries.entries, 1)
	require.Equal(t, id, entries.entries[0].workspaceMutationID)
	require.Empty(t, entries.entries[0].selections)
	entries.Close()

	replacement := newInsertBatchWithRowIDForTest(t, proc, []int64{33})
	replacementRowID := vector.MustFixedColWithTypeCheck[types.Rowid](
		replacement.Vecs[0])[0]
	require.NoError(t, workspace.replaceMemory(id, first, replacement, nil))
	entries, indexed, err = workspace.tablePointInsertEntriesByRowIDs(
		workspace.currentReadView(), 1, 2, 10, firstRowIDs)
	require.NoError(t, err)
	require.True(t, indexed)
	require.Empty(t, entries.entries)
	entries.Close()
	entries, indexed, err = workspace.tablePointInsertEntriesByRowIDs(
		workspace.currentReadView(), 1, 2, 10, []types.Rowid{replacementRowID})
	require.NoError(t, err)
	require.True(t, indexed)
	require.Len(t, entries.entries, 1)
	entries.Close()
	entries, indexed, err = workspace.tablePointInsertEntriesByRowIDs(
		historical, 1, 2, 10, []types.Rowid{replacementRowID})
	require.NoError(t, err)
	require.True(t, indexed)
	require.Empty(t, entries.entries)
	entries.Close()

	require.NoError(t, workspace.retireMutations([]workspaceMutationID{id}))
	entries, indexed, err = workspace.tablePointInsertEntriesByRowIDs(
		workspace.currentReadView(), 1, 2, 10, []types.Rowid{replacementRowID})
	require.NoError(t, err)
	require.True(t, indexed)
	require.Empty(t, entries.entries)
	entries.Close()
	entries, indexed, err = workspace.tablePointInsertEntriesByRowIDs(
		historical, 1, 2, 10, firstRowIDs[:1])
	require.NoError(t, err)
	require.True(t, indexed)
	require.Len(t, entries.entries, 1)
	entries.Close()
	require.NoError(t, workspace.validateUsage())
	require.NoError(t, workspace.close(proc.Mp()))
}

func TestTxnWorkspaceInsertRowIDIndexRejectsIncompleteCoverage(t *testing.T) {
	proc := testutil.NewProc(t)
	workspace := newTxnWorkspace()
	workspace.append(Entry{
		typ: INSERT, accountId: 1, databaseId: 2, tableId: 10,
		bat: newInt64BatchForTest(t, proc, []string{"pk"}, []int64{11}),
	})

	entries, indexed, err := workspace.tablePointInsertEntriesByRowIDs(
		workspace.currentReadView(), 1, 2, 10, []types.Rowid{types.RandomRowid()})
	require.NoError(t, err)
	require.False(t, indexed)
	require.Nil(t, entries)
	require.NoError(t, workspace.validateUsage())
	require.NoError(t, workspace.close(proc.Mp()))
}

func TestTxnWorkspaceMemoryDeleteIndexTracksCurrentPayload(t *testing.T) {
	proc := testutil.NewProc(t)
	workspace := newTxnWorkspace()
	objectID := types.NewObjectid()
	blockID := types.NewBlockidWithObjectID(&objectID, 1)
	otherBlockID := types.NewBlockidWithObjectID(&objectID, 2)
	rows := []types.Rowid{
		types.NewRowid(&blockID, 1),
		types.NewRowid(&blockID, 3),
		types.NewRowid(&otherBlockID, 2),
	}
	deleteBatch := newWorkspaceDeleteBatchForTest(t, proc, rows)
	id := workspace.append(Entry{
		typ: DELETE, accountId: 1, databaseId: 2, tableId: 10,
		bat: deleteBatch,
	})
	historical := workspace.currentReadView()

	deleted, indexed, err := workspace.tableMemoryDeleteOffsets(
		workspace.currentReadView(), 1, 2, 10, blockID, []int64{0, 1, 2, 3})
	require.NoError(t, err)
	require.True(t, indexed)
	require.Equal(t, []int64{1, 3}, deleted)
	deleted, indexed, err = workspace.tableMemoryDeleteOffsets(
		workspace.currentReadView(), 1, 2, 10, blockID, nil)
	require.NoError(t, err)
	require.True(t, indexed)
	require.Equal(t, []int64{1, 3}, deleted)
	deleted, indexed, err = workspace.tableMemoryDeleteOffsets(
		workspace.currentReadView(), 1, 2, 10, blockID, []int64{})
	require.NoError(t, err)
	require.True(t, indexed)
	require.Empty(t, deleted)

	require.NoError(t, workspace.replaceSelections(id, []int64{0}))
	deleted, indexed, err = workspace.tableMemoryDeleteOffsets(
		workspace.currentReadView(), 1, 2, 10, blockID, nil)
	require.NoError(t, err)
	require.True(t, indexed)
	require.Equal(t, []int64{3}, deleted)
	deleted, indexed, err = workspace.tableMemoryDeleteOffsets(
		historical, 1, 2, 10, blockID, nil)
	require.NoError(t, err)
	require.False(t, indexed)
	require.Nil(t, deleted)

	require.NoError(t, workspace.retireMutations([]workspaceMutationID{id}))
	deleted, indexed, err = workspace.tableMemoryDeleteOffsets(
		workspace.currentReadView(), 1, 2, 10, blockID, nil)
	require.NoError(t, err)
	require.True(t, indexed)
	require.Empty(t, deleted)
	require.NoError(t, workspace.validateUsage())
	require.NoError(t, workspace.close(proc.Mp()))
}

func TestTxnWorkspaceCompactionIndexSelectsOnlyInsertDeleteMutations(t *testing.T) {
	proc := testutil.NewProc(t)
	workspace := newTxnWorkspace()
	insertID := workspace.append(Entry{
		typ: INSERT, accountId: 1, databaseId: 2, tableId: 10,
		bat: newInt64BatchForTest(t, proc, []string{"pk"}, []int64{11}),
	})
	workspace.append(Entry{
		typ: ALTER, accountId: 1, databaseId: 2, tableId: 10,
		bat: newInt64BatchForTest(t, proc, []string{"pk"}, []int64{22}),
	})
	deleteID := workspace.append(Entry{
		typ: DELETE, accountId: 2, databaseId: catalog.MO_CATALOG_ID,
		tableId: catalog.MO_TABLES_ID,
		bat:     newInt64BatchForTest(t, proc, []string{"pk"}, []int64{33}),
	})
	objectInsertID := workspace.append(Entry{
		typ: INSERT, accountId: 3, databaseId: 4, tableId: 12,
		fileName: "object", bat: newInt64BatchForTest(
			t, proc, []string{"pk"}, []int64{44}),
	})

	plan, err := workspace.beginCompactionPlan(0)
	require.NoError(t, err)
	require.Equal(t, uint64(3), plan.newInputs)
	entries := plan.entries
	// Catalog mutations precede user-table mutations in protocol order. The
	// two INSERTs retain their publication order inside the INSERT class.
	require.Equal(t,
		[]workspaceMutationID{deleteID, insertID, objectInsertID},
		[]workspaceMutationID{
			entries.entries[0].workspaceMutationID,
			entries.entries[1].workspaceMutationID,
			entries.entries[2].workspaceMutationID,
		},
	)
	plan.Close()

	require.NoError(t, workspace.retireMutations([]workspaceMutationID{deleteID}))
	plan, err = workspace.beginCompactionPlan(0)
	require.NoError(t, err)
	require.Equal(t, uint64(3), plan.newInputs)
	entries = plan.entries
	require.Equal(t,
		[]workspaceMutationID{insertID, objectInsertID},
		[]workspaceMutationID{
			entries.entries[0].workspaceMutationID,
			entries.entries[1].workspaceMutationID,
		},
	)
	plan.Close()
	require.NoError(t, workspace.close(proc.Mp()))
}

func TestTxnWorkspaceCompactionPlanDoesNotRescanEvaluatedHistory(t *testing.T) {
	proc := testutil.NewProc(t)
	workspace := newTxnWorkspace()
	firstID := workspace.append(Entry{
		typ: INSERT, accountId: 1, databaseId: 2, tableId: 10,
		bat: newInt64BatchForTest(t, proc, []string{"pk"}, []int64{11}),
	})
	secondID := workspace.append(Entry{
		typ: DELETE, accountId: 1, databaseId: 2, tableId: 10,
		bat: newInt64BatchForTest(t, proc, []string{"pk"}, []int64{12}),
	})

	firstPlan, err := workspace.beginCompactionPlan(0)
	require.NoError(t, err)
	require.Equal(t, uint64(2), firstPlan.newInputs)
	require.Equal(t,
		[]workspaceMutationID{secondID, firstID},
		firstPlan.mutationIDs,
	)

	// A payload change after the snapshot is newer compaction input. Completing
	// the old plan may consume secondID, but must leave firstID pending.
	require.NoError(t, workspace.replaceSelections(firstID, []int64{0}))
	workspace.completeCompactionPlan(firstPlan, map[workspaceMutationID]struct{}{
		secondID: {},
	})
	firstPlan.Close()
	requeuedPlan, err := workspace.beginCompactionPlan(0)
	require.NoError(t, err)
	require.Equal(t, []workspaceMutationID{firstID}, requeuedPlan.mutationIDs)
	workspace.completeCompactionPlan(requeuedPlan, map[workspaceMutationID]struct{}{
		firstID: {},
	})
	requeuedPlan.Close()

	thirdID := workspace.append(Entry{
		typ: INSERT, accountId: 1, databaseId: 2, tableId: 10,
		bat: newInt64BatchForTest(t, proc, []string{"pk"}, []int64{13}),
	})
	secondPlan, err := workspace.beginCompactionPlan(0)
	require.NoError(t, err)
	require.Equal(t, uint64(1), secondPlan.newInputs)
	require.Equal(t, []workspaceMutationID{thirdID}, secondPlan.mutationIDs)
	secondPlan.Close()

	// Completing an older snapshot must not consume work published after that
	// snapshot.
	workspace.completeCompactionPlan(firstPlan, map[workspaceMutationID]struct{}{
		secondID: {},
	})
	thirdPlan, err := workspace.beginCompactionPlan(0)
	require.NoError(t, err)
	require.Equal(t, []workspaceMutationID{thirdID}, thirdPlan.mutationIDs)
	thirdPlan.Close()
	require.NoError(t, workspace.close(proc.Mp()))
}

func TestTxnWorkspaceCompactionPlanDefersBeforePinningPendingPayloads(t *testing.T) {
	proc := testutil.NewProc(t)
	workspace := newTxnWorkspace()
	for value := int64(1); value <= 3; value++ {
		workspace.append(Entry{
			typ: INSERT, accountId: 1, databaseId: 2, tableId: 10,
			bat: newInt64BatchForTest(t, proc, []string{"pk"}, []int64{value}),
		})
	}

	plan, err := workspace.beginCompactionPlan(4)
	require.NoError(t, err)
	require.Nil(t, plan)

	workspace.append(Entry{
		typ: INSERT, accountId: 1, databaseId: 2, tableId: 10,
		bat: newInt64BatchForTest(t, proc, []string{"pk"}, []int64{4}),
	})
	plan, err = workspace.beginCompactionPlan(4)
	require.NoError(t, err)
	require.NotNil(t, plan)
	require.Equal(t, uint64(4), plan.newInputs)
	require.Len(t, plan.entries.entries, 4)
	plan.Close()
	require.NoError(t, workspace.close(proc.Mp()))
}

func TestTxnWorkspaceBlockMetaIndexSelectsOnlyBlockMetaInsertMutations(t *testing.T) {
	proc := testutil.NewProc(t)
	workspace := newTxnWorkspace()
	blockMetaID := workspace.append(Entry{
		typ: INSERT, accountId: 1, databaseId: 2, tableId: 10,
		bat: newInt64BatchForTest(
			t, proc, []string{catalog.BlockMeta_BlockInfo}, []int64{11}),
	})
	workspace.append(Entry{
		typ: DELETE, accountId: 1, databaseId: 2, tableId: 10,
		bat: newInt64BatchForTest(
			t, proc, []string{catalog.BlockMeta_BlockInfo}, []int64{22}),
	})
	workspace.append(Entry{
		typ: INSERT, accountId: 1, databaseId: 2, tableId: 10,
		bat: newInt64BatchForTest(t, proc, []string{"pk"}, []int64{33}),
	})
	workspace.append(Entry{
		typ: ALTER, accountId: 1, databaseId: 2, tableId: 10,
		bat: newInt64BatchForTest(
			t, proc, []string{catalog.BlockMeta_BlockInfo}, []int64{44}),
	})

	entries, err := workspace.blockMetaEntries()
	require.NoError(t, err)
	require.Len(t, entries.entries, 1)
	require.Equal(t, blockMetaID, entries.entries[0].workspaceMutationID)
	entries.Close()

	require.NoError(t, workspace.retireMutations(
		[]workspaceMutationID{blockMetaID}))
	entries, err = workspace.blockMetaEntries()
	require.NoError(t, err)
	require.Empty(t, entries.entries)
	entries.Close()
	require.NoError(t, workspace.close(proc.Mp()))
}

func TestTxnWorkspaceObjectReferenceIndexTracksLiveMutations(t *testing.T) {
	proc := testutil.NewProc(t)
	workspace := newTxnWorkspace()
	objectID := types.Objectid{1, 2, 3}
	stats := objectio.NewObjectStatsWithObjectID(
		&objectID, false, false, false)
	objectName := stats.ObjectName().String()

	firstID := workspace.append(Entry{
		typ: INSERT, accountId: 1, databaseId: 2, tableId: 10,
		bat: newWorkspaceObjectBatchForTest(t, proc, objectID),
	})
	require.Equal(t, map[string]struct{}{objectName: {}},
		workspace.liveObjectReferences([]string{objectName}, nil))
	require.Empty(t, workspace.liveObjectReferences(
		[]string{objectName},
		func(entry Entry) bool { return entry.tableId == 10 },
	))

	secondID := workspace.append(Entry{
		typ: INSERT, accountId: 1, databaseId: 2, tableId: 11,
		bat: newWorkspaceObjectBatchForTest(t, proc, objectID),
	})
	require.Equal(t, map[string]struct{}{objectName: {}},
		workspace.liveObjectReferences(
			[]string{objectName},
			func(entry Entry) bool { return entry.tableId == 10 },
		))

	require.NoError(t, workspace.retireMutation(secondID))
	require.Empty(t, workspace.liveObjectReferences(
		[]string{objectName},
		func(entry Entry) bool { return entry.tableId == 10 },
	))
	require.Equal(t, map[string]struct{}{objectName: {}},
		workspace.liveObjectReferences([]string{objectName}, nil))

	require.NoError(t, workspace.retireMutation(firstID))
	require.Empty(t,
		workspace.liveObjectReferences([]string{objectName}, nil))
	require.NoError(t, workspace.close(proc.Mp()))
}

func TestTxnWorkspaceObjectStatsUseCurrentTableOverlay(t *testing.T) {
	proc := testutil.NewProc(t)
	workspace := newTxnWorkspace()
	beforeObjects := workspace.currentReadView()
	firstObjectID := types.Objectid{1, 2, 3}
	secondObjectID := types.Objectid{4, 5, 6}
	tombstoneObjectID := types.Objectid{7, 8, 9}

	firstID := workspace.append(Entry{
		typ: INSERT, accountId: 1, databaseId: 2, tableId: 3,
		bat: newWorkspaceObjectBatchForTest(t, proc, firstObjectID),
	})
	workspace.append(Entry{
		typ: INSERT, accountId: 1, databaseId: 2, tableId: 3,
		bat: newWorkspaceObjectBatchForTest(t, proc, secondObjectID),
	})
	workspace.append(Entry{
		typ: INSERT, accountId: 1, databaseId: 2, tableId: 4,
		bat: newWorkspaceObjectBatchForTest(t, proc, types.Objectid{10}),
	})
	deleteID := workspace.append(Entry{
		typ: DELETE, accountId: 1, databaseId: 2, tableId: 3,
		fileName: "tombstone-object",
		bat:      newWorkspaceObjectBatchForTest(t, proc, tombstoneObjectID),
	})

	objects, indexed, err := workspace.tableObjectStats(
		workspace.currentReadView(), 1, 2, 3, INSERT)
	require.NoError(t, err)
	require.True(t, indexed)
	require.Len(t, objects, 2)
	require.Equal(t, firstObjectID, *objects[0].ObjectName().ObjectId())
	require.Equal(t, secondObjectID, *objects[1].ObjectName().ObjectId())

	tombstones, indexed, err := workspace.tableObjectStats(
		workspace.currentReadView(), 1, 2, 3, DELETE)
	require.NoError(t, err)
	require.True(t, indexed)
	require.Len(t, tombstones, 1)
	require.Equal(t, tombstoneObjectID, *tombstones[0].ObjectName().ObjectId())

	// Current-state indexes cannot answer a historical statement view. The
	// caller must retain the immutable journal path for that visibility domain.
	objects, indexed, err = workspace.tableObjectStats(
		beforeObjects, 1, 2, 3, INSERT)
	require.NoError(t, err)
	require.False(t, indexed)
	require.Empty(t, objects)

	require.NoError(t, workspace.retireMutations(
		[]workspaceMutationID{firstID, deleteID}))
	objects, indexed, err = workspace.tableObjectStats(
		workspace.currentReadView(), 1, 2, 3, INSERT)
	require.NoError(t, err)
	require.True(t, indexed)
	require.Len(t, objects, 1)
	require.Equal(t, secondObjectID, *objects[0].ObjectName().ObjectId())
	tombstones, indexed, err = workspace.tableObjectStats(
		workspace.currentReadView(), 1, 2, 3, DELETE)
	require.NoError(t, err)
	require.True(t, indexed)
	require.Empty(t, tombstones)
	require.NoError(t, workspace.validateUsage())
	require.NoError(t, workspace.close(proc.Mp()))
}

func TestTxnWorkspaceObjectDeletesUseExactTableReadView(t *testing.T) {
	workspace := newTxnWorkspace()
	firstBlock := types.Blockid{1}
	secondBlock := types.Blockid{2}
	before := workspace.currentReadView()

	workspace.appendObjectDelete(1, 2, 3, firstBlock, []int64{4, 5})
	firstView := workspace.currentReadView()
	workspace.appendObjectDelete(1, 2, 3, secondBlock, []int64{6})
	current := workspace.currentReadView()

	deletes, err := workspace.tableObjectDeletes(before, 1, 2, 3)
	require.NoError(t, err)
	require.Empty(t, deletes)

	deletes, err = workspace.tableObjectDeletes(firstView, 1, 2, 3)
	require.NoError(t, err)
	require.Equal(t, map[types.Blockid][]int64{
		firstBlock: {4, 5},
	}, deletes)

	deletes, err = workspace.tableObjectDeletes(current, 1, 2, 3)
	require.NoError(t, err)
	require.Equal(t, map[types.Blockid][]int64{
		firstBlock:  {4, 5},
		secondBlock: {6},
	}, deletes)
	count, err := workspace.tableObjectDeleteCount(current, 1, 2, 3)
	require.NoError(t, err)
	require.Equal(t, uint64(3), count)
	count, err = workspace.tableObjectDeleteCount(firstView, 1, 2, 3)
	require.NoError(t, err)
	require.Equal(t, uint64(2), count)

	for _, identity := range []struct {
		accountID  uint32
		databaseID uint64
		tableID    uint64
	}{
		{accountID: 4, databaseID: 2, tableID: 3},
		{accountID: 1, databaseID: 4, tableID: 3},
		{accountID: 1, databaseID: 2, tableID: 4},
	} {
		deletes, err = workspace.tableObjectDeletes(
			current,
			identity.accountID,
			identity.databaseID,
			identity.tableID,
		)
		require.NoError(t, err)
		require.Empty(t, deletes)
		count, err = workspace.tableObjectDeleteCount(
			current,
			identity.accountID,
			identity.databaseID,
			identity.tableID,
		)
		require.NoError(t, err)
		require.Zero(t, count)
	}
}

func TestTxnWorkspaceTableTombstonesUseExactLogicalReadView(t *testing.T) {
	proc := testutil.NewProc(t)
	workspace := newTxnWorkspace()
	beforeDelete := workspace.currentReadView()

	workspace.append(Entry{
		typ:        DELETE,
		accountId:  1,
		databaseId: 2,
		tableId:    3,
		bat:        newDeleteBatchForTest(t, proc, []int64{11}),
	})
	targetDelete := workspace.currentReadView()
	workspace.append(Entry{
		typ:        DELETE,
		accountId:  1,
		databaseId: 2,
		tableId:    4,
		bat:        newDeleteBatchForTest(t, proc, []int64{22}),
	})

	has, err := workspace.hasTableTombstones(beforeDelete, 1, 2, 3)
	require.NoError(t, err)
	require.False(t, has)

	has, err = workspace.hasTableTombstones(targetDelete, 1, 2, 3)
	require.NoError(t, err)
	require.True(t, has)

	// A delete for another table must not reject this table's snapshot.
	has, err = workspace.hasTableTombstones(
		workspace.currentReadView(), 1, 2, 5)
	require.NoError(t, err)
	require.False(t, has)
	require.NoError(t, workspace.close(proc.Mp()))
}

func TestTxnWorkspaceTableTombstonesIncludePendingObjectDeletes(t *testing.T) {
	workspace := newTxnWorkspace()
	beforeDelete := workspace.currentReadView()
	workspace.appendObjectDelete(
		1, 2, 3, types.Blockid{1}, []int64{4})
	afterDelete := workspace.currentReadView()

	has, err := workspace.hasTableTombstones(beforeDelete, 1, 2, 3)
	require.NoError(t, err)
	require.False(t, has)
	has, err = workspace.hasTableTombstones(afterDelete, 1, 2, 3)
	require.NoError(t, err)
	require.True(t, has)
	has, err = workspace.hasTableTombstones(afterDelete, 1, 2, 4)
	require.NoError(t, err)
	require.False(t, has)
}

func TestTxnWorkspaceRollbackObjectDeletesPreservesReadViews(t *testing.T) {
	workspace := newTxnWorkspace()
	blockID := types.Blockid{1}
	workspace.appendObjectDelete(1, 2, 3, blockID, []int64{4, 5})
	beforeRollback := workspace.currentReadView()

	rollback, err := workspace.rollbackCurrentAttempt()
	require.NoError(t, err)
	rollback.Close()
	require.Empty(t, workspace.activeObjectDeletes)
	require.Empty(t, workspace.snapshotObjectDeletes().ids)

	deletes, err := workspace.tableObjectDeletes(
		workspace.currentReadView(), 1, 2, 3)
	require.NoError(t, err)
	require.Empty(t, deletes)

	deletes, err = workspace.tableObjectDeletes(beforeRollback, 1, 2, 3)
	require.NoError(t, err)
	require.Equal(t, map[types.Blockid][]int64{
		blockID: {4, 5},
	}, deletes)
}

func TestTxnWorkspaceObjectDeleteSnapshotConsumesOnlyCapturedDeletes(t *testing.T) {
	workspace := newTxnWorkspace()
	firstBlock := types.Blockid{1}
	secondBlock := types.Blockid{2}
	firstID := workspace.appendObjectDelete(1, 2, 3, firstBlock, []int64{4})
	snapshot := workspace.snapshotObjectDeletes()
	secondID := workspace.appendObjectDelete(1, 2, 3, secondBlock, []int64{5})
	beforeConsume := workspace.currentReadView()

	result, err := workspace.transitionMutationsAndConsumeObjectDeletes(
		nil, nil, snapshot)
	require.NoError(t, err)
	require.Empty(t, result.targetIDs)
	require.Equal(t, []workspaceObjectDeleteID{firstID}, snapshot.ids)
	require.Equal(t, map[workspaceObjectDeleteID]struct{}{
		secondID: {},
	}, workspace.activeObjectDeletes)
	require.Equal(t, []workspaceObjectDeleteID{secondID},
		workspace.snapshotObjectDeletes().ids)
	require.Equal(t, map[types.Blockid][]int64{
		firstBlock: {4},
	}, snapshot.blocks)

	deletes, err := workspace.tableObjectDeletes(
		workspace.currentReadView(), 1, 2, 3)
	require.NoError(t, err)
	require.Equal(t, map[types.Blockid][]int64{
		secondBlock: {5},
	}, deletes)

	deletes, err = workspace.tableObjectDeletes(beforeConsume, 1, 2, 3)
	require.NoError(t, err)
	require.Equal(t, map[types.Blockid][]int64{
		firstBlock:  {4},
		secondBlock: {5},
	}, deletes)
	require.Equal(t, workspaceObjectDeleteID(2), secondID)
}

func TestTxnWorkspaceEndStatementExpiresObjectDeleteReadViewAndReclaimsHistory(t *testing.T) {
	workspace := newTxnWorkspace()
	blockID := types.Blockid{1}
	deleteID := workspace.appendObjectDelete(
		1, 2, 3, blockID, []int64{4, 5})
	readView := workspace.currentReadView()
	snapshot := workspace.snapshotObjectDeletes()

	_, err := workspace.transitionMutationsAndConsumeObjectDeletes(
		nil, nil, snapshot)
	require.NoError(t, err)
	require.Contains(t, workspace.retiredObjectDeletes, deleteID)

	// The immutable record remains resolvable for every consumer of the
	// statement-scoped view, even though compaction has retired the active
	// delete from the current overlay.
	deletes, err := workspace.tableObjectDeletes(readView, 1, 2, 3)
	require.NoError(t, err)
	require.Equal(t, map[types.Blockid][]int64{
		blockID: {4, 5},
	}, deletes)

	require.NoError(t, workspace.beginStatementExecution())
	_, err = workspace.endStatementExecution()
	require.NoError(t, err)
	_, err = workspace.tableObjectDeletes(readView, 1, 2, 3)
	require.ErrorContains(t, err, "workspace read view has expired")

	// The completed attempt still owns the delete for statement rollback, so
	// the first boundary may expire visibility but must retain rollback state.
	require.NotNil(t, workspace.objectDeletes[deleteID])
	require.Contains(t, workspace.retiredObjectDeletes, deleteID)

	_, err = workspace.advanceStatement()
	require.NoError(t, err)
	require.NoError(t, workspace.beginStatementExecution())
	_, err = workspace.endStatementExecution()
	require.NoError(t, err)
	require.Nil(t, workspace.objectDeletes[deleteID])
	require.NotContains(t, workspace.retiredObjectDeletes, deleteID)
	overlay := workspace.overlays[workspaceOverlayKey{
		accountID: 1, databaseID: 2, tableID: 3,
	}]
	require.Empty(t, overlay.retiredObjectDeletes)
}

func TestTxnWorkspaceObjectDeleteCompactionUsesExactTableOverlay(t *testing.T) {
	proc := testutil.NewProc(t)
	workspace := newTxnWorkspace()
	objectID := types.NewObjectid()
	blockID := types.NewBlockidWithObjectID(&objectID, 0)
	workspace.append(Entry{
		typ:                INSERT,
		accountId:          1,
		databaseId:         2,
		tableId:            3,
		databaseName:       "db",
		tableName:          "tbl",
		autoIncrEpoch:      7,
		autoIncrEpochKnown: true,
		bat: newWorkspaceObjectBatchForTest(
			t, proc, objectID),
	})
	workspace.appendObjectDelete(1, 2, 3, blockID, []int64{4})

	snapshot, err := workspace.snapshotObjectDeleteCompaction()
	require.NoError(t, err)
	require.Equal(t, workspaceObjectMetadata{
		accountID:          1,
		databaseID:         2,
		tableID:            3,
		databaseName:       "db",
		tableName:          "tbl",
		autoIncrEpoch:      7,
		autoIncrEpochKnown: true,
	}, snapshot.objects[objectID])
	require.NoError(t, workspace.close(proc.Mp()))
}

func TestTxnWorkspaceObjectIndexFollowsRewriteAndRollback(t *testing.T) {
	proc := testutil.NewProc(t)
	workspace := newTxnWorkspace()
	objectID := types.NewObjectid()
	blockID := types.NewBlockidWithObjectID(&objectID, 0)
	originalBat := newWorkspaceObjectBatchForTest(t, proc, objectID)
	sourceID := workspace.append(Entry{
		typ:          INSERT,
		accountId:    1,
		databaseId:   2,
		tableId:      3,
		databaseName: "db",
		tableName:    "original",
		bat:          originalBat,
	})
	workspace.advanceStatement()

	replacementBat := newWorkspaceObjectBatchForTest(t, proc, objectID)
	results, err := workspace.rewriteMutations([]workspaceMutationRewrite{{
		mutationID: sourceID,
		oldBat:     originalBat,
		entry: Entry{
			typ:          INSERT,
			accountId:    1,
			databaseId:   2,
			tableId:      3,
			databaseName: "db",
			tableName:    "replacement",
			bat:          replacementBat,
		},
	}})
	require.NoError(t, err)
	require.Len(t, results, 1)
	workspace.appendObjectDelete(1, 2, 3, blockID, []int64{4})

	snapshot, err := workspace.snapshotObjectDeleteCompaction()
	require.NoError(t, err)
	require.Equal(t, "replacement", snapshot.objects[objectID].tableName)

	rollback, err := workspace.rollbackCurrentAttempt()
	require.NoError(t, err)
	rollback.Close()
	retry, err := workspace.advanceStatement()
	require.NoError(t, err)
	require.Equal(t, uint64(1), retry.statementID)
	require.Equal(t, uint64(2), retry.attemptID)
	workspace.appendObjectDelete(1, 2, 3, blockID, []int64{5})

	snapshot, err = workspace.snapshotObjectDeleteCompaction()
	require.NoError(t, err)
	require.Equal(t, "original", snapshot.objects[objectID].tableName)
	require.NoError(t, workspace.close(proc.Mp()))
}

func TestTxnWorkspaceObjectDeleteCompactionRejectsMultipleActiveOwners(t *testing.T) {
	proc := testutil.NewProc(t)
	workspace := newTxnWorkspace()
	objectID := types.NewObjectid()
	blockID := types.NewBlockidWithObjectID(&objectID, 0)
	for _, tableName := range []string{"first", "second"} {
		workspace.append(Entry{
			typ:          INSERT,
			accountId:    1,
			databaseId:   2,
			tableId:      3,
			databaseName: "db",
			tableName:    tableName,
			bat: newWorkspaceObjectBatchForTest(
				t, proc, objectID),
		})
	}
	workspace.appendObjectDelete(1, 2, 3, blockID, []int64{4})

	_, err := workspace.snapshotObjectDeleteCompaction()
	require.ErrorContains(t, err, "multiple active mutations")
	require.NoError(t, workspace.close(proc.Mp()))
}

func TestTxnWorkspaceDeleteTableCandidatesUseOverlayIndex(t *testing.T) {
	proc := testutil.NewProc(t)
	workspace := newTxnWorkspace()
	workspace.append(Entry{
		typ: INSERT, accountId: 1, databaseId: 2, tableId: 3,
		bat: newInt64BatchForTest(t, proc, []string{"pk"}, []int64{1}),
	})
	memoryDeleteID := workspace.append(Entry{
		typ: DELETE, accountId: 1, databaseId: 2, tableId: 3,
		bat: newInt64BatchForTest(t, proc, []string{"pk"}, []int64{2}),
	})
	workspace.append(Entry{
		typ: DELETE, accountId: 1, databaseId: 2, tableId: 4,
		fileName: "object", skipTransfer: true,
		bat: newInt64BatchForTest(t, proc, []string{"pk"}, []int64{3}),
	})
	workspace.append(Entry{
		typ: DELETE, accountId: 1, databaseId: 2, tableId: 5,
		fileName: "object",
		bat:      newInt64BatchForTest(t, proc, []string{"pk"}, []int64{4}),
	})

	memoryCandidates, err := workspace.deleteTableCandidates(false)
	require.NoError(t, err)
	require.Len(t, memoryCandidates, 1)
	require.Equal(t, uint64(3), memoryCandidates[0].tableId)
	require.Nil(t, memoryCandidates[0].bat)

	objectCandidates, err := workspace.deleteTableCandidates(true)
	require.NoError(t, err)
	require.Len(t, objectCandidates, 1)
	require.Equal(t, uint64(5), objectCandidates[0].tableId)
	require.Nil(t, objectCandidates[0].bat)

	require.NoError(t, workspace.selectAllMutationRows(memoryDeleteID, 1))
	memoryCandidates, err = workspace.deleteTableCandidates(false)
	require.NoError(t, err)
	require.Empty(t, memoryCandidates)

	require.NoError(t, workspace.close(proc.Mp()))
}

func TestTxnWorkspaceReadViewPinsSelectionGeneration(t *testing.T) {
	proc := testutil.NewProc(t)
	workspace := newTxnWorkspace()
	id := workspace.append(Entry{
		accountId:  1,
		databaseId: 2,
		tableId:    3,
		bat:        newInt64BatchForTest(t, proc, []string{"pk"}, []int64{11, 22, 33}),
	})
	before := workspace.currentReadView()
	require.NoError(t, workspace.replaceSelections(id, []int64{2, 1, 2}))
	after := workspace.currentReadView()

	oldEntries, err := workspace.tableEntries(before, 1, 2, 3)
	require.NoError(t, err)
	require.Len(t, oldEntries.entries, 1)
	require.Empty(t, oldEntries.entries[0].selections)

	newEntries, err := workspace.tableEntries(after, 1, 2, 3)
	require.NoError(t, err)
	require.Len(t, newEntries.entries, 1)
	require.Equal(t, []int64{1, 2}, newEntries.entries[0].selections)

	oldEntries.Close()
	newEntries.Close()
	require.NoError(t, workspace.close(proc.Mp()))
}

func TestTxnWorkspaceTransitionPublishesOneAtomicRevision(t *testing.T) {
	proc := testutil.NewProc(t)
	workspace := newTxnWorkspace()
	first := newInt64BatchForTest(t, proc, []string{"pk"}, []int64{11})
	second := newInt64BatchForTest(t, proc, []string{"pk"}, []int64{22})
	firstID := workspace.append(Entry{accountId: 1, databaseId: 2, tableId: 3, bat: first})
	secondID := workspace.append(Entry{accountId: 1, databaseId: 2, tableId: 3, bat: second})
	before := workspace.currentReadView()

	newFirst := newInt64BatchForTest(t, proc, []string{"pk"}, []int64{111})
	newSecond := newInt64BatchForTest(t, proc, []string{"pk"}, []int64{222})
	revision := workspace.revision
	result, err := workspace.transitionMutations(
		[]workspaceMutationTransitionSource{
			{mutationID: firstID, oldBat: first},
			{mutationID: secondID, oldBat: second},
		},
		[]workspaceMutationTransitionTarget{
			{entry: Entry{accountId: 1, databaseId: 2, tableId: 3, bat: newFirst}},
			{entry: Entry{accountId: 1, databaseId: 2, tableId: 3, bat: newSecond}},
		},
	)
	require.NoError(t, err)
	require.Len(t, result.targetIDs, 2)
	require.Equal(t, revision+1, workspace.revision)
	after := workspace.currentReadView()

	oldEntries, err := workspace.tableEntries(before, 1, 2, 3)
	require.NoError(t, err)
	require.Len(t, oldEntries.entries, 2)
	require.Same(t, first, oldEntries.entries[0].bat)
	require.Same(t, second, oldEntries.entries[1].bat)
	newEntries, err := workspace.tableEntries(after, 1, 2, 3)
	require.NoError(t, err)
	require.Len(t, newEntries.entries, 2)
	require.Same(t, newFirst, newEntries.entries[0].bat)
	require.Same(t, newSecond, newEntries.entries[1].bat)

	oldEntries.Close()
	newEntries.Close()
	require.NoError(t, workspace.close(proc.Mp()))
}

func TestTxnWorkspaceTransitionReplacementPreservesCommitPosition(t *testing.T) {
	proc := testutil.NewProc(t)
	workspace := newTxnWorkspace()
	before := newInt64BatchForTest(t, proc, []string{"pk"}, []int64{11})
	source := newInt64BatchForTest(t, proc, []string{"pk"}, []int64{22})
	after := newInt64BatchForTest(t, proc, []string{"pk"}, []int64{33})
	workspace.append(Entry{tableName: "before", bat: before})
	sourceID := workspace.append(Entry{tableName: "source", bat: source})
	workspace.append(Entry{tableName: "after", bat: after})

	replacement := newInt64BatchForTest(t, proc, []string{"pk"}, []int64{222})
	result, err := workspace.transitionMutations(
		[]workspaceMutationTransitionSource{{
			mutationID: sourceID,
			oldBat:     source,
		}},
		[]workspaceMutationTransitionTarget{{
			entry:         Entry{tableName: "replacement", bat: replacement},
			replacementOf: sourceID,
		}},
	)
	require.NoError(t, err)
	require.Len(t, result.targetIDs, 1)

	entries, err := workspace.commitEntries()
	require.NoError(t, err)
	require.Len(t, entries.entries, 3)
	require.Equal(t, "before", entries.entries[0].tableName)
	require.Equal(t, "replacement", entries.entries[1].tableName)
	require.Equal(t, "after", entries.entries[2].tableName)
	entries.Close()
	require.NoError(t, workspace.close(proc.Mp()))
}

func TestTxnWorkspaceNestedSplitPreservesLogicalCommitNeighborhood(t *testing.T) {
	proc := testutil.NewProc(t)
	workspace := newTxnWorkspace()
	source := newInt64BatchForTest(t, proc, []string{"pk"}, []int64{11})
	neighbor := newInt64BatchForTest(t, proc, []string{"pk"}, []int64{22})
	sourceID := workspace.append(Entry{tableName: "source", bat: source})
	workspace.append(Entry{tableName: "neighbor", bat: neighbor})

	left := newInt64BatchForTest(t, proc, []string{"pk"}, []int64{111})
	right := newInt64BatchForTest(t, proc, []string{"pk"}, []int64{112})
	firstSplit, err := workspace.transitionMutations(
		[]workspaceMutationTransitionSource{{
			mutationID: sourceID,
			oldBat:     source,
		}},
		[]workspaceMutationTransitionTarget{
			{
				entry:         Entry{tableName: "left", bat: left},
				replacementOf: sourceID,
			},
			{
				entry:         Entry{tableName: "right", bat: right},
				replacementOf: sourceID,
			},
		},
	)
	require.NoError(t, err)
	require.Len(t, firstSplit.targetIDs, 2)

	leftA := newInt64BatchForTest(t, proc, []string{"pk"}, []int64{1111})
	leftB := newInt64BatchForTest(t, proc, []string{"pk"}, []int64{1112})
	_, err = workspace.transitionMutations(
		[]workspaceMutationTransitionSource{{
			mutationID: firstSplit.targetIDs[0],
			oldBat:     left,
		}},
		[]workspaceMutationTransitionTarget{
			{
				entry:         Entry{tableName: "left-a", bat: leftA},
				replacementOf: firstSplit.targetIDs[0],
			},
			{
				entry:         Entry{tableName: "left-b", bat: leftB},
				replacementOf: firstSplit.targetIDs[0],
			},
		},
	)
	require.NoError(t, err)

	entries, err := workspace.commitEntries()
	require.NoError(t, err)
	require.Len(t, entries.entries, 4)
	require.Equal(t, "left-a", entries.entries[0].tableName)
	require.Equal(t, "left-b", entries.entries[1].tableName)
	require.Equal(t, "right", entries.entries[2].tableName)
	require.Equal(t, "neighbor", entries.entries[3].tableName)
	entries.Close()
	require.NoError(t, workspace.close(proc.Mp()))
}

func TestTxnWorkspaceTransitionRejectsAllOnStaleSource(t *testing.T) {
	proc := testutil.NewProc(t)
	workspace := newTxnWorkspace()
	first := newInt64BatchForTest(t, proc, []string{"pk"}, []int64{11})
	second := newInt64BatchForTest(t, proc, []string{"pk"}, []int64{22})
	firstID := workspace.append(Entry{accountId: 1, databaseId: 2, tableId: 3, bat: first})
	secondID := workspace.append(Entry{accountId: 1, databaseId: 2, tableId: 3, bat: second})
	newFirst := newInt64BatchForTest(t, proc, []string{"pk"}, []int64{111})
	newSecond := newInt64BatchForTest(t, proc, []string{"pk"}, []int64{222})
	stale := newInt64BatchForTest(t, proc, []string{"pk"}, []int64{999})
	revision := workspace.revision

	_, err := workspace.transitionMutations(
		[]workspaceMutationTransitionSource{
			{mutationID: firstID, oldBat: first},
			{mutationID: secondID, oldBat: stale},
		},
		[]workspaceMutationTransitionTarget{
			{entry: Entry{accountId: 1, databaseId: 2, tableId: 3, bat: newFirst}},
			{entry: Entry{accountId: 1, databaseId: 2, tableId: 3, bat: newSecond}},
		},
	)
	require.ErrorContains(t, err, "generation changed")
	require.Equal(t, revision, workspace.revision)
	entries, err := workspace.tableEntries(workspace.currentReadView(), 1, 2, 3)
	require.NoError(t, err)
	require.Len(t, entries.entries, 2)
	require.Same(t, first, entries.entries[0].bat)
	require.Same(t, second, entries.entries[1].bat)
	entries.Close()
	newFirst.Clean(proc.Mp())
	newSecond.Clean(proc.Mp())
	stale.Clean(proc.Mp())
	require.NoError(t, workspace.close(proc.Mp()))
}

func TestTxnWorkspaceRewriteMutationsPublishesMetadataAndPayloadAtomically(t *testing.T) {
	proc := testutil.NewProc(t)
	workspace := newTxnWorkspace()
	first := newInt64BatchForTest(t, proc, []string{"old_pk"}, []int64{11})
	second := newInt64BatchForTest(t, proc, []string{"old_pk"}, []int64{22})
	firstID := workspace.append(Entry{
		accountId: 1, databaseId: 2, tableId: 3, tableName: "old", bat: first,
	})
	secondID := workspace.append(Entry{
		accountId: 1, databaseId: 2, tableId: 3, tableName: "old", bat: second,
	})
	before := workspace.currentReadView()

	newFirst := newInt64BatchForTest(t, proc, []string{"new_pk"}, []int64{111})
	newSecond := newInt64BatchForTest(t, proc, []string{"new_pk"}, []int64{222})
	results, err := workspace.rewriteMutations([]workspaceMutationRewrite{
		{
			mutationID: firstID,
			oldBat:     first,
			entry: Entry{
				accountId: 1, databaseId: 2, tableId: 3,
				tableName: "new", bat: newFirst,
			},
		},
		{
			mutationID: secondID,
			oldBat:     second,
			entry: Entry{
				accountId: 1, databaseId: 2, tableId: 3,
				tableName: "new", bat: newSecond,
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, results, 2)
	after := workspace.currentReadView()

	oldEntries, err := workspace.tableEntries(before, 1, 2, 3)
	require.NoError(t, err)
	require.Len(t, oldEntries.entries, 2)
	for idx := range oldEntries.entries {
		require.Equal(t, "old", oldEntries.entries[idx].tableName)
		require.Equal(t, []string{"old_pk"}, oldEntries.entries[idx].bat.Attrs)
	}
	newEntries, err := workspace.tableEntries(after, 1, 2, 3)
	require.NoError(t, err)
	require.Len(t, newEntries.entries, 2)
	for idx := range newEntries.entries {
		require.Equal(t, "new", newEntries.entries[idx].tableName)
		require.Equal(t, []string{"new_pk"}, newEntries.entries[idx].bat.Attrs)
	}

	oldEntries.Close()
	newEntries.Close()
	require.NoError(t, workspace.close(proc.Mp()))
}

func TestTxnWorkspaceRollbackRestoresMutationRewrite(t *testing.T) {
	proc := testutil.NewProc(t)
	workspace := newTxnWorkspace()
	oldBat := newInt64BatchForTest(t, proc, []string{"old_pk"}, []int64{11, 22, 33})
	sourceID := workspace.append(Entry{
		accountId: 1, databaseId: 2, tableId: 3,
		tableName: "old", bat: oldBat,
	})
	require.NoError(t, workspace.addMutationSelections(sourceID, []int64{1}))
	workspace.advanceStatement()
	beforeRewrite := workspace.currentReadView()

	// A selection changed by the rewriting statement must be rolled back with
	// the metadata/payload rewrite, not restored onto the replacement payload.
	require.NoError(t, workspace.addMutationSelections(sourceID, []int64{2}))
	newBat := newInt64BatchForTest(t, proc, []string{"new_pk"}, []int64{111, 222, 333})
	results, err := workspace.rewriteMutations([]workspaceMutationRewrite{{
		mutationID: sourceID,
		oldBat:     oldBat,
		selections: []int64{1, 2},
		entry: Entry{
			accountId: 1, databaseId: 2, tableId: 3,
			tableName: "new", bat: newBat,
		},
	}})
	require.NoError(t, err)
	require.Len(t, results, 1)
	betweenRewriteAndRollback := workspace.currentReadView()

	rolledBack, err := workspace.rollbackCurrentAttempt()
	require.NoError(t, err)
	require.Equal(t, []workspaceMutationID{results[0].targetID}, rolledBack.mutationIDs)
	rolledBack.Close()
	afterRollback := workspace.currentReadView()

	beforeEntries, err := workspace.tableEntries(beforeRewrite, 1, 2, 3)
	require.NoError(t, err)
	require.Len(t, beforeEntries.entries, 1)
	require.Equal(t, "old", beforeEntries.entries[0].tableName)
	require.Same(t, oldBat, beforeEntries.entries[0].bat)
	require.Equal(t, []int64{1}, beforeEntries.entries[0].selections)

	betweenEntries, err := workspace.tableEntries(betweenRewriteAndRollback, 1, 2, 3)
	require.NoError(t, err)
	require.Len(t, betweenEntries.entries, 1)
	require.Equal(t, "new", betweenEntries.entries[0].tableName)
	require.Same(t, newBat, betweenEntries.entries[0].bat)
	require.Equal(t, []int64{1, 2}, betweenEntries.entries[0].selections)

	afterEntries, err := workspace.tableEntries(afterRollback, 1, 2, 3)
	require.NoError(t, err)
	require.Len(t, afterEntries.entries, 1)
	require.Equal(t, "old", afterEntries.entries[0].tableName)
	require.Same(t, oldBat, afterEntries.entries[0].bat)
	require.Equal(t, []int64{1}, afterEntries.entries[0].selections)

	beforeEntries.Close()
	betweenEntries.Close()
	afterEntries.Close()
	require.NoError(t, workspace.close(proc.Mp()))
}

func TestTxnWorkspaceRollbackDropsRewriteCreatedByCurrentAttempt(t *testing.T) {
	proc := testutil.NewProc(t)
	workspace := newTxnWorkspace()
	workspace.advanceStatement()
	oldBat := newInt64BatchForTest(t, proc, []string{"old_pk"}, []int64{11})
	sourceID := workspace.append(Entry{
		accountId: 1, databaseId: 2, tableId: 3,
		tableName: "old", bat: oldBat,
	})
	newBat := newInt64BatchForTest(t, proc, []string{"new_pk"}, []int64{111})
	results, err := workspace.rewriteMutations([]workspaceMutationRewrite{{
		mutationID: sourceID,
		oldBat:     oldBat,
		entry: Entry{
			accountId: 1, databaseId: 2, tableId: 3,
			tableName: "new", bat: newBat,
		},
	}})
	require.NoError(t, err)
	require.Len(t, results, 1)
	betweenRewriteAndRollback := workspace.currentReadView()

	rolledBack, err := workspace.rollbackCurrentAttempt()
	require.NoError(t, err)
	require.Equal(t,
		[]workspaceMutationID{results[0].targetID},
		rolledBack.mutationIDs,
	)
	rolledBack.Close()

	betweenEntries, err := workspace.tableEntries(betweenRewriteAndRollback, 1, 2, 3)
	require.NoError(t, err)
	require.Len(t, betweenEntries.entries, 1)
	require.Equal(t, "new", betweenEntries.entries[0].tableName)
	afterEntries, err := workspace.tableEntries(workspace.currentReadView(), 1, 2, 3)
	require.NoError(t, err)
	require.Empty(t, afterEntries.entries)

	betweenEntries.Close()
	afterEntries.Close()
	require.NoError(t, workspace.close(proc.Mp()))
}

func TestTxnWorkspaceAdvanceStatementTransitionBelongsToNewAttempt(t *testing.T) {
	proc := testutil.NewProc(t)
	workspace := newTxnWorkspace()
	oldBat := newInt64BatchForTest(t, proc, []string{"old_pk"}, []int64{11})
	sourceID := workspace.append(Entry{
		accountId: 1, databaseId: 2, tableId: 3,
		tableName: "old", bat: oldBat,
	})
	newBat := newInt64BatchForTest(t, proc, []string{"new_pk"}, []int64{111})
	result, err := workspace.advanceStatementWithTransition(
		[]workspaceMutationTransitionSource{{
			mutationID: sourceID,
			oldBat:     oldBat,
		}},
		[]workspaceMutationTransitionTarget{{
			entry: Entry{
				accountId: 1, databaseId: 2, tableId: 3,
				tableName: "new", bat: newBat,
			},
		}},
	)
	require.NoError(t, err)
	require.Len(t, result.targetIDs, 1)
	target := workspace.mutations[result.targetIDs[0]]
	require.Equal(t, uint64(1), target.statementID)
	require.Equal(t, uint64(1), target.attemptID)

	rolledBack, err := workspace.rollbackCurrentAttempt()
	require.NoError(t, err)
	require.Equal(t, result.targetIDs, rolledBack.mutationIDs)
	rolledBack.Close()

	visible, err := workspace.tableEntries(workspace.currentReadView(), 1, 2, 3)
	require.NoError(t, err)
	require.Len(t, visible.entries, 1)
	require.Equal(t, "old", visible.entries[0].tableName)
	require.Same(t, oldBat, visible.entries[0].bat)
	visible.Close()
	require.NoError(t, workspace.close(proc.Mp()))
}

func TestTxnWorkspaceRetryBoundaryTransitionBelongsToRetryAttempt(t *testing.T) {
	proc := testutil.NewProc(t)
	workspace := newTxnWorkspace()
	oldBat := newInt64BatchForTest(t, proc, []string{"old_pk"}, []int64{11})
	sourceID := workspace.append(Entry{
		accountId: 1, databaseId: 2, tableId: 3,
		tableName: "old", bat: oldBat,
	})
	_, err := workspace.advanceStatement()
	require.NoError(t, err)

	failed, err := workspace.rollbackCurrentAttempt()
	require.NoError(t, err)
	failed.Close()

	newBat := newInt64BatchForTest(t, proc, []string{"new_pk"}, []int64{111})
	result, err := workspace.advanceStatementWithTransition(
		[]workspaceMutationTransitionSource{{
			mutationID: sourceID,
			oldBat:     oldBat,
		}},
		[]workspaceMutationTransitionTarget{{
			replacementOf: sourceID,
			entry: Entry{
				accountId: 1, databaseId: 2, tableId: 3,
				tableName: "new", bat: newBat,
			},
		}},
	)
	require.NoError(t, err)
	require.Len(t, result.targetIDs, 1)
	target := workspace.mutations[result.targetIDs[0]]
	require.Equal(t, uint64(1), target.statementID)
	require.Equal(t, uint64(2), target.attemptID)

	retryRollback, err := workspace.rollbackCurrentAttempt()
	require.NoError(t, err)
	require.Equal(t, result.targetIDs, retryRollback.mutationIDs)
	retryRollback.Close()

	visible, err := workspace.tableEntries(
		workspace.currentReadView(), 1, 2, 3)
	require.NoError(t, err)
	require.Len(t, visible.entries, 1)
	require.Equal(t, "old", visible.entries[0].tableName)
	require.Same(t, oldBat, visible.entries[0].bat)
	visible.Close()
	require.NoError(t, workspace.close(proc.Mp()))
}

func TestTxnWorkspaceRolledBackAttemptRejectsOrdinaryTransition(t *testing.T) {
	proc := testutil.NewProc(t)
	workspace := newTxnWorkspace()
	oldBat := newInt64BatchForTest(t, proc, []string{"old_pk"}, []int64{11})
	sourceID := workspace.append(Entry{
		accountId: 1, databaseId: 2, tableId: 3,
		tableName: "old", bat: oldBat,
	})
	_, err := workspace.advanceStatement()
	require.NoError(t, err)

	failed, err := workspace.rollbackCurrentAttempt()
	require.NoError(t, err)
	failed.Close()

	newBat := newInt64BatchForTest(t, proc, []string{"new_pk"}, []int64{111})
	_, err = workspace.transitionMutations(
		[]workspaceMutationTransitionSource{{
			mutationID: sourceID,
			oldBat:     oldBat,
		}},
		[]workspaceMutationTransitionTarget{{
			replacementOf: sourceID,
			entry: Entry{
				accountId: 1, databaseId: 2, tableId: 3,
				tableName: "new", bat: newBat,
			},
		}},
	)
	require.ErrorContains(t, err, "invalid statement boundary")

	visible, err := workspace.tableEntries(
		workspace.currentReadView(), 1, 2, 3)
	require.NoError(t, err)
	require.Len(t, visible.entries, 1)
	require.Equal(t, "old", visible.entries[0].tableName)
	require.Same(t, oldBat, visible.entries[0].bat)
	visible.Close()
	newBat.Clean(proc.Mp())
	require.NoError(t, workspace.close(proc.Mp()))
}

func TestTxnWorkspaceAdvanceStatementTransitionFailureDoesNotAdvance(t *testing.T) {
	proc := testutil.NewProc(t)
	workspace := newTxnWorkspace()
	targetBat := newInt64BatchForTest(t, proc, []string{"pk"}, []int64{11})

	_, err := workspace.advanceStatementWithTransition(
		[]workspaceMutationTransitionSource{{
			mutationID: workspaceMutationID(999),
		}},
		[]workspaceMutationTransitionTarget{{
			entry: Entry{
				accountId: 1, databaseId: 2, tableId: 3, bat: targetBat,
			},
		}},
	)
	require.ErrorContains(t, err, "source is not active")
	require.Equal(t, uint64(0), workspace.journal.current.statementID)
	require.Equal(t, uint64(1), workspace.journal.current.attemptID)
	require.Equal(t, uint64(0), workspace.revision)

	targetBat.Clean(proc.Mp())
	require.NoError(t, workspace.close(proc.Mp()))
}

func TestTxnWorkspaceRCBoundaryPublishesWithTransition(t *testing.T) {
	proc := testutil.NewProc(t)
	workspace := newTxnWorkspace()
	oldBat := newInt64BatchForTest(t, proc, []string{"old_pk"}, []int64{11})
	sourceID := workspace.append(Entry{
		accountId: 1, databaseId: 2, tableId: 3,
		tableName: "old", bat: oldBat,
	})
	newBat := newInt64BatchForTest(t, proc, []string{"new_pk"}, []int64{111})
	snapshot := timestamp.Timestamp{PhysicalTime: 10}
	transferEnd := types.BuildTS(20, 0)
	result, err := workspace.publishRCBoundaryWithTransition(
		[]workspaceMutationTransitionSource{{
			mutationID: sourceID,
			oldBat:     oldBat,
		}},
		[]workspaceMutationTransitionTarget{{
			entry: Entry{
				accountId: 1, databaseId: 2, tableId: 3,
				tableName: "new", bat: newBat,
			},
		}},
		true,
		rcBoundaryPublication{
			recordStatement:   true,
			statementSnapshot: snapshot,
			lastTransferred:   transferEnd,
			pendingTransfer:   false,
		},
	)
	require.NoError(t, err)
	require.Len(t, result.targetIDs, 1)
	require.Equal(t, uint64(1), workspace.journal.current.statementID)
	require.Equal(t, uint64(1), workspace.mutations[result.targetIDs[0]].statementID)
	rcState := workspace.rcState()
	require.Equal(t, []timestamp.Timestamp{snapshot}, rcState.snapshots)
	require.Equal(t, transferEnd, rcState.lastTransferred)
	require.False(t, rcState.pendingTransfer)
	require.NoError(t, workspace.close(proc.Mp()))
}

func TestTxnWorkspaceRCBoundaryTransitionFailurePublishesNothing(t *testing.T) {
	proc := testutil.NewProc(t)
	workspace := newTxnWorkspace()
	targetBat := newInt64BatchForTest(t, proc, []string{"pk"}, []int64{11})
	snapshot := timestamp.Timestamp{PhysicalTime: 10}

	_, err := workspace.publishRCBoundaryWithTransition(
		[]workspaceMutationTransitionSource{{mutationID: workspaceMutationID(999)}},
		[]workspaceMutationTransitionTarget{{
			entry: Entry{accountId: 1, databaseId: 2, tableId: 3, bat: targetBat},
		}},
		true,
		rcBoundaryPublication{
			recordStatement:   true,
			statementSnapshot: snapshot,
			lastTransferred:   types.BuildTS(20, 0),
			pendingTransfer:   true,
		},
	)
	require.ErrorContains(t, err, "source is not active")
	require.Equal(t, uint64(0), workspace.journal.current.statementID)
	require.Equal(t, uint64(1), workspace.journal.current.attemptID)
	require.Equal(t, uint64(0), workspace.revision)
	rcState := workspace.rcState()
	require.Empty(t, rcState.snapshots)
	require.True(t, rcState.lastTransferred.IsEmpty())
	require.False(t, rcState.pendingTransfer)

	targetBat.Clean(proc.Mp())
	require.NoError(t, workspace.close(proc.Mp()))
}

func TestTxnWorkspaceUnfinishedWriteScopeRejectsBoundaryAtomically(t *testing.T) {
	proc := testutil.NewProc(t)
	workspace := newTxnWorkspace()
	oldBat := newInt64BatchForTest(t, proc, []string{"old_pk"}, []int64{11})
	sourceID := workspace.append(Entry{
		accountId: 1, databaseId: 2, tableId: 3,
		tableName: "old", bat: oldBat,
	})
	before := workspace.diagnosticSnapshot()
	beforeUsage := workspace.usageSnapshot()
	mark := workspace.beginWriteAttempt()
	newBat := newInt64BatchForTest(t, proc, []string{"new_pk"}, []int64{111})
	snapshot := timestamp.Timestamp{PhysicalTime: 10}

	_, err := workspace.publishRCBoundaryWithTransition(
		[]workspaceMutationTransitionSource{{
			mutationID: sourceID,
			oldBat:     oldBat,
		}},
		[]workspaceMutationTransitionTarget{{
			entry: Entry{
				accountId: 1, databaseId: 2, tableId: 3,
				tableName: "new", bat: newBat,
			},
		}},
		true,
		rcBoundaryPublication{
			recordStatement:   true,
			statementSnapshot: snapshot,
			lastTransferred:   types.BuildTS(20, 0),
			pendingTransfer:   true,
		},
	)
	require.ErrorContains(t, err, "unfinished write scope")

	after := workspace.diagnosticSnapshot()
	require.Equal(t, before.revision, after.revision)
	require.Equal(t, before.statementID, after.statementID)
	require.Equal(t, before.attemptID, after.attemptID)
	require.Equal(t, before.activeEntries, after.activeEntries)
	require.Equal(t, before.rc, after.rc)
	require.Equal(t, beforeUsage, workspace.usageSnapshot())
	require.Equal(t, 1, workspace.activeMutationCount())
	visible, err := workspace.tableEntries(workspace.currentReadView(), 1, 2, 3)
	require.NoError(t, err)
	require.Len(t, visible.entries, 1)
	require.Equal(t, "old", visible.entries[0].tableName)
	require.Same(t, oldBat, visible.entries[0].bat)
	visible.Close()

	require.NoError(t, workspace.adjustAttempt(mark))
	newBat.Clean(proc.Mp())
	require.NoError(t, workspace.close(proc.Mp()))
}

func TestTxnWorkspaceRollbackRCBoundaryRewindsJournal(t *testing.T) {
	proc := testutil.NewProc(t)
	workspace := newTxnWorkspace()
	firstSnapshot := timestamp.Timestamp{PhysicalTime: 10}
	workspace.advanceRCStatement(rcBoundaryPublication{
		recordStatement:   true,
		statementSnapshot: firstSnapshot,
		lastTransferred:   types.BuildTS(20, 0),
	})
	workspace.advanceRCStatement(rcBoundaryPublication{
		recordStatement:   true,
		statementSnapshot: timestamp.Timestamp{PhysicalTime: 30},
		lastTransferred:   types.BuildTS(40, 0),
		pendingTransfer:   true,
	})

	require.NoError(t, workspace.rollbackRCBoundary(1))
	rcState := workspace.rcState()
	require.Equal(t, []timestamp.Timestamp{firstSnapshot}, rcState.snapshots)
	require.Equal(t, types.BuildTS(10, 0), rcState.lastTransferred)
	require.True(t, rcState.pendingTransfer)

	require.NoError(t, workspace.rollbackRCBoundary(0))
	rcState = workspace.rcState()
	require.Empty(t, rcState.snapshots)
	require.True(t, rcState.lastTransferred.IsEmpty())
	require.False(t, rcState.pendingTransfer)
	require.NoError(t, workspace.close(proc.Mp()))
}

func TestTxnWorkspaceRollbackRCBoundaryIsAtomicWithMutationRollback(t *testing.T) {
	proc := testutil.NewProc(t)
	workspace := newTxnWorkspace()
	workspace.append(Entry{
		accountId: 1, databaseId: 2, tableId: 3,
		bat: newInt64BatchForTest(t, proc, []string{"pk"}, []int64{11}),
	})
	workspace.advanceRCStatement(rcBoundaryPublication{
		recordStatement:   true,
		statementSnapshot: timestamp.Timestamp{PhysicalTime: 10},
		lastTransferred:   types.BuildTS(20, 0),
	})
	workspace.append(Entry{
		accountId: 1, databaseId: 2, tableId: 3,
		bat: newInt64BatchForTest(t, proc, []string{"pk"}, []int64{22}),
	})
	before := workspace.currentReadView()

	workspace.journal.current.statementID = 3
	rolledBack, err := workspace.rollbackCurrentAttemptWithRC()
	require.ErrorContains(t, err, "RC history mismatch")
	require.Nil(t, rolledBack)
	require.Equal(t, before, workspace.currentReadView())
	require.Equal(t, uint64(3), workspace.journal.current.statementID)
	require.Equal(t, statementAttemptOpen, workspace.journal.current.state)
	entries, err := workspace.tableEntries(workspace.currentReadView(), 1, 2, 3)
	require.NoError(t, err)
	require.Len(t, entries.entries, 2)
	entries.Close()
	rcState := workspace.rcState()
	require.Len(t, rcState.snapshots, 1)
	require.Equal(t, types.BuildTS(20, 0), rcState.lastTransferred)

	workspace.journal.current.statementID = 1
	rolledBack, err = workspace.rollbackCurrentAttemptWithRC()
	require.NoError(t, err)
	rolledBack.Close()
	rcState = workspace.rcState()
	require.Empty(t, rcState.snapshots)
	require.True(t, rcState.lastTransferred.IsEmpty())
	require.NoError(t, workspace.close(proc.Mp()))
}

func TestTxnWorkspaceRewriteMutationsRejectsAllOnStaleSource(t *testing.T) {
	proc := testutil.NewProc(t)
	workspace := newTxnWorkspace()
	first := newInt64BatchForTest(t, proc, []string{"pk"}, []int64{11})
	second := newInt64BatchForTest(t, proc, []string{"pk"}, []int64{22})
	firstID := workspace.append(Entry{accountId: 1, databaseId: 2, tableId: 3, bat: first})
	secondID := workspace.append(Entry{accountId: 1, databaseId: 2, tableId: 3, bat: second})
	newFirst := newInt64BatchForTest(t, proc, []string{"pk"}, []int64{111})
	newSecond := newInt64BatchForTest(t, proc, []string{"pk"}, []int64{222})
	stale := newInt64BatchForTest(t, proc, []string{"pk"}, []int64{999})
	revision := workspace.revision

	_, err := workspace.rewriteMutations([]workspaceMutationRewrite{
		{mutationID: firstID, oldBat: first, entry: Entry{
			accountId: 1, databaseId: 2, tableId: 3, bat: newFirst,
		}},
		{mutationID: secondID, oldBat: stale, entry: Entry{
			accountId: 1, databaseId: 2, tableId: 3, bat: newSecond,
		}},
	})
	require.ErrorContains(t, err, "generation changed")
	require.Equal(t, revision, workspace.revision)
	entries, err := workspace.tableEntries(workspace.currentReadView(), 1, 2, 3)
	require.NoError(t, err)
	require.Len(t, entries.entries, 2)
	require.Same(t, first, entries.entries[0].bat)
	require.Same(t, second, entries.entries[1].bat)
	entries.Close()
	newFirst.Clean(proc.Mp())
	newSecond.Clean(proc.Mp())
	stale.Clean(proc.Mp())
	require.NoError(t, workspace.close(proc.Mp()))
}

func TestTxnWorkspaceSpillPublishesOneAtomicRevision(t *testing.T) {
	proc := testutil.NewProc(t)
	workspace := newTxnWorkspace()
	sourceBat := newInt64BatchForTest(t, proc, []string{"pk"}, []int64{11, 22})
	sourceID := workspace.append(Entry{
		accountId: 1, databaseId: 2, tableId: 3, bat: sourceBat,
	})
	before := workspace.currentReadView()
	attempt, err := workspace.beginSpill([]workspaceMutationID{sourceID})
	require.NoError(t, err)
	objectBat := newInt64BatchForTest(t, proc, []string{"object"}, []int64{33})
	objectIDs, err := workspace.commitSpill(attempt, []workspaceSpillObject{{
		statementID:       attempt.sources[0].statementID,
		attemptID:         attempt.sources[0].attemptID,
		sourceMutationIDs: []workspaceMutationID{sourceID},
		entry: Entry{
			accountId: 1, databaseId: 2, tableId: 3,
			fileName: "object", bat: objectBat,
		},
	}})
	require.NoError(t, err)
	require.Len(t, objectIDs, 1)
	after := workspace.currentReadView()

	oldEntries, err := workspace.tableEntries(before, 1, 2, 3)
	require.NoError(t, err)
	require.Len(t, oldEntries.entries, 1)
	require.Same(t, sourceBat, oldEntries.entries[0].bat)
	newEntries, err := workspace.tableEntries(after, 1, 2, 3)
	require.NoError(t, err)
	require.Len(t, newEntries.entries, 1)
	require.Same(t, objectBat, newEntries.entries[0].bat)
	require.Equal(t, "object", newEntries.entries[0].fileName)

	attempt.Close()
	oldEntries.Close()
	newEntries.Close()
	require.NoError(t, workspace.close(proc.Mp()))
}

func TestTxnWorkspaceTerminalSpillCoalescesCompletedStatements(t *testing.T) {
	proc := testutil.NewProc(t)
	workspace := newTxnWorkspace()
	firstBat := newInt64BatchForTest(t, proc, []string{"pk"}, []int64{11})
	firstID := workspace.append(Entry{
		typ: INSERT, accountId: 1, databaseId: 2, tableId: 3,
		databaseName: "db", tableName: "tbl", bat: firstBat,
	})
	_, err := workspace.advanceStatement()
	require.NoError(t, err)
	secondBat := newInt64BatchForTest(t, proc, []string{"pk"}, []int64{22})
	secondID := workspace.append(Entry{
		typ: INSERT, accountId: 1, databaseId: 2, tableId: 3,
		databaseName: "db", tableName: "tbl", bat: secondBat,
	})

	ordinary, err := workspace.beginSpill(
		[]workspaceMutationID{firstID, secondID})
	require.NoError(t, err)
	require.Len(t, groupWorkspaceSpillSources(ordinary), 2)
	ordinary.Close()

	terminal, err := workspace.beginTerminalSpill(
		[]workspaceMutationID{firstID, secondID})
	require.NoError(t, err)
	groups := groupWorkspaceSpillSources(terminal)
	require.Len(t, groups, 1)
	require.Len(t, groups[0].sources, 2)
	require.Zero(t, groups[0].key.statementID)
	require.Zero(t, groups[0].key.attemptID)

	objectBat := newInt64BatchForTest(
		t, proc, []string{"object"}, []int64{33})
	objectIDs, err := workspace.commitSpill(terminal, []workspaceSpillObject{{
		sourceMutationIDs: []workspaceMutationID{firstID, secondID},
		entry: Entry{
			typ: INSERT, accountId: 1, databaseId: 2, tableId: 3,
			databaseName: "db", tableName: "tbl",
			fileName: "object", bat: objectBat,
		},
	}})
	require.NoError(t, err)
	require.Len(t, objectIDs, 1)
	terminal.Close()

	workspace.mu.RLock()
	require.Equal(t, uint64(1), workspace.mutations[objectIDs[0]].statementID)
	require.Equal(t, uint64(1), workspace.mutations[objectIDs[0]].attemptID)
	require.Contains(t, workspace.journal.current.mutations, objectIDs[0])
	workspace.mu.RUnlock()
	_, err = workspace.rollbackCurrentAttempt()
	require.ErrorContains(t, err,
		"statement rollback cannot run after commit preparation")

	entries, err := workspace.tableEntries(
		workspace.currentReadView(), 1, 2, 3)
	require.NoError(t, err)
	require.Len(t, entries.entries, 1)
	require.Equal(t, "object", entries.entries[0].fileName)
	require.Same(t, objectBat, entries.entries[0].bat)
	entries.Close()
	require.NoError(t, workspace.close(proc.Mp()))
}

func TestTxnWorkspaceTerminalSpillRejectsChangedBoundary(t *testing.T) {
	proc := testutil.NewProc(t)
	workspace := newTxnWorkspace()
	sourceID := workspace.append(Entry{
		typ: INSERT, accountId: 1, databaseId: 2, tableId: 3,
		databaseName: "db", tableName: "tbl",
		bat: newInt64BatchForTest(t, proc, []string{"pk"}, []int64{11}),
	})
	attempt, err := workspace.beginTerminalSpill(
		[]workspaceMutationID{sourceID})
	require.NoError(t, err)
	_, err = workspace.advanceStatement()
	require.NoError(t, err)
	objectBat := newInt64BatchForTest(
		t, proc, []string{"object"}, []int64{22})
	_, err = workspace.commitSpill(attempt, []workspaceSpillObject{{
		sourceMutationIDs: []workspaceMutationID{sourceID},
		entry: Entry{
			typ: INSERT, accountId: 1, databaseId: 2, tableId: 3,
			databaseName: "db", tableName: "tbl",
			fileName: "object", bat: objectBat,
		},
	}})
	require.ErrorContains(t, err,
		"terminal spill boundary changed before publication")
	objectBat.Clean(proc.Mp())
	attempt.Close()
	require.NoError(t, workspace.close(proc.Mp()))
}

func TestTxnWorkspaceRetryPreparationSpillsCompletedAttempt(t *testing.T) {
	proc := testutil.NewProc(t)
	workspace := newTxnWorkspace()
	sourceBat := newInt64BatchForTest(
		t, proc, []string{"pk"}, []int64{11, 22})
	sourceID := workspace.append(Entry{
		accountId: 1, databaseId: 2, tableId: 3, bat: sourceBat,
	})
	_, err := workspace.advanceStatement()
	require.NoError(t, err)

	failed, err := workspace.rollbackCurrentAttempt()
	require.NoError(t, err)
	failed.Close()
	require.Equal(t, statementAttemptRolledBack, workspace.journal.current.state)
	require.True(t, workspace.journal.retryPending)

	attempt, err := workspace.beginSpill([]workspaceMutationID{sourceID})
	require.NoError(t, err)
	objectBat := newInt64BatchForTest(
		t, proc, []string{"object"}, []int64{33})
	objectIDs, err := workspace.commitSpill(attempt, []workspaceSpillObject{{
		statementID:       attempt.sources[0].statementID,
		attemptID:         attempt.sources[0].attemptID,
		sourceMutationIDs: []workspaceMutationID{sourceID},
		entry: Entry{
			accountId: 1, databaseId: 2, tableId: 3,
			fileName: "object", bat: objectBat,
		},
	}})
	require.NoError(t, err)
	require.Len(t, objectIDs, 1)
	attempt.Close()
	require.Equal(t, statementAttemptRolledBack, workspace.journal.current.state)
	require.True(t, workspace.journal.retryPending)
	require.Equal(t, uint64(0), workspace.mutations[objectIDs[0]].statementID)
	require.Equal(t, uint64(1), workspace.mutations[objectIDs[0]].attemptID)

	retry, err := workspace.advanceStatement()
	require.NoError(t, err)
	require.Equal(t, uint64(1), retry.statementID)
	require.Equal(t, uint64(2), retry.attemptID)
	retryRollback, err := workspace.rollbackCurrentAttempt()
	require.NoError(t, err)
	require.Empty(t, retryRollback.mutationIDs)
	retryRollback.Close()

	entries, err := workspace.tableEntries(
		workspace.currentReadView(), 1, 2, 3)
	require.NoError(t, err)
	require.Len(t, entries.entries, 1)
	require.Same(t, objectBat, entries.entries[0].bat)
	require.Equal(t, "object", entries.entries[0].fileName)
	entries.Close()
	require.NoError(t, workspace.close(proc.Mp()))
}

func TestTxnWorkspaceSpillPublishesZeroTargetForFullySelectedSource(t *testing.T) {
	proc := testutil.NewProc(t)
	workspace := newTxnWorkspace()
	sourceBat := newInt64BatchForTest(
		t, proc, []string{"pk"}, []int64{11, 22})
	sourceID := workspace.append(Entry{
		typ: INSERT, accountId: 1, databaseId: 2, tableId: 3, bat: sourceBat,
	})
	require.NoError(t, workspace.selectAllMutationRows(
		sourceID, sourceBat.RowCount()))

	attempt, err := workspace.beginSpill([]workspaceMutationID{sourceID})
	require.NoError(t, err)
	require.Len(t, attempt.sources, 1)
	objectIDs, err := workspace.commitSpill(attempt, nil)
	require.NoError(t, err)
	require.Empty(t, objectIDs)
	attempt.Close()

	entries, err := workspace.tableEntries(
		workspace.currentReadView(), 1, 2, 3)
	require.NoError(t, err)
	require.Empty(t, entries.entries)
	entries.Close()
	require.NotContains(t, workspace.journal.current.mutations, sourceID)
	require.NoError(t, workspace.close(proc.Mp()))
}

func TestTxnWorkspaceSpillRejectsUnclaimedVisibleSource(t *testing.T) {
	proc := testutil.NewProc(t)
	workspace := newTxnWorkspace()
	sourceBat := newInt64BatchForTest(
		t, proc, []string{"pk"}, []int64{11, 22})
	sourceID := workspace.append(Entry{
		typ: INSERT, accountId: 1, databaseId: 2, tableId: 3, bat: sourceBat,
	})
	before := workspace.currentReadView()

	attempt, err := workspace.beginSpill([]workspaceMutationID{sourceID})
	require.NoError(t, err)
	_, err = workspace.commitSpill(attempt, nil)
	require.ErrorContains(t, err,
		"workspace spill left a visible source without an object")
	require.Equal(t, before, workspace.currentReadView())
	attempt.Close()

	entries, err := workspace.tableEntries(before, 1, 2, 3)
	require.NoError(t, err)
	require.Len(t, entries.entries, 1)
	require.Same(t, sourceBat, entries.entries[0].bat)
	entries.Close()
	require.Contains(t, workspace.journal.current.mutations, sourceID)
	require.NoError(t, workspace.close(proc.Mp()))
}

func TestTxnWorkspaceSpillInheritsEarliestLogicalSourceOrder(t *testing.T) {
	proc := testutil.NewProc(t)
	workspace := newTxnWorkspace()
	firstBat := newInt64BatchForTest(t, proc, []string{"pk"}, []int64{11})
	unrelatedBat := newInt64BatchForTest(t, proc, []string{"pk"}, []int64{22})
	lastBat := newInt64BatchForTest(t, proc, []string{"pk"}, []int64{33})
	firstID := workspace.append(Entry{
		typ: INSERT, accountId: 1, databaseId: 2, tableId: 100, bat: firstBat,
	})
	unrelatedID := workspace.append(Entry{
		typ: INSERT, accountId: 1, databaseId: 2, tableId: 101, bat: unrelatedBat,
	})
	lastID := workspace.append(Entry{
		typ: INSERT, accountId: 1, databaseId: 2, tableId: 100, bat: lastBat,
	})
	before := workspace.currentReadView()
	workspace.mu.RLock()
	firstOrder := slices.Clone(workspace.mutations[firstID].commitOrder)
	unrelatedOrder := slices.Clone(workspace.mutations[unrelatedID].commitOrder)
	lastOrder := slices.Clone(workspace.mutations[lastID].commitOrder)
	workspace.mu.RUnlock()

	attempt, err := workspace.beginSpill(
		[]workspaceMutationID{firstID, lastID})
	require.NoError(t, err)
	objectBat := newInt64BatchForTest(t, proc, []string{"object"}, []int64{44})
	objectIDs, err := workspace.commitSpill(attempt, []workspaceSpillObject{{
		statementID:       attempt.sources[0].statementID,
		attemptID:         attempt.sources[0].attemptID,
		sourceMutationIDs: []workspaceMutationID{firstID, lastID},
		entry: Entry{
			typ: INSERT, accountId: 1, databaseId: 2, tableId: 100,
			fileName: "object", bat: objectBat,
		},
	}})
	require.NoError(t, err)
	require.Len(t, objectIDs, 1)
	attempt.Close()

	workspace.mu.RLock()
	require.Equal(t, firstOrder, workspace.mutations[firstID].commitOrder)
	require.Equal(t, unrelatedOrder, workspace.mutations[unrelatedID].commitOrder)
	require.Equal(t, lastOrder, workspace.mutations[lastID].commitOrder)
	require.Equal(t, firstOrder, workspace.mutations[objectIDs[0]].commitOrder)
	workspace.mu.RUnlock()

	oldEntries, err := workspace.entries(before)
	require.NoError(t, err)
	require.Len(t, oldEntries.entries, 3)
	require.Equal(t, firstID, oldEntries.entries[0].workspaceMutationID)
	require.Equal(t, unrelatedID, oldEntries.entries[1].workspaceMutationID)
	require.Equal(t, lastID, oldEntries.entries[2].workspaceMutationID)
	oldEntries.Close()

	commit, err := workspace.commitEntries()
	require.NoError(t, err)
	require.Len(t, commit.entries, 2)
	require.Equal(t, objectIDs[0], commit.entries[0].workspaceMutationID)
	require.Equal(t, unrelatedID, commit.entries[1].workspaceMutationID)
	commit.Close()
	require.NoError(t, workspace.close(proc.Mp()))
}

func TestTxnWorkspaceSpillRejectsDuplicateTargetsAtomically(t *testing.T) {
	proc := testutil.NewProc(t)
	workspace := newTxnWorkspace()
	firstBat := newInt64BatchForTest(t, proc, []string{"pk"}, []int64{11})
	secondBat := newInt64BatchForTest(t, proc, []string{"pk"}, []int64{22})
	firstID := workspace.append(Entry{
		accountId: 1, databaseId: 2, tableId: 3, bat: firstBat,
	})
	secondID := workspace.append(Entry{
		accountId: 1, databaseId: 2, tableId: 3, bat: secondBat,
	})
	before := workspace.currentReadView()
	attempt, err := workspace.beginSpill([]workspaceMutationID{firstID, secondID})
	require.NoError(t, err)
	require.Len(t, attempt.sources, 2)

	objectBat := newInt64BatchForTest(t, proc, []string{"object"}, []int64{33})
	_, err = workspace.commitSpill(attempt, []workspaceSpillObject{
		{
			statementID:       attempt.sources[0].statementID,
			attemptID:         attempt.sources[0].attemptID,
			sourceMutationIDs: []workspaceMutationID{firstID},
			entry: Entry{
				accountId: 1, databaseId: 2, tableId: 3,
				fileName: "first-object", bat: objectBat,
			},
		},
		{
			statementID:       attempt.sources[1].statementID,
			attemptID:         attempt.sources[1].attemptID,
			sourceMutationIDs: []workspaceMutationID{secondID},
			entry: Entry{
				accountId: 1, databaseId: 2, tableId: 3,
				fileName: "second-object", bat: objectBat,
			},
		},
	})
	require.ErrorContains(t, err, "duplicate target payload")
	require.Equal(t, before, workspace.currentReadView())

	entries, err := workspace.tableEntries(before, 1, 2, 3)
	require.NoError(t, err)
	require.Len(t, entries.entries, 2)
	require.Same(t, firstBat, entries.entries[0].bat)
	require.Same(t, secondBat, entries.entries[1].bat)
	entries.Close()

	attempt.Close()
	retry, err := workspace.beginSpill([]workspaceMutationID{firstID, secondID})
	require.NoError(t, err)
	require.Len(t, retry.sources, 2)
	retry.Close()
	objectBat.Clean(proc.Mp())
	require.NoError(t, workspace.close(proc.Mp()))
}

func TestLocalDatasourcePinsWorkspacePayloadUntilClose(t *testing.T) {
	proc := testutil.NewProc(t)
	workspace := newTxnWorkspace()
	oldBat := newInt64BatchForTest(t, proc, []string{"pk"}, []int64{11})
	id := workspace.append(Entry{
		accountId: 1, databaseId: 2, tableId: 3, bat: oldBat,
	})
	readView := workspace.currentReadView()
	entries, err := workspace.tableEntries(readView, 1, 2, 3)
	require.NoError(t, err)
	require.Len(t, entries.entries, 1)
	require.Same(t, oldBat, entries.entries[0].bat)
	datasource := &LocalDisttaeDataSource{
		readView:         readView,
		workspaceEntries: entries,
	}

	newBat := newInt64BatchForTest(t, proc, []string{"pk"}, []int64{22})
	require.NoError(t, workspace.replaceMemory(id, oldBat, newBat, nil))
	require.Empty(t, workspace.payloads.reclaimRetired(nil))
	// The datasource may cache raw vector references from its pinned entry.
	// Its Close method is therefore the ownership boundary that permits the
	// retired physical generation to leave PayloadStore.
	require.Same(t, oldBat, datasource.workspaceEntries.entries[0].bat)

	datasource.Close()
	reclaimed := workspace.payloads.reclaimRetired(nil)
	require.Equal(t, []*batch.Batch{oldBat}, reclaimed)
	oldBat.Clean(proc.Mp())
	require.NoError(t, workspace.close(proc.Mp()))
}

func TestTxnWorkspaceConcurrentSpillSkipsOwnedGeneration(t *testing.T) {
	proc := testutil.NewProc(t)
	workspace := newTxnWorkspace()
	firstBat := newInt64BatchForTest(t, proc, []string{"pk"}, []int64{11})
	secondBat := newInt64BatchForTest(t, proc, []string{"pk"}, []int64{22})
	firstID := workspace.append(Entry{
		accountId: 1, databaseId: 2, tableId: 3, bat: firstBat,
	})
	secondID := workspace.append(Entry{
		accountId: 1, databaseId: 2, tableId: 4, bat: secondBat,
	})

	firstAttempt, err := workspace.beginSpill([]workspaceMutationID{firstID})
	require.NoError(t, err)
	require.Equal(t, []workspaceMutationID{firstID}, firstAttempt.sourceIDs())

	// A reentrant write can start another dump while the first dump resolves
	// table metadata with the transaction lock released. The second attempt
	// leaves the generation owned by the first attempt alone and acquires only
	// the independent source.
	secondAttempt, err := workspace.beginSpill(
		[]workspaceMutationID{firstID, secondID})
	require.NoError(t, err)
	require.Equal(t, []workspaceMutationID{secondID}, secondAttempt.sourceIDs())

	secondAttempt.Close()
	firstAttempt.Close()
	require.NoError(t, workspace.close(proc.Mp()))
}

func TestTxnWorkspaceRollbackInvalidatesInFlightSpill(t *testing.T) {
	proc := testutil.NewProc(t)
	workspace := newTxnWorkspace()
	workspace.advanceStatement()
	sourceID := workspace.append(Entry{
		accountId: 1, databaseId: 2, tableId: 3,
		bat: newInt64BatchForTest(t, proc, []string{"pk"}, []int64{11}),
	})
	attempt, err := workspace.beginSpill([]workspaceMutationID{sourceID})
	require.NoError(t, err)
	rolledBack, err := workspace.rollbackCurrentAttempt()
	require.NoError(t, err)
	rolledBack.Close()
	objectBat := newInt64BatchForTest(t, proc, []string{"object"}, []int64{22})
	_, err = workspace.commitSpill(attempt, []workspaceSpillObject{{
		statementID:       attempt.sources[0].statementID,
		attemptID:         attempt.sources[0].attemptID,
		sourceMutationIDs: []workspaceMutationID{sourceID},
		entry: Entry{
			accountId: 1, databaseId: 2, tableId: 3,
			fileName: "object", bat: objectBat,
		},
	}})
	require.ErrorContains(t, err, "source changed before publication")
	objectBat.Clean(proc.Mp())
	attempt.Close()

	entries, err := workspace.tableEntries(workspace.currentReadView(), 1, 2, 3)
	require.NoError(t, err)
	require.Empty(t, entries.entries)
	entries.Close()
	require.NoError(t, workspace.close(proc.Mp()))
}

func TestTxnWorkspaceRollbackPinsReplacementPayloadAndAllAttemptIDs(t *testing.T) {
	proc := testutil.NewProc(t)
	workspace := newTxnWorkspace()
	workspace.advanceStatement()
	sourceBat := newInt64BatchForTest(t, proc, []string{"pk"}, []int64{11})
	sourceID := workspace.append(Entry{
		accountId: 1, databaseId: 2, tableId: 3, bat: sourceBat,
	})
	attempt, err := workspace.beginSpill([]workspaceMutationID{sourceID})
	require.NoError(t, err)
	objectBat := newInt64BatchForTest(t, proc, []string{"object"}, []int64{22})
	objectIDs, err := workspace.commitSpill(attempt, []workspaceSpillObject{{
		statementID:       attempt.sources[0].statementID,
		attemptID:         attempt.sources[0].attemptID,
		sourceMutationIDs: []workspaceMutationID{sourceID},
		entry: Entry{
			accountId: 1, databaseId: 2, tableId: 3,
			fileName: "object", bat: objectBat,
		},
	}})
	require.NoError(t, err)
	attempt.Close()

	rolledBack, err := workspace.rollbackCurrentAttempt()
	require.NoError(t, err)
	require.Equal(t, []workspaceMutationID{objectIDs[0]}, rolledBack.mutationIDs)
	require.Len(t, rolledBack.entries.entries, 1)
	require.Equal(t, objectIDs[0], rolledBack.entries.entries[0].workspaceMutationID)
	require.Same(t, objectBat, rolledBack.entries.entries[0].bat)

	visible, err := workspace.tableEntries(workspace.currentReadView(), 1, 2, 3)
	require.NoError(t, err)
	require.Empty(t, visible.entries)
	visible.Close()
	require.ErrorContains(t, workspace.close(proc.Mp()), "still has 1 lease")
	rolledBack.Close()
	require.NoError(t, workspace.close(proc.Mp()))
}

func TestTxnWorkspaceRollbackRestoresSelectionsOnEarlierMutation(t *testing.T) {
	proc := testutil.NewProc(t)
	workspace := newTxnWorkspace()
	id := workspace.append(Entry{
		accountId: 1, databaseId: 2, tableId: 3,
		bat: newInt64BatchForTest(t, proc, []string{"pk"}, []int64{11, 22, 33}),
	})
	beforeStatement := workspace.currentReadView()
	workspace.advanceStatement()

	// Multiple logical changes in one attempt must retain the state visible
	// before the first change, not an intermediate generation.
	require.NoError(t, workspace.addMutationSelections(id, []int64{2, 0}))
	require.NoError(t, workspace.addMutationSelections(id, []int64{1}))
	require.Equal(t, []int64{0, 1, 2}, mustMutationSelections(t, workspace, id))

	rolledBack, err := workspace.rollbackCurrentAttempt()
	require.NoError(t, err)
	require.Empty(t, rolledBack.mutationIDs)
	rolledBack.Close()
	require.Empty(t, mustMutationSelections(t, workspace, id))

	// A view published before the statement and the post-rollback view both
	// resolve the original logical state.
	beforeEntries, err := workspace.tableEntries(beforeStatement, 1, 2, 3)
	require.NoError(t, err)
	require.Len(t, beforeEntries.entries, 1)
	require.Empty(t, beforeEntries.entries[0].selections)
	afterEntries, err := workspace.tableEntries(workspace.currentReadView(), 1, 2, 3)
	require.NoError(t, err)
	require.Len(t, afterEntries.entries, 1)
	require.Empty(t, afterEntries.entries[0].selections)

	beforeEntries.Close()
	afterEntries.Close()
	require.NoError(t, workspace.close(proc.Mp()))
}

func TestTxnWorkspaceRollbackRetiresCurrentMutationWithSelections(t *testing.T) {
	proc := testutil.NewProc(t)
	workspace := newTxnWorkspace()
	workspace.advanceStatement()
	id := workspace.append(Entry{
		accountId: 1, databaseId: 2, tableId: 3,
		bat: newInt64BatchForTest(t, proc, []string{"pk"}, []int64{11, 22}),
	})
	require.NoError(t, workspace.addMutationSelections(id, []int64{1}))

	rolledBack, err := workspace.rollbackCurrentAttempt()
	require.NoError(t, err)
	require.Equal(t, []workspaceMutationID{id}, rolledBack.mutationIDs)
	rolledBack.Close()

	entries, err := workspace.tableEntries(workspace.currentReadView(), 1, 2, 3)
	require.NoError(t, err)
	require.Empty(t, entries.entries)
	entries.Close()
	require.NoError(t, workspace.close(proc.Mp()))
}

func TestTxnWorkspaceRetryKeepsStatementAndNeverReusesMutationID(t *testing.T) {
	proc := testutil.NewProc(t)
	workspace := newTxnWorkspace()
	committedID := workspace.append(Entry{
		accountId: 1, databaseId: 2, tableId: 3,
		bat: newInt64BatchForTest(t, proc, []string{"pk"}, []int64{11}),
	})
	firstAttempt, err := workspace.advanceStatement()
	require.NoError(t, err)
	failedID := workspace.append(Entry{
		accountId: 1, databaseId: 2, tableId: 3,
		bat: newInt64BatchForTest(t, proc, []string{"pk"}, []int64{22}),
	})
	rolledBack, err := workspace.rollbackCurrentAttempt()
	require.NoError(t, err)
	rolledBack.Close()
	retryAttempt, err := workspace.advanceStatement()
	require.NoError(t, err)
	retryID := workspace.append(Entry{
		accountId: 1, databaseId: 2, tableId: 3,
		bat: newInt64BatchForTest(t, proc, []string{"pk"}, []int64{33}),
	})

	require.Equal(t, firstAttempt.statementID, retryAttempt.statementID)
	require.Equal(t, firstAttempt.attemptID+1, retryAttempt.attemptID)
	require.Less(t, committedID, failedID)
	require.Less(t, failedID, retryID)
	require.NotZero(t, workspace.mutations[failedID].retiredRevision)
	require.Zero(t, workspace.mutations[retryID].retiredRevision)
	require.Equal(t,
		[]workspaceMutationID{retryID},
		workspaceMutationIDs(workspace.journal.current.mutations))

	entries, err := workspace.tableEntries(workspace.currentReadView(), 1, 2, 3)
	require.NoError(t, err)
	require.Len(t, entries.entries, 2)
	entries.Close()
	require.NoError(t, workspace.close(proc.Mp()))
}

func TestTxnWorkspaceCloseRejectsOutstandingPayloadLease(t *testing.T) {
	proc := testutil.NewProc(t)
	workspace := newTxnWorkspace()
	workspace.append(Entry{
		accountId: 1, databaseId: 2, tableId: 3,
		bat: newInt64BatchForTest(t, proc, []string{"pk"}, []int64{11}),
	})
	entries, err := workspace.tableEntries(workspace.currentReadView(), 1, 2, 3)
	require.NoError(t, err)
	require.ErrorContains(t, workspace.close(proc.Mp()), "still has 1 lease")
	entries.Close()
	require.NoError(t, workspace.close(proc.Mp()))
}

func TestTxnWorkspaceCommitEntriesFreezeCurrentLogicalState(t *testing.T) {
	proc := testutil.NewProc(t)
	workspace := newTxnWorkspace()
	first := newInt64BatchForTest(t, proc, []string{"pk"}, []int64{11})
	second := newInt64BatchForTest(t, proc, []string{"pk"}, []int64{22})
	third := newInt64BatchForTest(t, proc, []string{"pk"}, []int64{33})
	firstID := workspace.append(Entry{
		accountId: 1, databaseId: 2, tableId: 3, tableName: "first", bat: first,
	})
	secondID := workspace.append(Entry{
		accountId: 1, databaseId: 2, tableId: 3, tableName: "second", bat: second,
	})
	thirdID := workspace.append(Entry{
		accountId: 1, databaseId: 2, tableId: 3, tableName: "third", bat: third,
	})
	require.NoError(t, workspace.retireMutation(firstID))

	commit, err := workspace.commitEntries()
	require.NoError(t, err)
	require.Len(t, commit.entries, 2)
	require.Equal(t, secondID, commit.entries[0].workspaceMutationID)
	require.Equal(t, thirdID, commit.entries[1].workspaceMutationID)
	require.Same(t, second, commit.entries[0].bat)
	require.Same(t, third, commit.entries[1].bat)

	// A later physical generation must not change the payload already frozen
	// for commit encoding.
	replacement := newInt64BatchForTest(t, proc, []string{"pk"}, []int64{222})
	require.NoError(t, workspace.replaceMemory(secondID, second, replacement, nil))
	require.Same(t, second, commit.entries[0].bat)
	require.ErrorContains(t, workspace.close(proc.Mp()), "still has 1 lease")
	commit.Close()
	require.NoError(t, workspace.close(proc.Mp()))
}

func TestOrderedMutationSetMaintainsImmutableCommitOrder(t *testing.T) {
	set := newOrderedMutationSet()
	first := &workspaceMutation{id: 1, commitOrder: workspaceCommitOrder{30}}
	second := &workspaceMutation{id: 2, commitOrder: workspaceCommitOrder{10}}
	third := &workspaceMutation{id: 3, commitOrder: workspaceCommitOrder{20}}
	set.add(first)
	set.add(second)
	set.add(third)
	require.Equal(t, []workspaceMutationID{2, 3, 1}, set.ids())
	set.remove(third)
	require.Equal(t, []workspaceMutationID{2, 1}, set.ids())
}

func TestWorkspaceCommitBuilderMaterializesSelectionsWithoutMutatingWorkspace(t *testing.T) {
	proc := testutil.NewProc(t)
	workspace := newTxnWorkspace()
	original := newInsertBatchWithRowIDForTest(t, proc, []int64{11, 22, 33})
	id := workspace.append(Entry{
		typ: INSERT, accountId: 1, databaseId: 2, tableId: 3,
		bat: original,
	})
	require.NoError(t, workspace.replaceSelections(id, []int64{1}))
	entries, err := workspace.commitEntries()
	require.NoError(t, err)
	builder := &workspaceCommitBuilder{entries: entries, mp: proc.Mp()}

	entry, release, err := builder.materializeEntry(&entries.entries[0])
	require.NoError(t, err)
	require.Equal(t, 2, entry.bat.RowCount())
	require.Equal(t, []int64{11, 33},
		vector.MustFixedColWithTypeCheck[int64](entry.bat.Vecs[1]))
	require.Equal(t, 3, original.RowCount())
	require.Equal(t, []int64{1}, mustMutationSelections(t, workspace, id))
	release()
	builder.Close()
	require.NoError(t, workspace.close(proc.Mp()))
}

func TestTxnWorkspaceMergeMemoryManyPublishesOneRevision(t *testing.T) {
	proc := testutil.NewProc(t)
	workspace := newTxnWorkspace()
	dstBat := newInt64BatchForTest(t, proc, []string{"pk"}, []int64{11})
	srcBat := newInt64BatchForTest(t, proc, []string{"pk"}, []int64{22})
	dstID := workspace.append(Entry{
		accountId: 1, databaseId: 2, tableId: 3, bat: dstBat,
	})
	srcID := workspace.append(Entry{
		accountId: 1, databaseId: 2, tableId: 3, bat: srcBat,
	})
	before := workspace.currentReadView()
	mergedBat := newInt64BatchForTest(t, proc, []string{"pk"}, []int64{11, 22})
	revision := workspace.revision
	require.NoError(t, workspace.compactMemoryMany([]workspaceMutationCompaction{{
		dstMutationID:  dstID,
		dstOldBat:      dstBat,
		dstNewBat:      mergedBat,
		srcMutationIDs: []workspaceMutationID{srcID},
		srcOldBats:     []*batch.Batch{srcBat},
	}}))
	require.Equal(t, revision+1, workspace.revision)
	require.Equal(t, workspace.revision, workspace.mutations[srcID].retiredRevision)

	oldEntries, err := workspace.tableEntries(before, 1, 2, 3)
	require.NoError(t, err)
	require.Len(t, oldEntries.entries, 2)
	require.Same(t, dstBat, oldEntries.entries[0].bat)
	require.Same(t, srcBat, oldEntries.entries[1].bat)
	newEntries, err := workspace.tableEntries(workspace.currentReadView(), 1, 2, 3)
	require.NoError(t, err)
	require.Len(t, newEntries.entries, 1)
	require.Same(t, mergedBat, newEntries.entries[0].bat)

	oldEntries.Close()
	newEntries.Close()
	require.NoError(t, workspace.close(proc.Mp()))
}

func TestTxnWorkspaceRetryPreparationCompactsCompletedAttempt(t *testing.T) {
	proc := testutil.NewProc(t)
	workspace := newTxnWorkspace()
	dstBat := newInt64BatchForTest(t, proc, []string{"pk"}, []int64{11})
	srcBat := newInt64BatchForTest(t, proc, []string{"pk"}, []int64{22})
	dstID := workspace.append(Entry{
		accountId: 1, databaseId: 2, tableId: 3, bat: dstBat,
	})
	srcID := workspace.append(Entry{
		accountId: 1, databaseId: 2, tableId: 3, bat: srcBat,
	})
	_, err := workspace.advanceStatement()
	require.NoError(t, err)

	failed, err := workspace.rollbackCurrentAttempt()
	require.NoError(t, err)
	failed.Close()
	require.Equal(t, statementAttemptRolledBack, workspace.journal.current.state)
	require.True(t, workspace.journal.retryPending)

	mergedBat := newInt64BatchForTest(
		t, proc, []string{"pk"}, []int64{11, 22})
	require.NoError(t, workspace.compactMemoryMany(
		[]workspaceMutationCompaction{{
			dstMutationID:  dstID,
			dstOldBat:      dstBat,
			dstNewBat:      mergedBat,
			srcMutationIDs: []workspaceMutationID{srcID},
			srcOldBats:     []*batch.Batch{srcBat},
		}},
	))
	require.Equal(t, statementAttemptRolledBack, workspace.journal.current.state)
	require.True(t, workspace.journal.retryPending)
	require.Equal(t, uint64(0), workspace.mutations[dstID].statementID)
	require.Equal(t, uint64(1), workspace.mutations[dstID].attemptID)

	retry, err := workspace.advanceStatement()
	require.NoError(t, err)
	require.Equal(t, uint64(1), retry.statementID)
	require.Equal(t, uint64(2), retry.attemptID)
	retryRollback, err := workspace.rollbackCurrentAttempt()
	require.NoError(t, err)
	require.Empty(t, retryRollback.mutationIDs)
	retryRollback.Close()

	entries, err := workspace.tableEntries(
		workspace.currentReadView(), 1, 2, 3)
	require.NoError(t, err)
	require.Len(t, entries.entries, 1)
	require.Same(t, mergedBat, entries.entries[0].bat)
	entries.Close()
	require.NoError(t, workspace.close(proc.Mp()))
}

func TestTxnWorkspaceBatchOperationsValidateBeforePublishing(t *testing.T) {
	proc := testutil.NewProc(t)
	workspace := newTxnWorkspace()
	firstBat := newInt64BatchForTest(t, proc, []string{"pk"}, []int64{11})
	secondBat := newInt64BatchForTest(t, proc, []string{"pk"}, []int64{22})
	firstID := workspace.append(Entry{
		accountId: 1, databaseId: 2, tableId: 3, bat: firstBat,
	})
	secondID := workspace.append(Entry{
		accountId: 1, databaseId: 2, tableId: 3, bat: secondBat,
	})
	revision := workspace.revision

	err := workspace.retireMutations([]workspaceMutationID{firstID, 9999})
	require.ErrorContains(t, err, "not active")
	require.Equal(t, revision, workspace.revision)
	require.True(t, workspace.mutations[firstID].active)
	require.True(t, workspace.mutations[secondID].active)

	mergedBat := newInt64BatchForTest(t, proc, []string{"pk"}, []int64{11, 22})
	wrongBat := newInt64BatchForTest(t, proc, []string{"pk"}, []int64{33})
	err = workspace.compactMemoryMany([]workspaceMutationCompaction{{
		dstMutationID:  firstID,
		dstOldBat:      firstBat,
		dstNewBat:      mergedBat,
		srcMutationIDs: []workspaceMutationID{secondID},
		srcOldBats:     []*batch.Batch{wrongBat},
	}})
	require.ErrorContains(t, err, "generation changed")
	require.Equal(t, revision, workspace.revision)
	require.True(t, workspace.mutations[firstID].active)
	require.True(t, workspace.mutations[secondID].active)
	mergedBat.Clean(proc.Mp())
	wrongBat.Clean(proc.Mp())
	require.NoError(t, workspace.close(proc.Mp()))
}

func TestTxnWorkspaceDroppedTableUsesExactOverlayIdentity(t *testing.T) {
	workspace := newTxnWorkspace()
	require.NoError(t, workspace.markTableDropped(1, 10, 42))

	dropped := workspace.droppedTablesSnapshot()
	require.True(t, dropped.containsEntry(Entry{
		accountId: 1, databaseId: 10, tableId: 42,
	}))
	require.False(t, dropped.containsEntry(Entry{
		accountId: 2, databaseId: 10, tableId: 42,
	}))
	require.False(t, dropped.containsEntry(Entry{
		accountId: 1, databaseId: 11, tableId: 42,
	}))
	// Legacy ALTER notes only carry table ID, so the commit bridge derives
	// this secondary lookup from the exact overlay snapshot.
	require.True(t, dropped.containsTableID(42))
}

func TestTxnWorkspaceDroppedTableEntriesPinOnlyExactOverlays(t *testing.T) {
	proc := testutil.NewProc(t)
	workspace := newTxnWorkspace()
	droppedID := workspace.append(Entry{
		typ: INSERT, accountId: 1, databaseId: 10, tableId: 42,
		bat: newInt64BatchForTest(t, proc, []string{"pk"}, []int64{11}),
	})
	workspace.append(Entry{
		typ: INSERT, accountId: 2, databaseId: 10, tableId: 42,
		bat: newInt64BatchForTest(t, proc, []string{"pk"}, []int64{22}),
	})
	workspace.append(Entry{
		typ: DELETE, accountId: 1, databaseId: 11, tableId: 42,
		bat: newInt64BatchForTest(t, proc, []string{"pk"}, []int64{33}),
	})
	require.NoError(t, workspace.markTableDropped(1, 10, 42))

	dropped, entries, err := workspace.droppedTableEntries()
	require.NoError(t, err)
	require.True(t, dropped.containsEntry(Entry{
		accountId: 1, databaseId: 10, tableId: 42,
	}))
	require.Len(t, entries.entries, 1)
	require.Equal(t, droppedID, entries.entries[0].workspaceMutationID)
	entries.Close()
	require.NoError(t, workspace.close(proc.Mp()))
}

func TestTxnWorkspaceDroppedTableFollowsStatementRollbackAndRetry(t *testing.T) {
	workspace := newTxnWorkspace()
	attempt, err := workspace.advanceStatement()
	require.NoError(t, err)
	require.NoError(t, workspace.markTableDropped(1, 10, 42))
	require.False(t, workspace.droppedTablesSnapshot().empty())

	rolledBack, err := workspace.rollbackCurrentAttempt()
	require.NoError(t, err)
	rolledBack.Close()
	require.True(t, workspace.droppedTablesSnapshot().empty())

	retry, err := workspace.advanceStatement()
	require.NoError(t, err)
	require.Equal(t, attempt.statementID, retry.statementID)
	require.Equal(t, attempt.attemptID+1, retry.attemptID)
	require.NoError(t, workspace.markTableDropped(1, 10, 42))
	require.True(t, workspace.droppedTablesSnapshot().containsEntry(Entry{
		accountId: 1, databaseId: 10, tableId: 42,
	}))
}

func TestTxnWorkspaceDDLCatalogRollbackRestoresCompletedStatement(t *testing.T) {
	workspace := newTxnWorkspace()
	databaseKey := genDatabaseKey(1, "db")
	tableKey := genTableKey(1, "tbl", 7, "db")
	database := &txnDatabase{databaseId: 7, databaseName: "db"}
	table := &txnTable{tableId: 42, tableName: "tbl", db: database}

	require.NoError(t, workspace.addDatabaseOp(databaseKey, INSERT, database.databaseId, database))
	require.NoError(t, workspace.addTableOp(tableKey, INSERT, table.tableId, table))
	require.NoError(t, workspace.addCreatedTable(table.tableId))
	require.Same(t, database, workspace.activeDatabase(databaseKey))
	require.Same(t, table, workspace.activeTable(tableKey))
	require.True(t, workspace.tableCreatedInTxn(table.tableId))
	databaseName, tableName, deleted := workspace.tableNameByID(table.tableId)
	require.Equal(t, "db", databaseName)
	require.Equal(t, "tbl", tableName)
	require.False(t, deleted)

	workspace.advanceStatement()
	require.NoError(t, workspace.addTableOp(tableKey, DELETE, table.tableId, nil))
	require.NoError(t, workspace.addDatabaseOp(databaseKey, DELETE, database.databaseId, nil))
	require.Nil(t, workspace.activeDatabase(databaseKey))
	require.Nil(t, workspace.activeTable(tableKey))
	require.True(t, workspace.databaseDeleted(databaseKey))
	require.True(t, workspace.tableDeleted(tableKey))
	require.True(t, workspace.tableCreatedInTxn(table.tableId))
	_, _, deleted = workspace.tableNameByID(table.tableId)
	require.True(t, deleted)

	rollback, err := workspace.rollbackCurrentAttempt()
	require.NoError(t, err)
	rollback.Close()
	require.Same(t, database, workspace.activeDatabase(databaseKey))
	require.Same(t, table, workspace.activeTable(tableKey))
	require.False(t, workspace.databaseDeleted(databaseKey))
	require.False(t, workspace.tableDeleted(tableKey))
	require.True(t, workspace.tableCreatedInTxn(table.tableId))
	databaseName, tableName, deleted = workspace.tableNameByID(table.tableId)
	require.Equal(t, "db", databaseName)
	require.Equal(t, "tbl", tableName)
	require.False(t, deleted)
}

func TestTxnWorkspaceDDLCatalogRenameHasOneActiveNameAndRollsBack(t *testing.T) {
	workspace := newTxnWorkspace()
	database := &txnDatabase{databaseId: 7, databaseName: "db"}
	table := &txnTable{tableId: 42, tableName: "old", db: database}
	oldKey := genTableKey(1, "old", 7, "db")
	newKey := genTableKey(1, "new", 7, "db")

	require.NoError(t, workspace.addTableOp(oldKey, INSERT, table.tableId, table))
	_, err := workspace.advanceStatement()
	require.NoError(t, err)

	require.NoError(t, workspace.addTableOp(oldKey, DELETE, table.tableId, nil))
	table.tableName = "new"
	require.NoError(t, workspace.addTableOp(newKey, INSERT, table.tableId, table))
	require.Nil(t, workspace.activeTable(oldKey))
	require.Same(t, table, workspace.activeTable(newKey))
	databaseName, tableName, deleted := workspace.tableNameByID(table.tableId)
	require.Equal(t, "db", databaseName)
	require.Equal(t, "new", tableName)
	require.False(t, deleted)

	rollback, err := workspace.rollbackCurrentAttempt()
	require.NoError(t, err)
	rollback.Close()
	table.tableName = "old"
	require.Same(t, table, workspace.activeTable(oldKey))
	require.Nil(t, workspace.activeTable(newKey))
	databaseName, tableName, deleted = workspace.tableNameByID(table.tableId)
	require.Equal(t, "db", databaseName)
	require.Equal(t, "old", tableName)
	require.False(t, deleted)
}

func TestTxnWorkspaceDDLCatalogRejectsMultipleActiveNames(t *testing.T) {
	workspace := newTxnWorkspace()
	database := &txnDatabase{databaseId: 7, databaseName: "db"}
	table := &txnTable{tableId: 42, tableName: "old", db: database}
	oldKey := genTableKey(1, "old", 7, "db")
	newKey := genTableKey(1, "new", 7, "db")

	require.NoError(t, workspace.addTableOp(oldKey, INSERT, table.tableId, table))
	require.ErrorContains(t,
		workspace.addTableOp(newKey, INSERT, table.tableId, table),
		"multiple active name bindings",
	)
	require.Len(t, workspace.journal.current.tableOps, 1)
	require.Same(t, table, workspace.activeTable(oldKey))
	require.Nil(t, workspace.activeTable(newKey))
	databaseName, tableName, deleted := workspace.tableNameByID(table.tableId)
	require.Equal(t, "db", databaseName)
	require.Equal(t, "old", tableName)
	require.False(t, deleted)
}

func TestTxnWorkspaceDDLCatalogRollbackRemovesRepeatedCurrentAttemptOps(t *testing.T) {
	workspace := newTxnWorkspace()
	databaseKey := genDatabaseKey(1, "db")
	tableKey := genTableKey(1, "tbl", 7, "db")
	database := &txnDatabase{databaseId: 7, databaseName: "db"}
	table := &txnTable{tableId: 42, tableName: "tbl", db: database}

	require.NoError(t, workspace.addDatabaseOp(databaseKey, INSERT, database.databaseId, database))
	require.NoError(t, workspace.addDatabaseOp(databaseKey, DELETE, database.databaseId, nil))
	require.NoError(t, workspace.addTableOp(tableKey, INSERT, table.tableId, table))
	require.NoError(t, workspace.addTableOp(tableKey, DELETE, table.tableId, nil))
	require.NoError(t, workspace.addCreatedTable(table.tableId))
	require.True(t, workspace.databaseDeleted(databaseKey))
	require.True(t, workspace.tableDeleted(tableKey))

	rollback, err := workspace.rollbackCurrentAttempt()
	require.NoError(t, err)
	rollback.Close()
	require.Nil(t, workspace.activeDatabase(databaseKey))
	require.Nil(t, workspace.activeTable(tableKey))
	require.False(t, workspace.databaseDeleted(databaseKey))
	require.False(t, workspace.tableDeleted(tableKey))
	require.False(t, workspace.tableCreatedInTxn(table.tableId))
	_, _, deleted := workspace.tableNameByID(table.tableId)
	require.False(t, deleted)

	retry, err := workspace.advanceStatement()
	require.NoError(t, err)
	require.Equal(t, uint64(0), retry.statementID)
	require.Equal(t, uint64(2), retry.attemptID)
	require.NoError(t, workspace.addTableOp(tableKey, INSERT, table.tableId, table))
	require.Same(t, table, workspace.activeTable(tableKey))
}

func TestTxnWorkspaceStatementRollbackActionsFollowAttemptLifecycle(t *testing.T) {
	workspace := newTxnWorkspace()
	executed := make([]string, 0, 3)

	// A successful statement boundary releases its restoration state. A later
	// statement rollback must not restore state owned by the completed one.
	require.NoError(t, workspace.addStatementRollbackAction(func() {
		executed = append(executed, "completed")
	}))
	second, err := workspace.advanceStatement()
	require.NoError(t, err)
	require.Equal(t, uint64(1), second.statementID)
	require.Empty(t, workspace.journal.current.rollbackActions)

	require.NoError(t, workspace.addStatementRollbackAction(func() {
		executed = append(executed, "first")
	}))
	require.NoError(t, workspace.addStatementRollbackAction(func() {
		executed = append(executed, "second")
	}))
	rollback, err := workspace.rollbackCurrentAttempt()
	require.NoError(t, err)
	require.Empty(t, workspace.journal.current.rollbackActions)
	rollback.RunActions()
	rollback.RunActions()
	rollback.Close()
	require.Equal(t, []string{"second", "first"}, executed)

	// A retry has a new attempt identity and owns only its own actions.
	retry, err := workspace.advanceStatement()
	require.NoError(t, err)
	require.Equal(t, second.statementID, retry.statementID)
	require.Equal(t, second.attemptID+1, retry.attemptID)
	require.NoError(t, workspace.addStatementRollbackAction(func() {
		executed = append(executed, "retry")
	}))
	retryRollback, err := workspace.rollbackCurrentAttempt()
	require.NoError(t, err)
	retryRollback.RunActions()
	retryRollback.Close()
	require.Equal(t, []string{"second", "first", "retry"}, executed)
}

func TestTxnWorkspaceOwnsStatementExecutionLifecycle(t *testing.T) {
	workspace := newTxnWorkspace()

	require.ErrorContains(t, workspace.markStatementBoundaryAdvanced(),
		"StartStatement not called")
	require.NoError(t, workspace.beginStatementExecution())
	require.ErrorContains(t, workspace.beginStatementExecution(),
		"StartStatement called twice")
	require.NoError(t, workspace.markStatementBoundaryAdvanced())
	require.ErrorContains(t, workspace.markStatementBoundaryAdvanced(),
		"IncrStatementID called twice")

	workspace.reopenStatementBoundary()
	require.NoError(t, workspace.markStatementBoundaryAdvanced())
	retired, err := workspace.endStatementExecution()
	require.NoError(t, err)
	require.Empty(t, retired)
	_, err = workspace.endStatementExecution()
	require.ErrorContains(t, err,
		"StartStatement not called")
}

func TestWorkspacePayloadStoreReclaimsRetiredPhysicalGeneration(t *testing.T) {
	proc := testutil.NewProc(t)
	store := newWorkspacePayloadStore()
	original := newInt64BatchForTest(t, proc, []string{"pk"}, []int64{1})
	replacement := newInt64BatchForTest(t, proc, []string{"pk"}, []int64{2})
	payloadID := store.addMemory(original, nil, 1)
	require.NoError(t, store.replaceMemory(
		payloadID, original, replacement, nil, 2))

	reclaimed := store.reclaimRetired(nil)
	require.Equal(t, []*batch.Batch{original}, reclaimed)
	_, err := store.pin(payloadID, 1)
	require.ErrorContains(t, err, "not visible in read view")
	current, err := store.pin(payloadID, 2)
	require.NoError(t, err)
	require.Same(t, replacement, current.bat)
	current.Close()

	original.Clean(proc.Mp())
	require.NoError(t, store.close(proc.Mp()))
}

func TestTxnWorkspaceReplaceMemoryRejectsOwnedTargetAtomically(t *testing.T) {
	proc := testutil.NewProc(t)
	workspace := newTxnWorkspace()
	first := newInt64BatchForTest(t, proc, []string{"pk"}, []int64{1})
	second := newInt64BatchForTest(t, proc, []string{"pk"}, []int64{2})
	firstID := workspace.append(Entry{
		accountId: 1, databaseId: 2, tableId: 3, bat: first,
	})
	secondID := workspace.append(Entry{
		accountId: 1, databaseId: 2, tableId: 3, bat: second,
	})
	before := workspace.currentReadView()

	require.ErrorContains(t,
		workspace.replaceMemory(firstID, first, second, nil),
		"replacement target is already owned")
	require.Equal(t, before, workspace.currentReadView())

	entries, err := workspace.tableEntries(before, 1, 2, 3)
	require.NoError(t, err)
	require.Len(t, entries.entries, 2)
	require.Equal(t, firstID, entries.entries[0].workspaceMutationID)
	require.Same(t, first, entries.entries[0].bat)
	require.Equal(t, secondID, entries.entries[1].workspaceMutationID)
	require.Same(t, second, entries.entries[1].bat)
	entries.Close()
	require.NoError(t, workspace.close(proc.Mp()))
}

func TestWorkspacePayloadStoreDoesNotCleanSharedSelectionBatch(t *testing.T) {
	proc := testutil.NewProc(t)
	store := newWorkspacePayloadStore()
	physical := newInt64BatchForTest(t, proc, []string{"pk"}, []int64{1, 2})
	payloadID := store.addMemory(physical, []int64{0}, 1)
	require.NoError(t, store.replaceSelections(payloadID, []int64{1}, 2))

	require.Empty(t, store.reclaimRetired(nil))
	_, err := store.pin(payloadID, 1)
	require.ErrorContains(t, err, "not visible in read view")
	current, err := store.pin(payloadID, 2)
	require.NoError(t, err)
	require.Same(t, physical, current.bat)
	require.Equal(t, []int64{1}, current.selections)
	current.Close()
	require.NoError(t, store.close(proc.Mp()))
}

func TestWorkspacePayloadStorePinnedGenerationDefersReclamation(t *testing.T) {
	proc := testutil.NewProc(t)
	store := newWorkspacePayloadStore()
	original := newInt64BatchForTest(t, proc, []string{"pk"}, []int64{1})
	replacement := newInt64BatchForTest(t, proc, []string{"pk"}, []int64{2})
	payloadID := store.addMemory(original, nil, 1)
	require.NoError(t, store.replaceMemory(
		payloadID, original, replacement, nil, 2))

	oldView, err := store.pin(payloadID, 1)
	require.NoError(t, err)
	require.Empty(t, store.reclaimRetired(nil))
	oldView.Close()
	reclaimed := store.reclaimRetired(nil)
	require.Equal(t, []*batch.Batch{original}, reclaimed)

	original.Clean(proc.Mp())
	require.NoError(t, store.close(proc.Mp()))
}

func TestTxnWorkspaceEndStatementProtectsRewriteRollbackPayload(t *testing.T) {
	proc := testutil.NewProc(t)
	workspace := newTxnWorkspace()
	original := newInt64BatchForTest(t, proc, []string{"pk"}, []int64{1})
	mutationID := workspace.append(Entry{
		accountId: 1, databaseId: 2, tableId: 3, bat: original,
	})
	beforeRewrite := workspace.currentReadView()
	workspace.advanceStatement()

	replacement := newInt64BatchForTest(t, proc, []string{"pk"}, []int64{2})
	_, err := workspace.rewriteMutations([]workspaceMutationRewrite{{
		mutationID: mutationID,
		oldBat:     original,
		entry: Entry{
			accountId: 1, databaseId: 2, tableId: 3, bat: replacement,
		},
	}})
	require.NoError(t, err)
	require.NoError(t, workspace.beginStatementExecution())
	reclaimed, err := workspace.endStatementExecution()
	require.NoError(t, err)
	require.Empty(t, reclaimed)
	require.NotNil(t, workspace.mutations[mutationID])
	require.Contains(t, workspace.retiredMutationIDs, mutationID)

	undo := workspace.journal.current.rewriteUndo[mutationID]
	oldView, err := workspace.payloads.pin(undo.payloadID, beforeRewrite.Revision())
	require.NoError(t, err)
	require.Same(t, original, oldView.bat)
	oldView.Close()

	// Once the next statement boundary advances, the completed statement can
	// no longer roll back to original. Its unpinned physical generation is
	// reclaimed at the next execution boundary.
	workspace.advanceStatement()
	require.NoError(t, workspace.beginStatementExecution())
	reclaimed, err = workspace.endStatementExecution()
	require.NoError(t, err)
	require.Equal(t, []*batch.Batch{original}, reclaimed)
	require.Nil(t, workspace.mutations[mutationID])
	require.NotContains(t, workspace.retiredMutationIDs, mutationID)
	original.Clean(proc.Mp())
	require.NoError(t, workspace.close(proc.Mp()))
}

func TestTxnWorkspaceEndStatementReclaimsUnownedRetiredMetadata(t *testing.T) {
	proc := testutil.NewProc(t)
	workspace := newTxnWorkspace()
	original := newInt64BatchForTest(t, proc, []string{"pk"}, []int64{1})
	mutationID := workspace.append(Entry{
		accountId: 1, databaseId: 2, tableId: 3, bat: original,
	})
	readView := workspace.currentReadView()
	require.NoError(t, workspace.retireMutations([]workspaceMutationID{mutationID}))

	entries, err := workspace.tableEntries(readView, 1, 2, 3)
	require.NoError(t, err)
	require.Len(t, entries.entries, 1)
	require.Same(t, original, entries.entries[0].bat)
	entries.Close()

	require.NoError(t, workspace.beginStatementExecution())
	reclaimed, err := workspace.endStatementExecution()
	require.NoError(t, err)
	require.Equal(t, []*batch.Batch{original}, reclaimed)
	require.Nil(t, workspace.mutations[mutationID])
	require.NotContains(t, workspace.retiredMutationIDs, mutationID)
	overlay := workspace.overlays[workspaceOverlayKey{
		accountID: 1, databaseID: 2, tableID: 3,
	}]
	require.Empty(t, overlay.retiredMutations)

	original.Clean(proc.Mp())
	require.NoError(t, workspace.close(proc.Mp()))
}

func TestTxnWorkspaceReadViewCanRepinRewrittenPayloadDuringStatement(t *testing.T) {
	proc := testutil.NewProc(t)
	workspace := newTxnWorkspace()
	original := newInt64BatchForTest(t, proc, []string{"pk"}, []int64{1})
	mutationID := workspace.append(Entry{
		accountId: 1, databaseId: 2, tableId: 3, bat: original,
	})
	readView := workspace.currentReadView()
	workspace.advanceStatement()
	require.NoError(t, workspace.beginStatementExecution())

	firstRead, err := workspace.tableEntries(readView, 1, 2, 3)
	require.NoError(t, err)
	require.Len(t, firstRead.entries, 1)
	require.Same(t, original, firstRead.entries[0].bat)
	firstRead.Close()

	replacement := newInt64BatchForTest(t, proc, []string{"pk"}, []int64{2})
	_, err = workspace.rewriteMutations([]workspaceMutationRewrite{{
		mutationID: mutationID,
		oldBat:     original,
		entry: Entry{
			accountId: 1, databaseId: 2, tableId: 3, bat: replacement,
		},
	}})
	require.NoError(t, err)

	// Closing the first lease must not invalidate the statement's immutable
	// logical view. A later workspace consumer in the same execution can pin
	// the generation selected by that view again, even after a rewrite has
	// published a replacement generation.
	secondRead, err := workspace.tableEntries(readView, 1, 2, 3)
	require.NoError(t, err)
	require.Len(t, secondRead.entries, 1)
	require.Same(t, original, secondRead.entries[0].bat)

	currentRead, err := workspace.tableEntries(
		workspace.currentReadView(), 1, 2, 3)
	require.NoError(t, err)
	require.Len(t, currentRead.entries, 1)
	require.Same(t, replacement, currentRead.entries[0].bat)
	currentRead.Close()

	reclaimed, err := workspace.endStatementExecution()
	require.NoError(t, err)
	require.Empty(t, reclaimed)

	// EndStatement expires the logical ReadView, so retired metadata may be
	// reclaimed without retaining transaction-length publication history.
	_, err = workspace.tableEntries(readView, 1, 2, 3)
	require.ErrorContains(t, err, "workspace read view has expired")
	// A lease acquired while the view was valid remains usable until Close;
	// logical view expiry must never invalidate a Batch already handed out.
	require.Same(t, original, secondRead.entries[0].bat)
	secondRead.Close()
	require.NoError(t, workspace.close(proc.Mp()))
}

func TestTxnWorkspaceReadViewConcurrentWithPayloadRewrites(t *testing.T) {
	proc := testutil.NewProc(t)
	workspace := newTxnWorkspace()
	initial := newInt64BatchForTest(t, proc, []string{"pk"}, []int64{0})
	mutationID := workspace.append(Entry{
		accountId: 1, databaseId: 2, tableId: 3, bat: initial,
	})
	readView := workspace.currentReadView()
	replacements := make([]*batch.Batch, 32)
	for idx := range replacements {
		replacements[idx] = newInt64BatchForTest(
			t, proc, []string{"pk"}, []int64{int64(idx + 1)})
	}

	start := make(chan struct{})
	errs := make(chan error, 5)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for range 64 {
				entries, err := workspace.tableEntries(readView, 1, 2, 3)
				if err != nil {
					errs <- err
					return
				}
				if len(entries.entries) != 1 ||
					entries.entries[0].bat != initial {
					entries.Close()
					errs <- errors.New(
						"statement read view observed a rewritten payload")
					return
				}
				entries.Close()
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		current := initial
		for _, replacement := range replacements {
			if err := workspace.replaceMemory(
				mutationID, current, replacement, nil); err != nil {
				errs <- err
				return
			}
			current = replacement
		}
	}()

	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	current, err := workspace.tableEntries(
		workspace.currentReadView(), 1, 2, 3)
	require.NoError(t, err)
	require.Len(t, current.entries, 1)
	require.Same(t, replacements[len(replacements)-1], current.entries[0].bat)
	current.Close()
	require.NoError(t, workspace.close(proc.Mp()))
}

func TestTxnWorkspaceLoadFilesFollowAttemptLifecycle(t *testing.T) {
	workspace := newTxnWorkspace()
	require.NoError(t, workspace.appendLoadFiles(
		"completed.obj", "shared.obj"))
	workspace.advanceStatement()

	require.NoError(t, workspace.appendLoadFiles(
		"failed.obj", "shared.obj"))
	require.True(t, workspace.hasLoadFile("shared.obj"))
	rolledBack, err := workspace.rollbackCurrentAttempt()
	require.NoError(t, err)
	require.Equal(t, []string{"failed.obj", "shared.obj"}, rolledBack.loadFiles)
	failedFiles := slices.Clone(rolledBack.loadFiles)
	rolledBack.Close()

	physical, err := workspace.prepareLoadFileCleanup(failedFiles...)
	require.NoError(t, err)
	require.Equal(t, []string{"failed.obj"}, physical)
	require.NoError(t, workspace.completePhysicalLoadFileCleanup(physical...))
	require.True(t, workspace.hasLoadFile("shared.obj"))
	retry, err := workspace.advanceStatement()
	require.NoError(t, err)
	require.Equal(t, uint64(1), retry.statementID)
	require.Equal(t, uint64(2), retry.attemptID)
	require.NoError(t, workspace.appendLoadFiles("retry.obj"))
	require.ElementsMatch(t,
		[]string{"completed.obj", "shared.obj", "retry.obj"},
		workspace.allLoadFiles(),
	)
	require.NoError(t, workspace.validateUsage())

	retryRollback, err := workspace.rollbackCurrentAttempt()
	require.NoError(t, err)
	require.Equal(t, []string{"retry.obj"}, retryRollback.loadFiles)
	retryRollback.Close()
	require.NoError(t, workspace.close(mpool.MustNewZero()))
}

func BenchmarkTxnWorkspaceCurrentStateIgnoresRetiredHistory(b *testing.B) {
	for _, retired := range []int{0, 1_000, 10_000} {
		b.Run("retired="+strconv.Itoa(retired), func(b *testing.B) {
			proc := testutil.NewProc(b)
			workspace := newTxnWorkspace()
			objectID := types.Objectid{1, 2, 3}
			stats := objectio.NewObjectStatsWithObjectID(
				&objectID, false, false, false)
			objectName := stats.ObjectName().String()

			for idx := 0; idx < retired; idx++ {
				mutationID := workspace.append(Entry{
					typ: INSERT, accountId: 1, databaseId: 2, tableId: 3,
					bat:     newWorkspaceObjectBatchForTest(b, proc, objectID),
					pkCheck: workspacePKCheck{vectorPos: 0, enabled: true},
				})
				if err := workspace.retireMutation(mutationID); err != nil {
					b.Fatal(err)
				}
			}
			workspace.append(Entry{
				typ: INSERT, accountId: 1, databaseId: 2, tableId: 3,
				bat:     newWorkspaceObjectBatchForTest(b, proc, objectID),
				pkCheck: workspacePKCheck{vectorPos: 0, enabled: true},
			})
			if err := workspace.markTableDropped(1, 2, 3); err != nil {
				b.Fatal(err)
			}
			currentView := workspace.currentReadView()

			b.Run("table-entries", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for idx := 0; idx < b.N; idx++ {
					entries, err := workspace.tableEntries(
						currentView, 1, 2, 3)
					if err != nil {
						b.Fatal(err)
					}
					if len(entries.entries) != 1 {
						b.Fatalf("expected one active entry, got %d", len(entries.entries))
					}
					entries.Close()
				}
			})

			b.Run("commit-entries", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for idx := 0; idx < b.N; idx++ {
					entries, err := workspace.commitEntries()
					if err != nil {
						b.Fatal(err)
					}
					if len(entries.entries) != 1 {
						b.Fatalf("expected one active entry, got %d", len(entries.entries))
					}
					entries.Close()
				}
			})

			b.Run("compaction-candidates", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for idx := 0; idx < b.N; idx++ {
					plan, err := workspace.beginCompactionPlan(0)
					if err != nil {
						b.Fatal(err)
					}
					entries := plan.entries
					if len(entries.entries) != 1 {
						b.Fatalf("expected one active entry, got %d", len(entries.entries))
					}
					plan.Close()
				}
			})

			b.Run("pk-candidates", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for idx := 0; idx < b.N; idx++ {
					entries, err := workspace.pkCandidateEntries(
						workspace.currentReadView())
					if err != nil {
						b.Fatal(err)
					}
					if len(entries.entries) != 1 {
						b.Fatalf("expected one active PK candidate, got %d", len(entries.entries))
					}
					entries.Close()
				}
			})

			b.Run("dump-all", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for idx := 0; idx < b.N; idx++ {
					entries, err := workspace.entriesForDumpScope(workspaceDumpAll(false))
					if err != nil {
						b.Fatal(err)
					}
					if len(entries.entries) != 1 {
						b.Fatalf("expected one active dump entry, got %d", len(entries.entries))
					}
					entries.Close()
				}
			})

			b.Run("dropped-table-entries", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for idx := 0; idx < b.N; idx++ {
					_, entries, err := workspace.droppedTableEntries()
					if err != nil {
						b.Fatal(err)
					}
					if len(entries.entries) != 1 {
						b.Fatalf("expected one active dropped-table entry, got %d", len(entries.entries))
					}
					entries.Close()
				}
			})

			b.Run("object-references", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for idx := 0; idx < b.N; idx++ {
					live := workspace.liveObjectReferences(
						[]string{objectName}, nil)
					if len(live) != 1 {
						b.Fatalf("expected one live object, got %d", len(live))
					}
				}
			})

			b.StopTimer()
			if err := workspace.close(proc.Mp()); err != nil {
				b.Fatal(err)
			}
		})
	}
}

func BenchmarkTxnWorkspaceObjectDeleteSnapshotIgnoresRetiredHistory(b *testing.B) {
	for _, retired := range []int{0, 1_000, 10_000} {
		b.Run("retired="+strconv.Itoa(retired), func(b *testing.B) {
			workspace := newTxnWorkspace()
			for idx := 0; idx < retired; idx++ {
				deleteID := workspace.appendObjectDelete(
					1, 2, 3, types.Blockid{byte(idx)}, []int64{int64(idx)})
				workspace.mu.Lock()
				workspace.revision++
				workspace.retireObjectDeleteStateLocked(
					workspace.objectDeletes[deleteID], workspace.revision)
				workspace.mu.Unlock()
			}
			activeID := workspace.appendObjectDelete(
				1, 2, 3, types.Blockid{255}, []int64{1})

			b.ReportAllocs()
			b.ResetTimer()
			for idx := 0; idx < b.N; idx++ {
				snapshot := workspace.snapshotObjectDeletes()
				if len(snapshot.ids) != 1 || snapshot.ids[0] != activeID {
					b.Fatalf("unexpected active delete snapshot: %v", snapshot.ids)
				}
			}
		})
	}
}

func BenchmarkWorkspacePayloadReclaimIgnoresUnrelatedHistory(b *testing.B) {
	for _, unrelated := range []int{0, 1_000, 10_000} {
		b.Run("unrelated="+strconv.Itoa(unrelated), func(b *testing.B) {
			store := newWorkspacePayloadStore()
			for idx := 0; idx < unrelated; idx++ {
				store.addMemory(&batch.Batch{}, nil, 1)
			}
			physical := &batch.Batch{}
			payloadID := store.addMemory(physical, nil, 1)

			b.ReportAllocs()
			b.ResetTimer()
			for idx := 0; idx < b.N; idx++ {
				if err := store.replaceSelections(
					payloadID, []int64{int64(idx)}, uint64(idx+2)); err != nil {
					b.Fatal(err)
				}
				if reclaimed := store.reclaimRetired(nil); len(reclaimed) != 0 {
					b.Fatalf("shared physical Batch was reclaimed: %v", reclaimed)
				}
			}
		})
	}
}

func BenchmarkTxnWorkspaceRCBoundaryStateIgnoresHistory(b *testing.B) {
	for _, completed := range []int{0, 1_000, 10_000} {
		b.Run("completed="+strconv.Itoa(completed), func(b *testing.B) {
			workspace := newTxnWorkspace()
			for idx := 0; idx < completed; idx++ {
				workspace.advanceRCStatement(rcBoundaryPublication{
					recordStatement: true,
					statementSnapshot: timestamp.Timestamp{
						PhysicalTime: int64(idx + 1),
					},
				})
			}

			b.ReportAllocs()
			b.ResetTimer()
			for idx := 0; idx < b.N; idx++ {
				_ = workspace.rcBoundaryState()
			}
		})
	}
}

func BenchmarkTxnWorkspaceLoadFileRemovalIgnoresAttemptHistory(b *testing.B) {
	for _, completed := range []int{0, 1_000, 10_000} {
		b.Run("completed="+strconv.Itoa(completed), func(b *testing.B) {
			workspace := newTxnWorkspace()
			for idx := 0; idx < completed; idx++ {
				workspace.advanceStatement()
			}

			b.ReportAllocs()
			b.ResetTimer()
			for idx := 0; idx < b.N; idx++ {
				if err := workspace.appendLoadFiles("current.obj"); err != nil {
					b.Fatal(err)
				}
				physical, err := workspace.prepareLoadFileCleanup("current.obj")
				if err != nil {
					b.Fatal(err)
				}
				if err := workspace.completePhysicalLoadFileCleanup(physical...); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkOrderedMutationSetSnapshot(b *testing.B) {
	for _, active := range []int{1_000, 10_000, 100_000} {
		b.Run("active="+strconv.Itoa(active), func(b *testing.B) {
			set := newOrderedMutationSet()
			for idx := 0; idx < active; idx++ {
				set.add(&workspaceMutation{
					id: workspaceMutationID(idx + 1),
					commitOrder: workspaceCommitOrder{
						uint64(active - idx),
					},
				})
			}

			b.ReportAllocs()
			b.ResetTimer()
			for idx := 0; idx < b.N; idx++ {
				ids := set.ids()
				if len(ids) != active {
					b.Fatalf("expected %d mutations, got %d", active, len(ids))
				}
			}
		})
	}
}
