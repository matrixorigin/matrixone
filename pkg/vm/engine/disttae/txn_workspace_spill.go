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
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

type workspaceDumpScopeKind uint8

const (
	workspaceDumpScopeCurrentAttempt workspaceDumpScopeKind = iota + 1
	workspaceDumpScopeAll
)

// workspaceDumpScope describes logical statement ownership, never a physical
// slice position or an indirectly inferred read frontier. A statement-local
// dump processes the current StatementJournal attempt. Statement finalization
// and commit use the all scope; commit additionally enables the stricter commit
// thresholds and tombstone spill policy.
type workspaceDumpScope struct {
	kind     workspaceDumpScopeKind
	commit   bool
	terminal bool
}

func workspaceDumpCurrentAttempt() workspaceDumpScope {
	return workspaceDumpScope{kind: workspaceDumpScopeCurrentAttempt}
}

func workspaceDumpAll(commit bool) workspaceDumpScope {
	return workspaceDumpScope{
		kind:     workspaceDumpScopeAll,
		commit:   commit,
		terminal: commit,
	}
}

// workspaceDumpCommitBoundary is the final statement-boundary dump performed
// by Transaction.Commit. It keeps the ordinary write-threshold and tombstone
// policy because commit-time tombstone transfer has not run yet, but it may
// coalesce completed statements: any later error aborts the whole transaction,
// so no individual statement can be retried after this boundary begins.
func workspaceDumpCommitBoundary() workspaceDumpScope {
	return workspaceDumpScope{
		kind:     workspaceDumpScopeAll,
		terminal: true,
	}
}

// inMemoryInsertBytesForDumpScope returns the exact INSERT payload bytes that
// dumpInsertBatchLocked can consume from scope. The all-workspace path is a
// constant-time read of the mutation usage ledger; it must not materialize or
// pin every historical payload merely to decide whether quota can grow. A
// current-attempt dump is intentionally statement-local and normally contains
// only the mutations produced by the active Write call.
func (w *txnWorkspace) inMemoryInsertBytesForDumpScope(
	scope workspaceDumpScope,
) (uint64, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	if scope.kind == workspaceDumpScopeAll {
		return w.usage.spillEligibleInsertBytes, nil
	}
	if scope.kind != workspaceDumpScopeCurrentAttempt {
		return 0, moerr.NewInternalErrorNoCtx(
			"workspace dump scope is invalid")
	}

	var size uint64
	for _, id := range workspaceMutationIDs(w.journal.current.mutations) {
		mutation := w.mutations[id]
		if mutation == nil || !mutation.active {
			continue
		}
		bat, err := w.payloads.currentBatch(mutation.payloadID)
		if err != nil {
			return 0, err
		}
		size += usageOfWorkspaceEntry(
			mutation.entry,
			bat,
		).spillEligibleInsertBytes
	}
	return size, nil
}

func (w *txnWorkspace) entriesForDumpScope(
	scope workspaceDumpScope,
) (*workspaceEntrySet, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	view := client.NewWorkspaceReadView(
		w.id,
		w.revision,
		uint64(w.nextMutationID),
	)
	var ids []workspaceMutationID
	switch scope.kind {
	case workspaceDumpScopeCurrentAttempt:
		ids = workspaceMutationIDs(w.journal.current.mutations)
	case workspaceDumpScopeAll:
		ids = w.activeMutationIDsInCommitOrderLocked()
	default:
		return nil, moerr.NewInternalErrorNoCtx(
			"workspace dump scope is invalid")
	}
	activeIDs := ids[:0]
	for _, id := range ids {
		mutation := w.mutations[id]
		if mutation != nil && mutation.active {
			activeIDs = append(activeIDs, id)
		}
	}
	return w.entriesForMutationIDsLocked(view, activeIDs)
}

// workspaceSpillGroupKey preserves statement-attempt ownership for ordinary
// spills. Terminal commit spills deliberately leave statementID and attemptID
// zero, allowing all completed statements for one physical table to share an
// object after per-statement rollback has become impossible.
type workspaceSpillGroupKey struct {
	table         tableKey
	tableID       uint64
	autoIncrEpoch uint32
	autoIncrKnown bool
	statementID   uint64
	attemptID     uint64
}

type stagedWorkspaceSpill struct {
	objects []workspaceSpillObject
	stats   []objectio.ObjectStats
}

type workspaceSpillGroup struct {
	key     workspaceSpillGroupKey
	sources []workspaceSpillSource
}

// dumpWorkspaceMutationsLocked performs a three-phase spill. The transaction
// lock is held on entry and return, but never during remote object IO.
func (txn *Transaction) dumpWorkspaceMutationsLocked(
	ctx context.Context,
	fs fileservice.FileService,
	scope workspaceDumpScope,
	typ int,
	skipTable map[workspaceOverlayKey]bool,
) (rowCount int, err error) {
	entries, err := txn.workspace.entriesForDumpScope(scope)
	if err != nil {
		return 0, err
	}

	ids := make([]workspaceMutationID, 0, len(entries.entries))
	for idx := range entries.entries {
		entry := &entries.entries[idx]
		overlayKey := workspaceOverlayKey{
			accountID:  entry.accountId,
			databaseID: entry.databaseId,
			tableID:    entry.tableId,
		}
		if skipTable != nil && skipTable[overlayKey] {
			continue
		}
		if entry.isCatalog() || entry.bat == nil || entry.bat.RowCount() == 0 ||
			entry.typ != typ || entry.fileName != "" {
			continue
		}
		if err = txn.requireAutoIncrEpochFenceCommit(
			entry.autoIncrEpoch,
			entry.autoIncrEpochKnown,
		); err != nil {
			entries.Close()
			return 0, err
		}
		ids = append(ids, entry.workspaceMutationID)
	}
	entries.Close()
	if len(ids) == 0 {
		return 0, nil
	}

	var attempt *workspaceSpillAttempt
	if scope.terminal {
		attempt, err = txn.workspace.beginTerminalSpill(ids)
	} else {
		attempt, err = txn.workspace.beginSpill(ids)
	}
	if err != nil {
		return 0, err
	}
	defer attempt.Close()
	if len(attempt.sources) == 0 {
		return 0, nil
	}
	for idx := range attempt.sources {
		if attempt.sources[idx].entry.bat != nil {
			rowCount += attempt.sources[idx].entry.bat.RowCount()
		}
	}

	tables, err := txn.resolveDumpTablesLocked(ctx, attempt)
	if err != nil {
		return 0, err
	}
	if !txn.workspace.spillAttemptActive(attempt) {
		return 0, nil
	}

	// Payload leases make every selected generation immutable while the
	// transaction lock is released. Rollback may proceed concurrently, but a
	// stale spill can no longer publish after it reacquires the lock.
	txn.Unlock()
	staged, stageErr := txn.stageWorkspaceSpill(ctx, fs, typ, tables, attempt)
	txn.Lock()
	if stageErr != nil {
		txn.cleanStagedWorkspaceSpillLocked(staged)
		return 0, stageErr
	}

	_, err = txn.workspace.commitSpill(attempt, staged.objects)
	if err != nil {
		txn.cleanStagedWorkspaceSpillLocked(staged)
		return 0, err
	}

	for idx := range staged.objects {
		entry := staged.objects[idx].entry
		txn.publishSpilledObjectLocked(&entry)
	}
	txn.hasS3Op.Store(true)
	txn.readOnly.Store(false)
	return rowCount, nil
}

// spillAttemptActive is checked after table resolution, where txn.Lock was
// released. A concurrent statement rollback may retire the selected logical
// mutations during that window; in that case no remote object IO is started.
func (w *txnWorkspace) spillAttemptActive(attempt *workspaceSpillAttempt) bool {
	w.mu.RLock()
	defer w.mu.RUnlock()
	if attempt == nil || attempt.workspace != w || attempt.closed || attempt.committed {
		return false
	}
	for _, source := range attempt.sources {
		mutation := w.mutations[source.mutationID]
		if mutation == nil || !mutation.active || mutation.retiredRevision != 0 ||
			mutation.payloadID != source.payload.payloadID ||
			mutation.statementID != source.statementID ||
			mutation.attemptID != source.attemptID {
			return false
		}
	}
	return true
}

func (txn *Transaction) stageWorkspaceSpill(
	ctx context.Context,
	fs fileservice.FileService,
	typ int,
	tables map[tableKey]engine.Relation,
	attempt *workspaceSpillAttempt,
) (staged stagedWorkspaceSpill, err error) {
	orderedGroups := groupWorkspaceSpillSources(attempt)

	for _, group := range orderedGroups {
		table := tables[group.key.table]
		if table == nil {
			return staged, moerr.NewInternalError(ctx, "workspace spill table disappeared")
		}
		object, stats, hasRows, stageErr := txn.stageWorkspaceSpillGroup(
			ctx, fs, typ, table, group.key, group.sources)
		staged.stats = append(staged.stats, stats...)
		if stageErr != nil {
			return staged, stageErr
		}
		if hasRows {
			staged.objects = append(staged.objects, object)
		}
	}
	return staged, nil
}

// groupWorkspaceSpillSources is the only owner of physical spill grouping.
// Ordinary statement spills preserve retry ownership in the key. Terminal
// commit spills omit that ownership only after beginTerminalSpill has closed
// statement rollback, allowing one table object to represent the transaction's
// complete final state instead of one object per completed statement.
func groupWorkspaceSpillSources(
	attempt *workspaceSpillAttempt,
) []workspaceSpillGroup {
	groups := make(map[workspaceSpillGroupKey]int)
	orderedGroups := make([]workspaceSpillGroup, 0)
	for _, source := range attempt.sources {
		entry := source.entry
		key := workspaceSpillGroupKey{
			table: tableKey{
				accountId:  entry.accountId,
				databaseId: entry.databaseId,
				dbName:     entry.databaseName,
				name:       entry.tableName,
			},
			tableID:       entry.tableId,
			autoIncrEpoch: entry.autoIncrEpoch,
			autoIncrKnown: entry.autoIncrEpochKnown,
		}
		if !attempt.terminal {
			key.statementID = source.statementID
			key.attemptID = source.attemptID
		}
		index, ok := groups[key]
		if !ok {
			index = len(orderedGroups)
			groups[key] = index
			orderedGroups = append(orderedGroups, workspaceSpillGroup{key: key})
		}
		orderedGroups[index].sources = append(orderedGroups[index].sources, source)
	}
	return orderedGroups
}

func (txn *Transaction) stageWorkspaceSpillGroup(
	ctx context.Context,
	fs fileservice.FileService,
	typ int,
	table engine.Relation,
	key workspaceSpillGroupKey,
	sources []workspaceSpillSource,
) (
	object workspaceSpillObject,
	stats []objectio.ObjectStats,
	hasRows bool,
	err error,
) {
	var writer *colexec.CNS3Writer
	if typ == INSERT {
		writer = colexec.NewCNS3DataWriter(
			txn.proc.GetMPool(), fs, table.GetTableDef(txn.proc.Ctx), -1, false)
	} else {
		var pkCol *plan.ColDef = plan2.PkColByTableDef(table.GetTableDef(txn.proc.Ctx))
		writer = colexec.NewCNS3TombstoneWriter(
			txn.proc.GetMPool(), fs, plan2.ExprType2Type(&pkCol.Typ), -1)
	}
	writerClosed := false
	defer func() {
		if !writerClosed {
			closeErr := writer.Close()
			if err == nil {
				err = closeErr
			}
		}
	}()

	owned := make([]*batch.Batch, 0, len(sources))
	defer func() {
		for _, bat := range owned {
			bat.Clean(txn.proc.GetMPool())
		}
	}()

	for _, source := range sources {
		bat := source.payload.lease.bat
		selections := source.payload.lease.selections
		if len(selections) != 0 {
			bat, err = bat.Dup(txn.proc.GetMPool())
			if err != nil {
				return object, stats, false, err
			}
			owned = append(owned, bat)
			if typ == INSERT {
				shrinkBatchWithRowids(bat, selections)
			} else {
				bat.Shrink(selections, true)
			}
		}
		if bat.RowCount() == 0 {
			continue
		}
		hasRows = true
		input := bat
		if typ == INSERT {
			input = batch.NewWithSize(len(bat.Vecs) - 1)
			input.SetAttributes(bat.Attrs[1:])
			input.Vecs = bat.Vecs[1:]
			input.SetRowCount(bat.RowCount())
		}
		if err = writer.Write(ctx, input); err != nil {
			return object, stats, false, err
		}
	}
	if !hasRows {
		return object, nil, false, nil
	}

	if stats, err = writer.Sync(ctx); err != nil {
		return object, stats, false, err
	}
	if len(stats) == 0 {
		return object, stats, false, moerr.NewInternalError(
			ctx, "workspace spill produced no object stats")
	}
	blockInfo, err := writer.FillBlockInfoBat()
	if err != nil {
		return object, stats, false, err
	}
	ownedBlockInfo, err := blockInfo.Dup(txn.proc.GetMPool())
	if err != nil {
		return object, stats, false, err
	}
	if err = writer.Close(); err != nil {
		ownedBlockInfo.Clean(txn.proc.GetMPool())
		return object, stats, false, err
	}
	writerClosed = true

	source := sources[0]
	entry := source.entry
	entry.fileName = stats[0].ObjectLocation().String()
	entry.bat = ownedBlockInfo
	statementID := key.statementID
	attemptID := key.attemptID
	object = workspaceSpillObject{
		statementID:       statementID,
		attemptID:         attemptID,
		sourceMutationIDs: make([]workspaceMutationID, len(sources)),
		entry:             entry,
	}
	for idx := range sources {
		object.sourceMutationIDs[idx] = sources[idx].mutationID
	}
	return object, stats, true, nil
}

func (txn *Transaction) publishSpilledObjectLocked(entry *Entry) {
	if entry.typ == INSERT {
		server := colexec.MustGetServer(txn.engine.service)
		col, area := vector.MustVarlenaRawData(entry.bat.Vecs[1])
		for idx := range col {
			stats := objectio.ObjectStats(col[idx].GetByteSlice(area))
			oid := stats.ObjectName().ObjectId()
			server.PutCnSegment(
				txn.op.Txn().ID, entry.tableId, oid.Segment(), colexec.TxnWorkspaceUnCommitType)
		}
		return
	}
}

// cleanStagedWorkspaceSpillLocked releases unpublished metadata and schedules
// every known remote object for GC. It preserves the caller's lock contract.
func (txn *Transaction) cleanStagedWorkspaceSpillLocked(staged stagedWorkspaceSpill) {
	for idx := range staged.objects {
		if staged.objects[idx].entry.bat != nil {
			staged.objects[idx].entry.bat.Clean(txn.proc.GetMPool())
		}
	}
	if len(staged.stats) == 0 {
		return
	}
	txn.Unlock()
	_ = txn.GCObjsByStats(staged.stats...)
	txn.Lock()
}
