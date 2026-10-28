// Copyright 2026 MatrixOne
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
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
)

// DumpWorkspaceDeletesForTest is the deterministic engine-test door to the
// production commit-time tombstone flush (the client/testutil.go pattern: a
// dedicated testutil module exporting test-only helpers over the package's
// real internals). It runs the exact workspace sequence a commit's dumpAll
// pass executes for DELETE entries: resolve the owning tables, drain the
// in-memory (rowid, pk) delete rows into a REAL S3 tombstone object through
// the production CNS3TombstoneWriter pipeline, and replace every drained
// in-memory entry with the flushed entry — fileName set to the writer's
// first ObjectLocation and the batch carrying the writer's stats rows.
//
// The locking current-read D-4 harness uses it to give a still-OPEN RR
// transaction the exact workspace shape a commit-shaped dump installed, so
// the binding's tombstone mask consumes a real flushed entry across an
// observed compaction instead of staying on the in-memory arm only (the
// production hand-off point of that shape is commit, which would close the
// RR window the harness must keep open). It returns the flushed entry's
// fileName identity so callers can assert the drain actually happened.
func (txn *Transaction) DumpWorkspaceDeletesForTest(ctx context.Context, offset int) (string, error) {
	fs, err := colexec.GetSharedFSFromProc(txn.proc)
	if err != nil {
		return "", err
	}
	inMemoryDeletes := 0
	txn.Lock()
	defer txn.Unlock()
	for i := offset; i < len(txn.writes) && i >= 0; i++ {
		if txn.writes[i].typ == DELETE && txn.writes[i].fileName == "" {
			inMemoryDeletes++
		}
	}
	if inMemoryDeletes == 0 {
		return "", nil
	}
	if err := txn.dumpDeleteBatchLocked(ctx, fs, offset); err != nil {
		return "", err
	}
	fileName := ""
	for i := offset; i < len(txn.writes) && i >= 0; i++ {
		if txn.writes[i].typ == DELETE && txn.writes[i].fileName != "" {
			if fileName == "" {
				fileName = txn.writes[i].fileName
			}
		}
	}
	if fileName == "" {
		return "", moerr.NewInternalErrorNoCtx(
			"the workspace tombstone flush drained deletes but installed no flushed entry")
	}
	return fileName, nil
}

// MarkSubscribeAnsweredTableNotExistForTest plants, for the given table id,
// the production subscription state a "table not exist" subscribe answer
// arrives at (logtail_consumer's setTableSubNotExist — the SubRspTableNotExist
// reply the SubscribeState machine records when TN answers the subscribe this
// way). The next getPartitionState for that id therefore takes the REAL
// ErrNoSuchTable detour (issue 17012's too-old-table-id class: the live
// subscriber cannot serve the table any more), the exact "latest-state
// coverage lost" precondition of the round-6 historical-recovery regression:
// the caller then exercises the real recovery branch through the caller's
// transaction operator.
func (e *Engine) MarkSubscribeAnsweredTableNotExistForTest(dbID, tableID uint64) {
	ent := &subEntry{dbID: dbID, state: SubRspTableNotExist}
	ent.lastTs.Store(time.Now().UnixNano())
	c := e.PushClient()
	c.subscribed.rw.Lock()
	defer c.subscribed.rw.Unlock()
	c.subscribed.m[tableID] = ent
}

// TrackHistoricalSnapshotForTest registers a historical snapshot partition in
// the engine's snapshot cache (SnapshotManager.Add — the production cache the
// recovery lookup consults) under the caller's (dbID, tableID). The seeded
// partition carries the caller-bounded serving duration through the
// production UpdateDuration surface, so a cache entry that serves the
// subject snapshot S but NOT the execution frontier F is representable
// deterministically: the returned state is the seeded partition's own.
func (e *Engine) TrackHistoricalSnapshotForTest(
	dbID, tableID uint64,
	tableName string,
	start, end types.TS,
) *logtailreplay.PartitionState {
	part := logtailreplay.NewPartition(
		e.service, e.GetLatestCatalogCache(),
		0, dbID, tableID, e.getPrefetchOnSubscribed())
	state := part.Snapshot()
	state.UpdateDuration(start, end)
	e.snapshotMgr.Add(dbID, tableID, part, tableName, end)
	return state
}

// TrackSnapshotForTest registers an ALREADY-BUILT partition — e.g. the live
// subscribed partition obtained through GetOrCreateLatestPart, whose state
// covers the current applied watermark with the real committed content —
// under the caller's cache key.
func (e *Engine) TrackSnapshotForTest(
	dbID, tableID uint64, tableName string, part *logtailreplay.Partition, ts types.TS,
) {
	e.snapshotMgr.Add(dbID, tableID, part, tableName, ts)
}

// ServePartitionRangeForTest installs, on the partition's CURRENT state, the
// exact serving duration the production subscribe consumer assigns
// (logtail_consumer consumeSubscribeResp: UpdateDuration(checkpoint start,
// types.MaxTs()) — the harness's mock subscribe flow never delivers one, so
// a live partition state read through the recovery path would otherwise
// serve nothing. Test-only: the caller must own the table's quiescence (no
// further logtail mutations to this partition while the range is installed).
func (e *Engine) ServePartitionRangeForTest(
	part *logtailreplay.Partition, start, end types.TS,
) {
	part.Snapshot().UpdateDuration(start, end)
}

// GetPartitionStateThroughRecoveryForTest drives the PRODUCTION
// txnTable.getPartitionState for a caller-substituted table id carried on the
// same database identity, definition and handle machinery as the supplied
// relation, with the caller's transaction operator: an execution-read caller
// passes the ApplyExecutionReadTS carrier (SnapshotTS() resolves to the
// frontier F), an ordinary caller passes the plain transaction operator
// (SnapshotTS() is the transaction snapshot S). The substituted id is the id
// whose live coverage the test lost (MarkSubscribeAnsweredTableNotExistForTest),
// so the call exercises the real historical-recovery branch and its
// coverage of the caller's effective read point.
func (e *Engine) GetPartitionStateThroughRecoveryForTest(
	ctx context.Context,
	rel engine.Relation,
	tableID uint64,
	op client.TxnOperator,
) (*logtailreplay.PartitionState, error) {
	base, ok := rel.(*txnTable)
	if !ok {
		// The transaction's relation cache hands out its reusable delegate
		// wrappers (txnTableDelegate around the cached origin table).
		if dele, isDelegate := rel.(*txnTableDelegate); isDelegate && dele.origin != nil {
			origin := dele.origin
			base, ok = origin, true
		}
	}
	if !ok {
		return nil, moerr.NewInternalErrorNoCtx("the supplied relation is not a disttae table")
	}
	twin := &txnTable{
		accountId: base.accountId,
		tableId:   tableID,
		tableName: base.tableName,
		db: &txnDatabase{
			accountId:    base.db.accountId,
			op:           op,
			databaseId:   base.db.databaseId,
			databaseName: base.db.databaseName,
		},
		tableDef: base.tableDef,
		relKind:  base.relKind,
		eng:      base.eng,
	}
	return twin.getPartitionState(ctx)
}

// SwapRequestSnapshotReadForTest installs a deterministic response for the
// snapshot-read transport the recovery consults before the cache lookup. The
// production request path stays intact (the recovery still resolves its
// requested read point through RequestSnapshotRead's real signature and
// validates the returned checkpoint authority through the production chain);
// engine tests fabricate the checkpoint-coverage answer without the live TN
// RPC. The returned closure restores the production transport.
func SwapRequestSnapshotReadForTest(
	swapped func(ctx context.Context, snapshot *types.TS) (resp any, err error),
) (restore func()) {
	orig := RequestSnapshotRead
	RequestSnapshotRead = func(ctx context.Context, tbl *txnTable, snapshot *types.TS) (any, error) {
		return swapped(ctx, snapshot)
	}
	return func() { RequestSnapshotRead = orig }
}
