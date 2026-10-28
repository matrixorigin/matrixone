// Copyright 2021-2024 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
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
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/objectio/mergeutil"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"go.uber.org/zap"
)

type UT_ForceTransCheck struct{}

type TransferOption func(*TransferFlow)

const (
	// RowID transfer has a material fixed cost: every batch constructs an IN
	// filter, prunes replacement objects, and builds a reader.  One object block
	// (8192 rows) made large updates pay that setup cost thousands of times.
	// Aggregate up to eight blocks, with an 8 MiB soft limit that flushes wide
	// primary keys per row in memory or at persisted source-batch boundaries.
	transferBatchRowLimit  = objectio.BlockMaxRows * 8
	transferBatchSizeLimit = mpool.MB * 8
)

func transferBatchLimitReached(rowCount, byteSize int, checkSize bool) bool {
	if rowCount == 0 {
		return false
	}
	if rowCount >= transferBatchRowLimit {
		return true
	}
	// Persisted readers bound source batches and check their byte size once per
	// batch. In-memory staging checks after each appended row.
	return checkSize && byteSize >= transferBatchSizeLimit
}

func newDeletedObjectFilter(
	deletedObjects []objectio.ObjectStats,
) func(*objectio.ObjectId) bool {
	deletedObjectIDs := make(map[types.Objectid]struct{}, len(deletedObjects))
	for i := range deletedObjects {
		deletedObjectIDs[*deletedObjects[i].ObjectName().ObjectId()] = struct{}{}
	}
	return func(objID *objectio.ObjectId) bool {
		_, deleted := deletedObjectIDs[*objID]
		return deleted
	}
}

// collectCnTombstoneDeleteStats folds the (databaseId, tableId)'s
// file-backed DELETE entries into their ObjectStats. The transfer window
// reads these raw workspace entries because the stash view of flushed
// tombstones cannot pair them with this exact table's pending entries.
//
// Entries a pk-move transfer retired keep their DELETE type and fileName
// but released their batch in place (the nil-slot lifecycle, see
// releaseWorkspaceEntryBatchLocked): their replacements already published,
// and a statement rollback re-appends a live tail entry when that is
// undone — so skipping the nil slot here loses nothing, while
// dereferencing it panics on any later qualifying transfer window of the
// same transaction (r27 review F1 / D114). Every other persisted-DELETE
// scan in the package guards the slot identically.
func collectCnTombstoneDeleteStats(
	entries []Entry, databaseId, tableId uint64,
) []objectio.ObjectStats {
	tombstoneObjects := make([]objectio.ObjectStats, 0)
	for i := range entries {
		e := &entries[i]
		if e.tableId != tableId || e.databaseId != databaseId {
			continue
		}
		if e.fileName == "" || e.typ != DELETE ||
			e.bat == nil || e.bat.IsEmpty() {
			continue
		}
		for j := range e.bat.Vecs[0].Length() {
			stats := objectio.ObjectStats(e.bat.Vecs[0].GetBytesAt(j))
			tombstoneObjects = append(tombstoneObjects, stats)
		}
	}
	return tombstoneObjects
}

func ConstructCNTombstoneObjectsTransferFlow(
	ctx context.Context,
	start, end types.TS,
	table *txnTable,
	txn *Transaction,
	mp *mpool.MPool,
	fs fileservice.FileService,
) (*TransferFlow, []objectio.ObjectStats, []zap.Field, error) {

	//ctx := table.proc.Load().Ctx
	state, err := table.getPartitionState(ctx)
	if err != nil {
		return nil, nil, nil, err
	}

	var logs []zap.Field

	newDataObjects, deletedObjects := state.CollectObjectsBetween(start, end)

	logs = append(logs,
		zap.Int("newDataObjects", len(newDataObjects)),
		zap.Int("deletedObjects", len(deletedObjects)))

	if len(newDataObjects) == 0 || len(deletedObjects) == 0 {
		return nil, nil, logs, nil
	}

	deletedObjectPos := 0
	deletedObjectsIter := func() *types.Objectid {
		if deletedObjectPos == len(deletedObjects) {
			return nil
		}

		id := deletedObjects[deletedObjectPos].ObjectName().ObjectId()
		deletedObjectPos++
		return id
	}

	tombstoneObjects := collectCnTombstoneDeleteStats(
		txn.writes, table.db.databaseId, table.tableId)

	logs = append(logs, zap.Int("origin-tombstoneObjects", len(tombstoneObjects)))

	if tombstoneObjects, err = ioutil.CoarseFilterTombstoneObject(
		ctx, deletedObjectsIter, tombstoneObjects, fs); err != nil {
		return nil, nil, logs, err
	} else if len(tombstoneObjects) == 0 {
		return nil, nil, logs, nil
	}

	logs = append(logs, zap.Int("coarse-tombstoneObjects", len(tombstoneObjects)))

	// Only tombstones that point at an object deleted in this exact transfer
	// window need to move. Live appendable objects are intentionally absent
	// from PartitionState's persisted-object index, so treating an unknown
	// object as deleted would incorrectly stage their tombstones.
	isObjectDeletedFn := newDeletedObjectFilter(deletedObjects)

	pkColIdx := table.tableDef.Name2ColIndex[table.tableDef.Pkey.PkeyColName]
	pkCol := table.tableDef.Cols[pkColIdx]
	r := readutil.SimpleMultiObjectsReader(
		ctx, fs,
		tombstoneObjects,
		end.ToTimestamp(),
		readutil.WithColumns(
			[]uint16{0, 1},
			[]types.Type{types.T_Rowid.ToType(), plan2.ExprType2Type(&pkCol.Typ)},
		),
	)

	flow := ConstructTransferFlow(
		table,
		objectio.HiddenColumnSelection_None,
		r, isObjectDeletedFn, newDataObjects,
		mp, fs,
	)

	// r26 review F3 (D113): when the transaction holds pk-move provenance,
	// EVERY row of the consumed input objects must reach the output — re-keyed
	// when its target object died, verbatim otherwise — so the caller can
	// retire the superseded source entries atomically. Without the pass-through
	// the verbatim rows only survived in the still-pending original entries,
	// which then published beside their re-keyed twins: two tombstone objects
	// naming the same provenance identity, and the data-branch chain verdict
	// fails closed on the ambiguity. Ordinary transactions keep the selective
	// staging: their pending originals must keep publishing.
	flow.passThrough = txn != nil && txn.holdsPkMoveProvenanceLocked()

	return flow, tombstoneObjects, logs, nil
}

func ConstructTransferFlow(
	table *txnTable,
	hiddenSelection objectio.HiddenColumnSelection,
	sourcer engine.Reader,
	isObjectDeletedFn func(*objectio.ObjectId) bool,
	newDataObjects []objectio.ObjectStats,
	mp *mpool.MPool,
	fs fileservice.FileService,
	opts ...TransferOption,
) *TransferFlow {
	flow := &TransferFlow{
		table:             table,
		hiddenSelection:   hiddenSelection,
		sourcer:           sourcer,
		isObjectDeletedFn: isObjectDeletedFn,
		newDataObjects:    newDataObjects,
		mp:                mp,
		fs:                fs,
	}
	for _, opt := range opts {
		opt(flow)
	}
	flow.fillDefaults()
	return flow
}

type TransferFlow struct {
	table             *txnTable
	hiddenSelection   objectio.HiddenColumnSelection
	sourcer           engine.Reader
	isObjectDeletedFn func(*objectio.ObjectId) bool
	newDataObjects    []objectio.ObjectStats
	buffer            *containers.OneSchemaBatchBuffer
	staged            *batch.Batch
	sinker            *ioutil.Sinker
	mp                *mpool.MPool
	fs                fileservice.FileService

	// passThrough stages rows whose object is NOT deleted into leftover so
	// the output replaces every consumed input row (r26 review F3, D113).
	// leftover rows travel verbatim — they already name live objects.
	passThrough bool
	leftover    *batch.Batch

	transferred struct {
		rowCnt     int
		objDetails map[string]int
	}
}

func (flow *TransferFlow) fillDefaults() {
	pkType := plan2.ExprType2Type(&flow.table.tableDef.Cols[flow.table.primaryIdx].Typ)
	if flow.buffer == nil {
		attrs, attrTypes := objectio.GetTombstoneSchema(
			pkType, flow.hiddenSelection,
		)
		flow.buffer = containers.NewOneSchemaBatchBuffer(
			mpool.MB*8,
			attrs,
			attrTypes,
			false,
		)
	}
	if flow.sinker == nil {
		flow.sinker = ioutil.NewTombstoneSinker(
			flow.hiddenSelection,
			pkType,
			flow.mp,
			flow.fs,
			ioutil.WithBuffer(flow.buffer, false),
			ioutil.WithMemorySizeThreshold(mpool.MB*16),
			ioutil.WithTailSizeCap(0),
			//readutil.WithAllMergeSorted(),
		)
	}

	flow.transferred.objDetails = make(map[string]int)
}

func (flow *TransferFlow) getBuffer() *batch.Batch {
	return flow.buffer.Fetch()
}

func (flow *TransferFlow) putBuffer(bat *batch.Batch) {
	flow.buffer.Putback(bat, flow.mp)
}

func (flow *TransferFlow) Process(ctx context.Context) error {
	for {
		buffer := flow.getBuffer()
		defer flow.putBuffer(buffer)
		done, err := flow.sourcer.Read(ctx, buffer.Attrs, nil, flow.mp, buffer)
		if err != nil {
			return err
		}
		if done {
			break
		}
		if err := flow.processOneBatch(ctx, buffer); err != nil {
			return err
		}
	}

	if err := flow.transferStaged(ctx); err != nil {
		return err
	}

	if err := flow.writeLeftover(ctx); err != nil {
		return err
	}

	return flow.sinker.Sync(ctx)
}

func (flow *TransferFlow) getStaged() *batch.Batch {
	if flow.staged == nil {
		flow.staged = flow.getBuffer()
	}
	return flow.staged
}

func (flow *TransferFlow) orphanStaged() {
	flow.putBuffer(flow.staged)
	flow.staged = nil
}

func (flow *TransferFlow) getLeftover() *batch.Batch {
	if flow.leftover == nil {
		flow.leftover = flow.getBuffer()
	}
	return flow.leftover
}

func (flow *TransferFlow) orphanLeftover() {
	flow.putBuffer(flow.leftover)
	flow.leftover = nil
}

// writeLeftover publishes one sorted pass-through wave. The sinker merges
// sorted runs, so each wave sorts by primary key exactly like the re-keyed
// result batches in transferStaged.
func (flow *TransferFlow) writeLeftover(ctx context.Context) error {
	left := flow.getLeftover()
	if left.RowCount() == 0 {
		return nil
	}
	if err := mergeutil.SortColumnsByIndex(left.Vecs, 1, flow.mp); err != nil {
		return err
	}
	if err := flow.sinker.Write(ctx, left); err != nil {
		return err
	}
	flow.orphanLeftover()
	return nil
}

func (flow *TransferFlow) processOneBatch(ctx context.Context, buffer *batch.Batch) error {
	rowids := vector.MustFixedColWithTypeCheck[types.Rowid](buffer.Vecs[0])
	var (
		last    *objectio.ObjectId
		deleted bool
	)
	staged := flow.getStaged()
	for i, rowid := range rowids {
		objectid := rowid.BorrowObjectID()
		if last == nil || !objectid.EQ(last) {
			deleted = flow.isObjectDeletedFn(objectid)
			last = objectid
		}
		if !deleted {
			if flow.passThrough {
				left := flow.getLeftover()
				if err := left.UnionOne(buffer, int64(i), flow.mp); err != nil {
					return err
				}
				if transferBatchLimitReached(left.RowCount(), 0, false) {
					if err := flow.writeLeftover(ctx); err != nil {
						return err
					}
				}
			}
			continue
		}
		if err := staged.UnionOne(buffer, int64(i), flow.mp); err != nil {
			return err
		}

		flow.transferred.rowCnt++
		flow.transferred.objDetails[objectid.ShortStringEx()]++

		if transferBatchLimitReached(staged.RowCount(), 0, false) {
			if err := flow.transferStaged(ctx); err != nil {
				return err
			}
			staged = flow.getStaged()
		}
	}
	if transferBatchLimitReached(staged.RowCount(), staged.Size(), true) {
		if err := flow.transferStaged(ctx); err != nil {
			return err
		}
	}
	if flow.leftover != nil &&
		transferBatchLimitReached(flow.leftover.RowCount(), flow.leftover.Size(), true) {
		if err := flow.writeLeftover(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (flow *TransferFlow) transferStaged(ctx context.Context) error {
	staged := flow.getStaged()
	if staged.RowCount() == 0 {
		return nil
	}

	// sort staged batch by primary key
	// TODO: do not sort if fake pk
	if err := mergeutil.SortColumnsByIndex(
		staged.Vecs,
		1,
		flow.mp,
	); err != nil {
		return err
	}
	result := flow.getBuffer()
	defer flow.putBuffer(result)

	if err := doTransferRowids(
		ctx,
		flow.table,
		flow.newDataObjects,
		staged.Vecs[0], // rowid intents
		result.Vecs[0], // rowid results
		staged.Vecs[1], // pk intents
		result.Vecs[1], // pks results
		flow.mp,
		flow.fs,
	); err != nil {
		return err
	}

	// Publish the read-back count BEFORE the provenance join: the
	// pooled buffer's row count is stale until here (the r26 live gate
	// caught the hook reading a 0-count buffer as "1 staged rows read
	// back as 0").
	result.SetRowCount(result.Vecs[0].Length())

	// r26 review F3 (D110): the transfer just replaced every staged rowid
	// with a freshly minted one, and orphanStaged below discards the staged
	// batch — the old->new mapping. Pk-move provenance recorded before the
	// transfer names the OLD identities (staged link cells, heal and
	// lineage bindings, origin keys): without carrying the mapping, the
	// committed tombstone and its provenance split identities and the data
	// branch apply replays DELETE+INSERT. Skip the join entirely when the
	// transaction holds no provenance state — the ordinary transfer then
	// pays nothing. Runs under forEachTableHasDeletesLocked: txn.Lock is
	// held for the Locked-suffix call.
	txn := flow.table.getTxn()
	if txn != nil && len(txn.writes) != 0 && txn.holdsPkMoveProvenanceLocked() {
		oldIds, newIds, mapErr := pkMoveTransferRekeyPairs(staged, result)
		if mapErr != nil {
			return mapErr
		}
		// r37 review F1 (D142, re-scoped by r38 review F1/F2 into
		// D143/D144): journal this round's re-key — effective-only,
		// coalesced per statement, and budget-admitted BEFORE any map or
		// staged cell mutates — in the transaction-level carry journal at
		// carry time, ahead of the fallible Write below, on EVERY
		// rollback-reachable route (a boundary window and AdvanceSnapshot
		// alike): a write failure or a later table's failure leaves the
		// durable side reading the old identity again, and the journaled
		// exact inverse is what lets provenance agree with it after the
		// statement rollback (and after the resurrection returns any
		// retired originals). A refusal aborts the round — and the
		// enclosing statement — atomically, before half-carrying.
		effOld, effNew, admErr := txn.pkMoveCarryJournalAdmitLocked(oldIds, newIds)
		if admErr != nil {
			return admErr
		}
		txn.pkMoveCarryJournalAppendLocked(pkMoveCarryObject, effOld, effNew)
		if err := txn.carryPkMoveProvenanceThroughRekeyLocked(oldIds, newIds); err != nil {
			return err
		}
	}

	flow.orphanStaged()

	// D142/D143 (r42 approval): the design's planned test-only fault point
	// at the sinker write — the round's pk-move carry is already journaled
	// and applied above, so injecting here reproduces the deterministic
	// "write failed after carry" shape that statement rollback must unwind
	// through the carry journal's exact inverse.
	if msg, injected := objectio.CNTransferStagedWriteFailedInjected(); injected {
		return moerr.NewInternalErrorNoCtx("transfer staged write failed by injection: " + msg)
	}

	return flow.sinker.Write(ctx, result)
}

func (flow *TransferFlow) GetResult() ([]objectio.ObjectStats, []*batch.Batch) {
	return flow.sinker.GetResult()
}

func (flow *TransferFlow) Close() error {
	if flow.sourcer != nil {
		flow.sourcer.Close()
		flow.sourcer = nil
	}
	if flow.sinker != nil {
		flow.sinker.Close()
		flow.sinker = nil
	}
	if flow.buffer != nil {
		flow.buffer.Close(flow.mp)
		flow.buffer = nil
	}
	if flow.staged != nil {
		flow.staged.Clean(flow.mp)
		flow.staged = nil
	}
	if flow.leftover != nil {
		// A leftover batch still owned here means the flow aborted between
		// its pool checkout and orphanLeftover (reader, rekey, sorter, or
		// sinker error): OneSchemaBatchBuffer.Close frees only batches
		// RETURNED to the pool, never this checked-out one (r27 review
		// F4 / D117). Release it exactly once — orphanLeftover nils the
		// field after a putback, so this never double-frees, and the nil
		// assignment keeps a repeated Close safe.
		flow.leftover.Clean(flow.mp)
		flow.leftover = nil
	}
	flow.mp = nil
	flow.table = nil
	flow.transferred.objDetails = nil
	return nil
}
