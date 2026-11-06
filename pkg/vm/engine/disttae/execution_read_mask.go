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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"go.uber.org/zap"
)

// ExecutionReadTombstoneMask is design D-4's stage-1 ordinary-local transfer
// mask (approved revision 14, R14-F1): a mask-only tombstone input carried by
// an execution-read clone for exactly one execution attempt. It holds the
// alive-generation row offsets this transaction's own deletes were mapped to:
// an own DELETE whose original rowid references an object that DIED inside
// the window [session snapshot, F] would resurface through the compacted
// generation the frontier reader enumerates (workspace suppression is keyed
// by the original block/rowid), so per attempt every such delete is mapped
// by primary key to the rowids alive at F — the approved doTransferRowids
// primitive restricted to the in-window created objects.
//
// Ownership rules (design R-D4): computed only for a marked relation at its
// admitted frontier; consumed only by sources whose readTS equals that
// frontier; invisible to unmarked readers (nil); never persisted; never used
// by the commit path (workspace entries are never rewritten — this is the
// cursorless recompute form).
type ExecutionReadTombstoneMask struct {
	frontier types.TS
	byBlock  map[types.Blockid]map[uint32]struct{}
}

func (m *ExecutionReadTombstoneMask) addRowid(rowid types.Rowid) {
	bid := *rowid.BorrowBlockID()
	byBlock, ok := m.byBlock[bid]
	if !ok {
		byBlock = make(map[uint32]struct{}, 1)
		m.byBlock[bid] = byBlock
	}
	byBlock[rowid.GetRowOffset()] = struct{}{}
}

// applyOffsets removes the row offsets the mask suppresses for one block of
// the alive generation. Idempotent by construction (set membership).
func (m *ExecutionReadTombstoneMask) applyOffsets(
	bid *objectio.Blockid, offsets []int64,
) []int64 {
	if m == nil || len(m.byBlock) == 0 {
		return offsets
	}
	byBlock, ok := m.byBlock[*bid]
	if !ok || len(byBlock) == 0 {
		return offsets
	}
	return readutil.RemoveIf(offsets, func(t int64) bool {
		_, deleted := byBlock[uint32(t)]
		return deleted
	})
}

// markInto marks the suppressed row offsets of one block into a bitmap.
func (m *ExecutionReadTombstoneMask) markInto(bid *objectio.Blockid, target *objectio.Bitmap) {
	if m == nil || len(m.byBlock) == 0 {
		return
	}
	byBlock, ok := m.byBlock[*bid]
	if !ok || len(byBlock) == 0 {
		return
	}
	for off := range byBlock {
		target.Add(uint64(off))
	}
}

// computeExecutionReadTombstoneMask performs the D-4 cursorless recompute for
// one execution attempt over a carrier-bound clone: it enumerates this
// transaction's own workspace deletes for the table (in-memory entries and
// S3-flushed tombstone objects), determines which of their rowids reference
// objects that died inside [session snapshot, F]
// (GetChangedObjsBetweenForTombstoneTransfer — including deleted intermediate
// generations), and maps those deletes by primary key to the alive-generation
// rowids via the approved doTransferRowids search restricted to objects
// created inside the window and alive at F. Late-added deletes and
// O->P->P2 chains are covered by the same interval walk. Delete-free
// executions stop after the single workspace scan (design D-6: zero mask
// work). The reader clone is the search table so the PK probe runs at F.
func computeExecutionReadTombstoneMask(
	ctx context.Context,
	clone *txnTable,
) (*ExecutionReadTombstoneMask, error) {
	op := clone.db.op
	frontier := types.TimestampToTS(op.SnapshotTS())
	windowStart := types.TimestampToTS(op.Txn().SnapshotTS)
	mask := &ExecutionReadTombstoneMask{frontier: frontier}
	if clone.tableDef == nil {
		// A binding never carries a definition it cannot lock rows by.
		mask.byBlock = make(map[types.Blockid]map[uint32]struct{})
		return mask, nil
	}
	// The clone inherits the ORIGIN's lazily cached (seqnums, typs); a
	// relation whose readers never initialized them — the bind-time mask
	// runs before any reader assembly populates the cache — would carry an
	// empty typs slice and silently no-op the whole computation (pre-PR
	// review harness finding). The latch is idempotent and derived from
	// the bound definition only.
	if len(clone.tableDef.Cols) > 0 {
		clone.ensureSeqnumsAndTypesExpectRowid()
	}
	if clone.primaryIdx < 0 || clone.primaryIdx >= len(clone.typs) {
		// Locking row targets require a primary key; nothing can map.
		mask.byBlock = make(map[types.Blockid]map[uint32]struct{})
		return mask, nil
	}
	mask.byBlock = make(map[types.Blockid]map[uint32]struct{})
	if !windowStart.LT(&frontier) {
		// No in-window compaction can have happened (empty window): zero
		// mask work (design D-6).
		return mask, nil
	}
	proc := clone.proc.Load()
	if proc == nil {
		return nil, moerr.NewInternalErrorNoCtx(
			"execution-read mask requires the table's statement process")
	}
	mp := proc.Mp()
	pkType := clone.typs[clone.primaryIdx]

	txn := clone.getTxn()
	rawRowids := vector.NewVec(types.T_Rowid.ToType())
	defer rawRowids.Free(mp)
	rawPKs := vector.NewVec(pkType)
	defer rawPKs.Free(mp)

	// (1) Enumerate the transaction's own deletes for this table under the
	// workspace lock: in-memory entries carry (rowid, pk) rows; flushed
	// entries carry one marshalled ObjectStats row per tombstone object
	// (FillBlockInfoBat writes a multi-object stats column), whose (rowid, pk)
	// rows are read back below. Flushed tombstone objects are enumerated from
	// the owning DELETE entry itself so the identity matches the writer
	// exactly: entry.fileName only records stats[0].ObjectLocation() (see
	// txn.go flushTombstones), so a name-keyed registry lookup misses them.
	// Both forms keep their ORIGINAL rowids.
	var flushedStats []objectio.ObjectStats
	var collectErr error
	appendRaw := func(rowid types.Rowid, pkColumn *vector.Vector, j int64) {
		if collectErr != nil {
			return
		}
		if collectErr = vector.AppendFixed[types.Rowid](rawRowids, rowid, false, mp); collectErr != nil {
			return
		}
		collectErr = rawPKs.UnionOne(pkColumn, j, mp)
	}

	txn.Lock()
	for i := range txn.writes {
		entry := &txn.writes[i]
		if entry.tableId != clone.tableId || entry.typ != DELETE {
			continue
		}
		if entry.fileName == "" {
			if entry.bat == nil || len(entry.bat.Vecs) < 2 {
				continue
			}
			rowIds := vector.MustFixedColWithTypeCheck[types.Rowid](entry.bat.GetVector(0))
			pkColumn := entry.bat.GetVector(1)
			for j := range rowIds {
				appendRaw(rowIds[j], pkColumn, int64(j))
				if collectErr != nil {
					txn.Unlock()
					return nil, collectErr
				}
			}
		} else {
			flushedStats = append(flushedStats, flushedTombstoneStats(entry.bat)...)
		}
	}
	txn.Unlock()
	if collectErr != nil {
		return nil, collectErr
	}

	if len(flushedStats) > 0 {
		if err := collectFlushedMaskIntents(ctx, clone, flushedStats, pkType, appendRaw, func() bool {
			return collectErr != nil
		}); err != nil {
			return nil, err
		}
		// The flushed reader stops draining once the shared collector
		// failed, but its error lands here — the in-memory loop above
		// cannot see it because the collector closure owns it. A swallowed
		// allocation failure would return a successful TRUNCATED or EMPTY
		// mask and resurrect this transaction's own deleted rows on an
		// admitted frontier read (pre-PR review P2).
		if collectErr != nil {
			return nil, collectErr
		}
	}
	// Every collected delete maps exactly one (rowid, PK) pair: appendRaw
	// writes rowid first, so a failed/aborted row leaves the lengths
	// diverged. Reject an incomplete delete set instead of mapping a
	// prefix (the strictness also future-proofs later union steps).
	if rawRowids.Length() != rawPKs.Length() {
		return nil, moerr.NewInternalErrorNoCtx(
			"execution-read mask collected an inconsistent delete set")
	}

	if rawRowids.Length() == 0 {
		// D-6: delete-free executions have no mask work at all.
		return mask, nil
	}

	// (2) Which original objects died inside the window?
	pState, err := clone.getPartitionState(ctx)
	if err != nil {
		return nil, err
	}
	deletedObjs, createdObjs := pState.GetChangedObjsBetweenForTombstoneTransfer(windowStart, frontier)
	// The interval walk reads only the history the state still retains.
	// Partition-history GC (PushClient.doGCPartitionState -> Truncate) moves
	// the state's truncation floor forward and removes every object whose
	// delete entry landed at or below it from the timestamp index — including
	// the original object O of an in-window compaction, once GC passes that
	// compaction while the transaction that deleted from O is still open —
	// while the created-and-alive generation keeps its create entry. The walk
	// is complete for [windowStart, frontier] only while the floor still sits
	// at or below the transaction snapshot; once it moved past, empty
	// deletedObjs proves a truncated history, not a delete-free window, and
	// the empty-set shortcut must not fire.
	coverageComplete := true
	if stateFloor := pState.GetStart(); stateFloor.GT(&windowStart) {
		coverageComplete = false
	}
	if coverageComplete && (len(deletedObjs) == 0 || len(createdObjs) == 0) {
		return mask, nil
	}

	// (3) Collect exactly the intents whose original object died in-window.
	transferIntents := vector.NewVec(types.T_Rowid.ToType())
	defer transferIntents.Free(mp)
	searchPKColumn := vector.NewVec(pkType)
	defer searchPKColumn.Free(mp)
	rowIds := vector.MustFixedColWithTypeCheck[types.Rowid](rawRowids)
	for i := range rowIds {
		if coverageComplete {
			if _, dead := deletedObjs[*objectio.ShortName(rowIds[i].BorrowBlockID())]; !dead {
				continue
			}
		} else if pState.IsDataObjectVisible(rowIds[i].BorrowBlockID().Object(), frontier) {
			// Truncated history: decide per intent from the rowid and the
			// ALIVE state. The original object still lives at F, so the
			// workspace delete's rowid keeps matching the frontier
			// generation — the alive-object case of the complete regime. An
			// object the truncated state no longer knows died at or before
			// F; objects alive at the transaction snapshot can never be
			// dead before the window (own deletes reference rowids read at
			// or after that snapshot), so the death happened in-window and
			// the row must be mapped by primary key into the alive
			// generation below.
			continue
		}
		if err := vector.AppendFixed[types.Rowid](transferIntents, rowIds[i], false, mp); err != nil {
			return nil, err
		}
		if err := searchPKColumn.UnionOne(rawPKs, int64(i), mp); err != nil {
			return nil, err
		}
	}
	if transferIntents.Length() == 0 {
		return mask, nil
	}

	// (4) Search the alive generation — objects created inside the window
	// that still exist at F — for the mapped rowids (the approved
	// doTransferRowids primitive; the clone's operator resolves the search
	// read view at F).
	var objectList []objectio.ObjectStats
	for name := range createdObjs {
		if obj, ok := pState.GetObject(name); ok {
			objectList = append(objectList, obj.ObjectStats)
		}
	}
	if len(objectList) == 0 {
		return mask, nil
	}
	targetRowids := vector.NewVec(types.T_Rowid.ToType())
	defer targetRowids.Free(mp)
	readPKColumn := vector.NewVec(pkType)
	defer readPKColumn.Free(mp)
	if err := doTransferRowids(
		ctx, clone, objectList,
		transferIntents, targetRowids, searchPKColumn, readPKColumn,
		mp, txn.engine.fs,
	); err != nil {
		return nil, err
	}

	// (5) Register the mapped rowids. A PK that is absent from the alive
	// generation compacted away entirely — nothing left to suppress.
	targets := vector.MustFixedColWithTypeCheck[types.Rowid](targetRowids)
	for _, rid := range targets {
		mask.addRowid(rid)
	}
	if targetRowids.Length() != transferIntents.Length() {
		logutil.Warn("execution-read mask mapped a partial delete set",
			zap.Uint64("table", clone.tableId),
			zap.Int("intent", transferIntents.Length()),
			zap.Int("mapped", targetRowids.Length()),
			zap.Int("dead-objects", len(deletedObjs)),
			zap.Int("alive-objects", len(objectList)))
	}
	return mask, nil
}

// flushedTombstoneStats enumerates the tombstone objects of a flushed
// DELETE workspace entry. The S3 tombstone writer gives such entries a
// single ObjectMeta_ObjectStats column holding ONE MARSHALLED STATS ROW PER
// TOMBSTONE OBJECT (colexec.FillBlockInfoBat), while entry.fileName records
// only stats[0].ObjectLocation().String() — so the objects must be
// enumerated from the owning entry itself: a name-keyed registry lookup
// cannot match the writer's identity and would silently drop every flushed
// intent (pre-PR review P1).
func flushedTombstoneStats(bat *batch.Batch) []objectio.ObjectStats {
	if bat == nil || len(bat.Vecs) == 0 {
		return nil
	}
	statsColumn := bat.GetVector(0)
	if statsColumn == nil || statsColumn.Length() == 0 {
		return nil
	}
	col, area := vector.MustVarlenaRawData(statsColumn)
	var stats []objectio.ObjectStats
	for j := range col {
		if bs := col[j].GetByteSlice(area); len(bs) > 0 {
			stats = append(stats, objectio.ObjectStats(bs))
		}
	}
	return stats
}

// collectFlushedMaskIntents reads the (rowid, pk) rows out of the table's own
// S3-flushed tombstone objects so the mask also covers S3-dumped deletes
// (design D-4 regression census: in-memory AND flushed own deletes). The
// registry objects belong wholly to this transaction; the read is one
// metadata load plus one read per tombstone block. The shared collector's
// failure is queried through failed after every block so the loop stops
// draining tombstones as soon as the mask can no longer be complete, and
// each error path releases the block it holds before returning.
func collectFlushedMaskIntents(
	ctx context.Context,
	clone *txnTable,
	statsList []objectio.ObjectStats,
	pkType types.Type,
	appendRaw func(rowid types.Rowid, pkColumn *vector.Vector, j int64),
	failed func() bool,
) error {
	fs := clone.getTxn().engine.fs
	for si := range statsList {
		if failed() {
			return nil
		}
		stats := statsList[si]
		location := stats.ObjectLocation()
		objMeta, err := objectio.FastLoadObjectMeta(ctx, &location, false, fs)
		if err != nil {
			return err
		}
		dataMeta := objMeta.MustDataMeta()
		blkCnt := int(dataMeta.BlockCount())
		for pos := 0; pos < blkCnt; pos++ {
			if failed() {
				return nil
			}
			// Location is a byte slice, so a nil var would leave BlockLocationTo
			// no backing store; ObjectLocation is the fixed-size buffer the
			// production readers allocate before converting a block number.
			var blkLoc objectio.ObjectLocation
			stats.BlockLocationTo(uint16(pos), objectio.BlockMaxRows, blkLoc[:])
			vectors := containers.NewVectors(2)
			_, release, err := ioutil.ReadDeletes(
				ctx, blkLoc[:], fs, true, vectors, &pkType,
			)
			if err != nil {
				if release != nil {
					release()
				}
				return err
			}
			// ReadDeletes fills vectors as [rowid, pk] when a pk type is
			// requested (funcs.go ReadDeletes col order).
			rowidVec := vectors[0]
			pkVec := vectors[1]
			rowIds := vector.MustFixedColWithTypeCheck[types.Rowid](&rowidVec)
			aborted := false
			for j := range rowIds {
				appendRaw(rowIds[j], &pkVec, int64(j))
				if failed() {
					aborted = true
					break
				}
			}
			if release != nil {
				release()
			}
			if aborted {
				return nil
			}
		}
	}
	return nil
}
