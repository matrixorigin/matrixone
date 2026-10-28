// Copyright 2021-2024 Matrix Origin
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

package multi_update

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/rscthrottler"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/deletion"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	sqlutil "github.com/matrixorigin/matrixone/pkg/sql/util"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"go.uber.org/zap"
)

const (
	InsertWriteS3Threshold uint64 = 64 * mpool.MB
	DeleteWriteS3Threshold uint64 = 64 * mpool.MB

	RowIDIdx = 0
	PkIdx    = 1
)

var (
	DeleteBatchAttrs = []string{catalog.Row_ID, "pk"}
)

type deleteBlockData struct {
	bitmap *nulls.Nulls
	typ    int8
	bat    *batch.Batch
}

type deleteBlockInfo struct {
	name        string
	bat         *batch.Batch
	rawRowCount uint64
}

func newDeleteBlockData(inputBatch *batch.Batch, pkIdx int) *deleteBlockData {
	data := &deleteBlockData{
		bitmap: nulls.NewWithSize(int(options.DefaultBlockMaxRows)),
		typ:    deletion.DeletionOnCommitted,
		bat:    newDeleteBatch(inputBatch, pkIdx),
	}
	return data
}

type s3WriterDelegate struct {
	// persistent per-table insert sinkers, created lazily on first append.
	insertSinkers []*colexec.CNS3Writer
	// per-table delete column accumulators (rowid + pk only).
	deleteBatches []*batch.BatchSet
	segmentMap    map[string]int32

	action             actionType
	rejectZeroTemporal bool

	updateCtxs     []*MultiUpdateCtx
	updateCtxInfos map[string]*updateCtxInfo
	seqnums        [][]uint16
	sortIndexes    []int
	pkIndexes      []int
	schemaVersions []uint32
	isClusterBys   []bool

	deleteBlockMap      []map[types.Blockid]*deleteBlockData
	deleteBlockInfo     []*deleteBlockInfo
	insertBlockInfo     []*batch.Batch
	insertBlockRowCount []uint64

	// per-table batch free lists that persist across sortAndSync calls,
	// so pre-allocated vector capacity is reused instead of re-allocated.
	insertFreeLists []*containers.BatchFreeList

	outputBat *batch.Batch

	batchSize      uint64
	flushThreshold uint64

	checkSizeCols    []int
	buf              bytes.Buffer
	addAffectedRows  func(uint64)
	takeAffectedRows func() uint64
	seenTargetRows   map[uint64]*hashmap.StrHashMap
	admitSeenGrowth  func(int64) error

	// collectPkMoveLinkRows synthesizes one pk-move link batch for a
	// buffered (already target-deduped) batch, or returns nil when
	// nothing moved. The S3 route must not stage the batch in the
	// producer: remote execution writes into an independent mirror
	// workspace whose mutations never reach the committing transaction,
	// so remotely produced provenance used to vanish before commit. The
	// buffered batch is the last point where the old->new pairing still
	// exists, so synthesis happens here; the batch then rides the tail
	// output (actionPkMoveLink) to UpdateFlushS3Info, which stages it in
	// the coordinator's committing workspace. Bound from the owning
	// MultiUpdate at construction; nil disables link recording.
	collectPkMoveLinkRows func(proc *process.Process, linkCtx, ownerCtx *MultiUpdateCtx, contextBatch *batch.Batch) (*batch.Batch, error)
	// pkMoveLinkBufs holds the synthesized link batches (one entry per
	// link-context collection) until flushTailAndWriteToOutput appends
	// them to the tail output and frees them. Intermediate sortAndSync
	// flushes do NOT touch them; retention is bounded separately by
	// pkMoveLinkMemBudget through spillPkMoveLinkBufs.
	pkMoveLinkBufs []pkMoveLinkBuf
	// pkMoveLinkBytes approximates the in-memory footprint retained in
	// pkMoveLinkBufs (unset for already-spilled entries). It is reset
	// after a full spill and at the final ferry.
	pkMoveLinkBytes int64
	// pkMoveLinkSpillFiles tracks every temporary spill object this
	// writer created together with its crash-recovery cleanup marker.
	// The writer OWNS each file from spill until ownership transfers:
	// emitting the ferry tail row PUBLISHES the record and hands
	// ownership to the committing consumer, which deletes object and
	// marker after a successful read-back-and-stage; an unpublished
	// record is reclaimed by the writer's own terminal paths (reset,
	// free). A process crash abandons both sides: the marker's
	// sync-protection window then expires and the GC
	// unpublished-cleanup replay deletes the object (it is never
	// catalog-visible) and releases the marker.
	pkMoveLinkSpillFiles []pkMoveLinkSpillFile
	// pkMoveLinkSpillSeen dedupes marker creation by object name: the
	// sinker's name-before-persist hook observes every sync attempt, and
	// a commit-ambiguous retry must not double-register one file.
	pkMoveLinkSpillSeen map[string]bool
	// pkMoveLinkSpillJob / pkMoveLinkSpillValid fence GC replay away from
	// this writer's live spill objects: the marker carries the job id and
	// the end of the protection window (first spill time plus
	// pkMoveLinkSpillProtection), and replay skips cleanup while the
	// window holds.
	pkMoveLinkSpillJob   string
	pkMoveLinkSpillValid int64

	memController struct {
		grantedSize int64
		throttler   rscthrottler.RSCThrottler
	}
}

// pkMoveLinkMemBudget bounds the in-memory retention of synthesized
// pk-move link batches inside one S3 writer: link markers are emitted
// per row (they feed commit-time lineage healing), so a bulk UPDATE
// statement would otherwise retain two encoded pk copies plus identity
// columns for every row until the final ferry. Over the budget the
// writer spills all buffered batches (one object per link ctx) and
// ferries the object locations instead. A var so tests can shrink it.
var pkMoveLinkMemBudget int64 = 16 << 20

// pkMoveLinkSpillFile pairs one temporary spill object with the cleanup
// marker that secures its crash recovery. published marks ownership
// handed to the ferry consumer: the tail row naming the object (and its
// marker) has been emitted to the committing pipeline, whose read-back
// deletes object and marker after a successful stage; the producer's
// teardown (reset/free) must never delete a published object — the
// consumer may not have read it yet — and a consumer that never runs
// leaves the marker's expired GC replay as the cleanup owner.
type pkMoveLinkSpillFile struct {
	file      string
	marker    string
	published bool
}

// pkMoveLinkSpillProtection bounds how long the crash-recovery fence of a
// spilled link object defers GC replay cleanup. Live statements hold
// spill objects far shorter than this window; crash orphans are reclaimed
// once it (plus the GC protection manager's TTL) passes. A statement that
// still holds spill objects past the window can lose them to replay and
// then fails LOUDLY at ferried read-back instead of silently dropping
// provenance. A var so tests can shorten it.
var pkMoveLinkSpillProtection = 6 * time.Hour

// pkMoveLinkRecordSpillMarker is the cleanup-intent write seam for spill
// objects, split from the spill path so tests can inject ownership
// failures.
var pkMoveLinkRecordSpillMarker = ioutil.RecordUnpublishedObjectCleanup

// pkMoveLinkDeleteSpillObjects and pkMoveLinkReleaseSpillMarker are the
// terminal delete seams, split out so tests can inject cleanup failures
// and pin the ownership ordering — an object stays owned by its marker
// until the object is confirmed absent, never the other way round.
var pkMoveLinkDeleteSpillObjects = ioutil.DeleteUnpublishedObjects
var pkMoveLinkReleaseSpillMarker = ioutil.DeleteUnpublishedObjectCleanup

// pkMoveLinkBuf is one synthesized pk-move link batch awaiting its ferry
// slot in the tail output; idx is the synthetic link context's position
// in writer.updateCtxs (its TableDef supplies the link table id). bat
// is nil after a spill, when spilled carries the block-info of the
// object the rows were written to; rows is the link-row count either
// way (the buffer row count is needed at the ferry even when the raw
// batch no longer exists).
type pkMoveLinkBuf struct {
	idx     int
	bat     *batch.Batch
	spilled *batch.Batch
	rows    uint64
}

func newS3Writer(
	sid string,
	update *MultiUpdate,
) (*s3WriterDelegate, error) {

	tableCount := len(update.MultiUpdateCtx)
	writer := &s3WriterDelegate{
		insertSinkers:  make([]*colexec.CNS3Writer, tableCount),
		deleteBatches:  make([]*batch.BatchSet, tableCount),
		updateCtxInfos: update.ctr.updateCtxInfos,
		seqnums:        make([][]uint16, 0, tableCount),
		sortIndexes:    make([]int, 0, tableCount),
		pkIndexes:      make([]int, 0, tableCount),
		schemaVersions: make([]uint32, 0, tableCount),
		isClusterBys:   make([]bool, 0, tableCount),

		deleteBlockInfo:     make([]*deleteBlockInfo, tableCount),
		insertBlockInfo:     make([]*batch.Batch, tableCount),
		insertBlockRowCount: make([]uint64, tableCount),
		deleteBlockMap:      make([]map[types.Blockid]*deleteBlockData, tableCount),
		insertFreeLists:     make([]*containers.BatchFreeList, tableCount),
		rejectZeroTemporal:  update.RejectZeroTemporal,
		addAffectedRows:     update.addAffectedRowsFunc,
		takeAffectedRows:    update.takeS3AffectedRowsFunc,
		seenTargetRows:      update.ctr.seenTargetRows,
		admitSeenGrowth:     update.admitSeenTargetRowsGrowth,
	}
	for i := range writer.insertFreeLists {
		writer.insertFreeLists[i] = containers.NewBatchFreeList(nil, nil, true)
	}

	if throttler, ok := runtime.ServiceRuntime(sid).GetGlobalVariables(runtime.CNMemoryThrottler); !ok {
		return nil, moerr.NewInternalErrorNoCtxf("can not get global variable %s", runtime.CNMemoryThrottler)
	} else {
		writer.memController.throttler = throttler.(rscthrottler.RSCThrottler)
	}

	faultInjected := false

	for _, updateCtx := range update.MultiUpdateCtx {
		if !faultInjected {
			faultInjected, _ = objectio.LogCNFlushSmallObjsInjected(
				updateCtx.TableDef.DbName, updateCtx.TableDef.Name,
			)
		}

		appendCfgToWriter(writer, updateCtx.TableDef)
	}

	writer.updateCtxs = update.MultiUpdateCtx
	writer.collectPkMoveLinkRows = update.collectPkMoveLinkBatch

	threshold := InsertWriteS3Threshold
	if faultInjected {
		threshold = colexec.FaultInjectedS3Threshold
	}

	writer.action = s3WriterAction(writer.updateCtxs)
	if writer.action == actionDelete {
		writer.flushThreshold = DeleteWriteS3Threshold
	} else {
		writer.flushThreshold = threshold
	}
	writer.checkSizeCols = retainedS3InputCols(writer.updateCtxs, writer.action)

	return writer, nil
}

func (writer *s3WriterDelegate) refreshSelectorState(update *MultiUpdate) {
	writer.seenTargetRows = update.ctr.seenTargetRows
	writer.admitSeenGrowth = update.admitSeenTargetRowsGrowth
	writer.addAffectedRows = update.addAffectedRowsFunc
	writer.takeAffectedRows = update.takeS3AffectedRowsFunc
}

func s3WriterAction(updateCtxs []*MultiUpdateCtx) actionType {
	hasInsert := false
	hasDelete := false
	for _, updateCtx := range updateCtxs {
		hasInsert = hasInsert || len(updateCtx.InsertCols) > 0
		hasDelete = hasDelete || len(updateCtx.DeleteCols) > 0
	}
	if hasInsert && hasDelete {
		return actionUpdate
	}
	if hasInsert {
		return actionInsert
	}
	return actionDelete
}

func retainedS3InputCols(updateCtxs []*MultiUpdateCtx, action actionType) []int {
	var cols []int
	for _, updateCtx := range updateCtxs {
		if action != actionDelete {
			cols = append(cols, updateCtx.InsertCols...)
		}
		if action != actionInsert && len(updateCtx.DeleteCols) > 0 {
			deleteColCount := min(2, len(updateCtx.DeleteCols))
			cols = append(cols, updateCtx.DeleteCols[:deleteColCount]...)
		}
	}
	return cols
}

// ensureInsertSinkers lazily creates persistent per-table insert sinkers
// on first call. Requires proc for Mp() and fileservice.
func (writer *s3WriterDelegate) ensureInsertSinkers(proc *process.Process) error {
	if writer.insertSinkers[0] != nil {
		return nil
	}
	fs, err := colexec.GetSharedFSFromProc(proc)
	if err != nil {
		return err
	}
	for i, updateCtx := range writer.updateCtxs {
		if len(updateCtx.InsertCols) == 0 {
			continue
		}
		opts := []ioutil.SinkerOption{
			ioutil.WithBuffer(writer.insertFreeLists[i], false),
		}
		if v, ok := proc.Ctx.Value(ioutil.PipelineFlushKey).(bool); ok && v {
			opts = append(opts, ioutil.WithPipelineFlush())
		}
		writer.insertSinkers[i] = colexec.NewCNS3DataWriterForService(
			proc.GetService(), proc.Mp(), fs, updateCtx.TableDef, -1, false,
			opts...)
	}
	return nil
}

func (writer *s3WriterDelegate) cleanDeleteBatches(mp *mpool.MPool) {
	for i, bs := range writer.deleteBatches {
		if bs == nil {
			continue
		}
		bats := bs.TakeBatches()
		for _, bat := range bats {
			bat.Clean(mp)
		}
		writer.deleteBatches[i] = nil
	}

	writer.batchSize = 0

	if writer.memController.grantedSize > 0 {
		writer.memController.throttler.Release(writer.memController.grantedSize)
		writer.memController.grantedSize = 0
	}
}

func (writer *s3WriterDelegate) append(
	proc *process.Process,
	analyzer process.Analyzer,
	inBatch *batch.Batch,
) (err error) {

	if err = writer.ensureInsertSinkers(proc); err != nil {
		return
	}

	mp := proc.Mp()
	seenSizeBefore := writer.seenTargetRowsSize()
	targetBatches := make(map[int]*batch.Batch)
	defer func() {
		for _, targetBatch := range targetBatches {
			if targetBatch != inBatch {
				targetBatch.Clean(mp)
			}
		}
	}()
	contextBatches := make([]*batch.Batch, len(writer.updateCtxs))
	for i, updateCtx := range writer.updateCtxs {
		targetIdx := updateCtx.TargetUpdateCtxIdx
		if targetIdx < 0 || targetIdx >= len(writer.updateCtxs) {
			return moerr.NewInternalError(proc.Ctx, "invalid multi-target update context index")
		}
		targetBatch, ok := targetBatches[targetIdx]
		if !ok {
			var duplicateRows uint64
			targetBatch, _, duplicateRows, err = filterTargetRows(
				proc,
				writer.updateCtxs[targetIdx],
				inBatch,
				writer.seenTargetRows[targetTableID(writer.updateCtxs[targetIdx])],
			)
			if err != nil {
				return err
			}
			writer.addAffectedRows(duplicateRows)
			targetBatches[targetIdx] = targetBatch
		}
		contextBatches[i] = targetBatch
	}
	seenIncrement := writer.seenTargetRowsSize() - seenSizeBefore
	if err = writer.admitSeenGrowth(seenIncrement); err != nil {
		return err
	}

	// Synthesize pk-move update links from the same target-deduped batch
	// the writer is about to split into independent delete/insert
	// streams. Mirrors the link branch of updateOneBatch; the batches are
	// buffered (not written) and ferry to the coordinator with the tail:
	// only its workspace commits. Skip empty batches and let the shared
	// collector drop rows whose pk did not move.
	if writer.collectPkMoveLinkRows != nil {
		for i, updateCtx := range writer.updateCtxs {
			if !isUpdatePkMoveLinkCtx(updateCtx) {
				continue
			}
			targetIdx := updateCtx.TargetUpdateCtxIdx
			if targetIdx < 0 || targetIdx >= len(writer.updateCtxs) {
				return moerr.NewInternalError(proc.Ctx, "invalid multi-target update context index")
			}
			linkBat, lerr := writer.collectPkMoveLinkRows(
				proc, updateCtx,
				writer.updateCtxs[targetIdx], contextBatches[i],
			)
			if lerr != nil {
				return lerr
			}
			if linkBat == nil || linkBat.RowCount() == 0 {
				if linkBat != nil {
					linkBat.Clean(proc.Mp())
				}
				continue
			}
			writer.pkMoveLinkBufs = append(writer.pkMoveLinkBufs,
				pkMoveLinkBuf{idx: i, bat: linkBat, rows: uint64(linkBat.RowCount())})
			writer.pkMoveLinkBytes += int64(linkBat.Size())
		}
		if err = writer.spillPkMoveLinkBufs(proc, analyzer); err != nil {
			return err
		}
	}

	// Route insert columns directly to per-table sinkers (no clone).
	// Auto-spill S3 writes during Write use proc.Ctx; perfcounter tracking
	// is deferred to the final Sync in flushTailAndWriteToOutput.
	for i, updateCtx := range writer.updateCtxs {
		if len(updateCtx.InsertCols) == 0 || writer.insertSinkers[i] == nil {
			continue
		}
		contextBatch := contextBatches[i]
		if contextBatch.RowCount() == 0 {
			continue
		}
		insertAttrs := lookupUpdateCtxInfo(writer.updateCtxInfos, updateCtx).insertAttrs
		projBat := contextBatch.SelectColumns(updateCtx.InsertCols, insertAttrs)

		tableType := lookupUpdateCtxInfo(writer.updateCtxInfos, updateCtx).tableType

		mainTablePkProjectIdx := -1
		mainTableNullPkFilter := false
		if tableType == UpdateMainTable && updateCtx.SkipInsertOnNullPk {
			mainTablePkProjectIdx = updateCtx.InsertPkColIdx
			if mainTablePkProjectIdx < 0 || mainTablePkProjectIdx >= len(projBat.Vecs) {
				return moerr.NewInternalError(proc.Ctx, "invalid main table insert pk column index")
			}
			mainTableNullPkFilter = projBat.Vecs[mainTablePkProjectIdx].HasNull()
		}

		// Check NOT NULL constraints for main table columns (mirrors insert_main_table).
		if tableType == UpdateMainTable {
			if mainTableNullPkFilter {
				var checked *batch.Batch
				// This validation copy leaves any allocation-accounted join owner;
				// it is short-lived and never published back into that owner.
				if checked, err = sqlutil.CopyBatch(projBat, proc); err != nil {
					return
				}
				nulls := checked.Vecs[mainTablePkProjectIdx].GetNulls().GetBitmap().Clone()
				checked.ShrinkByMask(nulls, true, 0)
				if err = checkMainTableNotNull(proc, updateCtx, checked); err != nil {
					checked.Clean(mp)
					return
				}
				checked.Clean(mp)
			} else {
				for insertIdx, inputIdx := range updateCtx.InsertCols {
					col := updateCtx.TableDef.Cols[insertIdx]
					if col.Default != nil && !col.Default.NullAbility && !strings.HasPrefix(col.Name, catalog.PrefixCBColName) {
						if contextBatch.Vecs[inputIdx].HasNull() {
							return moerr.NewConstraintViolation(proc.Ctx, fmt.Sprintf("Column '%s' cannot be null", col.Name))
						}
					}
				}
			}
		}

		if tableType == UpdateMainTable && updateCtx.ChangedRowsCol != nil &&
			len(updateCtx.AffectedRowsCols) == 0 && !updateCtx.SuppressPhysicalAffectedRows {
			writer.addAffectedRows(insertAffectedRows(updateCtx, contextBatch))
		}

		// Index tables with a sort key need null rows stripped — the sinker
		// sorts by this key and nulls cannot participate.
		needNullFilter := tableType != UpdateMainTable &&
			!writer.isClusterBys[i] &&
			writer.sortIndexes[i] > -1
		if tableType == UpdateMainTable {
			needNullFilter = mainTableNullPkFilter
		}

		if needNullFilter && (tableType == UpdateMainTable || projBat.Vecs[writer.sortIndexes[i]].HasNull()) {
			// Clone because SelectColumns shares vectors, and ShrinkByMask
			// modifies in-place.
			var filtered *batch.Batch
			// The sinker owns the filtered copy independently of the input
			// pipeline, so cross the allocation ownership boundary explicitly.
			if filtered, err = sqlutil.CopyBatch(projBat, proc); err != nil {
				return
			}
			nullIdx := writer.sortIndexes[i]
			if tableType == UpdateMainTable {
				nullIdx = mainTablePkProjectIdx
			}
			nulls := filtered.Vecs[nullIdx].GetNulls().GetBitmap().Clone()
			filtered.ShrinkByMask(nulls, true, 0)
			if filtered.RowCount() > 0 {
				if tableType == UpdateMainTable {
					if err = checkZeroTemporalInStrictMode(writer.rejectZeroTemporal, proc, filtered); err != nil {
						filtered.Clean(mp)
						return
					}
				}
				err = writer.insertSinkers[i].Write(proc.Ctx, filtered)
			}
			filtered.Clean(mp)
			if err != nil {
				return
			}
			continue
		}

		if tableType == UpdateMainTable {
			if err = checkZeroTemporalInStrictMode(writer.rejectZeroTemporal, proc, projBat); err != nil {
				return
			}
		}

		if err = writer.insertSinkers[i].Write(proc.Ctx, projBat); err != nil {
			return
		}
	}

	// Accumulate delete columns in per-table BatchSets (2 cols only).
	for i, updateCtx := range writer.updateCtxs {
		if len(updateCtx.DeleteCols) == 0 {
			continue
		}
		contextBatch := contextBatches[i]
		if contextBatch.RowCount() == 0 {
			continue
		}
		if writer.deleteBatches[i] == nil {
			writer.deleteBatches[i] = batch.NewBatchSet(objectio.BlockMaxRows)
		}
		projBat := contextBatch.SelectColumns(updateCtx.DeleteCols[:2], DeleteBatchAttrs)
		if _, err = writer.deleteBatches[i].Extend(mp, projBat, nil); err != nil {
			return
		}
	}

	var (
		increment      uint64
		acquireGranted bool
	)
	for _, idx := range writer.checkSizeCols {
		increment += uint64(inBatch.Vecs[idx].Size())
	}

	writer.batchSize += increment
	if writer.batchSize >= writer.flushThreshold {
		err = writer.sortAndSync(proc, analyzer)
		return err
	}

	// acquire memory failed, should flush as soon as possible, and then
	// release the pinned batches to avoid oom.
	if _, acquireGranted =
		writer.memController.throttler.Acquire(int64(increment)); !acquireGranted {
		err = writer.sortAndSync(proc, analyzer)
		return err
	}

	writer.memController.grantedSize += int64(increment)

	return
}

// spillPkMoveLinkBufs bounds pk-move link retention across intermediate
// sortAndSync flushes. Once the buffered link batches exceed
// pkMoveLinkMemBudget, every buffered batch of a link ctx is written
// into one spilled object (one per ctx, in first-use order) and the
// buffer list collapses into location entries; the final ferry then
// carries the block-info instead of the rows. Same-key continuity
// markers ride along untouched — commit-time lineage healing consumes
// them — and rows spilled here are exactly the rows the receive side
// would have staged from memory.
//
// Cleanup ownership is write-ahead at byte level: the sinker invokes the
// writer's ownership hook after reserving an object's name and before any
// byte of that object reaches storage — including automatic threshold
// flushes inside Write — so the durable cleanup marker always precedes
// the object, and a statement whose marker cannot be secured fails before
// the object ever exists. The writer deletes objects and markers on every
// live terminal path (reset, free); a failed group removes anything it
// already persisted and releases those markers on the spot. A process
// crash abandons object and marker together: the marker's protection
// window (pkMoveLinkSpillProtection) fences live statements from GC
// replay, and after it expires the unpublished-cleanup replay deletes
// the never-catalog-visible object and releases its marker.
func (writer *s3WriterDelegate) spillPkMoveLinkBufs(proc *process.Process, analyzer process.Analyzer) (err error) {
	if writer.pkMoveLinkBytes <= pkMoveLinkMemBudget {
		return nil
	}
	if len(writer.pkMoveLinkBufs) == 0 {
		writer.pkMoveLinkBytes = 0
		return nil
	}
	mp := proc.Mp()
	fs, err := colexec.GetSharedFSFromProc(proc)
	if err != nil {
		return err
	}
	writeCtx := perfcounter.AttachS3RequestKey(proc.Ctx, analyzer.GetOpCounterSet())

	// Group the IN-MEMORY buffers by link ctx in first-use order (an
	// earlier spill's location entries carry no rows to re-write). Each
	// group gains one spilled-object buffer anchored at its first
	// position, preserving the cross-ctx ferry order of the rows.
	seen := make(map[int]bool)
	var groupOrder []int
	for _, buf := range writer.pkMoveLinkBufs {
		if buf.bat == nil {
			continue
		}
		if !seen[buf.idx] {
			seen[buf.idx] = true
			groupOrder = append(groupOrder, buf.idx)
		}
	}
	if len(groupOrder) == 0 {
		writer.pkMoveLinkBytes = 0
		return nil
	}
	spilledByIdx := make(map[int]pkMoveLinkBuf)
	for _, idx := range groupOrder {
		sinker := colexec.NewCNS3DataWriterForService(
			proc.GetService(), mp, fs, writer.updateCtxs[idx].TableDef, -1, false,
			ioutil.WithUnpublishedNameObserver(func(octx context.Context, name string) error {
				return writer.trackPkMoveLinkSpillObject(fs, octx, name)
			}))
		// A group that fails anywhere before joining the ferry deletes
		// whatever it already persisted and releases the matching
		// markers: partial spill output has no consumer and must not
		// outlive the statement. A file that cannot be deleted keeps its
		// marker, so crash recovery owns every survivor.
		deletePartial := func() {
			deleted, derr := sinker.DeletePersisted(writeCtx)
			if derr != nil {
				logutil.Warn("failed to delete partial pk-move link spill",
					zap.Int("link_ctx_idx", idx), zap.Error(derr))
				return
			}
			writer.releasePkMoveLinkSpillMarkers(writeCtx, fs, deleted)
		}
		var rows uint64
		for i := range writer.pkMoveLinkBufs {
			buf := &writer.pkMoveLinkBufs[i]
			if buf.idx != idx || buf.bat == nil {
				continue
			}
			rows += buf.rows
			if werr := process.MeasureFilesystemWaitErr(analyzer, func() error {
				return sinker.Write(writeCtx, buf.bat)
			}); werr != nil {
				deletePartial()
				_ = sinker.Close()
				return werr
			}
		}
		if _, serr := sinker.Sync(writeCtx); serr != nil {
			deletePartial()
			_ = sinker.Close()
			return serr
		}
		var infoBat *batch.Batch
		if infoBat, err = sinker.FillBlockInfoBat(); err != nil {
			deletePartial()
			_ = sinker.Close()
			return err
		}
		// FillBlockInfoBat returns the sinker-owned batch; a spill entry
		// must outlive the sinker, so clone it across the ownership
		// boundary before closing.
		var ownedInfo *batch.Batch
		if ownedInfo, err = sqlutil.CopyBatch(infoBat, proc); err != nil {
			deletePartial()
			_ = sinker.Close()
			return err
		}
		if err = sinker.Close(); err != nil {
			deletePartial()
			ownedInfo.Clean(mp)
			return err
		}
		// The group is durable — and already owned: the sinker's
		// name-before-persist hook recorded every file's cleanup marker
		// before its first byte reached storage, so a process crash at
		// any point from here on leaves reclaimable, marked objects that
		// transaction GC never sees but the unpublished-cleanup replay
		// reclaims once the protection window expires. Ride the markers
		// along the ferry itself as a second tail column: the committing
		// consumer needs them to release cleanup ownership after its own
		// successful read-back-and-stage (the producer's teardown skips
		// published records, so without the column nobody would ever
		// reclaim a consumed spill).
		if err = writer.attachPkMoveLinkSpillMarkers(proc, ownedInfo); err != nil {
			deletePartial()
			ownedInfo.Clean(mp)
			return err
		}
		spilledByIdx[idx] = pkMoveLinkBuf{idx: idx, spilled: ownedInfo, rows: rows}
		logutil.Info("pk-move link buffers spilled to object storage",
			zap.Int("link_ctx_idx", idx),
			zap.Uint64("rows", rows),
			zap.Int64("retained_bytes_before_spill", writer.pkMoveLinkBytes))
	}
	var next []pkMoveLinkBuf
	emitted := make(map[int]bool)
	for _, buf := range writer.pkMoveLinkBufs {
		if buf.spilled != nil {
			next = append(next, buf)
			continue
		}
		if sb, ok := spilledByIdx[buf.idx]; ok && !emitted[buf.idx] {
			emitted[buf.idx] = true
			next = append(next, sb)
		}
		if buf.bat != nil {
			buf.bat.Clean(mp)
		}
	}
	writer.pkMoveLinkBufs = next
	writer.pkMoveLinkBytes = 0
	return nil
}

// pkMoveLinkBlockInfoFiles decodes the object file names a spill group's
// block-info batch points at (one per block row).
func pkMoveLinkBlockInfoFiles(infoBat *batch.Batch) []string {
	if infoBat == nil || infoBat.RowCount() == 0 || len(infoBat.Vecs) == 0 {
		return nil
	}
	files := make([]string, 0, infoBat.RowCount())
	blkVec := infoBat.Vecs[0]
	for i := 0; i < infoBat.RowCount(); i++ {
		blk := objectio.DecodeBlockInfo(blkVec.GetBytesAt(i))
		files = append(files, blk.MetaLocation().Name().String())
	}
	return files
}

// trackPkMoveLinkSpillObject secures durable crash-recovery ownership for
// one spill object the sinker is about to persist. The sinker invokes
// this hook after reserving the object's name and before any byte of
// that object reaches storage, so the marker always precedes the file:
// a crash at any later point leaves a reclaimable, owned object, and a
// marker write failure aborts the sync before the object exists. The
// marker carries the writer's sync-protection identity: while the
// protection window holds, GC replay skips these files; after a crash it
// expires and the replay deletes the never-catalog-visible objects and
// releases their markers.
func (writer *s3WriterDelegate) trackPkMoveLinkSpillObject(
	fs fileservice.FileService,
	ctx context.Context,
	file string,
) error {
	if writer.pkMoveLinkSpillJob == "" ||
		time.Now().UnixNano() >= writer.pkMoveLinkSpillValid {
		// One armed lease per window: reset clears the job at the
		// execution boundary, and an expired window never extends over a
		// later spill — its marker must never brand a live, unconsumed
		// object reclaimable while the consumer may still read it back.
		writer.pkMoveLinkSpillJob = "pk-move-link-spill-" + uuid.NewString()
		writer.pkMoveLinkSpillValid = time.Now().Add(pkMoveLinkSpillProtection).UnixNano()
	}
	if writer.pkMoveLinkSpillSeen[file] {
		// The name reservation precedes every sync attempt of one file
		// sinker; a commit-ambiguous retry observes the same name twice.
		return nil
	}
	marker, err := pkMoveLinkRecordSpillMarker(ctx, fs, ioutil.UnpublishedObject{
		File:                  file,
		SyncProtectionJobID:   writer.pkMoveLinkSpillJob,
		SyncProtectionValidTS: writer.pkMoveLinkSpillValid,
	})
	if err != nil {
		return moerr.NewInternalErrorNoCtxf(
			"pk-move link spill ownership marker for %s: %v", file, err)
	}
	if writer.pkMoveLinkSpillSeen == nil {
		writer.pkMoveLinkSpillSeen = make(map[string]bool)
	}
	writer.pkMoveLinkSpillSeen[file] = true
	writer.pkMoveLinkSpillFiles = append(writer.pkMoveLinkSpillFiles,
		pkMoveLinkSpillFile{file: file, marker: marker})
	return nil
}

// releasePkMoveLinkSpillMarkers releases the cleanup markers of spill
// files confirmed deleted. The writer calls it only on the success path
// of a deletion pass; a file whose deletion failed keeps its marker, so
// crash recovery owns every surviving object.
func (writer *s3WriterDelegate) releasePkMoveLinkSpillMarkers(
	ctx context.Context,
	fs fileservice.FileService,
	deleted []string,
) {
	if len(deleted) == 0 || len(writer.pkMoveLinkSpillFiles) == 0 {
		return
	}
	gone := make(map[string]struct{}, len(deleted))
	for _, f := range deleted {
		gone[f] = struct{}{}
	}
	kept := writer.pkMoveLinkSpillFiles[:0]
	for _, rec := range writer.pkMoveLinkSpillFiles {
		if _, ok := gone[rec.file]; !ok {
			kept = append(kept, rec)
			continue
		}
		delete(writer.pkMoveLinkSpillSeen, rec.file)
		if err := pkMoveLinkReleaseSpillMarker(ctx, fs, rec.marker); err != nil {
			// The object is gone; a stale marker self-heals — the
			// replay finds the object absent and releases the marker.
			logutil.Warn("pk-move link spill marker release failed; crash recovery owns it",
				zap.String("marker", rec.marker), zap.Error(err))
		}
	}
	writer.pkMoveLinkSpillFiles = kept
}

// pkMoveLinkSpillCleanupTimeout bounds the terminal live cleanup. Pipeline
// teardown cancels proc.Ctx BEFORE operator reset runs, so deletes issued
// under that context fail immediately and every spill survives for the
// whole crash-recovery window; a fresh context keeps the live cleanup
// actually live, and the cap keeps a wedged object store from stalling
// the teardown path.
const pkMoveLinkSpillCleanupTimeout = 10 * time.Minute

// cleanupPkMoveLinkSpillFiles is the owner-side terminal action for
// reset and free: delete every UNPUBLISHED recorded spill object and then
// its cleanup marker. A published record's ownership already moved to
// the ferry consumer — deleting its object here could race the
// consumer's read-back (pipeline teardown resets this writer without
// waiting for the committing fragment), so the producer leaves published
// objects, and their markers, alone. Each marker of a deleted object is
// released only after the object is confirmed absent — releasing it
// while the object survives would discard the crash-recovery ownership
// of a file nothing else tracks. Best-effort with a crash backstop:
// anything that cannot be deleted stays owned by its marker, whose
// expired protection lets GC replay reclaim it; a leftover marker for
// an already-deleted object is self-healing (the replay finds the
// object absent and releases the marker).
func (writer *s3WriterDelegate) cleanupPkMoveLinkSpillFiles(proc *process.Process) {
	if len(writer.pkMoveLinkSpillFiles) == 0 {
		return
	}
	var owned []pkMoveLinkSpillFile
	for _, rec := range writer.pkMoveLinkSpillFiles {
		if rec.published {
			// Ownership transferred to the ferry consumer at
			// publication: the consumer reclaims the object and its
			// marker after a successful read-back, so the producer
			// sheds the record here — the producer ledger is
			// execution-scoped and must not pin a transferred object
			// into the next execution's pass.
			continue
		}
		owned = append(owned, rec)
	}
	writer.pkMoveLinkSpillFiles = nil
	if len(owned) == 0 {
		return
	}
	fs, err := colexec.GetSharedFSFromProc(proc)
	if err != nil {
		logutil.Warn("pk-move link spill cleanup lost its filesystem; crash recovery owns the remainder",
			zap.Error(err))
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), pkMoveLinkSpillCleanupTimeout)
	defer cancel()
	for _, rec := range owned {
		if _, err := pkMoveLinkDeleteSpillObjects(ctx, fs, rec.file); err != nil {
			logutil.Warn("pk-move link spill object cleanup failed; crash recovery owns it",
				zap.String("file", rec.file), zap.Error(err))
			continue
		}
		if err := pkMoveLinkReleaseSpillMarker(ctx, fs, rec.marker); err != nil {
			logutil.Warn("pk-move link spill marker cleanup failed; crash recovery owns the remainder",
				zap.String("marker", rec.marker), zap.Error(err))
		}
	}
}

// pkMoveLinkSpillMarkerCol names the ferried block-info batch's second
// column: per block row, the crash-recovery cleanup marker of the object
// that block lives in. The committing consumer needs it to release
// cleanup ownership after a successful read-back-and-stage — the
// producer's teardown skips published records, so without the column
// nobody would ever reclaim a consumed spill.
const pkMoveLinkSpillMarkerCol = "pk_move_link_spill_marker"

// attachPkMoveLinkSpillMarkers appends the marker column to a durable
// spill group's block-info batch, one marker string per block row (empty
// when a file has no recorded marker, which cannot happen on the success
// path — the name observer records before persistence).
func (writer *s3WriterDelegate) attachPkMoveLinkSpillMarkers(
	proc *process.Process, infoBat *batch.Batch,
) error {
	files := pkMoveLinkBlockInfoFiles(infoBat)
	if len(files) == 0 {
		return nil
	}
	markerVec := vector.NewVec(types.T_varchar.ToType())
	for _, file := range files {
		marker := ""
		for _, rec := range writer.pkMoveLinkSpillFiles {
			if rec.file == file {
				marker = rec.marker
				break
			}
		}
		if err := vector.AppendBytes(markerVec, []byte(marker), false, proc.Mp()); err != nil {
			markerVec.Free(proc.Mp())
			return err
		}
	}
	infoBat.Vecs = append(infoBat.Vecs, markerVec)
	infoBat.Attrs = append(infoBat.Attrs, pkMoveLinkSpillMarkerCol)
	return nil
}

// markPkMoveLinkSpillPublished hands the cleanup ownership of every spill
// object one ferried block-info batch names to the committing consumer:
// its tail row is on the way, and the producer's teardown (reset/free)
// must keep its hands off the object from here on — the consumer reads
// it back on its own schedule, possibly after this writer reset. A
// consumer that never runs leaves the object to the marker's expired GC
// replay, exactly like a producer crash.
func (writer *s3WriterDelegate) markPkMoveLinkSpillPublished(infoBat *batch.Batch) {
	if len(writer.pkMoveLinkSpillFiles) == 0 {
		return
	}
	for _, file := range pkMoveLinkBlockInfoFiles(infoBat) {
		for i := range writer.pkMoveLinkSpillFiles {
			if writer.pkMoveLinkSpillFiles[i].file == file {
				writer.pkMoveLinkSpillFiles[i].published = true
			}
		}
	}
}

func (writer *s3WriterDelegate) seenTargetRowsSize() int64 {
	var size int64
	for _, seen := range writer.seenTargetRows {
		size += seen.Size()
	}
	return size
}

func checkMainTableNotNull(proc *process.Process, updateCtx *MultiUpdateCtx, bat *batch.Batch) error {
	for insertIdx := range updateCtx.InsertCols {
		col := updateCtx.TableDef.Cols[insertIdx]
		if col.Default != nil && !col.Default.NullAbility && !strings.HasPrefix(col.Name, catalog.PrefixCBColName) {
			if bat.Vecs[insertIdx].HasNull() {
				return moerr.NewConstraintViolation(proc.Ctx, fmt.Sprintf("Column '%s' cannot be null", col.Name))
			}
		}
	}
	return nil
}

func (writer *s3WriterDelegate) prepareDeleteBatches(
	proc *process.Process,
	idx int,
	src []*batch.Batch,
	needClean bool,
) ([]*batch.Batch, error) {
	defer func() {
		if needClean {
			for _, bat := range src {
				bat.Clean(proc.GetMPool())
			}
		}
	}()

	// split delete batches by BlockID
	if writer.deleteBlockMap[idx] == nil {
		writer.deleteBlockMap[idx] = make(map[types.Blockid]*deleteBlockData)
	}
	blockMap := writer.deleteBlockMap[idx]

	for _, bat := range src {
		rowIDVec := bat.GetVector(RowIDIdx)
		nulls := rowIDVec.GetNulls()
		if nulls.Count() == bat.RowCount() {
			continue
		}

		rowIDs := vector.MustFixedColWithTypeCheck[types.Rowid](rowIDVec)
		for i, rowID := range rowIDs {
			if nulls.Contains(uint64(i)) {
				continue
			}

			blkid := rowID.CloneBlockID()
			segid := rowID.CloneSegmentID()
			// blkOffset := rowID.GetBlockOffset()
			rowOffset := rowID.GetRowOffset()

			if blockMap[blkid] == nil {
				blockMap[blkid] = newDeleteBlockData(bat, 1)

				tableId := uint64(0)
				txnID := []byte(nil)
				if idx < len(writer.updateCtxs) && writer.updateCtxs[idx] != nil && writer.updateCtxs[idx].TableDef != nil {
					tableId = writer.updateCtxs[idx].TableDef.TblId
				}
				if txnOp := proc.GetTxnOperator(); txnOp != nil {
					txnID = txnOp.Txn().ID
				}
				if colexec.IsDeletionOnTxnUnCommit(writer.segmentMap, &segid, tableId, txnID) {
					blockMap[blkid].typ = deletion.DeletionOnTxnUnCommit
				} else {
					blockMap[blkid].typ = deletion.DeletionOnCommitted
				}
			}

			block := blockMap[blkid]

			bitmap := block.bitmap
			if bitmap.Contains(uint64(rowOffset)) {
				continue
			}

			rowIDDst := block.bat.GetVector(RowIDIdx)
			pkDst := block.bat.GetVector(PkIdx)
			rowIDCheckpoint := rowIDDst.MakeAppendCheckpoint()
			pkCheckpoint := pkDst.MakeAppendCheckpoint()
			if err := vector.AppendFixed(rowIDDst, rowID, false, proc.GetMPool()); err != nil {
				rowIDDst.RollbackAppend(rowIDCheckpoint, 1)
				return nil, err
			}
			if err := pkDst.UnionOne(bat.GetVector(PkIdx), int64(i), proc.GetMPool()); err != nil {
				rowIDDst.RollbackAppend(rowIDCheckpoint, 1)
				pkDst.RollbackAppend(pkCheckpoint, 1)
				return nil, err
			}
			block.bat.SetRowCount(rowIDDst.Length())
			bitmap.Add(uint64(rowOffset))
		}
	}

	//collect batchs that can be flush
	blkids := make([]types.Blockid, 0, len(blockMap))
	for blkid, data := range blockMap {
		//Don't flush rowids belong to uncommitted cn block and raw data batch in txn's workspace.
		if data.typ == deletion.DeletionOnTxnUnCommit {
			continue
		}
		blkids = append(blkids, blkid)
	}
	slices.SortFunc(blkids, func(a, b types.Blockid) int {
		return a.Compare(&b)
	})
	deleteBats := batch.NewBatchSet(objectio.BlockMaxRows)
	defer deleteBats.Clean(proc.GetMPool())
	for _, blkid := range blkids {
		bat := blockMap[blkid].bat
		delete(blockMap, blkid)
		err := deleteBats.Push(proc.GetMPool(), bat)
		if err != nil {
			return nil, err
		}
	}
	retBatchs := deleteBats.TakeBatches()
	return retBatchs, nil
}

func (writer *s3WriterDelegate) sortAndSync(proc *process.Process, analyzer process.Analyzer) (err error) {

	defer func() {
		writer.cleanDeleteBatches(proc.Mp())
	}()

	for i, updateCtx := range writer.updateCtxs {
		if len(updateCtx.DeleteCols) == 0 || writer.deleteBatches[i] == nil {
			continue
		}
		// Take batches from the per-table delete accumulator.
		delSrcBats := writer.deleteBatches[i].TakeBatches()
		var delBatches []*batch.Batch
		// needClean=true: prepareDeleteBatches cleans source batches after use.
		if delBatches, err = writer.prepareDeleteBatches(proc, i, delSrcBats, true); err != nil {
			return
		}
		if err = writer.sortAndSyncOneTable(
			proc, updateCtx.TableDef, analyzer, i, true, delBatches, true,
		); err != nil {
			return
		}
		// Insert data is managed by persistent sinkers (auto-spill via Write).
	}

	return
}

func (writer *s3WriterDelegate) sortAndSyncOneTable(
	proc *process.Process,
	tblDef *plan.TableDef,
	analyzer process.Analyzer,
	idx int,
	isTombstone bool,
	bats []*batch.Batch,
	cleanBatchAfterUse bool,
	opts ...ioutil.SinkerOption,
) (err error) {

	if len(bats) == 0 {
		return
	}

	defer func() {
		if cleanBatchAfterUse {
			for i, bat := range bats {
				if bat != nil {
					bat.Clean(proc.GetMPool())
					bats[i] = nil
				}
			}
		}
	}()

	var (
		fs           fileservice.FileService
		blockInfoBat *batch.Batch
		s3Writer     *colexec.CNS3Writer
		rowCount     = 0
	)

	if fs, err = colexec.GetSharedFSFromProc(proc); err != nil {
		return
	}

	if isTombstone {
		pkCol := plan2.PkColByTableDef(tblDef)
		s3Writer = colexec.NewCNS3TombstoneWriterForService(
			proc.GetService(), proc.Mp(), fs, plan2.ExprType2Type(&pkCol.Typ), -1, opts...,
		)
	} else {
		s3Writer = colexec.NewCNS3DataWriterForService(
			proc.GetService(), proc.Mp(), fs, tblDef, -1, false, opts...,
		)
	}

	defer s3Writer.Close()

	counterSet := analyzer.GetOpCounterSet()
	writeCtx := perfcounter.AttachS3RequestKey(proc.Ctx, counterSet)

	for i := range bats {
		rowCount += bats[i].RowCount()
		// When cleanBatchAfterUse is true, the batch is exclusively owned
		// (cloned), so we can transfer it to the sinker without copying.
		if cleanBatchAfterUse {
			owned, writeErr := process.MeasureFilesystemWait(analyzer, func() (bool, error) {
				return s3Writer.WriteOwned(writeCtx, bats[i])
			})
			// Ownership may transfer even when the subsequent spill fails.
			if owned {
				bats[i] = nil
			}
			if writeErr != nil {
				err = writeErr
				return
			}
			if owned {
				continue
			}
			// WriteOwned returned false: data was already copied inside
			// WriteOwned via Write(), so skip the Write() call below.
			bats[i].Clean(proc.GetMPool())
			bats[i] = nil
			continue
		}
		if err = process.MeasureFilesystemWaitErr(analyzer, func() error {
			return s3Writer.Write(writeCtx, bats[i])
		}); err != nil {
			return
		}

		if cleanBatchAfterUse {
			bats[i].Clean(proc.GetMPool())
		}
		bats[i] = nil
	}

	if err = process.MeasureFilesystemWaitErr(analyzer, func() error {
		_, syncErr := s3Writer.Sync(writeCtx)
		return syncErr
	}); err != nil {
		return
	}

	if blockInfoBat, err = s3Writer.FillBlockInfoBat(); err != nil {
		return
	}

	if isTombstone {
		return writer.fillDeleteBlockInfo(proc, idx, blockInfoBat, rowCount)
	}

	return writer.fillInsertBlockInfo(proc, idx, blockInfoBat, rowCount)

}

// initBlockInfoBat creates a new off-heap batch matching the schema of src.
func initBlockInfoBat(src *batch.Batch) *batch.Batch {
	dst := batch.NewOffHeapWithSize(src.VectorCount())
	dst.Attrs = src.Attrs
	for i := 0; i < src.VectorCount(); i++ {
		dst.Vecs[i] = vector.NewOffHeapVecWithType(*src.Vecs[i].GetType())
	}
	return dst
}

// appendVarlenaBatch appends all varlena data from src vecs into dst vecs.
func appendVarlenaBatch(dst, src *batch.Batch, mp *mpool.MPool) error {
	for i := range src.Vecs {
		row, col := vector.MustVarlenaRawData(src.Vecs[i])
		for j := range src.Vecs[i].Length() {
			if err := vector.AppendBytes(
				dst.Vecs[i], row[j].GetByteSlice(col), false, mp); err != nil {
				return err
			}
		}
	}
	dst.SetRowCount(dst.Vecs[0].Length())
	return nil
}

func (writer *s3WriterDelegate) fillDeleteBlockInfo(
	proc *process.Process,
	idx int,
	bat *batch.Batch,
	rowCount int,
) (err error) {

	if writer.deleteBlockInfo[idx] == nil {
		writer.deleteBlockInfo[idx] = &deleteBlockInfo{
			bat: initBlockInfoBat(bat),
		}
	}

	info := writer.deleteBlockInfo[idx]

	stats := objectio.ObjectStats(bat.Vecs[0].GetBytesAt(0))
	objId := stats.ObjectName().ObjectId()[:]
	info.name = fmt.Sprintf("%s|%d", objId, deletion.FlushDeltaLoc)
	info.rawRowCount += uint64(rowCount)

	return appendVarlenaBatch(info.bat, bat, proc.GetMPool())
}

func (writer *s3WriterDelegate) fillInsertBlockInfo(
	proc *process.Process,
	idx int,
	bat *batch.Batch,
	rowCount int,
) (err error) {

	if writer.insertBlockInfo[idx] == nil {
		writer.insertBlockInfo[idx] = initBlockInfoBat(bat)
		writer.insertBlockRowCount[idx] = 0
	}

	writer.insertBlockRowCount[idx] += uint64(rowCount)

	return appendVarlenaBatch(writer.insertBlockInfo[idx], bat, proc.GetMPool())
}

func (writer *s3WriterDelegate) flushTailAndWriteToOutput(proc *process.Process, analyzer process.Analyzer) (err error) {
	counterSet := analyzer.GetOpCounterSet()
	writeCtx := perfcounter.AttachS3RequestKey(proc.Ctx, counterSet)

	// Sync all insert sinkers — flushes remaining in-memory data to S3.
	for i, s3w := range writer.insertSinkers {
		if s3w == nil {
			continue
		}
		stats, syncErr := process.MeasureFilesystemWait(analyzer, func() ([]objectio.ObjectStats, error) {
			return s3w.Sync(writeCtx)
		})
		if syncErr != nil {
			return syncErr
		}
		if len(stats) == 0 {
			continue
		}
		blockInfoBat, fillErr := s3w.FillBlockInfoBat()
		if fillErr != nil {
			return fillErr
		}
		rowCount := 0
		for _, st := range stats {
			rowCount += int(st.Rows())
		}
		if err = writer.fillInsertBlockInfo(proc, i, blockInfoBat, rowCount); err != nil {
			return
		}
	}

	// Flush remaining deletes — always call sortAndSync so that accumulated
	// deleteBatches are processed through prepareDeleteBatches into
	// deleteBlockMap / deleteBlockInfo. Without this, small delete batches
	// (below TagS3SizeForMOLogger) would be silently lost.
	if err = writer.sortAndSync(proc, analyzer); err != nil {
		return
	}

	if writer.outputBat == nil {
		writer.outputBat = makeS3OutputBatch()
	}
	mp := proc.GetMPool()

	//write delete block info to workspace
	for i, block := range writer.deleteBlockInfo {
		// normal table
		if block != nil && block.bat.RowCount() > 0 {
			err = writer.addBatchToOutput(mp, actionDelete, i, block.rawRowCount, block.name, block.bat)
			if err != nil {
				return
			}
		}
	}

	//write delete batch (which not flush to s3) to workspace
	for i, block := range writer.deleteBlockMap {
		// normal table
		for _, blockData := range block {
			name := objectio.PhysicalAddr_Attr
			err = writer.addBatchToOutput(mp, actionDelete, i, uint64(blockData.bat.RowCount()), name, blockData.bat)
			if err != nil {
				return
			}
		}
	}

	//write insert block info to workspace
	for i, bat := range writer.insertBlockInfo {
		// normal table
		if bat != nil && bat.RowCount() > 0 {
			resetMergeBlockForOldCN(proc, bat)
			err = writer.addBatchToOutput(mp, actionInsert, i, writer.insertBlockRowCount[i], "", bat)
			if err != nil {
				return
			}
		}
	}

	// Ferry the synthesized pk-move link batches: UpdateFlushS3Info stages
	// them in the coordinator's committing workspace via actionPkMoveLink.
	// Encode the OWNER table id: the link ctx's plan TableDef is
	// schema-only with TblId 0 (a per-account virtual catalog table), and
	// the receiver resolves the link relation by matching this owner id to
	// its own link ctx.
	for _, buf := range writer.pkMoveLinkBufs {
		if buf.rows == 0 {
			continue
		}
		linkBat := buf.bat
		name := ""
		if buf.spilled != nil {
			// Spilled: the tail row names the object, the receiver reads
			// the link rows back out of its block-info. Emitting the row
			// hands the object's cleanup ownership to the committing
			// consumer: teardown after this point must not delete it.
			linkBat = buf.spilled
			name = pkMoveLinkSpillName
			writer.markPkMoveLinkSpillPublished(buf.spilled)
		}
		if linkBat == nil {
			continue
		}
		ownerIdx := writer.updateCtxs[buf.idx].TargetUpdateCtxIdx
		if ownerIdx < 0 || ownerIdx >= len(writer.updateCtxs) {
			return moerr.NewInternalError(proc.Ctx, "invalid multi-target update context index")
		}
		if err = writer.addBatchToOutput(mp, actionPkMoveLink, ownerIdx,
			buf.rows, name, linkBat); err != nil {
			return
		}
	}
	for _, buf := range writer.pkMoveLinkBufs {
		if buf.bat != nil {
			buf.bat.Clean(mp)
		}
		if buf.spilled != nil {
			buf.spilled.Clean(mp)
		}
	}
	writer.pkMoveLinkBufs = nil
	writer.pkMoveLinkBytes = 0

	// Logical ODKU counts are independent of physical blocks: a pure no-op can
	// have a non-zero CLIENT_FOUND_ROWS result, and parallel writers may be
	// hidden below merge PreScopes. Emit one control record from the shared
	// pending owner; the first flushable writer drains it and later writers see
	// zero. The final FlushS3Info operator then counts it exactly once.
	if writer.takeAffectedRows != nil {
		if affectedRows := writer.takeAffectedRows(); affectedRows > 0 {
			if err = writer.addAffectedRowsToOutput(mp, affectedRows); err != nil {
				return
			}
		}
	}

	return nil
}

func (writer *s3WriterDelegate) reset(proc *process.Process) (err error) {

	writer.cleanDeleteBatches(proc.Mp())

	// Reset persistent insert sinkers so any buffered data from a failed
	// pipeline execution is discarded. The sinkers stay alive and reuse
	// their buffer pools and arenas on the next append() call.  On the
	// success path the sinkers were already flushed by
	// flushTailAndWriteToOutput(), so Reset() is a no-op on the data side.
	for _, s3w := range writer.insertSinkers {
		if s3w != nil {
			s3w.Reset()
		}
	}

	for _, bat := range writer.insertBlockInfo {
		if bat != nil {
			bat.CleanOnlyData()
		}
	}
	for i := range writer.insertBlockRowCount {
		writer.insertBlockRowCount[i] = 0
	}
	for _, block := range writer.deleteBlockInfo {
		if block != nil {
			block.bat.CleanOnlyData()
			block.rawRowCount = 0
		}
	}
	for _, data := range writer.deleteBlockMap {
		for k, block := range data {
			if block != nil && block.bat != nil {
				block.bat.Clean(proc.Mp())
			}
			delete(data, k)
		}
	}

	if writer.outputBat != nil {
		writer.outputBat.CleanOnlyData()
	}

	for _, buf := range writer.pkMoveLinkBufs {
		if buf.bat != nil {
			buf.bat.Clean(proc.Mp())
		}
		if buf.spilled != nil {
			buf.spilled.Clean(proc.Mp())
		}
	}
	writer.pkMoveLinkBufs = nil
	writer.pkMoveLinkBytes = 0
	writer.cleanupPkMoveLinkSpillFiles(proc)
	// Prepare reuses this writer across pipeline executions, so the
	// execution-scoped spill leases shed here: the next execution arms
	// its own protection job and deadline instead of inheriting this
	// execution's running (or already expired) window.
	writer.pkMoveLinkSpillSeen = nil
	writer.pkMoveLinkSpillJob = ""
	writer.pkMoveLinkSpillValid = 0

	writer.buf.Reset()
	return
}

func (writer *s3WriterDelegate) free(proc *process.Process) (err error) {

	writer.cleanDeleteBatches(proc.Mp())
	mp := proc.Mp()

	// Close persistent insert sinkers.
	for i, s3w := range writer.insertSinkers {
		if s3w != nil {
			s3w.Close()
			writer.insertSinkers[i] = nil
		}
	}
	writer.insertSinkers = nil

	for _, fl := range writer.insertFreeLists {
		if fl != nil {
			fl.Close(mp)
		}
	}
	writer.insertFreeLists = nil

	for _, bat := range writer.insertBlockInfo {
		if bat != nil {
			bat.Clean(mp)
		}
	}
	writer.insertBlockInfo = nil
	writer.insertBlockRowCount = nil

	for _, info := range writer.deleteBlockInfo {
		if info != nil {
			info.bat.Clean(mp)
		}
	}
	writer.deleteBlockInfo = nil

	for _, data := range writer.deleteBlockMap {
		for _, block := range data {
			if block != nil && block.bat != nil {
				block.bat.Clean(mp)
			}
		}
	}
	writer.deleteBlockMap = nil

	if writer.outputBat != nil {
		writer.outputBat.Clean(mp)
		writer.outputBat = nil
	}
	for _, buf := range writer.pkMoveLinkBufs {
		if buf.bat != nil {
			buf.bat.Clean(mp)
		}
		if buf.spilled != nil {
			buf.spilled.Clean(mp)
		}
	}
	writer.pkMoveLinkBufs = nil
	writer.pkMoveLinkBytes = 0
	writer.cleanupPkMoveLinkSpillFiles(proc)
	writer.buf.Reset()

	return
}

func (writer *s3WriterDelegate) addBatchToOutput(
	mp *mpool.MPool,
	action actionType,
	idx int,
	rowCount uint64,
	name string,
	bat *batch.Batch,
) (err error) {
	output := writer.outputBat
	if action == actionInsert {
		rowCount = physicalInsertAffectedRows(writer.updateCtxs[idx], rowCount)
	}

	if err = vector.AppendFixed(output.Vecs[0], uint8(action), false, mp); err != nil {
		return
	}
	if err = vector.AppendFixed(output.Vecs[1], writer.updateCtxs[idx].TableDef.TblId, false, mp); err != nil {
		return
	}
	if err = vector.AppendFixed(output.Vecs[2], rowCount, false, mp); err != nil {
		return
	}
	if err = vector.AppendBytes(output.Vecs[3], []byte(name), false, mp); err != nil {
		return
	}

	var val []byte
	if val, err = bat.MarshalBinaryWithBuffer(&writer.buf, true); err != nil {
		return
	}
	if err = vector.AppendBytes(output.Vecs[4], val, false, mp); err != nil {
		return
	}

	output.SetRowCount(output.Vecs[0].Length())
	return
}

func (writer *s3WriterDelegate) addAffectedRowsToOutput(
	mp *mpool.MPool,
	affectedRows uint64,
) (err error) {
	output := writer.outputBat
	if err = vector.AppendFixed(output.Vecs[0], uint8(actionAffectedRows), false, mp); err != nil {
		return
	}
	if err = vector.AppendFixed(output.Vecs[1], uint64(0), false, mp); err != nil {
		return
	}
	if err = vector.AppendFixed(output.Vecs[2], affectedRows, false, mp); err != nil {
		return
	}
	if err = vector.AppendBytes(output.Vecs[3], nil, false, mp); err != nil {
		return
	}
	if err = vector.AppendBytes(output.Vecs[4], nil, false, mp); err != nil {
		return
	}
	output.SetRowCount(output.Vecs[0].Length())
	return
}

func makeS3OutputBatch() *batch.Batch {
	bat := batch.NewOffHeapWithSize(5)
	bat.Vecs[0] = vector.NewOffHeapVecWithType(types.T_uint8.ToType())   // actionInsert/actionDelete/actionAffectedRows
	bat.Vecs[1] = vector.NewOffHeapVecWithType(types.T_uint64.ToType())  // tableID
	bat.Vecs[2] = vector.NewOffHeapVecWithType(types.T_uint64.ToType())  // rowCount of s3 blocks
	bat.Vecs[3] = vector.NewOffHeapVecWithType(types.T_varchar.ToType()) // name for delete. empty for insert
	bat.Vecs[4] = vector.NewOffHeapVecWithType(types.T_text.ToType())    // originBatch.MarshalBinary()
	return bat
}

func appendCfgToWriter(
	writer *s3WriterDelegate,
	tableDef *plan.TableDef,
) {

	seqnums, _, _, sortIdx, isPrimaryKey := colexec.GetSequmsAttrsSortKeyIdxFromTableDef(tableDef)

	thisIdx := len(writer.sortIndexes)
	writer.seqnums = append(writer.seqnums, seqnums)
	writer.sortIndexes = append(writer.sortIndexes, sortIdx)

	if isPrimaryKey {
		writer.pkIndexes = append(writer.pkIndexes, sortIdx)
	} else {
		writer.pkIndexes = append(writer.pkIndexes, -1)
	}

	writer.schemaVersions = append(writer.schemaVersions, tableDef.Version)
	writer.isClusterBys = append(writer.isClusterBys, tableDef.ClusterBy != nil)
	writer.deleteBlockMap[thisIdx] = make(map[types.Blockid]*deleteBlockData, 1)
	writer.deleteBlockInfo[thisIdx] = nil
	writer.insertBlockInfo[thisIdx] = nil
	writer.insertBlockRowCount[thisIdx] = 0
}

func resetMergeBlockForOldCN(proc *process.Process, bat *batch.Batch) error {
	if bat.Attrs[len(bat.Attrs)-1] != catalog.ObjectMeta_ObjectStats {
		// bat comes from old CN, no object stats vec in it
		bat.Attrs = append(bat.Attrs, catalog.ObjectMeta_ObjectStats)
		bat.Vecs = append(bat.Vecs, vector.NewOffHeapVecWithType(types.T_binary.ToType()))

		blkVec := bat.Vecs[0]
		destVec := bat.Vecs[1]
		fs, err := fileservice.Get[fileservice.FileService](proc.Base.FileService, defines.SharedFileServiceName)
		if err != nil {
			logutil.Error("get fs failed when split object stats. ", zap.Error(err))
			return err
		}
		// var objDataMeta objectio.ObjectDataMeta
		var objStats objectio.ObjectStats
		for idx := 0; idx < bat.RowCount(); idx++ {
			// DecodeBlockInfo from an owned copy: blkVec rows alias the
			// incoming batch's (possibly off-heap) memory, which is
			// released when its pipeline scope ends, while the meta load
			// below caches under keys derived from the object-name bytes
			// for the entry's whole lifetime.
			blkInfo := objectio.DecodeBlockInfo(append([]byte(nil), blkVec.GetBytesAt(idx)...))
			objStats, _, err = disttae.ConstructObjStatsByLoadObjMeta(proc.Ctx, blkInfo.MetaLocation(), fs)
			if err != nil {
				return err
			}
			vector.AppendBytes(destVec, objStats.Marshal(), false, proc.GetMPool())
		}

		vector.AppendBytes(destVec, objStats.Marshal(), false, proc.GetMPool())
	}
	return nil
}
