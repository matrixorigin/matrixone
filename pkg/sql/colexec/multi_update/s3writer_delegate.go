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
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/catalog"
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

	action   actionType
	isRemote bool

	updateCtxs     []*MultiUpdateCtx
	updateCtxInfos map[string]*updateCtxInfo
	seqnums        [][]uint16
	sortIndexes    []int
	pkIndexes      []int
	schemaVersions []uint32
	isClusterBys   []bool

	deleteBlockMap []map[types.Blockid]*deleteBlockData
	// Each retained delete source owns its future split/dedup/merge growth.
	// Credit remains live until preparation and synchronous serialization have
	// both retired the source and all intermediate split/merge buffers.
	deleteFutureBytes   []int64
	deleteKeys          []*deleteKeySet
	deleteBlockInfo     []*deleteBlockInfo
	insertBlockInfo     []*batch.Batch
	insertBlockRowCount []uint64

	// per-table batch free lists that persist across sortAndSync calls,
	// so pre-allocated vector capacity is reused instead of re-allocated.
	insertFreeLists []*containers.BatchFreeList

	outputBat        *batch.Batch
	ownedMP          *mpool.MPool
	outputMP         *mpool.MPool
	metadataSpool    *os.File
	metadataWritten  int64
	metadataRead     int64
	metadataPass     uint8
	tailFlushed      bool
	outputLease      *rscthrottler.InputLease
	outputFrameLimit int64
	activeWorkspace  *rscthrottler.WorkspaceLease
	activeMP         *mpool.MPool

	batchSize      uint64
	flushThreshold uint64

	buf bytes.Buffer

	memController struct {
		mu          sync.Mutex
		mutations   int
		grantedSize int64
		throttler   rscthrottler.RSCThrottler
	}
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
		deleteFutureBytes:   make([]int64, tableCount),
		deleteKeys:          make([]*deleteKeySet, tableCount),
		insertFreeLists:     make([]*containers.BatchFreeList, tableCount),
		isRemote:            update.IsRemote,
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

	mainIdx := 0
	for i, updateCtx := range update.MultiUpdateCtx {
		if update.ctr.updateCtxInfos[updateCtx.TableDef.Name].tableType == UpdateMainTable {
			mainIdx = i
		}

		if !faultInjected {
			faultInjected, _ = objectio.LogCNFlushSmallObjsInjected(
				updateCtx.TableDef.DbName, updateCtx.TableDef.Name,
			)
		}

		appendCfgToWriter(writer, updateCtx.TableDef)
	}

	writer.updateCtxs = update.MultiUpdateCtx

	threshold := InsertWriteS3Threshold
	if faultInjected {
		threshold = colexec.FaultInjectedS3Threshold
	}

	upCtx := writer.updateCtxs[mainIdx]
	if len(upCtx.DeleteCols) > 0 && len(upCtx.InsertCols) > 0 {
		//update
		writer.action = actionUpdate
		writer.flushThreshold = threshold
	} else if len(upCtx.InsertCols) > 0 {
		//insert
		writer.action = actionInsert
		writer.flushThreshold = threshold
	} else {
		//delete
		writer.action = actionDelete
		writer.flushThreshold = DeleteWriteS3Threshold
	}

	return writer, nil
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
		unit := rscthrottler.BatchBytes(writer.memController.throttler)
		if proc.Base.ImportMemory != nil {
			unit = min(unit, proc.Base.ImportMemory.UnitBytes)
		}
		opts := []ioutil.SinkerOption{
			ioutil.WithBuffer(writer.insertFreeLists[i], false),
			ioutil.WithBufferBytes(unit),
			ioutil.WithRetainedRelease(writer.sourceReleased),
		}
		if proc.Base.ImportMemory != nil {
			opts = append(opts, ioutil.WithMemoryWorkspace(proc.Base.ImportMemory.WorkingSet))
		}
		if v, ok := proc.Ctx.Value(ioutil.PipelineFlushKey).(bool); ok && v {
			opts = append(opts, ioutil.WithPipelineFlush())
		}
		writer.insertSinkers[i] = colexec.NewCNS3DataWriter(
			writer.memoryPool(proc), fs, updateCtx.TableDef, -1, false,
			opts...)
	}
	return nil
}

func (writer *s3WriterDelegate) cleanDeleteBatches(mp *mpool.MPool) {
	clear(writer.deleteFutureBytes)
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
}

func (writer *s3WriterDelegate) releaseMemory() {
	if size := writer.memController.grantedSize; size > 0 {
		writer.memController.throttler.Release(size)
		writer.memController.grantedSize = 0
	}
}

// Retire all private insert buffers before returning their admission grants.
// The byte-bounded global arena pool may still reuse their writer scratch.
func (writer *s3WriterDelegate) closeInsertSinkers(mp *mpool.MPool) (err error) {
	for i, s3w := range writer.insertSinkers {
		if s3w != nil {
			err = errors.Join(err, s3w.Close())
			writer.insertSinkers[i] = nil
		}
	}
	for _, fl := range writer.insertFreeLists {
		if fl != nil {
			fl.Close(mp)
		}
	}
	return
}

func (writer *s3WriterDelegate) ensureOutputSlot(proc *process.Process) error {
	if writer.outputLease != nil || proc.Base.ImportMemory == nil {
		return nil
	}
	lease, err := proc.Base.ImportMemory.AcquireOutput(proc.Ctx)
	if err != nil {
		return err
	}
	if lease == nil {
		return fmt.Errorf("admitted multi-update has no output slot")
	}
	limit := lease.Capacity() / 4
	if InsertOnlyOutput(writer.updateCtxs) {
		unit := min(proc.Base.ImportMemory.UnitBytes, rscthrottler.BatchBytes(writer.memController.throttler))
		metadata, boundErr := colexec.InsertMetadataFrameLimit(unit, len(writer.updateCtxs))
		if boundErr != nil {
			lease.Release()
			return boundErr
		}
		limit = min(limit, metadata)
	}
	writer.outputLease = lease
	writer.outputFrameLimit = limit
	return nil
}
func (writer *s3WriterDelegate) releaseOutputSlot() {
	if writer.outputLease != nil {
		writer.outputLease.Release()
		writer.outputLease = nil
		writer.outputFrameLimit = 0
	}
}

func (writer *s3WriterDelegate) metadataPool() *mpool.MPool {
	if writer.outputMP == nil {
		writer.outputMP = mpool.MustNew("multi-update-output")
	}
	return writer.outputMP
}

func (writer *s3WriterDelegate) memoryPool(proc *process.Process) *mpool.MPool {
	if writer.activeMP != nil {
		return writer.activeMP
	}
	if writer.ownedMP == nil {
		writer.ownedMP = mpool.MustNew("multi-update-writer")
	}
	return writer.ownedMP
}

// retainedBytes counts D-owned vector capacity and committed-block maps.
// Output and IO use separate pools; transaction-local delete dedup lives on disk.
func (writer *s3WriterDelegate) retainedBytes() int64 {
	var size int64
	if writer.ownedMP != nil {
		size = writer.ownedMP.AllocatedBytes()
	}
	for _, sinker := range writer.insertSinkers {
		if sinker != nil {
			size += sinker.RetainedBitmapBytes()
		}
	}
	for _, blocks := range writer.deleteBlockMap {
		// Include map growth slack and per-entry Go objects in addition to the
		// separately tracked vectors. These maps are discarded after a flush;
		// no empty high-water map remains behind a released grant.
		for _, block := range blocks {
			if block != nil {
				size += int64(nulls.Allocated(block.bitmap)) + deleteBlockObjectsBytes
				size += batchBitmapBytes(block.bat)
			}
		}
	}
	for _, batches := range writer.deleteBatches {
		if batches != nil {
			for i := 0; i < batches.Length(); i++ {
				size += batchBitmapBytes(batches.Get(i))
			}
		}
	}
	return size
}

func batchBitmapBytes(bat *batch.Batch) int64 {
	var size int64
	if bat != nil {
		for _, vec := range bat.Vecs {
			size += int64(nulls.Allocated(vec.GetNulls()) + nulls.Allocated(vec.GetGrouping()))
		}
	}
	return size
}

func (writer *s3WriterDelegate) chargedBytes() int64 {
	size := writer.retainedBytes()
	for _, future := range writer.deleteFutureBytes {
		size = addMemoryBytes(size, future)
	}
	return size
}

func (writer *s3WriterDelegate) beginMemoryMutation() {
	writer.memController.mu.Lock()
	writer.memController.mutations++
	writer.memController.mu.Unlock()
}
func (writer *s3WriterDelegate) endMemoryMutation() {
	writer.memController.mu.Lock()
	writer.memController.mutations--
	if writer.memController.mutations == 0 {
		writer.reconcileMemoryLocked()
	}
	writer.memController.mu.Unlock()
}
func (writer *s3WriterDelegate) sourceReleased() {
	writer.memController.mu.Lock()
	defer writer.memController.mu.Unlock()
	// A main operation may have reserved a complete new N that has not been
	// materialized yet. Never treat that future allocation as unused credit.
	if writer.memController.mutations == 0 {
		writer.reconcileMemoryLocked()
	}
}
func (writer *s3WriterDelegate) reconcileMemoryLocked() {
	if excess := writer.memController.grantedSize - writer.chargedBytes(); excess > 0 {
		writer.memController.throttler.Release(excess)
		writer.memController.grantedSize -= excess
	}
}
func (writer *s3WriterDelegate) reconcileMemory() {
	writer.memController.mu.Lock()
	defer writer.memController.mu.Unlock()
	writer.reconcileMemoryLocked()
}

var errBorrowedSpill = errors.New("use admitted IO workspace")

func (writer *s3WriterDelegate) acquireMemory(proc *process.Process, analyzer process.Analyzer, size int64, recompute func() int64) error {
	if err := context.Cause(proc.Ctx); err != nil {
		return err
	}
	if size <= 0 {
		return nil
	}
	if _, ok := writer.memController.throttler.Acquire(size); ok {
		writer.memController.grantedSize += size
		return nil
	}

	// Retire every destructible owner before waiting. Uncommitted delete
	// batches and metadata remain charged until they are actually destroyed.
	if err := writer.sortAndSync(proc, analyzer); err != nil {
		return err
	}
	if err := writer.flushInsertSinkers(proc, analyzer); err != nil {
		return err
	}
	if err := writer.closeInsertSinkers(writer.memoryPool(proc)); err != nil {
		return err
	}
	if err := writer.spoolMetadata(proc, false); err != nil {
		return err
	}
	writer.reconcileMemory()
	if recompute != nil {
		size = recompute()
	}
	if size <= 0 {
		return nil
	}
	objectio.ReleaseIdleArenas()
	if _, ok := writer.memController.throttler.Acquire(size); ok {
		writer.memController.grantedSize += size
		return nil
	}
	if proc.Base.ImportMemory != nil {
		return errBorrowedSpill
	}
	// The coordinator coalesces process sampling and wakes from Release or
	// reclaim. This path has no per-writer timer or periodic ForceRefresh.
	if err := rscthrottler.AcquireContext(proc.Ctx, writer.memController.throttler, size); err != nil {
		return err
	}
	writer.memController.grantedSize += size
	return nil
}

// projectionBytes counts every materialized projection and only the payload
// referenced by this window. Vector.Window shares the entire source area;
// using its Size would repeatedly count unrelated rows.
func (writer *s3WriterDelegate) projectionBytes(bat *batch.Batch, start, end int) int64 {
	var size int64
	for _, c := range writer.updateCtxs {
		for _, idx := range c.InsertCols {
			size += bat.Vecs[idx].WindowSize(start, end)
		}
		if len(c.DeleteCols) > 0 && bat.Vecs[c.DeleteCols[0]].GetNulls().Count() == bat.RowCount() {
			continue
		}
		for _, idx := range c.DeleteCols {
			size += bat.Vecs[idx].WindowSize(start, end)
		}
	}
	return size
}

func (writer *s3WriterDelegate) windowEnd(bat *batch.Batch, start int, unit int64) int {
	last := min(bat.RowCount(), start+objectio.BlockMaxRows)
	if writer.windowFootprint(bat, start, last) <= unit {
		return last
	}
	lo, hi := start+1, last
	for lo < hi {
		mid := lo + (hi-lo+1)/2
		if writer.windowFootprint(bat, start, mid) <= unit {
			lo = mid
		} else {
			hi = mid - 1
		}
	}
	return lo
}

func (writer *s3WriterDelegate) append(proc *process.Process, analyzer process.Analyzer, inBatch *batch.Batch) error {
	writer.beginMemoryMutation()
	defer writer.endMemoryMutation()
	unit := rscthrottler.BatchBytes(writer.memController.throttler)
	if proc.Base.ImportMemory != nil {
		unit = min(unit, proc.Base.ImportMemory.UnitBytes)
	}
	if writer.flushThreshold > uint64(unit) {
		writer.flushThreshold = uint64(unit)
	}
	if unit <= 0 {
		unit = 64 << 20
	}
	for start := 0; start < inBatch.RowCount(); {
		end := writer.windowEnd(inBatch, start, unit)
		window, err := inBatch.Window(start, end)
		if err != nil {
			return err
		}
		err = writer.appendWindow(proc, analyzer, window)
		window.Clean(writer.memoryPool(proc))
		if err != nil {
			return err
		}
		start = end
	}
	return nil
}

func (writer *s3WriterDelegate) appendAllocation(inBatch *batch.Batch, unit int64) int64 {
	var increment int64
	for i, updateCtx := range writer.updateCtxs {
		if len(updateCtx.InsertCols) > 0 {
			proj := inBatch.SelectColumns(updateCtx.InsertCols, nil)
			if i < len(writer.insertSinkers) && writer.insertSinkers[i] != nil {
				increment += writer.insertSinkers[i].AppendAllocation(proj)
			} else {
				var buffers containers.IBatchBuffer
				if i < len(writer.insertFreeLists) {
					buffers = writer.insertFreeLists[i]
				}
				increment += ioutil.SinkerAppendAllocation(proj, nil, buffers, unit)
			}
			// Constraint filtering clones a projection only when nulls exist.
			// Reserve the complete new capacity before that clone is made.
			tableType := UpdateMainTable
			if updateCtx.TableDef != nil {
				if info := writer.updateCtxInfos[updateCtx.TableDef.Name]; info != nil {
					tableType = info.tableType
				}
			}
			clone := tableType == UpdateMainTable && updateCtx.SkipInsertOnNullPk && updateCtx.InsertPkColIdx >= 0 && updateCtx.InsertPkColIdx < len(updateCtx.InsertCols) && inBatch.Vecs[updateCtx.InsertCols[updateCtx.InsertPkColIdx]].HasNull()
			if tableType != UpdateMainTable && i < len(writer.sortIndexes) && writer.sortIndexes[i] >= 0 && !writer.isClusterBys[i] {
				clone = inBatch.Vecs[updateCtx.InsertCols[writer.sortIndexes[i]]].HasNull()
			}
			if clone {
				for _, j := range updateCtx.InsertCols {
					tmp := vector.NewOffHeapVecWithType(*inBatch.Vecs[j].GetType())
					increment += tmp.AppendWindowAllocation(inBatch.Vecs[j], 0, inBatch.RowCount())
				}
			}
		}
		if len(updateCtx.DeleteCols) > 0 {
			rowids := inBatch.Vecs[updateCtx.DeleteCols[0]]
			if rowids.GetNulls().Count() == inBatch.RowCount() {
				continue
			}
			increment = addMemoryBytes(increment, writer.deleteAppendAllocation(i, inBatch, updateCtx.DeleteCols))
			increment = addMemoryBytes(increment, deletePreparationBytes(inBatch, 0, inBatch.RowCount(), updateCtx.DeleteCols))
		}
	}
	return increment
}

func (writer *s3WriterDelegate) appendWindow(
	proc *process.Process,
	analyzer process.Analyzer,
	inBatch *batch.Batch,
) (err error) {
	defer writer.reconcileMemory()
	// Reserve every projection before Write/Extend can copy it, including a
	// single wide batch that already exceeds the normal flush threshold.
	unit := rscthrottler.BatchBytes(writer.memController.throttler)
	if proc.Base.ImportMemory != nil {
		unit = min(unit, proc.Base.ImportMemory.UnitBytes)
	}
	// Keep the retained aggregate below one shared byte unit. Flush before a
	// crossing window so W's bound need not include two complete units.
	windowBytes := uint64(writer.windowFootprint(inBatch, 0, inBatch.RowCount()))
	if writer.batchSize > 0 && writer.batchSize+windowBytes > uint64(unit) {
		if err = writer.sortAndSync(proc, analyzer); err != nil {
			return err
		}
		if err = writer.flushInsertSinkers(proc, analyzer); err != nil {
			return err
		}
		if err = writer.closeInsertSinkers(writer.memoryPool(proc)); err != nil {
			return err
		}
		if err = writer.spoolMetadata(proc, false); err != nil {
			return err
		}
		writer.reconcileMemory()
	}
	recompute := func() int64 { return writer.appendAllocation(inBatch, unit) }
	increment := recompute()

	direct := false
	if err = writer.acquireMemory(proc, analyzer, increment, recompute); err != nil {
		if !errors.Is(err, errBorrowedSpill) {
			return err
		}
		direct = true
		err = nil
	}
	var ioLease *rscthrottler.WorkspaceLease
	if direct {
		if err = writer.ensureOutputSlot(proc); err != nil {
			return err
		}
		ioLease, err = proc.Base.ImportMemory.AcquireIO(proc.Ctx)
		if err != nil {
			return err
		}
		writer.activeWorkspace = ioLease
		ioMP := mpool.MustNew("multi-update-io")
		writer.activeMP = ioMP
		defer func() {
			if err != nil {
				err = errors.Join(err, writer.free(proc))
			} else {
				err = errors.Join(err, writer.closeInsertSinkers(ioMP))
			}
			writer.activeMP = nil
			mpool.DeleteMPool(ioMP)
			writer.activeWorkspace = nil
			ioLease.Release()
		}()
	}

	if err = writer.ensureInsertSinkers(proc); err != nil {
		return
	}

	mp := writer.memoryPool(proc)
	writeProjection := func(w *colexec.CNS3Writer, b *batch.Batch) error {
		if direct {
			return w.WriteBorrowedToFile(proc.Ctx, b, ioLease)
		}
		return w.Write(proc.Ctx, b)
	}

	// Route insert columns directly to per-table sinkers (no clone).
	// Auto-spill S3 writes during Write use proc.Ctx; perfcounter tracking
	// is deferred to the final Sync in flushTailAndWriteToOutput.
	for i, updateCtx := range writer.updateCtxs {
		if len(updateCtx.InsertCols) == 0 || writer.insertSinkers[i] == nil {
			continue
		}
		insertAttrs := writer.updateCtxInfos[updateCtx.TableDef.Name].insertAttrs
		projBat := inBatch.SelectColumns(updateCtx.InsertCols, insertAttrs)

		tableType := writer.updateCtxInfos[updateCtx.TableDef.Name].tableType

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
				if checked, err = projBat.Clone(mp, false); err != nil {
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
						if inBatch.Vecs[inputIdx].HasNull() {
							return moerr.NewConstraintViolation(proc.Ctx, fmt.Sprintf("Column '%s' cannot be null", col.Name))
						}
					}
				}
			}
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
			if filtered, err = projBat.Clone(mp, false); err != nil {
				return
			}
			nullIdx := writer.sortIndexes[i]
			if tableType == UpdateMainTable {
				nullIdx = mainTablePkProjectIdx
			}
			nulls := filtered.Vecs[nullIdx].GetNulls().GetBitmap().Clone()
			filtered.ShrinkByMask(nulls, true, 0)
			if filtered.RowCount() > 0 {
				err = writeProjection(writer.insertSinkers[i], filtered)
			}
			filtered.Clean(mp)
			if err != nil {
				return
			}
			continue
		}

		if err = writeProjection(writer.insertSinkers[i], projBat); err != nil {
			return
		}
	}

	// Accumulate delete columns in per-table BatchSets (2 cols only).
	for i, updateCtx := range writer.updateCtxs {
		if len(updateCtx.DeleteCols) == 0 || inBatch.Vecs[updateCtx.DeleteCols[0]].GetNulls().Count() == inBatch.RowCount() {
			continue
		}
		if writer.deleteBatches[i] == nil {
			writer.deleteBatches[i] = batch.NewBatchSet(objectio.BlockMaxRows)
		}
		if !direct {
			if len(writer.deleteFutureBytes) < len(writer.updateCtxs) {
				writer.deleteFutureBytes = append(writer.deleteFutureBytes, make([]int64, len(writer.updateCtxs)-len(writer.deleteFutureBytes))...)
			}
			writer.deleteFutureBytes[i] = addMemoryBytes(writer.deleteFutureBytes[i], deletePreparationBytes(inBatch, 0, inBatch.RowCount(), updateCtx.DeleteCols))
		}
		projBat := inBatch.SelectColumns(updateCtx.DeleteCols, DeleteBatchAttrs)
		if _, err = writer.deleteBatches[i].Extend(mp, projBat, nil); err != nil {
			return
		}
		if direct {
			// W is sized for the largest individual projection/singleton.
			// Several wide index keys must not coexist as cloned delete
			// sources. Serialize this table before materializing the next;
			// retain the already acquired O slot until all metadata is spooled.
			if err = writer.prepareAndSyncDeleteTable(proc, analyzer, i); err != nil {
				return
			}
			writer.deleteBatches[i] = nil
		}
	}

	writer.batchSize += windowBytes

	if direct || writer.batchSize >= writer.flushThreshold {
		if err = writer.sortAndSync(proc, analyzer); err != nil {
			return err
		}
		if err = writer.flushInsertSinkers(proc, analyzer); err != nil {
			return err
		}
		if err = writer.closeInsertSinkers(mp); err != nil {
			return err
		}
		return writer.spoolMetadata(proc, false)
	}

	return
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
	writer.beginMemoryMutation()
	defer writer.endMemoryMutation()
	defer func() {
		if needClean {
			for _, bat := range src {
				bat.Clean(writer.memoryPool(proc))
			}
		}
	}()

	// split delete batches by BlockID
	if writer.deleteBlockMap[idx] == nil {
		writer.deleteBlockMap[idx] = make(map[types.Blockid]*deleteBlockData)
	}
	blockMap := writer.deleteBlockMap[idx]

	var uncommitted *batch.Batch
	defer func() {
		if uncommitted != nil {
			uncommitted.Clean(writer.memoryPool(proc))
		}
	}()
	if len(writer.deleteKeys) < len(writer.deleteBlockMap) {
		writer.deleteKeys = make([]*deleteKeySet, len(writer.deleteBlockMap))
	}

	for _, bat := range src {
		rowIDVec := bat.GetVector(RowIDIdx)
		nulls := rowIDVec.GetNulls()
		if nulls.Count() == bat.RowCount() {
			continue
		}

		rowIDs := vector.MustFixedColWithTypeCheck[types.Rowid](rowIDVec)
		for i, rowID := range rowIDs {
			if err := context.Cause(proc.Ctx); err != nil {
				return nil, err
			}
			if nulls.Contains(uint64(i)) {
				continue
			}

			blkid := rowID.CloneBlockID()
			segid := rowID.CloneSegmentID()
			// blkOffset := rowID.GetBlockOffset()
			rowOffset := rowID.GetRowOffset()

			tableID := uint64(0)
			var txnID []byte
			if idx < len(writer.updateCtxs) && writer.updateCtxs[idx] != nil && writer.updateCtxs[idx].TableDef != nil {
				tableID = writer.updateCtxs[idx].TableDef.TblId
			}
			if op := proc.GetTxnOperator(); op != nil {
				txnID = op.Txn().ID
			}
			if colexec.IsDeletionOnTxnUnCommit(writer.segmentMap, &segid, tableID, txnID) {
				if writer.deleteKeys[idx] == nil {
					writer.deleteKeys[idx] = &deleteKeySet{}
				}
				fresh, keyErr := writer.deleteKeys[idx].add(proc.Ctx, rowID)
				if keyErr != nil {
					return nil, keyErr
				}
				if !fresh {
					continue
				}
				if uncommitted == nil {
					uncommitted = newDeleteBatch(bat, PkIdx)
				}
				if e := vector.AppendFixed(uncommitted.Vecs[RowIDIdx], rowID, false, writer.memoryPool(proc)); e != nil {
					return nil, e
				}
				if e := uncommitted.Vecs[PkIdx].UnionOne(bat.Vecs[PkIdx], int64(i), writer.memoryPool(proc)); e != nil {
					return nil, e
				}
				uncommitted.SetRowCount(uncommitted.Vecs[0].Length())
				continue
			}

			if blockMap[blkid] == nil {
				blockMap[blkid] = newDeleteBlockData(bat, 1)

			}

			block := blockMap[blkid]

			bitmap := block.bitmap
			if bitmap.Contains(uint64(rowOffset)) {
				continue
			} else {
				bitmap.Add(uint64(rowOffset))
			}

			if e := vector.AppendFixed(block.bat.GetVector(RowIDIdx), rowID, false, writer.memoryPool(proc)); e != nil {
				return nil, e
			}
			if e := block.bat.GetVector(PkIdx).UnionOne(bat.GetVector(PkIdx), int64(i), writer.memoryPool(proc)); e != nil {
				return nil, e
			}
			block.bat.SetRowCount(block.bat.Vecs[0].Length())
		}
	}

	if uncommitted != nil && uncommitted.RowCount() > 0 {
		if e := writer.spoolFrame(proc, actionDelete, idx, uint64(uncommitted.RowCount()), objectio.PhysicalAddr_Attr, uncommitted); e != nil {
			return nil, e
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
	for _, blkid := range blkids {
		bat := blockMap[blkid].bat
		delete(blockMap, blkid)
		err := deleteBats.Push(writer.memoryPool(proc), bat)
		if err != nil {
			deleteBats.Clean(writer.memoryPool(proc))
			return nil, err
		}
	}
	if len(blockMap) == 0 {
		writer.deleteBlockMap[idx] = nil
	}
	retBatchs := deleteBats.TakeBatches()
	return retBatchs, nil
}

func (writer *s3WriterDelegate) sortAndSync(proc *process.Process, analyzer process.Analyzer) (err error) {
	if err = writer.ensureOutputSlot(proc); err != nil {
		return err
	}

	defer func() {
		writer.cleanDeleteBatches(writer.memoryPool(proc))
	}()

	for i, updateCtx := range writer.updateCtxs {
		if len(updateCtx.DeleteCols) == 0 || writer.deleteBatches[i] == nil {
			continue
		}
		if err = writer.prepareAndSyncDeleteTable(proc, analyzer, i); err != nil {
			return
		}
		// Insert data is managed by persistent sinkers (auto-spill via Write).
	}

	return
}

func (writer *s3WriterDelegate) prepareAndSyncDeleteTable(proc *process.Process, analyzer process.Analyzer, idx int) error {
	src := writer.deleteBatches[idx].TakeBatches()
	prepared, err := writer.prepareDeleteBatches(proc, idx, src, true)
	if err != nil {
		return err
	}
	return writer.sortAndSyncOneTable(proc, writer.updateCtxs[idx].TableDef, analyzer, idx, true, prepared, true)
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
	if err = writer.ensureOutputSlot(proc); err != nil {
		return err
	}

	defer func() {
		if cleanBatchAfterUse {
			for i, bat := range bats {
				if bat != nil {
					bat.Clean(writer.memoryPool(proc))
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

	unit := rscthrottler.BatchBytes(writer.memController.throttler)
	if proc.Base.ImportMemory != nil {
		unit = min(unit, proc.Base.ImportMemory.UnitBytes)
	}
	opts = append(opts, ioutil.WithBufferBytes(unit))
	if proc.Base.ImportMemory != nil && writer.activeWorkspace == nil {
		opts = append(opts, ioutil.WithMemoryWorkspace(proc.Base.ImportMemory.WorkingSet))
	}

	if isTombstone {
		pkCol := plan2.PkColByTableDef(tblDef)
		s3Writer = colexec.NewCNS3TombstoneWriter(
			writer.memoryPool(proc), fs, plan2.ExprType2Type(&pkCol.Typ), -1, opts...,
		)
	} else {
		s3Writer = colexec.NewCNS3DataWriter(writer.memoryPool(proc), fs, tblDef, -1, false, opts...)
	}

	defer s3Writer.Close()

	counterSet := analyzer.GetOpCounterSet()
	writeCtx := perfcounter.AttachS3RequestKey(proc.Ctx, counterSet)

	for i := range bats {
		rowCount += bats[i].RowCount()
		// When cleanBatchAfterUse is true, the batch is exclusively owned
		// (cloned), so we can transfer it to the sinker without copying.
		if cleanBatchAfterUse {
			owned, writeErr := s3Writer.WriteOwned(writeCtx, bats[i])
			if writeErr != nil {
				err = writeErr
				return
			}
			if owned {
				bats[i] = nil
				continue
			}
			// WriteOwned returned false: data was already copied inside
			// WriteOwned via Write(), so skip the Write() call below.
			bats[i].Clean(writer.memoryPool(proc))
			bats[i] = nil
			continue
		}
		if err = s3Writer.Write(writeCtx, bats[i]); err != nil {
			return
		}

		if cleanBatchAfterUse {
			bats[i].Clean(writer.memoryPool(proc))
		}
		bats[i] = nil
	}

	if _, err = s3Writer.Sync(writeCtx); err != nil {
		return
	}

	analyzer.AddS3RequestCount(counterSet)
	analyzer.AddFileServiceCacheInfo(counterSet)
	analyzer.AddDiskIO(counterSet)

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

	return appendVarlenaBatch(info.bat, bat, writer.memoryPool(proc))
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

	return appendVarlenaBatch(writer.insertBlockInfo[idx], bat, writer.memoryPool(proc))
}

func (writer *s3WriterDelegate) flushInsertSinkers(proc *process.Process, analyzer process.Analyzer) (err error) {
	if err = writer.ensureOutputSlot(proc); err != nil {
		return err
	}
	counterSet := analyzer.GetOpCounterSet()
	writeCtx := perfcounter.AttachS3RequestKey(proc.Ctx, counterSet)

	// Sync all insert sinkers — flushes remaining in-memory data to S3.
	for i, s3w := range writer.insertSinkers {
		if s3w == nil {
			continue
		}
		stats, syncErr := s3w.Sync(writeCtx)
		if syncErr != nil {
			return syncErr
		}
		if len(stats) == 0 {
			continue
		}
		blockInfoBat, fillErr := s3w.SyncAndFillBlockInfoBat(writeCtx)
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
	analyzer.AddS3RequestCount(counterSet)
	analyzer.AddFileServiceCacheInfo(counterSet)
	analyzer.AddDiskIO(counterSet)
	return nil
}

func (writer *s3WriterDelegate) flushTailAndWriteToOutput(proc *process.Process, analyzer process.Analyzer) error {
	writer.beginMemoryMutation()
	defer writer.endMemoryMutation()
	if !writer.tailFlushed {
		if err := writer.flushInsertSinkers(proc, analyzer); err != nil {
			return err
		}
		if err := writer.sortAndSync(proc, analyzer); err != nil {
			return err
		}
		if err := writer.closeInsertSinkers(writer.memoryPool(proc)); err != nil {
			return err
		}
		if err := writer.spoolMetadata(proc, true); err != nil {
			return err
		}
		writer.tailFlushed = true
		writer.reconcileMemory()
	}
	return writer.readMetadataFrame(proc)
}

// Object metadata is transaction output. Spool it while building so a long
// import does not retain every object's encoded metadata until Eval. A frame
// becomes visible only after its complete write; failed frames are truncated.
func (writer *s3WriterDelegate) spoolFrame(proc *process.Process, action actionType, idx int, rows uint64, name string, src *batch.Batch) error {
	if err := writer.ensureOutputSlot(proc); err != nil {
		return err
	}
	// The protocol envelope reserves four copies plus fixed metadata
	// overhead. Reject an impossible frame before serializing its
	// borrowed source or growing the output vector.
	if err := writer.checkMetadataFrameSize(src.MarshalBinarySize() + int64(len(name)) + 4096); err != nil {
		return err
	}
	mp := writer.metadataPool()
	if writer.outputBat != nil {
		writer.outputBat.Clean(mp)
	}
	writer.outputBat = makeS3OutputBatch()
	if err := writer.addBatchToOutput(mp, action, idx, rows, name, src); err != nil {
		return err
	}
	if writer.metadataSpool == nil {
		f, err := os.CreateTemp("", "mo-multi-update-*.spool")
		if err != nil {
			return err
		}
		if err = os.Remove(f.Name()); err != nil {
			f.Close()
			return err
		}
		writer.metadataSpool = f
	}
	start := writer.metadataWritten
	if _, err := writer.metadataSpool.Seek(start, io.SeekStart); err != nil {
		return err
	}
	var head [9]byte
	n := writer.outputBat.MarshalBinarySize()
	if err := writer.checkMetadataFrameSize(n); err != nil {
		return err
	}
	binary.LittleEndian.PutUint64(head[:8], uint64(n))
	head[8] = byte(action)
	if _, err := writer.metadataSpool.Write(head[:]); err != nil {
		return err
	}
	if err := writer.outputBat.MarshalBinaryTo(writer.metadataSpool); err != nil {
		_ = writer.metadataSpool.Truncate(start)
		return err
	}
	writer.metadataWritten += 9 + n
	writer.outputBat.Clean(mp)
	writer.outputBat = nil
	// The serializer's old whole-frame scratch is not retained after a frame.
	writer.buf = bytes.Buffer{}
	return nil
}

func (writer *s3WriterDelegate) checkMetadataFrameSize(n int64) error {
	if n < 0 || writer.outputLease != nil && n > writer.outputFrameLimit {
		return fmt.Errorf("import metadata frame exceeds admitted output capacity: %d bytes", n)
	}
	return nil
}

func (writer *s3WriterDelegate) spoolMetadata(proc *process.Process, final bool) error {
	mp := writer.memoryPool(proc)
	for i, block := range writer.deleteBlockInfo {
		if block == nil || block.bat.RowCount() == 0 {
			continue
		}
		if err := writer.spoolFrame(proc, actionDelete, i, block.rawRowCount, block.name, block.bat); err != nil {
			return err
		}
		block.bat.Clean(mp)
		writer.deleteBlockInfo[i] = nil
	}
	if final {
		for i, blocks := range writer.deleteBlockMap {
			for key, block := range blocks {
				if block == nil || block.bat == nil {
					continue
				}
				if err := writer.spoolFrame(proc, actionDelete, i, uint64(block.bat.RowCount()), objectio.PhysicalAddr_Attr, block.bat); err != nil {
					return err
				}
				block.bat.Clean(mp)
				delete(blocks, key)
			}
			writer.deleteBlockMap[i] = nil
		}
	}
	for i, bat := range writer.insertBlockInfo {
		if bat == nil || bat.RowCount() == 0 {
			continue
		}
		if err := resetMergeBlockForOldCN(proc, bat); err != nil {
			return err
		}
		if err := writer.spoolFrame(proc, actionInsert, i, writer.insertBlockRowCount[i], "", bat); err != nil {
			return err
		}
		bat.Clean(mp)
		writer.insertBlockInfo[i] = nil
		writer.insertBlockRowCount[i] = 0
	}
	writer.releaseOutputSlot()
	return nil
}

func (writer *s3WriterDelegate) readMetadataFrame(proc *process.Process) error {
	mp := writer.metadataPool()
	if writer.outputBat != nil {
		writer.outputBat.Clean(mp)
		writer.outputBat = nil
	}
	writer.releaseOutputSlot()
	// Preserve MergeBlock's per-writer delete-before-insert order even when
	// metadata was spooled by many early flushes. The two passes read only
	// headers of skipped frames, rather than retaining frame offsets in RAM.
	for {
		if writer.metadataRead == writer.metadataWritten {
			if writer.metadataPass == 0 {
				writer.metadataPass = 1
				writer.metadataRead = 0
				continue
			}
			return nil
		}
		var head [9]byte
		if _, err := writer.metadataSpool.ReadAt(head[:], writer.metadataRead); err != nil {
			return err
		}
		n := binary.LittleEndian.Uint64(head[:8])
		if n > uint64(writer.metadataWritten-writer.metadataRead-9) {
			return io.ErrUnexpectedEOF
		}
		offset := writer.metadataRead + 9
		writer.metadataRead = offset + int64(n)
		action := actionType(head[8])
		if action != actionDelete && action != actionInsert {
			return fmt.Errorf("invalid import metadata action %d", action)
		}
		if writer.metadataPass == 0 && action != actionDelete || writer.metadataPass == 1 && action != actionInsert {
			continue
		}
		if err := writer.ensureOutputSlot(proc); err != nil {
			return err
		}
		if err := writer.checkMetadataFrameSize(int64(n)); err != nil {
			return err
		}
		writer.outputBat = batch.NewOffHeapEmpty()
		r := io.NewSectionReader(writer.metadataSpool, offset, int64(n))
		if err := writer.outputBat.UnmarshalFromReader(r, mp); err != nil {
			return err
		}
		return nil
	}
}

func (writer *s3WriterDelegate) closeDeleteKeys() error {
	var err error
	for i, keys := range writer.deleteKeys {
		err = errors.Join(err, keys.close())
		writer.deleteKeys[i] = nil
	}
	return err
}

func (writer *s3WriterDelegate) closeMetadata() error {
	var err error
	if writer.metadataSpool != nil {
		err = writer.metadataSpool.Close()
		writer.metadataSpool = nil
	}
	writer.metadataWritten = 0
	writer.metadataRead = 0
	writer.metadataPass = 0
	writer.tailFlushed = false
	return err
}

func (writer *s3WriterDelegate) reset(proc *process.Process) (err error) {
	writer.beginMemoryMutation()
	defer writer.endMemoryMutation()
	defer func() { err = errors.Join(err, writer.closeMetadata(), writer.closeDeleteKeys()) }()

	writer.cleanDeleteBatches(writer.memoryPool(proc))

	// A new execution must not inherit private buffers with released grants.
	// Normal append calls still share persistent sinkers within an execution.
	err = writer.closeInsertSinkers(writer.memoryPool(proc))

	for i, bat := range writer.insertBlockInfo {
		if bat != nil {
			bat.Clean(writer.memoryPool(proc))
			writer.insertBlockInfo[i] = nil
		}
	}
	for i := range writer.insertBlockRowCount {
		writer.insertBlockRowCount[i] = 0
	}
	for i, block := range writer.deleteBlockInfo {
		if block != nil {
			block.bat.Clean(writer.memoryPool(proc))
			writer.deleteBlockInfo[i] = nil
		}
	}
	for i, data := range writer.deleteBlockMap {
		for k, block := range data {
			if block != nil && block.bat != nil {
				block.bat.Clean(writer.memoryPool(proc))
			}
			delete(data, k)
		}
		writer.deleteBlockMap[i] = nil
	}

	if writer.outputBat != nil {
		writer.outputBat.Clean(writer.metadataPool())
		writer.outputBat = nil
	}

	writer.buf = bytes.Buffer{}
	writer.releaseOutputSlot()
	writer.reconcileMemory()
	return
}

func (writer *s3WriterDelegate) free(proc *process.Process) (err error) {
	writer.beginMemoryMutation()
	defer writer.endMemoryMutation()
	defer func() { err = errors.Join(err, writer.closeMetadata(), writer.closeDeleteKeys()) }()
	defer writer.releaseMemory()

	writer.cleanDeleteBatches(writer.memoryPool(proc))
	mp := writer.memoryPool(proc)

	err = writer.closeInsertSinkers(mp)
	writer.insertSinkers = nil

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
		writer.outputBat.Clean(writer.metadataPool())
		writer.outputBat = nil
	}
	writer.buf = bytes.Buffer{}
	writer.releaseOutputSlot()
	mpool.DeleteMPool(writer.ownedMP)
	writer.ownedMP = nil
	mpool.DeleteMPool(writer.outputMP)
	writer.outputMP = nil

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

func makeS3OutputBatch() *batch.Batch {
	bat := batch.NewOffHeapWithSize(5)
	bat.Vecs[0] = vector.NewOffHeapVecWithType(types.T_uint8.ToType())   // action type  0=actionInsert, 1=actionDelete
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
			blkInfo := objectio.DecodeBlockInfo(blkVec.GetBytesAt(idx))
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
