// Copyright 2023 Matrix Origin
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

package logtailreplay

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"time"

	goSort "sort"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/sort"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"

	"github.com/tidwall/btree"

	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/ckputil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
)

const (
	JTCDCLoad tasks.JobType = 300 + iota
)

const maxChangeObjectBlockCount uint32 = 1 << 16

func validateChangeCollectionInputs(
	ctx context.Context,
	start, end types.TS,
	maxRow uint32,
	primarySeqnum int,
	mp *mpool.MPool,
	fs fileservice.FileService,
) error {
	if ctx == nil || mp == nil || fs == nil {
		return moerr.NewInvalidInputNoCtx(
			"change collection requires context, mpool, and file service",
		)
	}
	if end.IsEmpty() || start.GT(&end) {
		return moerr.NewInvalidInputNoCtx("change collection has invalid timestamp range")
	}
	if maxRow == 0 {
		return moerr.NewInvalidInputNoCtx("change collection maximum row count is zero")
	}
	if primarySeqnum < -1 || primarySeqnum >= int(objectio.SEQNUM_UPPER) {
		return moerr.NewInvalidInputNoCtxf(
			"change collection has invalid primary sequence %d", primarySeqnum,
		)
	}
	return nil
}

func validateChangeObjectBlockCount(stats *objectio.ObjectStats) (int, error) {
	if stats == nil {
		return 0, moerr.NewInternalErrorNoCtx("change object has no statistics")
	}
	count := stats.BlkCnt()
	if count == 0 || count > maxChangeObjectBlockCount {
		return 0, moerr.NewInternalErrorNoCtxf(
			"invalid change object block count %d; expected 1..%d",
			count, maxChangeObjectBlockCount,
		)
	}
	return int(count), nil
}

// changeObjectIdentity returns the complete binary object identity. ShortString
// is deliberately only a logging abbreviation (the final 48 UUID bits plus
// object number) and must never be used as a map key for correctness state.
func changeObjectIdentity(obj *objectio.ObjectEntry) string {
	if obj == nil {
		return ""
	}
	return string(obj.ObjectShortName()[:])
}

func changePKFilterSegmentsValid(segments [][]byte) (valid bool) {
	if len(segments) == 0 {
		return true
	}
	defer func() {
		if recover() != nil {
			valid = false
		}
	}()
	var previous index.ZM
	for i, encoded := range segments {
		if len(encoded) != index.ZMSize {
			return false
		}
		segment := index.ZM(encoded)
		if !segment.IsInited() {
			return false
		}
		if intersects, ok := segment.Intersect(segment); !ok || !intersects {
			return false
		}
		if i > 0 {
			if segment.GetType() != previous.GetType() ||
				segment.GetScale() != previous.GetScale() {
				return false
			}
			// The binary overlap search requires strictly separated segments:
			// previous.max < segment.min.
			if overlaps, ok := previous.AnyGE(segment); !ok || overlaps {
				return false
			}
		}
		previous = segment
	}
	return true
}

// changeZoneMapDisjoint is a fail-open wrapper around PK pruning. Zone maps
// and filter segments are optimization metadata; malformed bytes must never
// suppress a real row or crash change collection.
func changeZoneMapDisjoint(zm objectio.ZoneMap, segments [][]byte) (disjoint bool) {
	if !zm.IsInited() || len(segments) == 0 {
		return false
	}
	defer func() {
		if recover() != nil {
			disjoint = false
		}
	}()
	return !index.AnySegmentOverlaps(zm, segments)
}

func changeBlockPKDisjoint(
	blk objectio.BlockObject,
	seqnum uint16,
	segments [][]byte,
) (disjoint bool) {
	defer func() {
		if recover() != nil {
			disjoint = false
		}
	}()
	if seqnum >= blk.GetMetaColumnCount() {
		return false
	}
	column := blk.ColumnMeta(seqnum)
	if column.DataType() == uint8(types.T_any) {
		return false
	}
	return changeZoneMapDisjoint(column.ZoneMap(), segments)
}

func changeObjectCreatedBy(snapshot, createTS types.TS) bool {
	return !createTS.Equal(&txnif.UncommitTS) && !createTS.GT(&snapshot)
}

func changeObjectDeletedBy(snapshot, deleteTS types.TS) bool {
	return !deleteTS.IsEmpty() && !deleteTS.Equal(&txnif.UncommitTS) &&
		deleteTS.LE(&snapshot)
}

var (
	_jobPool = sync.Pool{
		New: func() any {
			return new(tasks.Job)
		},
	}
)

func getJob(
	ctx context.Context,
	id string,
	typ tasks.JobType,
	exec tasks.JobExecutor) *tasks.Job {
	job := _jobPool.Get().(*tasks.Job)
	job.Init(ctx, id, typ, exec)
	return job
}

func putJob(job *tasks.Job) {
	job.Reset()
	_jobPool.Put(job)
}

const (
	ChangesHandle_Object uint8 = iota
	ChangesHandle_Row
)

const (
	RowHandle_DataBatchIDX uint8 = iota
	RowHandle_TombstoneBatchIDX
)

const (
	SmallBatchThreshold = objectio.BlockMaxRows
	CoarseMaxRow        = objectio.BlockMaxRows

	LoadParallism = 20
	LogThreshold  = time.Minute
)

type BatchHandle struct {
	rowOffsetCursor int
	mp              *mpool.MPool

	batches     *batch.Batch
	batchLength int
	ctx         context.Context

	baseHandle *baseHandle
	tombstone  bool
}

func batchesShareAppendSchema(dst, src *batch.Batch) bool {
	if dst == nil || src == nil {
		return true
	}
	if len(dst.Vecs) != len(src.Vecs) || len(dst.Attrs) != len(src.Attrs) {
		return false
	}
	for i := range dst.Attrs {
		if dst.Attrs[i] != src.Attrs[i] {
			return false
		}
	}
	for i := range dst.Vecs {
		if dst.Vecs[i] == nil || src.Vecs[i] == nil {
			if dst.Vecs[i] != src.Vecs[i] {
				return false
			}
			continue
		}
		if *dst.Vecs[i].GetType() != *src.Vecs[i].GetType() {
			return false
		}
	}
	return true
}

type changeAppendState struct {
	vec         *vector.Vector
	checkpoint  vector.AppendCheckpoint
	replacement bool
}

func cleanChangeAppendReplacements(states []changeAppendState, mp *mpool.MPool) {
	for i := range states {
		if states[i].replacement && states[i].vec != nil {
			states[i].vec.Free(mp)
			states[i].vec = nil
		}
	}
}

// appendChangeBatchWindow appends one contiguous source window atomically.
// A previous source can leave a const destination (notably commitTS); writing
// directly to it either panics or preserves only its original broadcast value.
// Materialize const destinations off to the side, then publish replacements
// only after every column append succeeds. Non-const destinations use vector
// append checkpoints so an allocation failure cannot leave columns misaligned.
func appendChangeBatchWindow(
	dst, src *batch.Batch,
	start, end int,
	mp *mpool.MPool,
) error {
	if dst == nil || src == nil || dst == src || len(dst.Vecs) == 0 ||
		!batchesShareAppendSchema(dst, src) {
		return moerr.NewInternalErrorNoCtx("cannot append incompatible change batches")
	}
	if dst.Vecs[0] == nil || src.Vecs[0] == nil {
		return moerr.NewInternalErrorNoCtx("change batch has a nil leading column")
	}
	if start < 0 || end < start {
		return moerr.NewInternalErrorNoCtxf(
			"invalid change batch window [%d,%d)", start, end,
		)
	}
	dstRows := dst.Vecs[0].Length()
	srcRows := src.Vecs[0].Length()
	if dst.RowCount() != dstRows || src.RowCount() != srcRows || end > srcRows {
		return moerr.NewInternalErrorNoCtx("change batch row count is inconsistent")
	}
	rows := end - start

	const inlineColumns = 16
	var inline [inlineColumns]changeAppendState
	states := inline[:]
	if len(dst.Vecs) <= inlineColumns {
		states = states[:len(dst.Vecs)]
	} else {
		states = make([]changeAppendState, len(dst.Vecs))
	}
	for i := range dst.Vecs {
		dstVec, srcVec := dst.Vecs[i], src.Vecs[i]
		if dstVec == nil || srcVec == nil || dstVec == srcVec ||
			*dstVec.GetType() != *srcVec.GetType() {
			cleanChangeAppendReplacements(states, mp)
			return moerr.NewInternalErrorNoCtxf(
				"incompatible change batch column %d", i,
			)
		}
		if dstVec.Length() != dstRows || srcVec.Length() != srcRows || end > srcVec.Length() {
			cleanChangeAppendReplacements(states, mp)
			return moerr.NewInternalErrorNoCtxf(
				"change batch column %d has %d rows, cannot append [%d,%d)",
				i, srcVec.Length(), start, end,
			)
		}
		states[i].vec = dstVec
		if !dstVec.IsConst() && !dstVec.NeedDup() {
			continue
		}
		materialized := vector.NewOffHeapVecWithType(*dstVec.GetType())
		if selection := dstVec.AllocationAccountSelection(); selection != nil {
			if err := materialized.SetAllocationAccount(selection); err != nil {
				materialized.Free(mp)
				cleanChangeAppendReplacements(states, mp)
				return err
			}
		}
		if err := materialized.UnionBatch(
			dstVec, 0, dstVec.Length(), nil, mp,
		); err != nil {
			materialized.Free(mp)
			cleanChangeAppendReplacements(states, mp)
			return err
		}
		states[i].vec = materialized
		states[i].replacement = true
	}
	if rows == 0 {
		cleanChangeAppendReplacements(states, mp)
		return nil
	}

	for i := range states {
		states[i].checkpoint = states[i].vec.MakeAppendCheckpoint()
	}
	for i := range states {
		if err := states[i].vec.UnionBatch(src.Vecs[i], int64(start), rows, nil, mp); err != nil {
			for pos := range states {
				states[pos].vec.RollbackAppend(states[pos].checkpoint, rows)
			}
			cleanChangeAppendReplacements(states, mp)
			return err
		}
	}

	for i := range states {
		if !states[i].replacement {
			continue
		}
		dst.Vecs[i].Free(mp)
		dst.Vecs[i] = states[i].vec
	}
	dst.SetRowCount(dst.Vecs[0].Length())
	return nil
}

func NewRowHandle(data *batch.Batch, mp *mpool.MPool, baseHandle *baseHandle, ctx context.Context, tombstone bool) (handle *BatchHandle) {
	handle = &BatchHandle{
		mp:         mp,
		batches:    data,
		ctx:        ctx,
		baseHandle: baseHandle,
		tombstone:  tombstone,
	}
	if data != nil {
		handle.batchLength = data.Vecs[0].Length()
	}
	return
}

func (r *BatchHandle) init(quick bool, mp *mpool.MPool) (err error) {
	if quick || r == nil {
		return
	}
	err = sortBatch(r.batches, len(r.batches.Vecs)-1, mp)
	return
}
func (r *BatchHandle) IsEmpty() bool {
	if r == nil {
		return true
	}
	return r.batchLength == 0
}
func (r *BatchHandle) Rows() int {
	if r == nil {
		return 0
	}
	return r.batchLength
}
func (r *BatchHandle) isEnd() bool {
	return r == nil || r.batches == nil || r.rowOffsetCursor >= r.batchLength
}
func (r *BatchHandle) NextTS() types.TS {
	if r.isEnd() {
		return types.TS{}
	}
	commitTSVec := r.batches.Vecs[len(r.batches.Vecs)-1]
	return vector.GetFixedAtNoTypeCheck[types.TS](commitTSVec, r.rowOffsetCursor)
}
func (r *BatchHandle) Close() {
	if r == nil || r.batches == nil {
		return
	}
	r.batches.Clean(r.mp)
	r.batches = nil
	r.batchLength = 0
}
func (r *BatchHandle) Next(data **batch.Batch, mp *mpool.MPool) (err error) {
	if r.isEnd() {
		return moerr.GetOkExpectedEOF()
	}
	err = r.next(data, mp, r.rowOffsetCursor, r.rowOffsetCursor+1)
	if err != nil {
		return
	}
	r.rowOffsetCursor++
	return
}

func (r *BatchHandle) QuickNext(data **batch.Batch, mp *mpool.MPool) (err error) {
	if r.isEnd() {
		return moerr.GetOkExpectedEOF()
	}
	err = r.next(data, mp, r.rowOffsetCursor, r.batchLength)
	if err != nil {
		return
	}
	r.rowOffsetCursor = r.batchLength
	return
}

func (r *BatchHandle) next(bat **batch.Batch, mp *mpool.MPool, start, end int) (err error) {
	t0 := time.Now()
	if *bat == nil {
		result := batch.NewWithSize(0)
		result.Attrs = append(result.Attrs, r.batches.Attrs...)
		for _, vec := range r.batches.Vecs {
			newVec, err := vec.CloneWindow(start, end, mp)
			if err != nil {
				result.Clean(mp)
				return err
			}
			result.Vecs = append(result.Vecs, newVec)
		}
		result.SetRowCount(result.Vecs[0].Length())
		*bat = result
	} else {
		if !batchesShareAppendSchema(*bat, r.batches) {
			return moerr.GetOkExpectedEOB()
		}
		if err = appendChangeBatchWindow(*bat, r.batches, start, end, mp); err != nil {
			return err
		}
	}
	r.baseHandle.changesHandle.copyDuration += time.Since(t0)
	return
}

type CNObjectHandle struct {
	isTombstone        bool
	objectOffsetCursor int
	blkOffsetCursor    int
	objects            []*objectio.ObjectEntry
	fs                 fileservice.FileService
	mp                 *mpool.MPool
	base               *baseHandle

	cache    []*batch.Batch
	blks     []types.Blockid
	TSs      []types.TS
	layouts  []objectio.SpecialColumnLayout
	seqnums  [][]uint16
	prepared []bool

	terminalErr error
}

func NewCNObjectHandle(isTombstone bool, objects []*objectio.ObjectEntry, fs fileservice.FileService, baseHandle *baseHandle, mp *mpool.MPool) *CNObjectHandle {
	return &CNObjectHandle{
		base:        baseHandle,
		isTombstone: isTombstone,
		objects:     objects,
		fs:          fs,
		mp:          mp,
		cache:       make([]*batch.Batch, 0),
		blks:        make([]types.Blockid, 0),
	}
}

func (h *CNObjectHandle) terminalError() error {
	if h == nil {
		return nil
	}
	return h.terminalErr
}

// fail records structural failures that make the remaining cache unsafe to
// consume. Prefetch and allocation errors remain retryable because their
// cursors and source batches have not advanced.
func (h *CNObjectHandle) fail(err error) error {
	if err == nil {
		return nil
	}
	if h.terminalErr == nil {
		h.terminalErr = err
		h.Close()
	}
	return h.terminalErr
}

func (h *CNObjectHandle) prefetch(ctx context.Context) (err error) {
	t0 := time.Now()
	initialObjectOffset, initialBlockOffset := h.objectOffsetCursor, h.blkOffsetCursor
	jobs := make([]*tasks.Job, 0)
	blks := make([]types.Blockid, 0)
	commitTSs := make([]types.TS, 0)
	for i := 0; i < LoadParallism; i++ {
		if h.objectOffsetCursor >= len(h.objects) {
			break
		}
		entry := h.objects[h.objectOffsetCursor]
		if entry == nil {
			err = moerr.NewInternalErrorNoCtx("CN change object entry is nil")
			break
		}
		stats := entry.ObjectStats
		blockCount, validateErr := validateChangeObjectBlockCount(&stats)
		if validateErr != nil {
			err = validateErr
			break
		}
		if h.blkOffsetCursor < 0 || h.blkOffsetCursor >= blockCount {
			err = moerr.NewInternalErrorNoCtxf(
				"CN change object block cursor %d is outside [0,%d)",
				h.blkOffsetCursor, blockCount,
			)
			break
		}
		blk := uint16(h.blkOffsetCursor)
		job, scheduleErr := prefetchObjects(
			ctx, uint32(h.blkOffsetCursor), h.fs, &stats, h.base.changesHandle.scheduler,
		)
		if scheduleErr != nil {
			err = scheduleErr
			break
		}
		jobs = append(jobs, job)
		blks = append(blks, objectio.NewBlockidWithObjectID(stats.ObjectName().ObjectId(), blk))
		commitTSs = append(commitTSs, entry.CreateTime)
		h.blkOffsetCursor++
		if h.blkOffsetCursor >= blockCount {
			h.blkOffsetCursor = 0
			h.objectOffsetCursor++
		}
	}
	loadedBlocks := make([]*loadedAObjectBlock, len(jobs))
	for i, job := range jobs {
		res := job.GetResult()
		putJob(job)
		if res == nil {
			if err == nil {
				err = moerr.NewInternalErrorNoCtx("CN object prefetch job returned no result")
			}
			continue
		}
		if res.Err != nil {
			if err == nil {
				err = res.Err
			}
			if moerr.IsMoErrCode(res.Err, moerr.ErrFileNotFound) {
				logutil.Info("ChangesHandle-FileNotFound",
					zap.String("err", res.Err.Error()))
			}
			continue
		}
		loaded, ok := res.Res.(*loadedAObjectBlock)
		if !ok || loaded == nil || loaded.batch == nil {
			if err == nil {
				err = moerr.NewInternalErrorNoCtx("CN object prefetch job returned invalid data")
			}
			continue
		}
		loadedBlocks[i] = loaded
	}
	if err != nil {
		for _, loaded := range loadedBlocks {
			if loaded != nil && loaded.batch != nil {
				loaded.batch.Clean(h.mp)
			}
		}
		h.objectOffsetCursor = initialObjectOffset
		h.blkOffsetCursor = initialBlockOffset
		h.base.changesHandle.readDuration += time.Since(t0)
		return
	}
	for i, loaded := range loadedBlocks {
		h.cache = append(h.cache, loaded.batch)
		h.blks = append(h.blks, blks[i])
		h.TSs = append(h.TSs, commitTSs[i])
		h.layouts = append(h.layouts, loaded.specialLayout)
		h.seqnums = append(h.seqnums, loaded.columnSeqnums)
		h.prepared = append(h.prepared, false)
	}
	h.base.changesHandle.readDuration += time.Since(t0)
	return
}
func (h *CNObjectHandle) isEnd() bool {
	return h.objectOffsetCursor >= len(h.objects) && len(h.cache) == 0
}
func (h *CNObjectHandle) IsEmpty() bool {
	return h == nil || len(h.objects) == 0
}
func (h *CNObjectHandle) Next(ctx context.Context, bat **batch.Batch, mp *mpool.MPool) (err error) {
	if terminalErr := h.terminalError(); terminalErr != nil {
		return terminalErr
	}
	if h.isEnd() {
		return moerr.GetOkExpectedEOF()
	}
	if len(h.cache) == 0 {
		err = h.prefetch(ctx)
		if err != nil {
			return
		}
	}
	if len(h.cache) != len(h.prepared) || len(h.cache) != len(h.TSs) ||
		len(h.cache) != len(h.layouts) ||
		(len(h.seqnums) != 0 && len(h.cache) != len(h.seqnums)) ||
		(len(h.blks) != 0 && len(h.cache) != len(h.blks)) {
		return h.fail(moerr.NewInternalErrorNoCtx(
			"CN object block cache metadata is inconsistent",
		))
	}
	data := h.cache[0]
	discardSource := func() {
		h.cache = h.cache[1:]
		h.prepared = h.prepared[1:]
		if len(h.blks) > 0 {
			h.blks = h.blks[1:]
		}
		h.TSs = h.TSs[1:]
		h.layouts = h.layouts[1:]
		if len(h.seqnums) > 0 {
			h.seqnums = h.seqnums[1:]
		}
		data.Clean(h.mp)
	}
	var blk *types.Blockid
	if len(h.blks) > 0 {
		blk = &h.blks[0]
	}
	ts := h.TSs[0]
	layout := h.layouts[0]
	var seqnums []uint16
	if len(h.seqnums) > 0 {
		seqnums = h.seqnums[0]
	}
	if !h.prepared[0] {
		t0 := time.Now()
		if h.isTombstone {
			err = updateCNTombstoneBatch(
				data,
				ts,
				blk,
				layout,
				h.base.changesHandle.retainRowID,
				h.mp,
			)
		} else {
			err = updateCNDataBatchWithSchema(
				data,
				ts,
				blk,
				layout,
				seqnums,
				h.base.changesHandle.dataSchema,
				h.base.changesHandle.retainRowID,
				h.mp,
			)
		}
		if err != nil {
			// Preparation is atomic: keep the source at the current cursor so a
			// transient allocation failure can be retried, and a malformed block
			// cannot be silently skipped by a caller that invokes Next again.
			return err
		}
		h.prepared[0] = true
		h.base.changesHandle.updateDuration += time.Since(t0)
	}
	t0 := time.Now()
	createdOutput := false
	if *bat == nil {
		result := batch.NewWithSize(0)
		result.Attrs = append(result.Attrs, data.Attrs...)
		for _, vec := range data.Vecs {
			newVec := vector.NewVec(*vec.GetType())
			result.Vecs = append(result.Vecs, newVec)
		}
		*bat = result
		createdOutput = true
	} else if !batchesShareAppendSchema(*bat, data) {
		return moerr.GetOkExpectedEOB()
	}
	srcLen := data.Vecs[0].Length()
	if err = appendChangeBatchWindow(*bat, data, 0, srcLen, mp); err != nil {
		if createdOutput {
			(*bat).Clean(mp)
			*bat = nil
		}
		// Keep the prepared source in place: after transient allocation
		// pressure clears, a retry can copy the same block without losing rows.
		return err
	}
	discardSource()
	h.base.changesHandle.copyDuration += time.Since(t0)
	return
}

func (h *CNObjectHandle) QuickNext(ctx context.Context, data **batch.Batch, mp *mpool.MPool) (err error) {
	return h.Next(ctx, data, mp)
}

func (h *CNObjectHandle) Close() {
	if h == nil {
		return
	}
	for _, bat := range h.cache {
		if bat != nil {
			bat.Clean(h.mp)
		}
	}
	h.cache = nil
	h.blks = nil
	h.TSs = nil
	h.layouts = nil
	h.seqnums = nil
	h.prepared = nil
	h.objectOffsetCursor = len(h.objects)
	h.objects = nil
	h.blkOffsetCursor = 0
}

func (h *CNObjectHandle) NextTS() types.TS {
	if h == nil || h.terminalErr != nil || h.isEnd() {
		return types.TS{}
	}
	if len(h.cache) > 0 {
		if len(h.TSs) == 0 {
			return types.TS{}
		}
		return h.TSs[0]
	}
	if h.objectOffsetCursor < 0 || h.objectOffsetCursor >= len(h.objects) ||
		h.objects[h.objectOffsetCursor] == nil {
		return types.TS{}
	}
	return h.objects[h.objectOffsetCursor].CreateTime
}

type AObjectHandle struct {
	isTombstone        bool
	start, end         types.TS
	objectOffsetCursor int
	blkOffsetCursor    int
	rowOffsetCursor    int
	currentBatch       *batch.Batch
	batchLength        int
	objects            []*objectio.ObjectEntry
	quick              bool
	fs                 fileservice.FileService
	mp                 *mpool.MPool
	cache              []*batch.Batch
	specialLayouts     []objectio.SpecialColumnLayout
	columnSeqnums      [][]uint16
	blks               []types.Blockid
	p                  *baseHandle
	terminalErr        error

	// blockPlans caches block-level commit-ts overlap decisions for objects.
	// It is only populated when checkpoint-range mode enables block pruning.
	blockPlans map[string]*aobjBlockPlan
}

func (h *AObjectHandle) terminalError() error {
	if h == nil {
		return nil
	}
	return h.terminalErr
}

// fail records an error after a prefetched block has been detached from the
// retryable prefetch cursor. At that point conversion may already have mutated
// the block and later blocks are cached. Keep the first error sticky and
// release owned batches so a retry cannot silently skip the bad block.
func (h *AObjectHandle) fail(err error) error {
	if err == nil {
		return nil
	}
	if h.terminalErr == nil {
		h.terminalErr = err
		h.Close()
	}
	return h.terminalErr
}

type aobjBlockPlan struct {
	initialized      bool
	evaluable        bool
	shouldReadByBlks []bool
	totalBlocks      int
	evaluableBlocks  int
	overlapBlocks    int
	prunedBlocks     int
	// nonEvaluableReasons counts why a block cannot be pruned by commit-ts
	// zonemap, for example missing metadata or unsupported tail column type.
	nonEvaluableReasons map[string]int
	// nonEvaluableSamples stores a few representative block-level diagnostics.
	nonEvaluableSamples []string
	// evaluableSamples stores a few representative successful evaluations.
	evaluableSamples []string
}

func NewAObjectHandle(ctx context.Context, p *baseHandle, isTombstone bool, start, end types.TS, objects []*objectio.ObjectEntry, fs fileservice.FileService, mp *mpool.MPool) *AObjectHandle {
	handle := &AObjectHandle{
		isTombstone: isTombstone,
		start:       start,
		end:         end,
		objects:     objects,
		fs:          fs,
		mp:          mp,
		p:           p,
		cache:       make([]*batch.Batch, 0),
		blks:        make([]types.Blockid, 0),
		blockPlans:  make(map[string]*aobjBlockPlan),
	}
	return handle
}

// nextPrefetchTarget returns the next object/block pair that should be loaded.
// In checkpoint-range mode, TN-created non-appendable objects can be pruned by
// commit-ts zonemap at block granularity before loading block data.
func (h *AObjectHandle) nextPrefetchTarget(
	ctx context.Context,
) (obj *objectio.ObjectEntry, blk uint16, ok bool, err error) {
	for {
		if ctx != nil {
			select {
			case <-ctx.Done():
				return nil, 0, false, context.Cause(ctx)
			default:
			}
		}
		if h.objectOffsetCursor >= len(h.objects) {
			return nil, 0, false, nil
		}
		obj = h.objects[h.objectOffsetCursor]
		if obj == nil {
			return nil, 0, false, moerr.NewInternalErrorNoCtx("appendable change object entry is nil")
		}
		blockCount, validateErr := validateChangeObjectBlockCount(&obj.ObjectStats)
		if validateErr != nil {
			return nil, 0, false, validateErr
		}
		if h.blkOffsetCursor < 0 || h.blkOffsetCursor >= blockCount {
			return nil, 0, false, moerr.NewInternalErrorNoCtxf(
				"appendable change object block cursor %d is outside [0,%d)",
				h.blkOffsetCursor, blockCount,
			)
		}
		blk = uint16(h.blkOffsetCursor)
		h.blkOffsetCursor++
		if h.blkOffsetCursor >= blockCount {
			h.blkOffsetCursor = 0
			h.objectOffsetCursor++
		}
		okToRead, planErr := h.shouldReadBlock(ctx, obj, blk)
		if planErr != nil {
			return nil, 0, false, planErr
		}
		if okToRead {
			return obj, blk, true, nil
		}
	}
}

// shouldReadBlock decides whether one block may contain rows in [start, end].
//
// For checkpoint-range recovery of TN-created non-appendable objects, this
// method uses commit-ts zonemap to skip irrelevant blocks. If strict mode is
// enabled and commit-ts zonemap is unavailable, it returns ErrFileNotFound so
// caller can fall back to exact visible-state reconstruction.
func (h *AObjectHandle) shouldReadBlock(
	ctx context.Context,
	obj *objectio.ObjectEntry,
	blk uint16,
) (bool, error) {
	if obj == nil {
		return false, nil
	}
	if h == nil || h.p == nil || h.p.changesHandle == nil {
		return false, moerr.NewInternalErrorNoCtx("appendable change handle is not initialized")
	}
	changes := h.p.changesHandle
	if !changes.enableCommitTSBlockPrune {
		return true, nil
	}
	// Row-commit-ts pruning is only meaningful for TN-created non-appendable
	// objects. Appendable objects are kept on the existing path.
	if obj.GetAppendable() || obj.GetCNCreated() {
		return true, nil
	}
	key := changeObjectIdentity(obj)
	plan, ok := h.blockPlans[key]
	if !ok {
		plan = &aobjBlockPlan{}
		h.blockPlans[key] = plan
	}
	if !plan.initialized {
		if err := h.buildBlockPlan(ctx, obj, plan); err != nil {
			return false, err
		}
	}
	if !plan.evaluable {
		if changes.strictCommitTSBlockPrune {
			logutil.Warn(
				"ChangesHandle-CommitTSBlockPlan strict fallback",
				zap.String("object", obj.ObjectShortName().ShortString()),
				zap.Bool("tombstone", h.isTombstone),
				zap.String("start", h.start.ToString()),
				zap.String("end", h.end.ToString()),
				zap.Int("total-blocks", plan.totalBlocks),
				zap.Int("evaluable-blocks", plan.evaluableBlocks),
				zap.Int("overlap-blocks", plan.overlapBlocks),
				zap.Int("pruned-blocks", plan.prunedBlocks),
				zap.Float64("prune-rate", calcPruneRate(plan.prunedBlocks, plan.totalBlocks)),
				zap.Any("non-evaluable-reasons", plan.nonEvaluableReasons),
				zap.Strings("non-evaluable-samples", plan.nonEvaluableSamples),
				zap.Strings("evaluable-samples", plan.evaluableSamples),
			)
			return false, moerr.NewFileNotFoundNoCtx(obj.ObjectName().String())
		}
		return true, nil
	}
	if int(blk) >= len(plan.shouldReadByBlks) {
		return false, moerr.NewInternalErrorNoCtxf(
			"change block %d is outside planned block range [0,%d)",
			blk, len(plan.shouldReadByBlks),
		)
	}
	return plan.shouldReadByBlks[blk], nil
}

func (h *AObjectHandle) buildBlockPlan(
	ctx context.Context,
	obj *objectio.ObjectEntry,
	plan *aobjBlockPlan,
) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			if plan != nil {
				plan.initialized = false
			}
			err = moerr.NewInternalErrorNoCtxf(
				"cannot decode change object block metadata: %v", recovered,
			)
		}
	}()
	if ctx == nil || h == nil || h.p == nil || h.p.changesHandle == nil ||
		obj == nil || plan == nil || h.fs == nil {
		return moerr.NewInternalErrorNoCtx("cannot build a change block plan without object and destination")
	}
	blockCount, err := validateChangeObjectBlockCount(&obj.ObjectStats)
	if err != nil {
		return err
	}
	plan.initialized = false
	plan.evaluable = false
	plan.shouldReadByBlks = make([]bool, blockCount)
	plan.totalBlocks = blockCount
	plan.evaluableBlocks = 0
	plan.overlapBlocks = 0
	plan.prunedBlocks = 0
	collectDiagnostics := h.p.changesHandle.strictCommitTSBlockPrune ||
		h.p.changesHandle.debugLabel != ""
	if collectDiagnostics {
		plan.nonEvaluableReasons = make(map[string]int, 4)
		plan.nonEvaluableSamples = make([]string, 0, 5)
		plan.evaluableSamples = make([]string, 0, 5)
	} else {
		plan.nonEvaluableReasons = nil
		plan.nonEvaluableSamples = nil
		plan.evaluableSamples = nil
	}
	for i := range plan.shouldReadByBlks {
		plan.shouldReadByBlks[i] = true
	}
	metaLoc := obj.ObjectLocation()
	meta, err := objectio.FastLoadObjectMeta(ctx, &metaLoc, false, h.fs)
	if err != nil {
		logutil.Warn(
			"ChangesHandle-CommitTSBlockPlan load object meta failed",
			zap.String("object", obj.ObjectShortName().ShortString()),
			zap.String("object-name", obj.ObjectName().String()),
			zap.String("location", metaLoc.String()),
			zap.Error(err),
		)
		return err
	}
	dataMeta, err := ioutil.GetDataMetaForLocation(meta, metaLoc)
	if err != nil {
		return err
	}
	if uint32(blockCount) != dataMeta.BlockCount() {
		return moerr.NewInternalErrorNoCtxf(
			"object %s reports %d blocks but metadata contains %d",
			obj.ObjectShortName().ShortString(), obj.BlkCnt(), dataMeta.BlockCount(),
		)
	}
	evaluableBlockCnt := 0
	overlapBlockCnt := 0
	pkf := h.p.changesHandle.pkFilter
	pkSeqnum, canPruneByPK := uint16(0), false
	if pkf != nil && len(pkf.Segments) > 0 {
		if h.isTombstone {
			// A tombstone object's sort key is the target physical rowid at
			// sequence zero.  PKFilter segments, however, encode the table's user
			// primary key, which tombstones persist at sequence one.  Comparing the
			// two is especially dangerous when the user PK itself has ROWID type:
			// the types match, so the zonemap helper cannot conservatively detect
			// the semantic mismatch and may prune real deletes.
			pkSeqnum = objectio.TombstoneAttr_PK_SeqNum
			canPruneByPK = true
		} else if pkf.PrimarySeqnum >= 0 && pkf.PrimarySeqnum < int(objectio.SEQNUM_UPPER) {
			// primarySeqnum on ChangeHandler is also used as a positional index
			// while reconciling logical output batches.  PKFilter.PrimarySeqnum is
			// the explicit physical sequence number and is therefore the only safe
			// source for object metadata lookup when schemas contain sequence gaps.
			pkSeqnum = uint16(pkf.PrimarySeqnum)
			canPruneByPK = true
		}
	}
	for i := 0; i < blockCount; i++ {
		if i&255 == 0 {
			select {
			case <-ctx.Done():
				return context.Cause(ctx)
			default:
			}
		}
		blk := dataMeta.GetBlockMeta(uint32(i))
		overlap, evaluable, reason, _ := blockCommitTSOverlapsRangeDetailed(
			blk, h.start, h.end, false,
		)
		if !evaluable {
			if collectDiagnostics {
				plan.nonEvaluableReasons[reason]++
			}
			if collectDiagnostics && len(plan.nonEvaluableSamples) < 5 {
				_, _, _, detail := blockCommitTSOverlapsRangeDetailed(
					blk, h.start, h.end, true,
				)
				plan.nonEvaluableSamples = append(
					plan.nonEvaluableSamples,
					fmt.Sprintf("blk=%d reason=%s %s", i, reason, detail),
				)
			}
			// Even for non-evaluable blocks, PK pruning can still skip them.
			if canPruneByPK && changeBlockPKDisjoint(blk, pkSeqnum, pkf.Segments) {
				plan.shouldReadByBlks[i] = false
				plan.prunedBlocks++
			}
			continue
		}
		evaluableBlockCnt++
		plan.shouldReadByBlks[i] = overlap
		// Apply PK pruning as a secondary filter on blocks that survived commit-TS check.
		if overlap && canPruneByPK && changeBlockPKDisjoint(blk, pkSeqnum, pkf.Segments) {
			plan.shouldReadByBlks[i] = false
			overlap = false
		}
		if overlap {
			overlapBlockCnt++
		} else {
			plan.prunedBlocks++
		}
		if collectDiagnostics && len(plan.evaluableSamples) < 5 {
			_, _, _, detail := blockCommitTSOverlapsRangeDetailed(
				blk, h.start, h.end, true,
			)
			plan.evaluableSamples = append(
				plan.evaluableSamples,
				fmt.Sprintf("blk=%d overlap=%t %s", i, overlap, detail),
			)
		}
	}
	// "evaluable" here means at least one block exposes usable commit-ts zonemap.
	// If none does, strict mode can still choose the exact-scan fallback path.
	plan.evaluable = evaluableBlockCnt > 0
	plan.evaluableBlocks = evaluableBlockCnt
	plan.overlapBlocks = overlapBlockCnt
	if h.p.changesHandle.debugLabel != "" {
		fields := []zap.Field{
			zap.String("object", obj.ObjectShortName().ShortString()),
			zap.Bool("tombstone", h.isTombstone),
			zap.Int("total-blocks", plan.totalBlocks),
			zap.Int("evaluable-blocks", plan.evaluableBlocks),
			zap.Int("overlap-blocks", plan.overlapBlocks),
			zap.Int("pruned-blocks", plan.prunedBlocks),
			zap.Float64("prune-rate", calcPruneRate(plan.prunedBlocks, plan.totalBlocks)),
			zap.Any("non-evaluable-reasons", plan.nonEvaluableReasons),
			zap.Strings("non-evaluable-samples", plan.nonEvaluableSamples),
			zap.Strings("evaluable-samples", plan.evaluableSamples),
		}
		fields = append(fields, zap.String("debug-label", h.p.changesHandle.debugLabel))
		logutil.Info("ChangesHandle-CommitTSBlockPlan summary", fields...)
	}
	plan.initialized = true
	return nil
}

// blockCommitTSOverlapsRange checks whether one block's commit-ts zonemap
// intersects [start, end]. The second return value is false when the block
// does not expose a usable commit-ts zonemap.
func blockCommitTSOverlapsRange(
	blk objectio.BlockObject,
	start, end types.TS,
) (bool, bool, string, string) {
	return blockCommitTSOverlapsRangeDetailed(blk, start, end, true)
}

func blockCommitTSOverlapsRangeDetailed(
	blk objectio.BlockObject,
	start, end types.TS,
	withDetail bool,
) (overlap, evaluable bool, reason, detail string) {
	defer func() {
		if recovered := recover(); recovered != nil {
			overlap = false
			evaluable = false
			reason = "malformed_metadata"
			if withDetail {
				detail = fmt.Sprintf("metadata_panic=%v", recovered)
			} else {
				detail = ""
			}
		}
	}()
	if start.GT(&end) {
		return false, false, "invalid_range", ""
	}
	metaColCnt := blk.GetMetaColumnCount()
	maxSeqnum := blk.GetMaxSeqnum()
	base := ""
	if withDetail {
		base = fmt.Sprintf("meta_col_cnt=%d max_seqnum=%d", metaColCnt, maxSeqnum)
	}
	if metaColCnt == 0 {
		return false, false, "no_meta_columns", base
	}
	commitPos, ok := objectio.ResolveSpecialColumnLayout(blk).Resolve(objectio.SEQNUM_COMMITTS)
	if !ok {
		return false, false, "tail_column_not_ts", base
	}
	commitCol := blk.ColumnMeta(commitPos)
	if withDetail {
		base = fmt.Sprintf("%s commit_pos=%d", base, commitPos)
	}
	zm := commitCol.ZoneMap()
	if !zm.IsInited() {
		return false, false, "zonemap_not_inited", base
	}
	if zm.GetType() != types.T_TS {
		if withDetail {
			base = fmt.Sprintf("%s zm_type=%s", base, zm.GetType().String())
		}
		return false, false, "zonemap_type_not_ts", base
	}
	minBuf, maxBuf := zm.GetMinBuf(), zm.GetMaxBuf()
	if len(minBuf) != types.TxnTsSize || len(maxBuf) != types.TxnTsSize {
		return false, false, "zonemap_invalid_bounds", base
	}
	minTS := types.DecodeFixed[types.TS](minBuf)
	maxTS := types.DecodeFixed[types.TS](maxBuf)
	if minTS.GT(&maxTS) {
		return false, false, "zonemap_reversed_bounds", base
	}
	detail = ""
	if withDetail {
		detail = fmt.Sprintf(
			"%s zm_type=%s zm_min=%s zm_max=%s range=[%s,%s]",
			base,
			zm.GetType().String(),
			minTS.ToString(),
			maxTS.ToString(),
			start.ToString(),
			end.ToString(),
		)
	}
	if maxTS.LT(&start) || minTS.GT(&end) {
		return false, true, "", detail
	}
	return true, true, "", detail
}

func calcPruneRate(pruned, total int) float64 {
	if total <= 0 {
		return 0
	}
	return float64(pruned) / float64(total)
}

func (h *AObjectHandle) prefetch(ctx context.Context) (err error) {
	t0 := time.Now()
	initialObjectOffset, initialBlockOffset := h.objectOffsetCursor, h.blkOffsetCursor
	jobs := make([]*tasks.Job, 0)
	blks := make([]types.Blockid, 0)
	for i := 0; i < LoadParallism; i++ {
		obj, blk, ok, targetErr := h.nextPrefetchTarget(ctx)
		if targetErr != nil {
			err = targetErr
			break
		}
		if !ok {
			break
		}
		stats := obj.ObjectStats
		job, scheduleErr := prefetchObjects(
			ctx, uint32(blk), h.fs, &stats, h.p.changesHandle.scheduler,
		)
		if scheduleErr != nil {
			err = scheduleErr
			break
		}
		jobs = append(jobs, job)
		blks = append(blks, objectio.NewBlockidWithObjectID(stats.ObjectName().ObjectId(), blk))
	}
	loadedBlocks := make([]*loadedAObjectBlock, len(jobs))
	for i, job := range jobs {
		res := job.GetResult()
		putJob(job)
		if res == nil {
			if err == nil {
				err = moerr.NewInternalErrorNoCtx("appendable object prefetch job returned no result")
			}
			continue
		}
		if res.Err != nil {
			if err == nil {
				err = res.Err
			}
			if moerr.IsMoErrCode(res.Err, moerr.ErrFileNotFound) {
				logutil.Info("ChangesHandle-FileNotFound",
					zap.String("err", res.Err.Error()))
			}
			continue
		}
		loaded, ok := res.Res.(*loadedAObjectBlock)
		if !ok || loaded == nil || loaded.batch == nil {
			if err == nil {
				err = moerr.NewInternalErrorNoCtx("appendable object prefetch job returned invalid data")
			}
			continue
		}
		loadedBlocks[i] = loaded
	}
	if err != nil {
		for _, loaded := range loadedBlocks {
			if loaded != nil && loaded.batch != nil {
				loaded.batch.Clean(h.mp)
			}
		}
		h.objectOffsetCursor = initialObjectOffset
		h.blkOffsetCursor = initialBlockOffset
		h.p.changesHandle.readDuration += time.Since(t0)
		return
	}
	for i, loaded := range loadedBlocks {
		h.cache = append(h.cache, loaded.batch)
		h.specialLayouts = append(h.specialLayouts, loaded.specialLayout)
		h.columnSeqnums = append(h.columnSeqnums, loaded.columnSeqnums)
		h.blks = append(h.blks, blks[i])
	}
	h.p.changesHandle.readDuration += time.Since(t0)
	return
}
func (h *AObjectHandle) init(ctx context.Context, quick bool) (err error) {
	h.quick = quick
	err = h.getNextAObject(ctx)
	return
}
func (h *AObjectHandle) IsEmpty() bool {
	return h == nil || len(h.objects) == 0
}
func (h *AObjectHandle) RowCount() int {
	if h == nil {
		return 0
	}
	cnt := 0
	for _, obj := range h.objects {
		if obj != nil {
			cnt += int(obj.ObjectStats.Rows())
		}
	}
	return cnt
}
func (h *AObjectHandle) getNextAObject(ctx context.Context) (err error) {
	if terminalErr := h.terminalError(); terminalErr != nil {
		return terminalErr
	}
	for {
		if h.isEnd() {
			return
		}
		if len(h.cache) == 0 {
			err = h.prefetch(ctx)
			if err != nil {
				return
			}
			if len(h.cache) == 0 {
				if h.isEnd() {
					return
				}
				continue
			}
		}
		h.currentBatch = h.cache[0]
		h.cache = h.cache[1:]
		if (len(h.specialLayouts) != 0 && len(h.specialLayouts) != len(h.cache)+1) ||
			(len(h.columnSeqnums) != 0 && len(h.columnSeqnums) != len(h.cache)+1) ||
			(len(h.blks) != 0 && len(h.blks) != len(h.cache)+1) {
			return h.fail(moerr.NewInternalErrorNoCtx(
				"appendable object block cache metadata is inconsistent",
			))
		}
		var specialLayout *objectio.SpecialColumnLayout
		if len(h.specialLayouts) > 0 {
			specialLayout = &h.specialLayouts[0]
			h.specialLayouts = h.specialLayouts[1:]
		}
		var columnSeqnums []uint16
		if len(h.columnSeqnums) > 0 {
			columnSeqnums = h.columnSeqnums[0]
			h.columnSeqnums = h.columnSeqnums[1:]
		}
		var blk *types.Blockid
		if len(h.blks) > 0 {
			blk = &h.blks[0]
			h.blks = h.blks[1:]
		}
		t0 := time.Now()
		if h.isTombstone {
			if err = updateTombstoneBatch(h.currentBatch, h.start, h.end, h.p.skipTS, !h.quick, blk, specialLayout, h.p.changesHandle.retainRowID, h.mp); err != nil {
				return h.fail(err)
			}
		} else {
			if err = updateDataBatchWithSchema(
				h.currentBatch, h.start, h.end, blk, specialLayout, columnSeqnums,
				h.p.changesHandle.dataSchema, h.p.changesHandle.retainRowID, h.mp,
			); err != nil {
				return h.fail(err)
			}
		}
		h.p.changesHandle.updateDuration += time.Since(t0)
		h.batchLength = h.currentBatch.Vecs[0].Length()
		if h.batchLength > 0 {
			return
		}
		h.currentBatch.Clean(h.mp)
		h.currentBatch = nil
	}
}
func (h *AObjectHandle) isEnd() bool {
	return h.objectOffsetCursor >= len(h.objects) && len(h.cache) == 0
}

func (h *AObjectHandle) QuickNext(ctx context.Context, data **batch.Batch, mp *mpool.MPool) (err error) {
	if terminalErr := h.terminalError(); terminalErr != nil {
		return terminalErr
	}
	if h.currentBatch == nil && h.rowOffsetCursor >= h.batchLength {
		if err = h.getNextAObject(ctx); err != nil {
			return err
		}
	}
	if h.currentBatch == nil && h.isEnd() {
		return moerr.GetOkExpectedEOF()
	}
	err = h.next(ctx, data, mp, h.rowOffsetCursor, h.batchLength)
	if err != nil {
		return
	}
	return
}

func (h *AObjectHandle) Next(ctx context.Context, bat **batch.Batch, mp *mpool.MPool) (err error) {
	if terminalErr := h.terminalError(); terminalErr != nil {
		return terminalErr
	}
	if h.currentBatch == nil && h.rowOffsetCursor >= h.batchLength {
		if err = h.getNextAObject(ctx); err != nil {
			return err
		}
	}
	if h.currentBatch == nil && h.isEnd() {
		return moerr.GetOkExpectedEOF()
	}
	return h.next(ctx, bat, mp, h.rowOffsetCursor, h.rowOffsetCursor+1)
}
func (h *AObjectHandle) next(ctx context.Context, bat **batch.Batch, mp *mpool.MPool, start, end int) (err error) {
	if terminalErr := h.terminalError(); terminalErr != nil {
		return terminalErr
	}
	if h.isEnd() && h.rowOffsetCursor >= h.batchLength {
		return moerr.GetOkExpectedEOF()
	}
	t0 := time.Now()
	if *bat == nil {
		result := batch.NewWithSize(len(h.currentBatch.Vecs))
		result.Attrs = append(result.Attrs, h.currentBatch.Attrs...)
		for i, vec := range h.currentBatch.Vecs {
			newVec, err := vec.CloneWindow(start, end, mp)
			if err != nil {
				result.Clean(mp)
				h.p.changesHandle.copyDuration += time.Since(t0)
				return err
			}
			result.Vecs[i] = newVec
		}
		result.SetRowCount(result.Vecs[0].Length())
		*bat = result
	} else {
		if !batchesShareAppendSchema(*bat, h.currentBatch) {
			return moerr.GetOkExpectedEOB()
		}
		if err = appendChangeBatchWindow(*bat, h.currentBatch, start, end, mp); err != nil {
			h.p.changesHandle.copyDuration += time.Since(t0)
			return err
		}
	}
	h.p.changesHandle.copyDuration += time.Since(t0)
	h.rowOffsetCursor = end
	if h.rowOffsetCursor >= h.batchLength {
		h.currentBatch.Clean(h.mp)
		h.currentBatch = nil
		h.batchLength = 0
		h.rowOffsetCursor = 0
		// Ordered mode needs the next row's commit timestamp immediately for
		// cross-source merge ordering. Quick mode drains sources sequentially;
		// defer the next block's conversion until the next call so a future I/O
		// error cannot turn an already-copied output block into an error result.
		if !h.quick && !h.isEnd() {
			err = h.getNextAObject(ctx)
			if err != nil {
				return
			}
		}
	}
	return
}
func (h *AObjectHandle) NextTS() types.TS {
	if h == nil || h.terminalErr != nil ||
		(h.isEnd() && h.rowOffsetCursor >= h.batchLength) {
		return types.TS{}
	}
	if h.currentBatch == nil || h.batchLength <= 0 ||
		h.rowOffsetCursor < 0 || h.rowOffsetCursor >= h.batchLength ||
		h.currentBatch.RowCount() != h.batchLength || len(h.currentBatch.Vecs) == 0 {
		h.fail(moerr.NewInternalErrorNoCtx(
			"appendable change handle has an invalid current batch",
		))
		return types.TS{}
	}
	commitTSVec := h.currentBatch.Vecs[len(h.currentBatch.Vecs)-1]
	if commitTSVec == nil || commitTSVec.GetType().Oid != types.T_TS ||
		commitTSVec.Length() != h.batchLength {
		h.fail(moerr.NewInternalErrorNoCtx(
			"appendable change handle has an invalid commit-ts column",
		))
		return types.TS{}
	}
	return vector.GetFixedAtNoTypeCheck[types.TS](commitTSVec, h.rowOffsetCursor)
}

func (h *AObjectHandle) Close() {
	if h == nil {
		return
	}
	if h.currentBatch != nil {
		h.currentBatch.Clean(h.mp)
		h.currentBatch = nil
	}
	for _, bat := range h.cache {
		if bat != nil {
			bat.Clean(h.mp)
		}
	}
	h.cache = nil
	h.specialLayouts = nil
	h.columnSeqnums = nil
	h.blks = nil
	h.blockPlans = nil
	h.batchLength = 0
	h.rowOffsetCursor = 0
	h.objectOffsetCursor = len(h.objects)
	h.objects = nil
	h.blkOffsetCursor = 0
}

type baseHandle struct {
	aobjHandle     *AObjectHandle
	cnObjectHandle *CNObjectHandle
	inMemoryHandle *BatchHandle

	changesHandle *ChangeHandler
	isTombstone   bool

	skipTS map[types.TS]struct{}
}

func (p *baseHandle) terminalError() error {
	if p == nil {
		return nil
	}
	if err := p.aobjHandle.terminalError(); err != nil {
		return err
	}
	return p.cnObjectHandle.terminalError()
}

const (
	NextChangeHandle_AObj = iota
	NextChangeHandle_CNObj
	NextChangeHandle_InMemory

	NextChangeHandle_Tombstone
	NextChangeHandle_Data
)

func NewBaseHandler(state *PartitionState, changesHandle *ChangeHandler, start, end types.TS, mp *mpool.MPool, tombstone bool, fs fileservice.FileService, ctx context.Context) (p *baseHandle, err error) {
	if state == nil || changesHandle == nil || ctx == nil || mp == nil || fs == nil {
		return nil, moerr.NewInvalidInputNoCtx(
			"base change handler requires state, parent, context, mpool, and file service",
		)
	}
	p = &baseHandle{
		skipTS:        make(map[types.TS]struct{}),
		changesHandle: changesHandle,
		isTombstone:   tombstone,
	}
	defer func() {
		if err != nil {
			p.Close()
			p = nil
		}
	}()
	var iter btree.IterG[objectio.ObjectEntry]
	if tombstone {
		iter = state.tombstoneObjectsNameIndex.Iter()
	} else {
		iter = state.dataObjectsNameIndex.Iter()
	}
	defer iter.Release()
	if tombstone {
		dataIter := state.dataObjectsNameIndex.Iter()
		fillErr := p.fillInSkipTSWithContext(ctx, dataIter, start, end)
		dataIter.Release()
		if fillErr != nil {
			return nil, fillErr
		}
	}
	rowIter, rowIterKind, pkFilterApplied := p.newReplayRowsIter(state, start, end, tombstone)
	defer rowIter.Close()
	p.inMemoryHandle, err = p.newBatchHandleWithRowIterator(
		ctx, rowIter, rowIterKind, pkFilterApplied, start, end, tombstone, mp,
	)
	if err != nil {
		return nil, err
	}
	aobj, cnObj, tnByCreateTS, collectErr := p.getObjectEntriesWithContext(
		ctx, iter, start, end,
	)
	if collectErr != nil {
		return nil, collectErr
	}
	if p.changesHandle.enableDeleteChainResolve {
		resolvedAObj, resolveErr := p.resolveVisibleObjectsByDeleteChain(
			ctx, start, end, aobj, tnByCreateTS, tombstone, "appendable",
		)
		if resolveErr != nil {
			return nil, resolveErr
		}
		resolvedCNObj, resolveErr := p.resolveVisibleObjectsByDeleteChain(
			ctx, start, end, cnObj, tnByCreateTS, tombstone, "constant-commit-ts",
		)
		if resolveErr != nil {
			return nil, resolveErr
		}
		aobj, cnObj = classifyResolvedObjects(resolvedAObj, resolvedCNObj)
	}
	p.aobjHandle = NewAObjectHandle(ctx, p, tombstone, start, end, aobj, fs, mp)
	p.cnObjectHandle = NewCNObjectHandle(tombstone, cnObj, fs, p, mp)
	return
}

func (p *baseHandle) newReplayRowsIter(
	state *PartitionState,
	start, end types.TS,
	tombstone bool,
) (RowsIter, string, bool) {
	if p.changesHandle == nil || p.changesHandle.pkFilter == nil || p.changesHandle.pkFilter.ReplaySpec == nil {
		return state.NewRawReplayRowsIter(), "full-row-btree", false
	}

	spec := p.changesHandle.pkFilter.ReplaySpec
	if spec.Op == function.EQUAL && len(spec.Keys) == 1 {
		return state.NewExactPrimaryKeyReplayIter(start, end, spec.Keys[0], tombstone), "primary-key-exact-replay", true
	}

	return state.NewRawReplayRowsIter(), "full-row-btree", false
}

func NewBaseHandlerWithObjEntries(
	ctx context.Context,
	changesHandle *ChangeHandler,
	start, end types.TS,
	aobj, cnObj []*objectio.ObjectEntry,
	tombstone bool,
	mp *mpool.MPool,
	fs fileservice.FileService,
) (p *baseHandle, err error) {
	if changesHandle == nil || ctx == nil || mp == nil || fs == nil {
		return nil, moerr.NewInvalidInputNoCtx(
			"object change handler requires parent, context, mpool, and file service",
		)
	}
	p = &baseHandle{
		skipTS:        make(map[types.TS]struct{}),
		changesHandle: changesHandle,
		isTombstone:   tombstone,
	}
	p.aobjHandle = NewAObjectHandle(ctx, p, tombstone, start, end, aobj, fs, mp)
	p.cnObjectHandle = NewCNObjectHandle(tombstone, cnObj, fs, p, mp)
	return
}
func (p *baseHandle) init(ctx context.Context, quick bool, mp *mpool.MPool) (err error) {
	err = p.aobjHandle.init(ctx, quick)
	if err != nil {
		return
	}
	err = p.inMemoryHandle.init(quick, mp)
	return
}
func (p *baseHandle) fillInSkipTS(iter btree.IterG[objectio.ObjectEntry], start, end types.TS) {
	_ = p.fillInSkipTSWithContext(context.Background(), iter, start, end)
}

func (p *baseHandle) fillInSkipTSWithContext(
	ctx context.Context,
	iter btree.IterG[objectio.ObjectEntry],
	start, end types.TS,
) error {
	index := 0
	for iter.Next() {
		if index&1023 == 0 {
			select {
			case <-ctx.Done():
				return context.Cause(ctx)
			default:
			}
		}
		index++
		obj := iter.Item()
		if !obj.DeleteTime.IsEmpty() && !obj.DeleteTime.Equal(&txnif.UncommitTS) {
			ts := obj.DeleteTime
			if ts.GE(&start) && ts.LE(&end) {
				p.skipTS[obj.DeleteTime] = struct{}{}
			}
		}
	}
	return nil
}

func (p *baseHandle) fillInSkipTSFromObjects(start, end types.TS, groups ...[]*objectio.ObjectEntry) {
	for _, group := range groups {
		for _, obj := range group {
			if obj == nil || obj.DeleteTime.IsEmpty() ||
				obj.DeleteTime.Equal(&txnif.UncommitTS) {
				continue
			}
			ts := obj.DeleteTime
			if ts.GE(&start) && ts.LE(&end) {
				p.skipTS[ts] = struct{}{}
			}
		}
	}
}
func (p *baseHandle) IsEmpty() bool {
	return p.aobjHandle.IsEmpty() && p.inMemoryHandle.IsEmpty() && p.cnObjectHandle.IsEmpty()
}

func (p *baseHandle) IsSmall() bool {
	if !p.cnObjectHandle.IsEmpty() {
		return false
	}
	count := p.aobjHandle.RowCount() + p.inMemoryHandle.Rows()
	return count < SmallBatchThreshold
}
func (p *baseHandle) Close() {
	if p == nil {
		return
	}
	if p.inMemoryHandle != nil {
		p.inMemoryHandle.Close()
	}
	if p.aobjHandle != nil {
		p.aobjHandle.Close()
	}
	if p.cnObjectHandle != nil {
		p.cnObjectHandle.Close()
	}
}
func (p *baseHandle) less(a, b types.TS) bool {
	if a.IsEmpty() {
		return false
	}
	if b.IsEmpty() {
		return true
	}
	return a.LE(&b)
}
func (p *baseHandle) nextTS() (types.TS, int) {
	inMemoryTS := p.inMemoryHandle.NextTS()
	aobjTS := p.aobjHandle.NextTS()
	cnObjTS := p.cnObjectHandle.NextTS()
	if p.less(inMemoryTS, aobjTS) && p.less(inMemoryTS, cnObjTS) {
		return inMemoryTS, NextChangeHandle_InMemory
	}
	if p.less(aobjTS, cnObjTS) {
		return aobjTS, NextChangeHandle_AObj
	}
	return cnObjTS, NextChangeHandle_CNObj
}
func (p *baseHandle) NextTS() types.TS {
	ts, _ := p.nextTS()
	return ts
}
func (p *baseHandle) Next(ctx context.Context, bat **batch.Batch, mp *mpool.MPool) (err error) {
	if err = p.terminalError(); err != nil {
		return err
	}
	_, typ := p.nextTS()
	if err = p.terminalError(); err != nil {
		return err
	}
	switch typ {
	case NextChangeHandle_AObj:
		err = p.aobjHandle.Next(ctx, bat, mp)
	case NextChangeHandle_InMemory:
		err = p.inMemoryHandle.Next(bat, mp)
	case NextChangeHandle_CNObj:
		err = p.cnObjectHandle.Next(ctx, bat, mp)
	}
	return
}
func (p *baseHandle) QuickNext(ctx context.Context, bat **batch.Batch, mp *mpool.MPool) (err error) {
	if err = p.terminalError(); err != nil {
		return err
	}
	if p.aobjHandle != nil {
		err = p.aobjHandle.QuickNext(ctx, bat, mp)
		if moerr.IsMoErrCode(err, moerr.OkExpectedEOF) {
			if terminalErr := p.aobjHandle.terminalError(); terminalErr != nil {
				return terminalErr
			}
			p.aobjHandle.Close()
			p.aobjHandle = nil
			err = nil
		}
		if err != nil {
			return
		}
	}
	if (*bat) != nil && (*bat).RowCount() > p.changesHandle.coarseMaxRow {
		return
	}
	if p.inMemoryHandle != nil {
		err = p.inMemoryHandle.QuickNext(bat, mp)
		if moerr.IsMoErrCode(err, moerr.OkExpectedEOF) {
			p.inMemoryHandle.Close()
			p.inMemoryHandle = nil
			err = nil
		}
		if err != nil {
			return
		}
	}
	if (*bat) != nil && (*bat).RowCount() > p.changesHandle.coarseMaxRow {
		return
	}
	err = p.cnObjectHandle.QuickNext(ctx, bat, mp)
	return
}
func (p *baseHandle) newBatchHandleWithRowIterator(
	ctx context.Context,
	iter RowsIter,
	iterKind string,
	pkFilterApplied bool,
	start, end types.TS,
	tombstone bool,
	mp *mpool.MPool,
) (h *BatchHandle, err error) {
	bat, err := p.getBatchesFromRowIterator(
		ctx, iter, iterKind, pkFilterApplied, start, end, tombstone, mp,
	)
	if err != nil {
		return nil, err
	}
	if bat == nil {
		return nil, nil
	}
	h = NewRowHandle(bat, mp, p, ctx, tombstone)
	return h, nil
}
func (p *baseHandle) getBatchesFromRowIterator(
	ctx context.Context,
	iter RowsIter,
	iterKind string,
	pkFilterApplied bool,
	start, end types.TS,
	tombstone bool,
	mp *mpool.MPool,
) (bat *batch.Batch, err error) {
	defer func() {
		if err != nil && bat != nil {
			bat.Clean(mp)
			bat = nil
		}
	}()
	var scanned, tsMatched, emitted int
	for iter.Next() {
		if scanned&1023 == 0 && ctx != nil {
			select {
			case <-ctx.Done():
				return nil, context.Cause(ctx)
			default:
			}
		}
		scanned++
		entry := iter.Entry()
		if entry == nil {
			return nil, moerr.NewInternalErrorNoCtx("change row iterator returned a nil entry")
		}
		if checkTS(start, end, entry.Time) {
			tsMatched++
			if !entry.Deleted && !tombstone {
				if err = fillInInsertBatchWithSchema(
					&bat, entry, p.changesHandle.dataSchema,
					p.changesHandle.retainRowID, mp,
				); err != nil {
					return nil, err
				}
				emitted++
			}
			if entry.Deleted && tombstone {
				if p.skipTS != nil {
					_, ok := p.skipTS[entry.Time]
					if ok {
						continue
					}
				}
				if err = fillInDeleteBatch(&bat, entry, p.changesHandle.retainRowID, mp); err != nil {
					return nil, err
				}
				emitted++
			}
		}
	}
	if p.changesHandle.debugLabel != "" {
		logutil.Info(
			"ChangesHandle-PKFilterRowIterSummary",
			zap.String("debug-label", p.changesHandle.debugLabel),
			zap.Bool("tombstone", tombstone),
			zap.String("iter-kind", iterKind),
			zap.Bool("has-pk-filter", p.changesHandle.pkFilter != nil && len(p.changesHandle.pkFilter.Segments) > 0),
			zap.Bool("pk-filter-applied", pkFilterApplied),
			zap.Int("scanned", scanned),
			zap.Int("ts-matched", tsMatched),
			zap.Int("emitted", emitted),
		)
	}
	return bat, nil
}
func (p *baseHandle) getObjectEntries(
	objIter btree.IterG[objectio.ObjectEntry],
	start, end types.TS,
) (
	aobj, cnObj []*objectio.ObjectEntry,
	tnByCreateTS map[types.TS][]*objectio.ObjectEntry,
) {
	aobj, cnObj, tnByCreateTS, _ = p.getObjectEntriesWithContext(
		context.Background(), objIter, start, end,
	)
	return
}

func (p *baseHandle) getObjectEntriesWithContext(
	ctx context.Context,
	objIter btree.IterG[objectio.ObjectEntry],
	start, end types.TS,
) (
	aobj, cnObj []*objectio.ObjectEntry,
	tnByCreateTS map[types.TS][]*objectio.ObjectEntry,
	err error,
) {
	if ctx == nil {
		return nil, nil, nil, moerr.NewInvalidInputNoCtx(
			"change object collection requires context",
		)
	}
	aobj = make([]*objectio.ObjectEntry, 0)
	cnObj = make([]*objectio.ObjectEntry, 0)
	tnByCreateTS = make(map[types.TS][]*objectio.ObjectEntry)
	var pkf *engine.PKFilter
	debugLabel := ""
	if p.changesHandle != nil {
		pkf = p.changesHandle.pkFilter
		debugLabel = p.changesHandle.debugLabel
	}
	var (
		totalAppendable, prunedAppendable int
		totalCNCreated, prunedCNCreated   int
		totalTNStatic                     int
	)
	objectIndex := 0
	for objIter.Next() {
		if objectIndex&1023 == 0 {
			select {
			case <-ctx.Done():
				return nil, nil, nil, context.Cause(ctx)
			default:
			}
		}
		objectIndex++
		entry := objIter.Item()
		entryCopy := entry
		if entry.CreateTime.Equal(&txnif.UncommitTS) {
			continue
		}
		if entry.GetAppendable() {
			totalAppendable++
			if entry.CreateTime.GT(&end) {
				continue
			}
			if !entry.DeleteTime.IsEmpty() &&
				!entry.DeleteTime.Equal(&txnif.UncommitTS) &&
				entry.DeleteTime.LT(&start) {
				continue
			}
			// PK zonemap pruning: skip appendable objects whose sort-key range
			// does not overlap with the requested PK values.
			if !p.isTombstone && pkf != nil && pkf.ObjectZMIsPK && len(pkf.Segments) > 0 {
				zm := entry.SortKeyZoneMap()
				if changeZoneMapDisjoint(zm, pkf.Segments) {
					prunedAppendable++
					continue
				}
			}
			aobj = append(aobj, &entryCopy)
		} else {
			if entry.ObjectStats.GetCNCreated() {
				totalCNCreated++
				if entry.CreateTime.LT(&start) || entry.CreateTime.GT(&end) {
					continue
				}
				if !p.isTombstone && pkf != nil && pkf.ObjectZMIsPK && len(pkf.Segments) > 0 {
					zm := entry.SortKeyZoneMap()
					if changeZoneMapDisjoint(zm, pkf.Segments) {
						prunedCNCreated++
						continue
					}
				}
				cnObj = append(cnObj, &entryCopy)
				continue
			}
			totalTNStatic++
			if entry.CreateTime.GT(&end) {
				continue
			}
			// Keep every TN-produced non-appendable object in the create-time index,
			// even when its object-level PK zonemap does not match. A deleted object
			// may have been rewritten into this object by compaction while the queried
			// PK itself was deleted. Pruning the successor before delete-chain
			// resolution would then turn a valid empty result into ErrFileNotFound.
			// Block-level PK pruning still avoids reading unrelated data blocks after
			// the chain has been resolved.
			//
			// Keeping all successors is also required so
			// delete-chain resolution can rewrite a deleted/missing predecessor to the
			// replacement object created at the predecessor's delete timestamp.
			tnByCreateTS[entry.CreateTime] = append(tnByCreateTS[entry.CreateTime], &entryCopy)
			// After checkpoint + GC + restart, older appendable predecessors may be gone;
			// resolveVisibleObjectsByDeleteChain sweeps for orphaned live TN objects.
		}
	}
	goSort.Slice(aobj, func(i, j int) bool {
		return aobj[i].CreateTime.LT(&aobj[j].CreateTime)
	})
	goSort.Slice(cnObj, func(i, j int) bool {
		return cnObj[i].CreateTime.LT(&cnObj[j].CreateTime)
	})
	if debugLabel != "" {
		logutil.Info(
			"ChangesHandle-PKFilterObjectSummary",
			zap.String("debug-label", debugLabel),
			zap.Bool("has-pk-filter", pkf != nil && len(pkf.Segments) > 0),
			zap.Int("appendable-total", totalAppendable),
			zap.Int("appendable-pruned", prunedAppendable),
			zap.Int("cn-created-total", totalCNCreated),
			zap.Int("cn-created-pruned", prunedCNCreated),
			zap.Int("tn-static-total", totalTNStatic),
			zap.Int("tn-static-pruned", 0),
			zap.String("start", start.ToString()),
			zap.String("end", end.ToString()),
		)
	}
	return
}

func (p *baseHandle) resolveVisibleObjectsByDeleteChain(
	ctx context.Context,
	start, end types.TS,
	visible []*objectio.ObjectEntry,
	tnByCreateTS map[types.TS][]*objectio.ObjectEntry,
	isTombstone bool,
	kind string,
) ([]*objectio.ObjectEntry, error) {
	if ctx == nil || p == nil || p.changesHandle == nil {
		return nil, moerr.NewInvalidInputNoCtx(
			"delete-chain resolution requires handle and context",
		)
	}
	if len(visible) == 0 && len(tnByCreateTS) == 0 {
		return visible, nil
	}
	if p.changesHandle.fs == nil {
		return nil, moerr.NewInvalidInputNoCtx(
			"delete-chain resolution requires file service for non-empty inputs",
		)
	}
	resolved := make([]*objectio.ObjectEntry, 0, len(visible))
	queue := make([]*objectio.ObjectEntry, 0, len(visible))
	queue = append(queue, visible...)
	visited := make(map[string]struct{}, len(visible))
	missingCnt := 0
	rewriteHopCnt := 0
	processed := 0
	for len(queue) > 0 {
		if processed&255 == 0 {
			select {
			case <-ctx.Done():
				return nil, context.Cause(ctx)
			default:
			}
		}
		processed++
		current := queue[0]
		queue = queue[1:]
		if current == nil {
			continue
		}
		identity := changeObjectIdentity(current)
		name := current.ObjectShortName().ShortString()
		if _, ok := visited[identity]; ok {
			continue
		}
		visited[identity] = struct{}{}
		// For snapshot-state range replay, we only need terminal objects that are
		// still visible at range end. If an object has already been deleted at or
		// before end, keep following its delete-time chain instead of reading this
		// transient intermediate object.
		if changeObjectDeletedBy(end, current.DeleteTime) {
			next := lookupDeleteChainSuccessor(current.DeleteTime, tnByCreateTS)
			if len(next) == 0 {
				logutil.Warn(
					"ChangesHandle-DeleteChain no successor for non-visible object at end",
					zap.String("kind", kind),
					zap.Bool("tombstone", isTombstone),
					zap.String("start", start.ToString()),
					zap.String("end", end.ToString()),
					zap.String("current", name),
					zap.String("delete-time", current.DeleteTime.ToString()),
				)
				return nil, moerr.NewFileNotFoundNoCtx(current.ObjectName().String())
			}
			rewriteHopCnt++
			queue = append(queue, next...)
			continue
		}
		exists, err := p.objectFileExists(ctx, current)
		if err != nil {
			return nil, err
		}
		if exists {
			resolved = append(resolved, current)
			continue
		}
		missingCnt++
		if current.DeleteTime.IsEmpty() || current.DeleteTime.Equal(&txnif.UncommitTS) {
			logutil.Warn(
				"ChangesHandle-DeleteChain unresolved object without delete-time",
				zap.String("kind", kind),
				zap.Bool("tombstone", isTombstone),
				zap.String("start", start.ToString()),
				zap.String("end", end.ToString()),
				zap.String("missing", name),
			)
			return nil, moerr.NewFileNotFoundNoCtx(current.ObjectName().String())
		}
		next := lookupDeleteChainSuccessor(current.DeleteTime, tnByCreateTS)
		if len(next) == 0 {
			logutil.Warn(
				"ChangesHandle-DeleteChain no replacement at delete-time",
				zap.String("kind", kind),
				zap.Bool("tombstone", isTombstone),
				zap.String("start", start.ToString()),
				zap.String("end", end.ToString()),
				zap.String("missing", name),
				zap.String("delete-time", current.DeleteTime.ToString()),
			)
			return nil, moerr.NewFileNotFoundNoCtx(current.ObjectName().String())
		}
		rewriteHopCnt++
		queue = append(queue, next...)
	}
	// Sweep for orphaned TN objects whose appendable predecessors were GC'd.
	// After checkpoint + GC + restart, no appendable seed remains in the visible
	// set, so these live TN objects are never reached by chain walking above.
	orphanCnt := 0
	orphanIndex := 0
	for _, objs := range tnByCreateTS {
		for _, obj := range objs {
			if orphanIndex&255 == 0 {
				select {
				case <-ctx.Done():
					return nil, context.Cause(ctx)
				default:
				}
			}
			orphanIndex++
			if obj == nil {
				continue
			}
			identity := changeObjectIdentity(obj)
			if _, ok := visited[identity]; ok {
				continue
			}
			visited[identity] = struct{}{}
			if changeObjectDeletedBy(end, obj.DeleteTime) {
				continue
			}
			exists, err := p.objectFileExists(ctx, obj)
			if err != nil {
				return nil, err
			}
			if exists {
				resolved = append(resolved, obj)
				orphanCnt++
			}
		}
	}
	goSort.Slice(resolved, func(i, j int) bool {
		return resolved[i].CreateTime.LT(&resolved[j].CreateTime)
	})
	if missingCnt > 0 || orphanCnt > 0 {
		logutil.Info(
			"ChangesHandle-DeleteChain resolved visible objects",
			zap.String("kind", kind),
			zap.Bool("tombstone", isTombstone),
			zap.String("start", start.ToString()),
			zap.String("end", end.ToString()),
			zap.Int("input-visible", len(visible)),
			zap.Int("output-readable", len(resolved)),
			zap.Int("missing", missingCnt),
			zap.Int("orphan-tn", orphanCnt),
			zap.Int("rewrite-hops", rewriteHopCnt),
		)
	}
	return resolved, nil
}

// lookupDeleteChainSuccessor returns replacement TN non-appendable objects
// created by the same transaction that deleted the predecessor. A later
// create-time alone is not a lineage signal: accepting it can attach an
// unrelated compaction's objects and return rows that never belonged to the
// predecessor. Terminal TN objects whose predecessors were GC'd are recovered
// independently by the orphan sweep above.
func lookupDeleteChainSuccessor(
	deleteTS types.TS,
	tnByCreateTS map[types.TS][]*objectio.ObjectEntry,
) []*objectio.ObjectEntry {
	return tnByCreateTS[deleteTS]
}

// classifyResolvedObjects routes resolved objects into:
//   - cnObjs: still CN-created non-appendable objects (constant commit-ts path)
//   - aobjs: appendable objects and TN-created non-appendable objects
//
// TN-created replacements must run through the row-level commit-ts filter path,
// so they must not remain on the CN-object constant commit-ts path.
func classifyResolvedObjects(groups ...[]*objectio.ObjectEntry) (aobjs, cnObjs []*objectio.ObjectEntry) {
	aobjs = make([]*objectio.ObjectEntry, 0)
	cnObjs = make([]*objectio.ObjectEntry, 0)
	seenA := make(map[string]struct{})
	seenCN := make(map[string]struct{})
	for _, group := range groups {
		for _, obj := range group {
			if obj == nil {
				continue
			}
			name := changeObjectIdentity(obj)
			if obj.ObjectStats.GetCNCreated() {
				if _, ok := seenCN[name]; ok {
					continue
				}
				seenCN[name] = struct{}{}
				cnObjs = append(cnObjs, obj)
				continue
			}
			if _, ok := seenA[name]; ok {
				continue
			}
			seenA[name] = struct{}{}
			aobjs = append(aobjs, obj)
		}
	}
	goSort.Slice(aobjs, func(i, j int) bool {
		return aobjs[i].CreateTime.LT(&aobjs[j].CreateTime)
	})
	goSort.Slice(cnObjs, func(i, j int) bool {
		return cnObjs[i].CreateTime.LT(&cnObjs[j].CreateTime)
	})
	return
}

func (p *baseHandle) objectFileExists(ctx context.Context, obj *objectio.ObjectEntry) (bool, error) {
	if obj == nil {
		return false, nil
	}
	if ctx == nil || p == nil || p.changesHandle == nil || p.changesHandle.fs == nil {
		return false, moerr.NewInvalidInputNoCtx(
			"object existence check requires handle, context, and file service",
		)
	}
	// FastLoadObjectMeta may be satisfied by object-meta cache even after file
	// GC. Use StatFile to validate physical existence before replay.
	_, err := p.changesHandle.fs.StatFile(ctx, obj.ObjectName().String())
	if err == nil {
		return true, nil
	}
	if moerr.IsMoErrCode(err, moerr.ErrFileNotFound) {
		return false, nil
	}
	return false, err
}

type ChangeHandler struct {
	isRecoveryMode  bool // When true, Case 2.2 (insert->delete) will keep the delete for CDC restart scenarios
	tombstoneHandle *baseHandle
	dataHandle      *baseHandle
	coarseMaxRow    int
	quick           bool
	primarySeqnum   int // stable physical sequence number
	primaryPosition int // logical position in dataSchema/output batches
	dataSchema      *engine.CollectChangesSchema
	scheduler       tasks.JobScheduler
	mp              *mpool.MPool
	outputMP        *mpool.MPool

	readDuration, copyDuration    time.Duration
	updateDuration, totalDuration time.Duration
	dataLength, tombstoneLength   int
	lastPrint                     time.Time

	start, end  types.TS
	fs          fileservice.FileService
	minTS       types.TS
	skipDeletes bool

	// commit-ts block prune is only enabled on the exact-range replay path used
	// by snapshot-read semantics; CDC recovery keeps its existing behavior.
	enableCommitTSBlockPrune bool
	strictCommitTSBlockPrune bool

	// When enabled, visible objects that were already GC-ed can be rewritten
	// through delete-time linked TN non-appendable objects before replay starts.
	enableDeleteChainResolve bool

	// pkFilter, when non-nil, enables PK-based pruning at the object, block,
	// and row level.  Only DATA BRANCH PICK sets this; other callers leave it nil.
	pkFilter *engine.PKFilter
	// debugLabel scopes temporary diagnostics to a single CollectChanges call chain.
	debugLabel string

	retainRowID bool
	terminalErr error

	LogThreshold time.Duration
}

func (p *ChangeHandler) terminalError() error {
	if p == nil {
		return nil
	}
	if p.terminalErr != nil {
		return p.terminalErr
	}
	if err := p.dataHandle.terminalError(); err != nil {
		return err
	}
	return p.tombstoneHandle.terminalError()
}

// fail makes post-consumption failures sticky. Once either input cursor has
// advanced, accepting another Next call would omit the consumed rows. Closing
// both streams also bounds retained cache memory while callers hold the failed
// handle.
func (p *ChangeHandler) fail(err error) error {
	if err == nil {
		return nil
	}
	if p.terminalErr == nil {
		p.terminalErr = err
		_ = p.Close()
	}
	return p.terminalErr
}

func (p *ChangeHandler) applyRequestSchema(ctx context.Context) error {
	p.primaryPosition = p.primarySeqnum
	p.dataSchema = engine.CollectChangesSchemaFromContext(ctx)
	if p.pkFilter != nil {
		segmentsValid := changePKFilterSegmentsValid(p.pkFilter.Segments)
		replayValid := p.pkFilter.ReplaySpec == nil ||
			p.pkFilter.ReplaySpec.Op != function.EQUAL ||
			len(p.pkFilter.ReplaySpec.Keys) != 1 ||
			len(p.pkFilter.ReplaySpec.Keys[0]) > 0
		if !segmentsValid || !replayValid {
			// PKFilter is an optional optimization hint. Preserve any valid half
			// and fail open on malformed metadata instead of rejecting a query or
			// allowing binary-search assumptions to create false negatives.
			filterCopy := *p.pkFilter
			if !segmentsValid {
				filterCopy.Segments = nil
				filterCopy.ObjectZMIsPK = false
			}
			if !replayValid {
				filterCopy.ReplaySpec = nil
			}
			if len(filterCopy.Segments) == 0 && filterCopy.ReplaySpec == nil {
				p.pkFilter = nil
			} else {
				p.pkFilter = &filterCopy
			}
			logutil.Warn(
				"ChangesHandle disabled malformed PK filter optimization",
				zap.Bool("segments-valid", segmentsValid),
				zap.Bool("replay-valid", replayValid),
			)
		}
	}
	if p.dataSchema == nil {
		return nil
	}
	if !p.dataSchema.Valid() {
		return moerr.NewInternalErrorNoCtx("collect changes request has an invalid data schema")
	}
	seenSeqnums := make(map[uint16]struct{}, len(p.dataSchema.Seqnums))
	for position, seqnum := range p.dataSchema.Seqnums {
		if p.dataSchema.Attrs[position] == "" || seqnum >= objectio.SEQNUM_UPPER ||
			p.dataSchema.Types[position].Oid == types.T_any {
			return moerr.NewInternalErrorNoCtxf(
				"collect changes request column %d is invalid", position,
			)
		}
		if _, duplicate := seenSeqnums[seqnum]; duplicate {
			return moerr.NewInternalErrorNoCtxf(
				"collect changes request has duplicate sequence %d", seqnum,
			)
		}
		seenSeqnums[seqnum] = struct{}{}
	}
	if p.primarySeqnum < 0 {
		p.primaryPosition = -1
		return nil
	}
	for position, seqnum := range p.dataSchema.Seqnums {
		if int(seqnum) == p.primarySeqnum {
			p.primaryPosition = position
			return nil
		}
	}
	return moerr.NewInternalErrorNoCtxf(
		"collect changes primary sequence %d is absent from the logical schema",
		p.primarySeqnum,
	)
}

type checkpointObjectSelection uint8

const (
	checkpointObjectSelectionRecovery checkpointObjectSelection = iota
	checkpointObjectSelectionRange
)

type checkpointObjectKind uint8

const (
	checkpointObjectKindIgnore checkpointObjectKind = iota
	checkpointObjectKindRowCommitTS
	checkpointObjectKindConstantCommitTS
)

func NewChangesHandlerWithCheckpointEntries(
	ctx context.Context,
	tid uint64,
	sid string,
	checkpoints []*checkpoint.CheckpointEntry,
	start, end types.TS,
	skipDeletes bool,
	maxRow uint32,
	primarySeqnum int,
	mp *mpool.MPool,
	fs fileservice.FileService,
) (changeHandle *ChangeHandler, err error) {
	return newChangesHandlerWithCheckpointEntries(
		ctx,
		tid,
		sid,
		checkpoints,
		start,
		end,
		skipDeletes,
		maxRow,
		primarySeqnum,
		mp,
		fs,
		checkpointObjectSelectionRecovery,
		true,
	)
}

// NewChangesHandlerWithCheckpointRange rebuilds CollectChanges(start, end)
// semantics from checkpoint metadata. It uses the same object eligibility rules
// as the normal partition-state path:
//   - row-commit-ts objects are selected when their object lifetime can still
//     contain rows committed in [start, end]
//   - constant-commit-ts objects are selected by object create ts because that
//     ts is also the commit ts of every row in the object
//
// This keeps snapshot-read recovery aligned with the meaning of the original
// CollectChanges arguments instead of using CDC restart semantics.
func NewChangesHandlerWithCheckpointRange(
	ctx context.Context,
	tid uint64,
	sid string,
	checkpoints []*checkpoint.CheckpointEntry,
	start, end types.TS,
	skipDeletes bool,
	maxRow uint32,
	primarySeqnum int,
	mp *mpool.MPool,
	fs fileservice.FileService,
) (changeHandle *ChangeHandler, err error) {
	return newChangesHandlerWithCheckpointEntries(
		ctx,
		tid,
		sid,
		checkpoints,
		start,
		end,
		skipDeletes,
		maxRow,
		primarySeqnum,
		mp,
		fs,
		checkpointObjectSelectionRange,
		false,
	)
}

// NewChangesHandlerWithCheckpointRangeRecovery rebuilds CollectChanges(start,
// end) from checkpoint metadata using range-aware object selection while
// preserving CDC/checkpoint recovery merge semantics.
func NewChangesHandlerWithCheckpointRangeRecovery(
	ctx context.Context,
	tid uint64,
	sid string,
	checkpoints []*checkpoint.CheckpointEntry,
	start, end types.TS,
	skipDeletes bool,
	maxRow uint32,
	primarySeqnum int,
	mp *mpool.MPool,
	fs fileservice.FileService,
) (changeHandle *ChangeHandler, err error) {
	return newChangesHandlerWithCheckpointEntries(
		ctx,
		tid,
		sid,
		checkpoints,
		start,
		end,
		skipDeletes,
		maxRow,
		primarySeqnum,
		mp,
		fs,
		checkpointObjectSelectionRange,
		true,
	)
}

// NewChangesHandlerWithPartitionStateRange rebuilds CollectChanges(start, end)
// from the partition state visible at the range end snapshot.
//
// Unlike CDC recovery, this path keeps exact range semantics and enables:
//   - delete-time chain rewrite for GC-ed visible objects
//   - commit-ts zonemap block pruning on TN non-appendable objects
//
// It is used only by snapshot-read policies that need exact range meaning after
// normal partition-state replay can no longer read older object files.
func NewChangesHandlerWithPartitionStateRange(
	ctx context.Context,
	state *PartitionState,
	start, end types.TS,
	skipDeletes bool,
	maxRow uint32,
	primarySeqnum int,
	mp *mpool.MPool,
	fs fileservice.FileService,
) (changeHandle *ChangeHandler, err error) {
	if state == nil {
		return nil, moerr.NewInvalidInputNoCtx("partition-state range change collection requires state")
	}
	if err = validateChangeCollectionInputs(
		ctx, start, end, maxRow, primarySeqnum, mp, fs,
	); err != nil {
		return nil, err
	}
	stateStart := state.GetStart()
	if stateStart.GT(&start) {
		logutil.Info("ChangesHandlerWithPartitionStateRange: stateStart > start, proceeding with range-aware scan",
			zap.String("stateStart", stateStart.ToString()),
			zap.String("start", start.ToString()),
			zap.String("end", end.ToString()),
		)
	}
	changeHandle = &ChangeHandler{
		coarseMaxRow:             int(maxRow),
		start:                    start,
		end:                      end,
		fs:                       fs,
		minTS:                    stateStart,
		skipDeletes:              skipDeletes,
		LogThreshold:             LogThreshold,
		primarySeqnum:            primarySeqnum,
		mp:                       mp,
		scheduler:                tasks.NewParallelJobScheduler(LoadParallism),
		enableCommitTSBlockPrune: true,
		strictCommitTSBlockPrune: true,
		enableDeleteChainResolve: true,
		pkFilter:                 engine.PKFilterFromContext(ctx),
		debugLabel:               engine.CollectChangesDebugLabelFromContext(ctx),
		retainRowID:              engine.RetainRowIDFromContext(ctx),
	}
	defer func() {
		if err != nil {
			if changeHandle != nil {
				_ = changeHandle.Close()
				changeHandle = nil
			}
		}
	}()
	if err = changeHandle.applyRequestSchema(ctx); err != nil {
		return nil, err
	}
	changeHandle.tombstoneHandle, err = NewBaseHandler(state, changeHandle, start, end, mp, true, fs, ctx)
	if err != nil {
		return nil, err
	}
	changeHandle.dataHandle, err = NewBaseHandler(state, changeHandle, start, end, mp, false, fs, ctx)
	if err != nil {
		return nil, err
	}
	changeHandle.decideMode()
	if err = changeHandle.dataHandle.init(ctx, changeHandle.quick, mp); err != nil {
		return nil, err
	}
	if err = changeHandle.tombstoneHandle.init(ctx, changeHandle.quick, mp); err != nil {
		return nil, err
	}
	changeHandle.tombstoneHandle.fillInSkipTSFromObjects(
		start,
		end,
		changeHandle.dataHandle.aobjHandle.objects,
		changeHandle.dataHandle.cnObjectHandle.objects,
	)
	logRangeReplaySelection(
		start,
		end,
		changeHandle.dataHandle.aobjHandle.objects,
		changeHandle.dataHandle.cnObjectHandle.objects,
		changeHandle.tombstoneHandle.aobjHandle.objects,
		changeHandle.tombstoneHandle.cnObjectHandle.objects,
	)
	return changeHandle, nil
}

func newChangesHandlerWithCheckpointEntries(
	ctx context.Context,
	tid uint64,
	sid string,
	checkpoints []*checkpoint.CheckpointEntry,
	start, end types.TS,
	skipDeletes bool,
	maxRow uint32,
	primarySeqnum int,
	mp *mpool.MPool,
	fs fileservice.FileService,
	selection checkpointObjectSelection,
	isRecoveryMode bool,
) (changeHandle *ChangeHandler, err error) {
	if err = validateChangeCollectionInputs(
		ctx, start, end, maxRow, primarySeqnum, mp, fs,
	); err != nil {
		return nil, err
	}
	changeHandle = &ChangeHandler{
		coarseMaxRow:   int(maxRow),
		start:          start,
		end:            end,
		fs:             fs,
		minTS:          start,
		skipDeletes:    skipDeletes,
		LogThreshold:   LogThreshold,
		primarySeqnum:  primarySeqnum,
		mp:             mp,
		scheduler:      tasks.NewParallelJobScheduler(LoadParallism),
		isRecoveryMode: isRecoveryMode,
		pkFilter:       engine.PKFilterFromContext(ctx),
		debugLabel:     engine.CollectChangesDebugLabelFromContext(ctx),
		retainRowID:    engine.RetainRowIDFromContext(ctx),
	}
	defer func() {
		if err == nil {
			return
		}
		if changeHandle != nil {
			_ = changeHandle.Close()
			changeHandle = nil
		}
	}()
	if err = changeHandle.applyRequestSchema(ctx); err != nil {
		return nil, err
	}
	if selection == checkpointObjectSelectionRange {
		changeHandle.enableCommitTSBlockPrune = true
		changeHandle.strictCommitTSBlockPrune = true
	}
	dataAobj, dataCNObj, tombstoneAobj, tombstoneCNObj, err := getObjectsFromCheckpointEntries(
		ctx,
		tid,
		sid,
		start,
		end,
		checkpoints,
		mp,
		fs,
		selection,
	)
	if err != nil {
		return
	}
	changeHandle.dataHandle, err = NewBaseHandlerWithObjEntries(ctx, changeHandle, start, end, dataAobj, dataCNObj, false, mp, fs)
	if err != nil {
		return
	}
	changeHandle.tombstoneHandle, err = NewBaseHandlerWithObjEntries(ctx, changeHandle, start, end, tombstoneAobj, tombstoneCNObj, true, mp, fs)
	if err != nil {
		return
	}
	changeHandle.decideMode()
	if err = changeHandle.dataHandle.init(ctx, changeHandle.quick, mp); err != nil {
		return
	}
	if err = changeHandle.tombstoneHandle.init(ctx, changeHandle.quick, mp); err != nil {
		return
	}
	if selection == checkpointObjectSelectionRange {
		changeHandle.tombstoneHandle.fillInSkipTSFromObjects(start, end, dataAobj, dataCNObj)
		logRangeReplaySelection(start, end, dataAobj, dataCNObj, tombstoneAobj, tombstoneCNObj)
	}
	return changeHandle, nil
}

func logRangeReplaySelection(
	start, end types.TS,
	dataAobj, dataCNObj, tombstoneAobj, tombstoneCNObj []*objectio.ObjectEntry,
) {
	sumRows := func(entries []*objectio.ObjectEntry) uint64 {
		var total uint64
		for _, entry := range entries {
			if entry == nil {
				continue
			}
			total += uint64(entry.Rows())
		}
		return total
	}
	logutil.Info(
		"ChangesHandle-RangeReplaySelection",
		zap.String("start", start.ToString()),
		zap.String("end", end.ToString()),
		zap.Int("data-aobj-count", len(dataAobj)),
		zap.Uint64("data-aobj-rows", sumRows(dataAobj)),
		zap.Int("data-cnobj-count", len(dataCNObj)),
		zap.Uint64("data-cnobj-rows", sumRows(dataCNObj)),
		zap.Int("tombstone-aobj-count", len(tombstoneAobj)),
		zap.Uint64("tombstone-aobj-rows", sumRows(tombstoneAobj)),
		zap.Int("tombstone-cnobj-count", len(tombstoneCNObj)),
		zap.Uint64("tombstone-cnobj-rows", sumRows(tombstoneCNObj)),
	)
}

func getObjectsFromCheckpointEntries(
	ctx context.Context,
	tid uint64,
	sid string,
	start, end types.TS,
	checkpoint []*checkpoint.CheckpointEntry,
	mp *mpool.MPool,
	fs fileservice.FileService,
	selection checkpointObjectSelection,
) (
	dataAobj, dataCNObj, tombstoneAobj, tombstoneCNObj []*objectio.ObjectEntry,
	err error,
) {
	defer func() {
		if recovered := recover(); recovered != nil {
			dataAobj, dataCNObj, tombstoneAobj, tombstoneCNObj = nil, nil, nil, nil
			err = moerr.NewInternalErrorNoCtxf(
				"cannot decode checkpoint object entries: %v", recovered,
			)
		}
	}()
	if ctx == nil || mp == nil || fs == nil || start.GT(&end) {
		return nil, nil, nil, nil, moerr.NewInvalidInputNoCtx(
			"checkpoint change collection requires context, ordered timestamps, mpool, and file service",
		)
	}
	if selection != checkpointObjectSelectionRecovery &&
		selection != checkpointObjectSelectionRange {
		return nil, nil, nil, nil, moerr.NewInvalidInputNoCtx(
			"checkpoint change collection has invalid selection mode",
		)
	}
	dataAobjMap := make(map[string]*objectio.ObjectEntry)
	dataCNObjMap := make(map[string]*objectio.ObjectEntry)
	tombstoneAobjMap := make(map[string]*objectio.ObjectEntry)
	tombstoneCNObjMap := make(map[string]*objectio.ObjectEntry)
	readers := make([]checkpointEntryReader, 0)
	for _, entry := range checkpoint {
		if entry == nil {
			return nil, nil, nil, nil, moerr.NewInternalErrorNoCtx(
				"checkpoint change collection received a nil entry",
			)
		}
		reader := newCKPReaderWithTableID(entry.GetVersion(), entry.GetLocation(), tid, mp, fs)
		if checkpointEntryReaderIsNil(reader) {
			return nil, nil, nil, nil, moerr.NewInternalErrorNoCtx(
				"checkpoint reader factory returned nil",
			)
		}
		readers = append(readers, reader)
		if loc := entry.GetLocation(); !loc.IsEmpty() {
			ioutil.Prefetch(sid, fs, loc)
		}
	}
	for _, reader := range readers {
		if err = reader.ReadMeta(ctx); err != nil {
			return
		}
		reader.PrefetchData(sid)
	}

	for _, reader := range readers {
		if err = reader.ConsumeCheckpointWithTableID(
			ctx,
			func(ctx context.Context, fs fileservice.FileService, obj objectio.ObjectEntry, isTombstone bool) (err error) {
				if obj.ObjectStats.IsZero() {
					return moerr.NewInternalErrorNoCtx(
						"checkpoint change collection received empty object statistics",
					)
				}
				if _, validateErr := validateChangeObjectBlockCount(&obj.ObjectStats); validateErr != nil {
					return validateErr
				}
				if obj.CreateTime.Equal(&txnif.UncommitTS) ||
					obj.DeleteTime.Equal(&txnif.UncommitTS) ||
					(!obj.DeleteTime.IsEmpty() && obj.DeleteTime.LT(&obj.CreateTime)) {
					return moerr.NewInternalErrorNoCtx(
						"checkpoint change collection received an invalid object lifetime",
					)
				}
				switch classifyCheckpointObject(obj, isTombstone, start, end, selection) {
				case checkpointObjectKindRowCommitTS:
					if isTombstone {
						tombstoneAobjMap[changeObjectIdentity(&obj)] = &obj
					} else {
						dataAobjMap[changeObjectIdentity(&obj)] = &obj
					}
				case checkpointObjectKindConstantCommitTS:
					if isTombstone {
						tombstoneCNObjMap[changeObjectIdentity(&obj)] = &obj
					} else {
						dataCNObjMap[changeObjectIdentity(&obj)] = &obj
					}
				}
				return
			},
		); err != nil {
			return
		}
	}
	sortByCreateTime := selection == checkpointObjectSelectionRange
	dataAobj = checkpointObjectMapToSlice(dataAobjMap, sortByCreateTime)
	dataCNObj = checkpointObjectMapToSlice(dataCNObjMap, sortByCreateTime)
	tombstoneAobj = checkpointObjectMapToSlice(tombstoneAobjMap, sortByCreateTime)
	tombstoneCNObj = checkpointObjectMapToSlice(tombstoneCNObjMap, sortByCreateTime)
	return
}

func classifyCheckpointObject(
	obj objectio.ObjectEntry,
	isTombstone bool,
	start, end types.TS,
	selection checkpointObjectSelection,
) checkpointObjectKind {
	switch selection {
	case checkpointObjectSelectionRange:
		if obj.GetAppendable() {
			if obj.CreateTime.GT(&end) {
				return checkpointObjectKindIgnore
			}
			if !obj.DeleteTime.IsEmpty() && obj.DeleteTime.LT(&start) {
				return checkpointObjectKindIgnore
			}
			return checkpointObjectKindRowCommitTS
		}
		if obj.GetCNCreated() {
			if obj.CreateTime.LT(&start) || obj.CreateTime.GT(&end) {
				return checkpointObjectKindIgnore
			}
			return checkpointObjectKindConstantCommitTS
		}
		// A TN-created non-appendable object can preserve row-level commit
		// timestamps from an older appendable/backup object.  Its object create
		// time is therefore only a lifetime bound, not the commit time of every
		// row.  Keep any object whose lifetime overlaps the requested range and
		// let updateDataBatch/updateTombstoneBatch apply the exact row filter.
		if obj.CreateTime.GT(&end) ||
			(!obj.DeleteTime.IsEmpty() && obj.DeleteTime.LT(&start)) {
			return checkpointObjectKindIgnore
		}
		return checkpointObjectKindRowCommitTS
	default:
		if obj.GetAppendable() && obj.CreateTime.GE(&start) {
			return checkpointObjectKindRowCommitTS
		}
		if obj.GetCNCreated() && obj.CreateTime.GE(&start) {
			return checkpointObjectKindConstantCommitTS
		}
		return checkpointObjectKindIgnore
	}
}

func checkpointObjectMapToSlice(entries map[string]*objectio.ObjectEntry, sortByCreateTime bool) []*objectio.ObjectEntry {
	ret := make([]*objectio.ObjectEntry, 0, len(entries))
	for _, obj := range entries {
		ret = append(ret, obj)
	}
	if sortByCreateTime {
		goSort.Slice(ret, func(i, j int) bool {
			return ret[i].CreateTime.LT(&ret[j].CreateTime)
		})
	}
	return ret
}

// NewChangesHandler creates a ChangeHandler that reads changes from the partition state.
//
// Error contract:
//   - Returns ErrStaleRead if state.start > start (logical range not covered).
//   - Returns ErrFileNotFound if a referenced object file has been physically
//     deleted by GC. Callers should treat this as recoverable and fall back
//     to the snapshot read path (reading from checkpoint files).
func NewChangesHandler(
	ctx context.Context,
	state *PartitionState,
	start, end types.TS,
	skipDeletes bool,
	maxRow uint32,
	primarySeqnum int,
	mp *mpool.MPool,
	fs fileservice.FileService,
) (changeHandle *ChangeHandler, err error) {
	if state == nil {
		return nil, moerr.NewInvalidInputNoCtx("partition change collection requires state")
	}
	if err = validateChangeCollectionInputs(
		ctx, start, end, maxRow, primarySeqnum, mp, fs,
	); err != nil {
		return nil, err
	}
	if state.start.GT(&start) {
		return nil, moerr.NewErrStaleReadNoCtx(state.start.ToString(), start.ToString())
	}
	changeHandle = &ChangeHandler{
		coarseMaxRow:  int(maxRow),
		start:         start,
		end:           end,
		fs:            fs,
		minTS:         state.start,
		skipDeletes:   skipDeletes,
		LogThreshold:  LogThreshold,
		primarySeqnum: primarySeqnum,
		mp:            mp,
		scheduler:     tasks.NewParallelJobScheduler(LoadParallism),
		pkFilter:      engine.PKFilterFromContext(ctx),
		debugLabel:    engine.CollectChangesDebugLabelFromContext(ctx),
		retainRowID:   engine.RetainRowIDFromContext(ctx),
	}
	if err = changeHandle.applyRequestSchema(ctx); err != nil {
		if changeHandle.scheduler != nil {
			changeHandle.scheduler.Stop()
		}
		return nil, err
	}
	defer func() {
		if err != nil {
			changeHandle.scheduler.Stop()
			changeHandle = nil
		}
	}()
	changeHandle.tombstoneHandle, err = NewBaseHandler(state, changeHandle, start, end, mp, true, fs, ctx)
	if err != nil {
		return
	}
	changeHandle.dataHandle, err = NewBaseHandler(state, changeHandle, start, end, mp, false, fs, ctx)
	if err != nil {
		changeHandle.tombstoneHandle.Close()
		return
	}
	changeHandle.decideMode()
	err = changeHandle.dataHandle.init(ctx, changeHandle.quick, mp)
	if err != nil {
		changeHandle.dataHandle.Close()
		changeHandle.tombstoneHandle.Close()
		return
	}
	err = changeHandle.tombstoneHandle.init(ctx, changeHandle.quick, mp)
	if err != nil {
		changeHandle.dataHandle.Close()
		changeHandle.tombstoneHandle.Close()
	}
	return
}

func (p *ChangeHandler) Close() error {
	if p == nil {
		return nil
	}
	if p.dataHandle != nil {
		p.dataHandle.Close()
	}
	if p.tombstoneHandle != nil {
		p.tombstoneHandle.Close()
	}
	if p.scheduler != nil {
		p.scheduler.Stop()
		p.scheduler = nil
	}
	return nil
}
func (p *ChangeHandler) decideMode() {
	if p.tombstoneHandle.IsEmpty() {
		p.quick = true
		return
	}
	if p.dataHandle.IsEmpty() {
		p.quick = true
		return
	}
	// todo:
	// if p.dataHandle.IsSmall() && p.tombstoneHandle.IsSmall() {
	// 	p.quick = true
	// }
}
func (p *ChangeHandler) decideNextHandle() int {
	tombstoneTS := p.tombstoneHandle.NextTS()
	dataTS := p.dataHandle.NextTS()
	if dataTS.IsEmpty() {
		return NextChangeHandle_Tombstone
	}
	if !tombstoneTS.IsEmpty() && tombstoneTS.LE(&dataTS) {
		return NextChangeHandle_Tombstone
	}
	return NextChangeHandle_Data
}
func (p *ChangeHandler) quickNext(ctx context.Context, mp *mpool.MPool) (data, tombstone *batch.Batch, err error) {
	if terminalErr := p.terminalError(); terminalErr != nil {
		return nil, nil, terminalErr
	}
	for {
		dataEnd := false
		tombstoneEnd := false
		err = p.dataHandle.QuickNext(ctx, &data, mp)
		if moerr.IsMoErrCode(err, moerr.OkExpectedEOF) {
			dataEnd = true
			err = nil
		} else if moerr.IsMoErrCode(err, moerr.OkExpectedEOB) {
			if err = filterBatchWithContext(ctx, data, tombstone, p.primaryPosition, p.skipDeletes, p.isRecoveryMode); err != nil {
				err = p.fail(err)
				return
			}
			return
		}
		if err != nil {
			return
		}
		err = p.tombstoneHandle.QuickNext(ctx, &tombstone, mp)
		if moerr.IsMoErrCode(err, moerr.OkExpectedEOF) {
			tombstoneEnd = true
			err = nil
		} else if moerr.IsMoErrCode(err, moerr.OkExpectedEOB) {
			if err = filterBatchWithContext(ctx, data, tombstone, p.primaryPosition, p.skipDeletes, p.isRecoveryMode); err != nil {
				err = p.fail(err)
				return
			}
			return
		}
		if err != nil {
			return
		}
		if err = filterBatchWithContext(ctx, data, tombstone, p.primaryPosition, p.skipDeletes, p.isRecoveryMode); err != nil {
			err = p.fail(err)
			return
		}
		if tombstoneEnd && dataEnd {
			break
		}
		if dataEnd && tombstone != nil && tombstone.RowCount() > p.coarseMaxRow {
			break
		}
		if tombstoneEnd && data != nil && data.RowCount() > p.coarseMaxRow {
			break
		}
	}
	return
}

func changeBatchHasLeadingRowID(bat *batch.Batch) bool {
	if bat == nil || len(bat.Vecs) == 0 || bat.Vecs[0] == nil {
		return false
	}
	if len(bat.Attrs) == len(bat.Vecs) {
		return bat.Attrs[0] == catalog.Row_ID
	}
	// Attribute-less legacy output used the leading ROWID type as the protocol
	// marker. Complete schemas must use the semantic attribute instead, because
	// a user column can itself have type ROWID.
	return bat.Vecs[0].GetType().Oid == types.T_Rowid
}

// filterBatch merges operations on the same primary key (pk) from data and tombstone batches.
// For each pk, it keeps only the latest operation based on timestamp order.
//
// The function takes:
// - data: batch containing insert/update operations
// - tombstone: batch containing delete operations
// - primarySeqnum: index of primary key column
//
// It works by:
// 1. Building a map of all operations (both data and tombstone) keyed by pk
// 2. For each pk, sorting operations by timestamp
// 3. Marking older operations for deletion to keep only the latest one
// 4. Shrinking both batches to remove the marked rows
//
// This ensures that for any pk, we only keep the most recent operation,
// whether it's an insert/update from data batch or a delete from tombstone batch.
//
// isRecoveryMode: When true (e.g., CDC restart from checkpoint), Case 2.2 (first insert, last delete)
// will keep the delete to ensure downstream consistency. When false (normal operation),
// Case 2.2 deletes all rows since the net effect is "no change".
func filterBatch(data, tombstone *batch.Batch, primarySeqnum int, skipDeletes bool, isRecoveryMode bool) error {
	return filterBatchWithContext(
		context.Background(), data, tombstone, primarySeqnum, skipDeletes, isRecoveryMode,
	)
}

func filterBatchWithContext(
	ctx context.Context,
	data, tombstone *batch.Batch,
	primarySeqnum int,
	skipDeletes bool,
	isRecoveryMode bool,
) (err error) {
	if data == nil || tombstone == nil {
		return
	}
	if ctx == nil {
		return moerr.NewInvalidInputNoCtx("change reconciliation requires context")
	}
	if len(data.Vecs) == 0 || len(tombstone.Vecs) == 0 {
		return moerr.NewInternalErrorNoCtx("cannot reconcile empty change batch schema")
	}

	type rowInfo struct {
		row      int
		ts       types.TS
		isDelete bool
	}

	// Build maps for data and tombstone batches
	rowInfoMap := make(map[any][]rowInfo)
	pkKeyTypeChecked := false
	normalizePKKey := func(value any) (any, error) {
		if bytes, ok := value.([]byte); ok {
			value = string(bytes)
		}
		if !pkKeyTypeChecked {
			keyType := reflect.TypeOf(value)
			if keyType == nil || !keyType.Comparable() {
				return nil, moerr.NewInternalErrorNoCtxf(
					"change batch primary-key value of type %T is not comparable", value,
				)
			}
			pkKeyTypeChecked = true
		}
		return value, nil
	}

	// Process data batch
	dataPKIdx := primarySeqnum
	if changeBatchHasLeadingRowID(data) {
		dataPKIdx++
	}
	if dataPKIdx < 0 || dataPKIdx >= len(data.Vecs) || data.Vecs[dataPKIdx] == nil ||
		data.Vecs[len(data.Vecs)-1] == nil {
		return moerr.NewInternalErrorNoCtxf(
			"invalid data change batch primary-key position %d", dataPKIdx,
		)
	}
	pkVec := data.Vecs[dataPKIdx]
	dataPKType := *pkVec.GetType()
	tsVec := data.Vecs[len(data.Vecs)-1]
	if err = validateChangeBatchShape(data, pkVec.Length(), "data change batch"); err != nil {
		return err
	}
	timestamps, err := ioutil.ValidateTombstoneCommitTSColumn(pkVec.Length(), tsVec)
	if err != nil {
		return err
	}
	for i := 0; i < pkVec.Length(); i++ {
		if i&1023 == 0 {
			select {
			case <-ctx.Done():
				return context.Cause(ctx)
			default:
			}
		}
		if pkVec.IsNull(uint64(i)) {
			return moerr.NewInternalErrorNoCtx("data change batch contains a null primary key")
		}
		pkVal := vector.GetAny(pkVec, i, false)
		if pkVal == nil {
			return moerr.NewInternalErrorNoCtxf(
				"data change batch has unsupported primary-key type %s", dataPKType.String(),
			)
		}
		pkVal, err = normalizePKKey(pkVal)
		if err != nil {
			return err
		}
		rowInfoMap[pkVal] = append(rowInfoMap[pkVal], rowInfo{
			row:      i,
			ts:       timestamps.At(i),
			isDelete: false,
		})
	}

	// Process tombstone batch
	tombstonePKIdx := 0
	tombstoneTSIdx := 1
	if changeBatchHasLeadingRowID(tombstone) {
		tombstonePKIdx = 1
		tombstoneTSIdx = 2
	}
	if tombstonePKIdx >= len(tombstone.Vecs) || tombstoneTSIdx >= len(tombstone.Vecs) ||
		tombstone.Vecs[tombstonePKIdx] == nil || tombstone.Vecs[tombstoneTSIdx] == nil {
		return moerr.NewInternalErrorNoCtx("invalid tombstone change batch schema")
	}
	pkVec = tombstone.Vecs[tombstonePKIdx]
	if *pkVec.GetType() != dataPKType {
		return moerr.NewInternalErrorNoCtxf(
			"change batch primary-key types differ: data %s, tombstone %s",
			dataPKType.String(), pkVec.GetType().String(),
		)
	}
	tsVec = tombstone.Vecs[tombstoneTSIdx]
	if err = validateChangeBatchShape(tombstone, pkVec.Length(), "tombstone change batch"); err != nil {
		return err
	}
	timestamps, err = ioutil.ValidateTombstoneCommitTSColumn(pkVec.Length(), tsVec)
	if err != nil {
		return err
	}
	for i := 0; i < pkVec.Length(); i++ {
		if i&1023 == 0 {
			select {
			case <-ctx.Done():
				return context.Cause(ctx)
			default:
			}
		}
		if pkVec.IsNull(uint64(i)) {
			return moerr.NewInternalErrorNoCtx("tombstone change batch contains a null primary key")
		}
		pkVal := vector.GetAny(pkVec, i, false)
		if pkVal == nil {
			return moerr.NewInternalErrorNoCtxf(
				"tombstone change batch has unsupported primary-key type %s", dataPKType.String(),
			)
		}
		pkVal, err = normalizePKKey(pkVal)
		if err != nil {
			return err
		}
		rowInfoMap[pkVal] = append(rowInfoMap[pkVal], rowInfo{
			row:      i,
			ts:       timestamps.At(i),
			isDelete: true,
		})
	}

	dataRowsToDelete := make([]int64, 0)
	tombstoneRowsToDelete := make([]int64, 0)

	processedKeys := 0
	for _, rowInfos := range rowInfoMap {
		if processedKeys&1023 == 0 {
			select {
			case <-ctx.Done():
				return context.Cause(ctx)
			default:
			}
		}
		processedKeys++
		// Sort by timestamp
		goSort.Slice(rowInfos, func(i, j int) bool {
			if rowInfos[i].ts.EQ(&rowInfos[j].ts) {
				if rowInfos[i].isDelete && !rowInfos[j].isDelete {
					return true
				}
				return false
			}
			return rowInfos[i].ts.LT(&rowInfos[j].ts)
		})

		if len(rowInfos) <= 1 {
			continue
		}

		first := rowInfos[0]
		last := rowInfos[len(rowInfos)-1]

		// Case 1: First is delete
		if first.isDelete {
			if !last.isDelete {
				if skipDeletes {
					// Keep only last insert
					for _, ri := range rowInfos[0 : len(rowInfos)-1] {
						if ri.isDelete {
							tombstoneRowsToDelete = append(tombstoneRowsToDelete, int64(ri.row))
						} else {
							dataRowsToDelete = append(dataRowsToDelete, int64(ri.row))
						}
					}
				} else {
					// Keep first delete and last insert
					for _, ri := range rowInfos[1 : len(rowInfos)-1] {
						if ri.isDelete {
							tombstoneRowsToDelete = append(tombstoneRowsToDelete, int64(ri.row))
						} else {
							dataRowsToDelete = append(dataRowsToDelete, int64(ri.row))
						}
					}
				}
			} else {
				// Keep only last delete
				for _, ri := range rowInfos[:len(rowInfos)-1] {
					if ri.isDelete {
						tombstoneRowsToDelete = append(tombstoneRowsToDelete, int64(ri.row))
					} else {
						dataRowsToDelete = append(dataRowsToDelete, int64(ri.row))
					}
				}
			}
		} else {
			// Case 2: First is insert
			if !last.isDelete {
				// Keep only last insert
				for _, ri := range rowInfos[:len(rowInfos)-1] {
					if ri.isDelete {
						tombstoneRowsToDelete = append(tombstoneRowsToDelete, int64(ri.row))
					} else {
						dataRowsToDelete = append(dataRowsToDelete, int64(ri.row))
					}
				}
			} else {
				// Case 2.2: First is insert, last is delete
				if isRecoveryMode {
					// Recovery mode (e.g., CDC restart): Keep the last delete
					// This ensures that if the insert was already sent to downstream
					// before CDC restart, the delete will still be sent to maintain consistency.
					for _, ri := range rowInfos[:len(rowInfos)-1] {
						if ri.isDelete {
							tombstoneRowsToDelete = append(tombstoneRowsToDelete, int64(ri.row))
						} else {
							dataRowsToDelete = append(dataRowsToDelete, int64(ri.row))
						}
					}
				} else {
					// Normal mode: Delete all rows (both insert and delete)
					// Net effect: PK was created and deleted in this range, so no change to report
					for _, ri := range rowInfos {
						if ri.isDelete {
							tombstoneRowsToDelete = append(tombstoneRowsToDelete, int64(ri.row))
						} else {
							dataRowsToDelete = append(dataRowsToDelete, int64(ri.row))
						}
					}
				}
			}
		}
	}

	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	default:
	}
	goSort.Slice(tombstoneRowsToDelete, func(i, j int) bool {
		return tombstoneRowsToDelete[i] < tombstoneRowsToDelete[j]
	})
	goSort.Slice(dataRowsToDelete, func(i, j int) bool {
		return dataRowsToDelete[i] < dataRowsToDelete[j]
	})
	tombstone.Shrink(tombstoneRowsToDelete, true)
	data.Shrink(dataRowsToDelete, true)
	return
}
func (p *ChangeHandler) Next(ctx context.Context, mp *mpool.MPool) (data, tombstone *batch.Batch, hint engine.ChangesHandle_Hint, err error) {
	if p == nil || ctx == nil || mp == nil {
		return nil, nil, engine.ChangesHandle_Tail_done, moerr.NewInvalidInputNoCtx(
			"change collection requires handle, context, and mpool",
		)
	}
	if terminalErr := p.terminalError(); terminalErr != nil {
		return nil, nil, engine.ChangesHandle_Tail_done, terminalErr
	}
	if p.outputMP == nil {
		p.outputMP = mp
	} else if p.outputMP != mp {
		return nil, nil, engine.ChangesHandle_Tail_done, moerr.NewInvalidInputNoCtx(
			"change collection cannot switch output mpool",
		)
	}
	if time.Since(p.lastPrint) > p.LogThreshold {
		p.lastPrint = time.Now()
		if p.dataLength != 0 || p.tombstoneLength != 0 {
			// use the max compact checkpoint end ts as the gc ts
			gcTS, err := ckputil.GetMaxTSOfCompactCKP(ctx, p.fs)
			if err != nil {
				logutil.Warnf("ChangesHandle-Slow, get GC TS failed: %v", err)
			}
			logutil.Warn(
				"SLOW-LOG-ChangeHandle",
				zap.String("start", p.start.ToString()),
				zap.String("min-ts", p.minTS.ToString()),
				zap.String("gc-ts", gcTS.ToString()),
				zap.Int("data-length", p.dataLength),
				zap.Int("tombstone-length", p.tombstoneLength),
				zap.Duration("read-duration", p.readDuration),
				zap.Duration("copy-duration", p.copyDuration),
				zap.Duration("update-duration", p.updateDuration),
				zap.Duration("total-duration", p.totalDuration),
			)
		}
	}
	defer func() {
		if err != nil {
			if data != nil {
				data.Clean(mp)
				data = nil
			}
			if tombstone != nil {
				tombstone.Clean(mp)
				tombstone = nil
			}
			return
		}
		if data != nil && data.RowCount() == 0 {
			data.Clean(mp)
			data = nil
		}
		if tombstone != nil && tombstone.RowCount() == 0 {
			tombstone.Clean(mp)
			tombstone = nil
		}
	}()
	hint = engine.ChangesHandle_Tail_done
	t0 := time.Now()
	if p.quick {
		if data, tombstone, err = p.quickNext(ctx, mp); err != nil {
			if terminalErr := p.terminalError(); terminalErr != nil {
				err = terminalErr
			} else if data != nil || tombstone != nil {
				err = p.fail(err)
			}
			return
		}
		p.totalDuration += time.Since(t0)
		p.dataLength += changeBatchMetricRows(data)
		p.tombstoneLength += changeBatchMetricRows(tombstone)
		return
	}
	for {
		typ := p.decideNextHandle()
		if terminalErr := p.terminalError(); terminalErr != nil {
			err = terminalErr
			return
		}
		switch typ {
		case NextChangeHandle_Data:
			err = p.dataHandle.Next(ctx, &data, mp)
			if err == nil && (data == nil || len(data.Vecs) == 0 || data.Vecs[0] == nil) {
				err = p.fail(moerr.NewInternalErrorNoCtx(
					"data change handle returned no batch",
				))
			}
			if err == nil && data.Vecs[0].Length() >= p.coarseMaxRow*2 {
				if err = filterBatchWithContext(ctx, data, tombstone, p.primaryPosition, p.skipDeletes, p.isRecoveryMode); err != nil {
					err = p.fail(err)
					return
				}
				if data.Vecs[0].Length() > p.coarseMaxRow {
					p.totalDuration += time.Since(t0)
					p.dataLength += changeBatchMetricRows(data)
					p.tombstoneLength += changeBatchMetricRows(tombstone)
					return
				}
			}
		case NextChangeHandle_Tombstone:
			err = p.tombstoneHandle.Next(ctx, &tombstone, mp)
			if err == nil && (tombstone == nil || len(tombstone.Vecs) == 0 || tombstone.Vecs[0] == nil) {
				err = p.fail(moerr.NewInternalErrorNoCtx(
					"tombstone change handle returned no batch",
				))
			}
			if err == nil && tombstone.Vecs[0].Length() >= p.coarseMaxRow*2 {
				if err = filterBatchWithContext(ctx, data, tombstone, p.primaryPosition, p.skipDeletes, p.isRecoveryMode); err != nil {
					err = p.fail(err)
					return
				}
				if tombstone.Vecs[0].Length() > p.coarseMaxRow {
					p.totalDuration += time.Since(t0)
					p.dataLength += changeBatchMetricRows(data)
					p.tombstoneLength += changeBatchMetricRows(tombstone)
					return
				}
			}
		}
		if moerr.IsMoErrCode(err, moerr.OkExpectedEOB) {
			err = nil
			if data != nil || tombstone != nil {
				if err = filterBatchWithContext(ctx, data, tombstone, p.primaryPosition, p.skipDeletes, p.isRecoveryMode); err != nil {
					err = p.fail(err)
					return
				}
				p.totalDuration += time.Since(t0)
				p.dataLength += changeBatchMetricRows(data)
				p.tombstoneLength += changeBatchMetricRows(tombstone)
				return
			}
			continue
		}
		if moerr.IsMoErrCode(err, moerr.OkExpectedEOF) {
			err = nil
			if err = filterBatchWithContext(ctx, data, tombstone, p.primaryPosition, p.skipDeletes, p.isRecoveryMode); err != nil {
				err = p.fail(err)
				return
			}
			p.totalDuration += time.Since(t0)
			p.dataLength += changeBatchMetricRows(data)
			p.tombstoneLength += changeBatchMetricRows(tombstone)
			return
		}
		if err != nil {
			if terminalErr := p.terminalError(); terminalErr != nil {
				err = terminalErr
			} else if data != nil || tombstone != nil {
				err = p.fail(err)
			}
			p.totalDuration += time.Since(t0)
			p.dataLength += changeBatchMetricRows(data)
			p.tombstoneLength += changeBatchMetricRows(tombstone)
			return
		}
	}
}

func changeBatchMetricRows(bat *batch.Batch) int {
	if bat == nil || len(bat.Vecs) == 0 || bat.Vecs[0] == nil {
		return 0
	}
	return bat.Vecs[0].Length()
}

func validateChangeBatchShape(bat *batch.Batch, rowCount int, kind string) error {
	if bat == nil {
		return moerr.NewInternalErrorNoCtxf("%s is nil", kind)
	}
	if rowCount < 0 || bat.RowCount() != rowCount {
		return moerr.NewInternalErrorNoCtxf(
			"%s reports %d rows, expected %d", kind, bat.RowCount(), rowCount,
		)
	}
	for pos, vec := range bat.Vecs {
		if vec == nil {
			return moerr.NewInternalErrorNoCtxf("%s column %d is nil", kind, pos)
		}
		if vec.Length() != rowCount {
			return moerr.NewInternalErrorNoCtxf(
				"%s column %d has %d rows, expected %d",
				kind, pos, vec.Length(), rowCount,
			)
		}
	}
	return nil
}

func applyTSFilterForBatch(bat *batch.Batch, sortIdx int, skipTS map[types.TS]struct{}, start, end types.TS) error {
	if bat == nil {
		return nil
	}
	if len(bat.Vecs) == 0 || sortIdx < 0 || sortIdx >= len(bat.Vecs) {
		return moerr.NewInternalErrorNoCtx("invalid commit-ts position in change batch")
	}
	if bat.Vecs[sortIdx] == nil {
		return moerr.NewInternalErrorNoCtx("nil commit-ts column in change batch")
	}
	if bat.Vecs[sortIdx].GetType().Oid != types.T_TS {
		return moerr.NewInternalErrorNoCtxf(
			"change batch commit column %d has type %s, expected TS",
			sortIdx, bat.Vecs[sortIdx].GetType().String(),
		)
	}
	if bat.Vecs[0] == nil {
		return moerr.NewInternalErrorNoCtx("nil leading column in change batch")
	}
	rowCount := bat.Vecs[0].Length()
	if err := validateChangeBatchShape(bat, rowCount, "change batch"); err != nil {
		return err
	}
	commitTSs, err := ioutil.ValidateTombstoneCommitTSColumn(rowCount, bat.Vecs[sortIdx])
	if err != nil {
		return err
	}
	deletes := make([]int64, 0)
	for i := 0; i < rowCount; i++ {
		ts := commitTSs.At(i)
		if ts.Equal(&txnif.UncommitTS) || ts.LT(&start) || ts.GT(&end) {
			deletes = append(deletes, int64(i))
		} else {
			if skipTS != nil {
				_, ok := skipTS[ts]
				if ok {
					deletes = append(deletes, int64(i))
				}
			}
		}
	}
	bat.Shrink(deletes, true)
	return nil
}
func sortBatch(bat *batch.Batch, sortIdx int, mp *mpool.MPool) error {
	if bat == nil {
		return nil
	}
	if len(bat.Vecs) == 0 || sortIdx < 0 || sortIdx >= len(bat.Vecs) || bat.Vecs[sortIdx] == nil {
		return moerr.NewInternalErrorNoCtx("invalid commit-ts position in change batch sort")
	}
	if bat.Vecs[sortIdx].GetType().Oid != types.T_TS {
		return moerr.NewInternalErrorNoCtxf(
			"change batch sort column %d has type %s, expected TS",
			sortIdx, bat.Vecs[sortIdx].GetType().String(),
		)
	}
	if bat.Vecs[0] == nil {
		return moerr.NewInternalErrorNoCtx("nil leading column in change batch sort")
	}
	rowCount := bat.Vecs[0].Length()
	if err := validateChangeBatchShape(bat, rowCount, "change batch sort"); err != nil {
		return err
	}
	if _, err := ioutil.ValidateTombstoneCommitTSColumn(rowCount, bat.Vecs[sortIdx]); err != nil {
		return err
	}
	sortedIdx := make([]int64, rowCount)
	for i := 0; i < len(sortedIdx); i++ {
		sortedIdx[i] = int64(i)
	}
	sort.Sort(false, false, true, sortedIdx, bat.Vecs[sortIdx])
	for i := 0; i < len(bat.Vecs); i++ {
		err := bat.Vecs[i].Shuffle(sortedIdx, mp)
		if err != nil {
			return err
		}
	}
	return nil
}

//func checkObjectEntry(entry *ObjectEntry, start, end types.TS) bool {
//	if entry.GetAppendable() {
//		if entry.CreateTime.GT(&end) {
//			return false
//		}
//		if !entry.DeleteTime.IsEmpty() && entry.DeleteTime.LT(&start) {
//			return false
//		}
//		return true
//	} else {
//		if !entry.ObjectStats.GetCNCreated() {
//			return false
//		}
//		return entry.CreateTime.GE(&start) && entry.DeleteTime.LE(&end)
//	}
//}

func newDataBatchWithBatch(src *batch.Batch, retainRowID bool) (data *batch.Batch) {
	data = batch.NewWithSize(0)
	hasCompleteAttrs := len(src.Attrs) == len(src.Vecs)
	if retainRowID {
		data.Vecs = append(data.Vecs, vector.NewVec(types.T_Rowid.ToType()))
		if hasCompleteAttrs {
			data.Attrs = append(data.Attrs, catalog.Row_ID)
		}
	}
	if hasCompleteAttrs {
		data.Attrs = append(data.Attrs, src.Attrs[2:]...)
	}
	// RowEntry batches have a positional protocol: [rowid, commitTS, users...].
	// User columns may themselves be T_TS or T_Rowid, so type-based stripping
	// corrupts the output schema.
	for _, vec := range src.Vecs[2:] {
		newVec := vector.NewVec(*vec.GetType())
		data.Vecs = append(data.Vecs, newVec)
	}
	if hasCompleteAttrs {
		data.Attrs = append(data.Attrs, objectio.DefaultCommitTS_Attr)
	}
	newVec := vector.NewVec(types.T_TS.ToType())
	data.Vecs = append(data.Vecs, newVec)
	return
}

func appendFromEntry(src, dst *vector.Vector, offset int, mp *mpool.MPool) error {
	if src == nil || dst == nil || offset < 0 || offset >= src.Length() ||
		*src.GetType() != *dst.GetType() {
		return moerr.NewInternalErrorNoCtx("invalid change-row append source")
	}
	return dst.UnionOne(src, int64(offset), mp)
}

func validateReplayRowSource(entry *RowEntry, sourceRows, sourceRow int) error {
	rowIDs, err := ioutil.ValidateTombstoneRowIDColumn(
		sourceRows, entry.Batch.Vecs[0],
	)
	if err != nil {
		return err
	}
	commits, err := ioutil.ValidateTombstoneCommitTSColumn(
		sourceRows, entry.Batch.Vecs[1],
	)
	if err != nil {
		return err
	}
	commitTS := commits.At(sourceRow)
	if commitTS.Equal(&txnif.UncommitTS) {
		return moerr.NewInternalErrorNoCtx("change source row is uncommitted")
	}
	if !entry.Time.IsEmpty() && !commitTS.Equal(&entry.Time) {
		return moerr.NewInternalErrorNoCtxf(
			"change source commit timestamp %s does not match row entry %s",
			commitTS.ToString(), entry.Time.ToString(),
		)
	}
	var zeroRowID types.Rowid
	if entry.RowID != zeroRowID && !rowIDs[sourceRow].EQ(&entry.RowID) {
		return moerr.NewInternalErrorNoCtx("change source rowid does not match row entry")
	}
	return nil
}

func fillInInsertBatch(
	bat **batch.Batch,
	entry *RowEntry,
	retainRowID bool,
	mp *mpool.MPool,
) error {
	return fillInInsertBatchWithSchema(bat, entry, nil, retainRowID, mp)
}

func fillInInsertBatchWithSchema(
	bat **batch.Batch,
	entry *RowEntry,
	schema *engine.CollectChangesSchema,
	retainRowID bool,
	mp *mpool.MPool,
) error {
	if schema != nil {
		return fillInInsertBatchUsingSchema(bat, entry, schema, retainRowID, mp)
	}
	if bat == nil || entry == nil || entry.Batch == nil || len(entry.Batch.Vecs) < 3 || mp == nil {
		return moerr.NewInvalidInputNoCtx("insert change row requires output, source batch, and mpool")
	}
	sourceRow := int(entry.Offset)
	sourceRows := entry.Batch.RowCount()
	if sourceRow < 0 || sourceRow >= sourceRows {
		return moerr.NewInternalErrorNoCtxf("insert change source has no row %d", sourceRow)
	}
	if err := validateChangeBatchShape(entry.Batch, sourceRows, "insert change source"); err != nil {
		return err
	}
	if err := validateReplayRowSource(entry, sourceRows, sourceRow); err != nil {
		return err
	}
	if *bat == nil {
		*bat = newDataBatchWithBatch(entry.Batch, retainRowID)
	}
	dst := *bat
	expectedColumns := len(entry.Batch.Vecs) - 1
	if retainRowID {
		expectedColumns++
	}
	if len(dst.Vecs) != expectedColumns || len(dst.Vecs) == 0 {
		return moerr.NewInternalErrorNoCtx("insert change destination schema is inconsistent")
	}
	if len(entry.Batch.Attrs) == len(entry.Batch.Vecs) {
		if len(dst.Attrs) != expectedColumns {
			return moerr.NewInternalErrorNoCtx("insert change destination attributes are inconsistent")
		}
		destPos := 0
		if retainRowID {
			if dst.Attrs[0] != catalog.Row_ID {
				return moerr.NewInternalErrorNoCtx("insert change destination rowid attribute is inconsistent")
			}
			destPos++
		}
		for sourcePos := 2; sourcePos < len(entry.Batch.Attrs); sourcePos++ {
			if dst.Attrs[destPos] != entry.Batch.Attrs[sourcePos] {
				return moerr.NewInternalErrorNoCtxf(
					"insert change destination attribute %d is %q, expected %q",
					destPos, dst.Attrs[destPos], entry.Batch.Attrs[sourcePos],
				)
			}
			destPos++
		}
		if dst.Attrs[destPos] != objectio.DefaultCommitTS_Attr {
			return moerr.NewInternalErrorNoCtxf(
				"insert change destination attribute %d is %q, expected %q",
				destPos, dst.Attrs[destPos], objectio.DefaultCommitTS_Attr,
			)
		}
	} else if len(dst.Attrs) != 0 {
		return moerr.NewInternalErrorNoCtx("insert change destination attributes are inconsistent")
	}
	oldRows := dst.Vecs[0].Length()
	if dst.RowCount() != oldRows {
		return moerr.NewInternalErrorNoCtx("insert change destination row count is inconsistent")
	}
	const inlineColumns = 16
	var inline [inlineColumns]vector.AppendCheckpoint
	checkpoints := inline[:]
	if len(dst.Vecs) <= inlineColumns {
		checkpoints = checkpoints[:len(dst.Vecs)]
	} else {
		checkpoints = make([]vector.AppendCheckpoint, len(dst.Vecs))
	}
	for destPos, dest := range dst.Vecs {
		sourcePos := destPos + 2
		if retainRowID {
			if destPos == 0 {
				sourcePos = 0
			} else {
				sourcePos = destPos + 1
			}
		}
		if destPos == len(dst.Vecs)-1 {
			sourcePos = 1
		}
		source := entry.Batch.Vecs[sourcePos]
		if dest == nil || dest.IsConst() || dest.NeedDup() || dest.Length() != oldRows ||
			*dest.GetType() != *source.GetType() || dest == source {
			return moerr.NewInternalErrorNoCtxf("insert change destination column %d is not appendable", destPos)
		}
		checkpoints[destPos] = dest.MakeAppendCheckpoint()
	}
	for destPos, dest := range dst.Vecs {
		sourcePos := destPos + 2
		if retainRowID {
			if destPos == 0 {
				sourcePos = 0
			} else {
				sourcePos = destPos + 1
			}
		}
		if destPos == len(dst.Vecs)-1 {
			sourcePos = 1
		}
		if err := appendFromEntry(entry.Batch.Vecs[sourcePos], dest, sourceRow, mp); err != nil {
			for pos, rollbackVec := range dst.Vecs {
				rollbackVec.RollbackAppend(checkpoints[pos], 1)
			}
			return err
		}
	}
	dst.SetRowCount(oldRows + 1)
	return nil
}

func fillInInsertBatchUsingSchema(
	bat **batch.Batch,
	entry *RowEntry,
	schema *engine.CollectChangesSchema,
	retainRowID bool,
	mp *mpool.MPool,
) error {
	if bat == nil || entry == nil || entry.Batch == nil || schema == nil || !schema.Valid() || mp == nil ||
		len(entry.Batch.Vecs) < 2 {
		return moerr.NewInvalidInputNoCtx(
			"insert change row requires output, source batch, schema, and mpool",
		)
	}
	sourceRow := int(entry.Offset)
	sourceRows := entry.Batch.RowCount()
	if sourceRow < 0 || sourceRow >= sourceRows {
		return moerr.NewInternalErrorNoCtxf("insert change source has no row %d", sourceRow)
	}
	hasCompleteAttrs := len(entry.Batch.Attrs) == len(entry.Batch.Vecs)
	if len(entry.Batch.Attrs) != 0 && !hasCompleteAttrs {
		return moerr.NewInternalErrorNoCtx("insert change source has partial attributes")
	}
	for position, source := range entry.Batch.Vecs {
		if source == nil {
			return moerr.NewInternalErrorNoCtxf(
				"insert change source column %d is nil", position,
			)
		}
		placeholder := position >= 2 && hasCompleteAttrs && entry.Batch.Attrs[position] == ""
		if placeholder {
			if source.Length() == 0 {
				continue
			}
			return moerr.NewInternalErrorNoCtxf(
				"insert change source placeholder column %d contains %d rows",
				position, source.Length(),
			)
		}
		if source.Length() != sourceRows {
			return moerr.NewInternalErrorNoCtxf(
				"insert change source column %d has %d rows, expected %d",
				position, source.Length(), sourceRows,
			)
		}
	}
	if err := validateReplayRowSource(entry, sourceRows, sourceRow); err != nil {
		return err
	}

	expectedColumns := len(schema.Seqnums) + 1
	if retainRowID {
		expectedColumns++
	}
	if *bat == nil {
		result := batch.NewWithSize(expectedColumns)
		position := 0
		if retainRowID {
			result.Attrs = append(result.Attrs, catalog.Row_ID)
			result.Vecs[position] = vector.NewVec(types.T_Rowid.ToType())
			position++
		}
		for logicalPosition := range schema.Seqnums {
			result.Attrs = append(result.Attrs, schema.Attrs[logicalPosition])
			result.Vecs[position] = vector.NewVec(schema.Types[logicalPosition])
			position++
		}
		result.Attrs = append(result.Attrs, objectio.DefaultCommitTS_Attr)
		result.Vecs[position] = vector.NewVec(types.T_TS.ToType())
		*bat = result
	}
	dst := *bat
	if len(dst.Vecs) != expectedColumns || len(dst.Attrs) != expectedColumns {
		return moerr.NewInternalErrorNoCtx("insert change destination schema is inconsistent")
	}
	oldRows := dst.RowCount()
	for position, dstVec := range dst.Vecs {
		if dstVec == nil || dstVec.IsConst() || dstVec.NeedDup() || dstVec.Length() != oldRows {
			return moerr.NewInternalErrorNoCtxf(
				"insert change destination column %d is not appendable", position,
			)
		}
	}
	destinationPosition := 0
	if retainRowID {
		if dst.Attrs[0] != catalog.Row_ID || dst.Vecs[0].GetType().Oid != types.T_Rowid {
			return moerr.NewInternalErrorNoCtx("insert change destination rowid is incompatible")
		}
		destinationPosition++
	}
	for logicalPosition := range schema.Seqnums {
		if dst.Attrs[destinationPosition] != schema.Attrs[logicalPosition] ||
			*dst.Vecs[destinationPosition].GetType() != schema.Types[logicalPosition] {
			return moerr.NewInternalErrorNoCtxf(
				"insert change destination column %d is incompatible", destinationPosition,
			)
		}
		destinationPosition++
	}
	if dst.Attrs[destinationPosition] != objectio.DefaultCommitTS_Attr ||
		dst.Vecs[destinationPosition].GetType().Oid != types.T_TS {
		return moerr.NewInternalErrorNoCtx("insert change destination commit-ts is incompatible")
	}

	const inlineColumns = 16
	var inline [inlineColumns]vector.AppendCheckpoint
	checkpoints := inline[:]
	if len(dst.Vecs) <= inlineColumns {
		checkpoints = checkpoints[:len(dst.Vecs)]
	} else {
		checkpoints = make([]vector.AppendCheckpoint, len(dst.Vecs))
	}
	for position, dstVec := range dst.Vecs {
		checkpoints[position] = dstVec.MakeAppendCheckpoint()
	}
	rollback := func() {
		for position, dstVec := range dst.Vecs {
			dstVec.RollbackAppend(checkpoints[position], 1)
		}
	}
	appendSource := func(destination int, source *vector.Vector) error {
		if err := appendFromEntry(source, dst.Vecs[destination], sourceRow, mp); err != nil {
			rollback()
			return err
		}
		return nil
	}
	destinationPosition = 0
	if retainRowID {
		if err := appendSource(destinationPosition, entry.Batch.Vecs[0]); err != nil {
			return err
		}
		destinationPosition++
	}
	for logicalPosition, seqnum := range schema.Seqnums {
		sourcePosition := 2 + int(seqnum)
		sourcePresent := sourcePosition < len(entry.Batch.Vecs)
		if sourcePresent && hasCompleteAttrs && entry.Batch.Attrs[sourcePosition] == "" &&
			entry.Batch.Vecs[sourcePosition].Length() == 0 {
			sourcePresent = false
		}
		if sourcePresent && entry.Batch.Vecs[sourcePosition].Length() == 0 {
			sourcePresent = false
		}
		if sourcePresent {
			source := entry.Batch.Vecs[sourcePosition]
			if source.Length() != sourceRows || *source.GetType() != schema.Types[logicalPosition] {
				rollback()
				return moerr.NewInternalErrorNoCtxf(
					"insert change source column %q at sequence %d is incompatible",
					schema.Attrs[logicalPosition], seqnum,
				)
			}
			if err := appendSource(destinationPosition, source); err != nil {
				return err
			}
		} else if err := vector.AppendNull(dst.Vecs[destinationPosition], mp); err != nil {
			rollback()
			return err
		}
		destinationPosition++
	}
	if err := appendSource(destinationPosition, entry.Batch.Vecs[1]); err != nil {
		return err
	}
	dst.SetRowCount(oldRows + 1)
	return nil
}

func fillInDeleteBatch(
	bat **batch.Batch,
	entry *RowEntry,
	retainRowID bool,
	mp *mpool.MPool,
) error {
	if bat == nil || entry == nil || entry.Batch == nil || len(entry.Batch.Vecs) < 3 || mp == nil {
		return moerr.NewInvalidInputNoCtx("delete change row requires output, source batch, and mpool")
	}
	sourceRow := int(entry.Offset)
	sourceRows := entry.Batch.RowCount()
	if sourceRow < 0 || sourceRow >= sourceRows {
		return moerr.NewInternalErrorNoCtx("delete change source row is unavailable")
	}
	if err := validateChangeBatchShape(entry.Batch, sourceRows, "delete change source"); err != nil {
		return err
	}
	rowIDSource, pkVec := entry.Batch.Vecs[0], entry.Batch.Vecs[2]
	if err := validateReplayRowSource(entry, sourceRows, sourceRow); err != nil {
		return err
	}
	if pkVec == nil {
		return moerr.NewInternalErrorNoCtx("delete change source row is unavailable")
	}
	if *bat == nil {
		vecCnt := 2
		attrs := []string{objectio.TombstoneAttr_PK_Attr, objectio.DefaultCommitTS_Attr}
		if retainRowID {
			vecCnt = 3
			attrs = []string{catalog.Row_ID, objectio.TombstoneAttr_PK_Attr, objectio.DefaultCommitTS_Attr}
		}
		(*bat) = batch.NewWithSize(vecCnt)
		(*bat).SetAttributes(attrs)
		if retainRowID {
			(*bat).Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
			(*bat).Vecs[1] = vector.NewVec(*pkVec.GetType())
			(*bat).Vecs[2] = vector.NewVec(types.T_TS.ToType())
		} else {
			(*bat).Vecs[0] = vector.NewVec(*pkVec.GetType())
			(*bat).Vecs[1] = vector.NewVec(types.T_TS.ToType())
		}
	}
	dst := *bat
	pkIdx := 0
	tsIdx := 1
	if retainRowID {
		pkIdx = 1
		tsIdx = 2
	}
	if len(dst.Vecs) != tsIdx+1 || len(dst.Vecs) == 0 {
		return moerr.NewInternalErrorNoCtx("delete change destination schema is inconsistent")
	}
	expectedAttrs := []string{
		objectio.TombstoneAttr_PK_Attr,
		objectio.DefaultCommitTS_Attr,
	}
	if retainRowID {
		expectedAttrs = []string{
			catalog.Row_ID,
			objectio.TombstoneAttr_PK_Attr,
			objectio.DefaultCommitTS_Attr,
		}
	}
	if len(dst.Attrs) != len(expectedAttrs) {
		return moerr.NewInternalErrorNoCtx("delete change destination attributes are inconsistent")
	}
	for pos := range expectedAttrs {
		if dst.Attrs[pos] != expectedAttrs[pos] {
			return moerr.NewInternalErrorNoCtxf(
				"delete change destination attribute %d is %q, expected %q",
				pos, dst.Attrs[pos], expectedAttrs[pos],
			)
		}
	}
	oldRows := dst.Vecs[0].Length()
	for pos, vec := range dst.Vecs {
		if vec == nil || vec.IsConst() || vec.NeedDup() || vec.Length() != oldRows {
			return moerr.NewInternalErrorNoCtxf("delete change destination column %d is not appendable", pos)
		}
	}
	if dst.RowCount() != oldRows || *dst.Vecs[pkIdx].GetType() != *pkVec.GetType() ||
		dst.Vecs[tsIdx].GetType().Oid != types.T_TS ||
		(retainRowID && dst.Vecs[0].GetType().Oid != types.T_Rowid) {
		return moerr.NewInternalErrorNoCtx("delete change destination schema is incompatible")
	}
	var checkpoints [3]vector.AppendCheckpoint
	for pos, vec := range dst.Vecs {
		checkpoints[pos] = vec.MakeAppendCheckpoint()
	}
	rollback := func() {
		for pos, vec := range dst.Vecs {
			vec.RollbackAppend(checkpoints[pos], 1)
		}
	}
	if retainRowID {
		if err := appendFromEntry(rowIDSource, dst.Vecs[0], sourceRow, mp); err != nil {
			rollback()
			return err
		}
	}
	if err := appendFromEntry(pkVec, dst.Vecs[pkIdx], sourceRow, mp); err != nil {
		rollback()
		return err
	}
	// The source commit-ts column is the canonical replay value. RowEntry.Time is
	// an index key and may be empty in synthetic/recovered entries; when present,
	// validateReplayRowSource already checked that the two values agree.
	if err := appendFromEntry(entry.Batch.Vecs[1], dst.Vecs[tsIdx], sourceRow, mp); err != nil {
		rollback()
		return err
	}
	dst.SetRowCount(oldRows + 1)
	return nil
}

// PXU TODO
func checkTS(start, end types.TS, ts types.TS) bool {
	return !ts.Equal(&txnif.UncommitTS) && ts.LE(&end) && ts.GE(&start)
}

func prefetchObjects(
	ctx context.Context,
	blockID uint32,
	fs fileservice.FileService,
	stats *objectio.ObjectStats,
	scheduler tasks.JobScheduler,
) (job *tasks.Job, err error) {
	if fs == nil {
		return nil, moerr.NewInternalErrorNoCtx("object prefetch file service is nil")
	}
	blockCount, err := validateChangeObjectBlockCount(stats)
	if err != nil {
		return nil, err
	}
	if blockID >= uint32(blockCount) {
		return nil, moerr.NewInternalErrorNoCtxf(
			"object prefetch block %d is outside [0,%d)", blockID, blockCount,
		)
	}
	if scheduler == nil {
		return nil, moerr.NewInternalErrorNoCtx("object prefetch scheduler is nil")
	}
	job = getJob(
		ctx,
		stats.ObjectName().String(),
		JTCDCLoad,
		func(ctx context.Context) (res *tasks.JobResult) {
			loc := stats.BlockLocation(uint16(blockID), 8192)
			bat, _, specialLayout, columnSeqnums, err := ioutil.LoadOneBlockWithColumnLayout(
				ctx,
				fs,
				loc,
				objectio.SchemaData,
			)
			res = &tasks.JobResult{}
			if err != nil {
				res.Err = err
			} else {
				res.Res = &loadedAObjectBlock{
					batch: bat, specialLayout: specialLayout, columnSeqnums: columnSeqnums,
				}
			}
			return
		},
	)
	if err = scheduler.Schedule(job); err != nil {
		putJob(job)
		return nil, err
	}
	return job, nil
}

type loadedAObjectBlock struct {
	batch         *batch.Batch
	specialLayout objectio.SpecialColumnLayout
	columnSeqnums []uint16
}

func prependRowIDVectorIfNeeded(bat *batch.Batch, blk *types.Blockid, mp *mpool.MPool) error {
	if bat == nil || len(bat.Vecs) == 0 {
		return moerr.NewInternalErrorNoCtx("cannot add rowid to an empty change batch")
	}
	rowCount := bat.RowCount()
	if err := validateChangeBatchShape(bat, rowCount, "change batch before rowid insertion"); err != nil {
		return err
	}
	firstRowIDIdx := -1
	rowIDCnt := 0
	hasCompleteAttrs := len(bat.Attrs) == len(bat.Vecs)
	if hasCompleteAttrs {
		for i, attr := range bat.Attrs {
			if attr == catalog.Row_ID || attr == objectio.PhysicalAddr_Attr ||
				attr == objectio.TombstoneAttr_Rowid_Attr {
				rowIDCnt++
				if firstRowIDIdx == -1 {
					firstRowIDIdx = i
				}
			}
		}
	} else {
		// The legacy positional protocol marks only a leading ROWID as the
		// hidden physical address. A later ROWID can be a perfectly valid user
		// column (including the primary key) and must be preserved.
		if bat.Vecs[0] != nil && bat.Vecs[0].GetType().Oid == types.T_Rowid {
			rowIDCnt = 1
			firstRowIDIdx = 0
		}
	}
	if firstRowIDIdx >= 0 {
		if rowIDCnt != 1 {
			return moerr.NewInternalErrorNoCtxf(
				"change batch has %d rowid columns without layout metadata", rowIDCnt,
			)
		}
		if _, err := ioutil.ValidateTombstoneRowIDColumn(
			rowCount, bat.Vecs[firstRowIDIdx],
		); err != nil {
			return moerr.NewInternalErrorNoCtxf("change batch has invalid rowid column: %v", err)
		}
		origVecs := bat.Vecs
		rebuiltVecs := make([]*vector.Vector, 0, len(origVecs))
		rebuiltVecs = append(rebuiltVecs, origVecs[firstRowIDIdx])
		for i, vec := range origVecs {
			if i == firstRowIDIdx {
				continue
			}
			rebuiltVecs = append(rebuiltVecs, vec)
		}
		bat.Vecs = rebuiltVecs
		if hasCompleteAttrs {
			rebuiltAttrs := make([]string, 0, len(rebuiltVecs))
			rebuiltAttrs = append(rebuiltAttrs, catalog.Row_ID)
			for i, attr := range bat.Attrs {
				if i == firstRowIDIdx {
					continue
				}
				rebuiltAttrs = append(rebuiltAttrs, attr)
			}
			bat.Attrs = rebuiltAttrs
		}
		return nil
	}
	if rowCount > 0 && blk == nil {
		return moerr.NewInternalErrorNoCtx("cannot synthesize rowid without block id")
	}
	if err := validateSyntheticRowIDCount(rowCount); err != nil {
		return err
	}
	rowIDVec := vector.NewVec(types.T_Rowid.ToType())
	for i := 0; i < rowCount; i++ {
		if err := vector.AppendFixed(rowIDVec, types.NewRowid(blk, uint32(i)), false, mp); err != nil {
			rowIDVec.Free(mp)
			return err
		}
	}
	bat.Vecs = append([]*vector.Vector{rowIDVec}, bat.Vecs...)
	if hasCompleteAttrs {
		bat.Attrs = append([]string{catalog.Row_ID}, bat.Attrs...)
	}
	return nil
}

func validateSyntheticRowIDCount(rowCount int) error {
	if rowCount < 0 || uint64(rowCount) > uint64(^uint32(0)) {
		return moerr.NewInternalErrorNoCtxf(
			"cannot synthesize rowids for invalid block row count %d", rowCount,
		)
	}
	return nil
}

func changeCommitTSPosition(bat *batch.Batch) (int, error) {
	if bat == nil || len(bat.Vecs) == 0 {
		return -1, moerr.NewInternalErrorNoCtx("change batch has no commit-ts column")
	}
	commitPos := -1
	hasCompleteAttrs := len(bat.Attrs) == len(bat.Vecs)
	if hasCompleteAttrs {
		for pos, attr := range bat.Attrs {
			if attr != objectio.DefaultCommitTS_Attr {
				continue
			}
			if commitPos >= 0 {
				return -1, moerr.NewInternalErrorNoCtx("change batch has duplicate commit-ts attributes")
			}
			commitPos = pos
		}
	}
	if commitPos < 0 {
		if hasCompleteAttrs {
			return -1, moerr.NewInternalErrorNoCtx(
				"change batch attributes do not identify the commit-ts column",
			)
		}
		// Legacy collect-change batches have no complete attributes, but their
		// protocol still defines commitTS as the trailing column. Type alone is
		// ambiguous because a user primary key or data column may also be T_TS.
		commitPos = len(bat.Vecs) - 1
	}
	if bat.Vecs[commitPos] == nil || bat.Vecs[commitPos].GetType().Oid != types.T_TS {
		return -1, moerr.NewInternalErrorNoCtxf(
			"change batch trailing commit column %d is not TS", commitPos,
		)
	}
	return commitPos, nil
}

func updateTombstoneBatch(
	bat *batch.Batch,
	start, end types.TS,
	skipTS map[types.TS]struct{},
	sort bool,
	blk *types.Blockid,
	specialLayout *objectio.SpecialColumnLayout,
	retainRowID bool,
	mp *mpool.MPool,
) error {
	if specialLayout != nil {
		return updatePersistedTombstoneBatch(
			bat, start, end, skipTS, sort, *specialLayout, retainRowID, mp,
		)
	}
	if bat == nil || len(bat.Vecs) < 2 {
		return moerr.NewInternalErrorNoCtx("invalid tombstone batch layout for collect changes")
	}
	if retainRowID {
		if err := prependRowIDVectorIfNeeded(bat, blk, mp); err != nil {
			return err
		}
	}
	rowCount := bat.RowCount()
	if err := validateChangeBatchShape(bat, rowCount, "tombstone change batch"); err != nil {
		return err
	}
	commitPos, err := changeCommitTSPosition(bat)
	if err != nil {
		return err
	}
	rowIDPos, pkPos := -1, -1
	if len(bat.Attrs) == len(bat.Vecs) {
		for pos, attr := range bat.Attrs {
			switch attr {
			case catalog.Row_ID, objectio.TombstoneAttr_Rowid_Attr:
				if rowIDPos >= 0 && rowIDPos != pos {
					return moerr.NewInternalErrorNoCtx("tombstone change batch has duplicate rowid attributes")
				}
				rowIDPos = pos
			case objectio.TombstoneAttr_PK_Attr:
				if pkPos >= 0 {
					return moerr.NewInternalErrorNoCtx("tombstone change batch has duplicate primary-key attributes")
				}
				pkPos = pos
			}
		}
	}
	if len(bat.Attrs) != len(bat.Vecs) {
		if bat.Vecs[0] != nil && bat.Vecs[0].GetType().Oid == types.T_Rowid {
			rowIDPos = 0
		}
	}
	if rowIDPos >= 0 {
		if _, err = ioutil.ValidateTombstoneRowIDColumn(rowCount, bat.Vecs[rowIDPos]); err != nil {
			return err
		}
	}
	if pkPos < 0 {
		if len(bat.Attrs) == len(bat.Vecs) {
			return moerr.NewInternalErrorNoCtx(
				"tombstone change batch attributes do not identify the primary-key column",
			)
		}
		for pos := range bat.Vecs {
			if pos == rowIDPos || pos == commitPos {
				continue
			}
			if pkPos >= 0 {
				return moerr.NewInternalErrorNoCtx(
					"tombstone batch layout is ambiguous without special-column metadata",
				)
			}
			pkPos = pos
		}
	}
	if pkPos < 0 || pkPos == rowIDPos || pkPos == commitPos ||
		(retainRowID && rowIDPos < 0) {
		return moerr.NewInternalErrorNoCtx("invalid tombstone batch layout for collect changes")
	}
	for pos := range bat.Vecs {
		if pos != rowIDPos && pos != pkPos && pos != commitPos {
			return moerr.NewInternalErrorNoCtx(
				"tombstone batch has unsupported columns without special-column metadata",
			)
		}
	}
	if err = applyTSFilterForBatch(bat, commitPos, skipTS, start, end); err != nil {
		return err
	}
	rowIDVec := (*vector.Vector)(nil)
	if rowIDPos >= 0 {
		rowIDVec = bat.Vecs[rowIDPos]
	}
	pkVec := bat.Vecs[pkPos]
	commitTSVec := bat.Vecs[commitPos]
	if retainRowID {
		bat.Vecs = []*vector.Vector{rowIDVec, pkVec, commitTSVec}
		bat.Attrs = []string{
			catalog.Row_ID,
			objectio.TombstoneAttr_PK_Attr,
			objectio.DefaultCommitTS_Attr,
		}
	} else {
		if rowIDVec != nil {
			rowIDVec.Free(mp)
		}
		bat.Vecs = []*vector.Vector{pkVec, commitTSVec}
		bat.Attrs = []string{
			objectio.TombstoneAttr_PK_Attr,
			objectio.DefaultCommitTS_Attr}
	}
	bat.SetRowCount(pkVec.Length())
	if sort {
		sortIdx := len(bat.Vecs) - 1
		return sortBatch(bat, sortIdx, mp)
	}
	return nil
}

func updatePersistedTombstoneBatch(
	bat *batch.Batch,
	start, end types.TS,
	skipTS map[types.TS]struct{},
	sortBatchByTS bool,
	layout objectio.SpecialColumnLayout,
	retainRowID bool,
	mp *mpool.MPool,
) error {
	if bat == nil || len(bat.Vecs) <= int(objectio.TombstoneAttr_PK_SeqNum) {
		return moerr.NewInternalErrorNoCtx("invalid persisted tombstone batch layout for collect changes")
	}
	commitPos, ok := layout.Resolve(objectio.SEQNUM_COMMITTS)
	if !ok || int(commitPos) >= len(bat.Vecs) ||
		int(commitPos) <= int(objectio.TombstoneAttr_PK_SeqNum) ||
		bat.Vecs[commitPos] == nil || bat.Vecs[commitPos].GetType().Oid != types.T_TS {
		return moerr.NewInternalErrorNoCtx("persisted tombstone object has no valid commit-ts column")
	}
	abortPos, hasAbort := layout.Resolve(objectio.SEQNUM_ABORT)
	if hasAbort && (int(abortPos) >= len(bat.Vecs) || bat.Vecs[abortPos] == nil ||
		bat.Vecs[abortPos].GetType().Oid != types.T_bool) {
		return moerr.NewInternalErrorNoCtx("persisted tombstone object has an invalid abort column")
	}
	if hasAbort && (abortPos == commitPos ||
		int(abortPos) <= int(objectio.TombstoneAttr_PK_SeqNum)) {
		return moerr.NewInternalErrorNoCtx("persisted tombstone object has overlapping special columns")
	}
	rowIDVec := bat.Vecs[objectio.TombstoneAttr_Rowid_SeqNum]
	pkVec := bat.Vecs[objectio.TombstoneAttr_PK_SeqNum]
	commitTSVec := bat.Vecs[commitPos]
	if rowIDVec == nil || pkVec == nil || commitTSVec.IsConstNull() {
		return moerr.NewInternalErrorNoCtx("invalid persisted tombstone special columns")
	}
	rowCount := bat.RowCount()
	if err := validateChangeBatchShape(bat, rowCount, "persisted tombstone batch"); err != nil {
		return err
	}
	if _, err := ioutil.ValidateTombstoneRowIDColumn(rowCount, rowIDVec); err != nil {
		return err
	}
	physicalPos := uint16(objectio.InvalidSpecialColumnPosition)
	if declaredPhysicalPos := layout.PhysicalAddr; declaredPhysicalPos != objectio.InvalidSpecialColumnPosition &&
		declaredPhysicalPos != objectio.TombstoneAttr_Rowid_SeqNum {
		// Position zero is the mandatory semantic tombstone rowid.  Treat it
		// as an unspecified physical address for compatibility with legacy
		// callers that construct SpecialColumnLayout values without setting
		// PhysicalAddr (whose Go zero value is also zero).
		if int(declaredPhysicalPos) <= int(objectio.TombstoneAttr_PK_SeqNum) ||
			declaredPhysicalPos == commitPos || (hasAbort && declaredPhysicalPos == abortPos) ||
			int(declaredPhysicalPos) >= len(bat.Vecs) {
			return moerr.NewInternalErrorNoCtx("persisted tombstone object has an invalid physical rowid position")
		}
		if _, err := ioutil.ValidateTombstoneRowIDColumn(
			rowCount, bat.Vecs[declaredPhysicalPos],
		); err != nil {
			return err
		}
		physicalPos = declaredPhysicalPos
	}
	for pos := range bat.Vecs {
		seqnum := uint16(pos)
		if seqnum == objectio.TombstoneAttr_Rowid_SeqNum ||
			seqnum == objectio.TombstoneAttr_PK_SeqNum || seqnum == commitPos ||
			(hasAbort && seqnum == abortPos) || seqnum == physicalPos {
			continue
		}
		return moerr.NewInternalErrorNoCtxf(
			"persisted tombstone object has undeclared column %d", pos,
		)
	}
	commits, err := ioutil.ValidateTombstoneCommitTSColumn(rowCount, commitTSVec)
	if err != nil {
		return err
	}
	var aborts ioutil.TombstoneAbortColumn
	if hasAbort {
		aborts, err = ioutil.ValidateTombstoneAbortColumn(rowCount, bat.Vecs[abortPos])
		if err != nil {
			return err
		}
	}
	deletes := make([]int64, 0)
	for i := 0; i < rowCount; i++ {
		ts := commits.At(i)
		_, skip := skipTS[ts]
		if (aborts.IsPresent() && aborts.At(i)) || ts.Equal(&txnif.UncommitTS) ||
			ts.LT(&start) || ts.GT(&end) || skip {
			deletes = append(deletes, int64(i))
		}
	}

	for i, vec := range bat.Vecs {
		if i == int(objectio.TombstoneAttr_Rowid_SeqNum) ||
			i == int(objectio.TombstoneAttr_PK_SeqNum) ||
			i == int(commitPos) {
			continue
		}
		vec.Free(mp)
	}
	if retainRowID {
		bat.Vecs = []*vector.Vector{rowIDVec, pkVec, commitTSVec}
		bat.Attrs = []string{catalog.Row_ID, objectio.TombstoneAttr_PK_Attr, objectio.DefaultCommitTS_Attr}
	} else {
		rowIDVec.Free(mp)
		bat.Vecs = []*vector.Vector{pkVec, commitTSVec}
		bat.Attrs = []string{objectio.TombstoneAttr_PK_Attr, objectio.DefaultCommitTS_Attr}
	}
	for _, vec := range bat.Vecs {
		vec.Shrink(deletes, true)
	}
	bat.SetRowCount(bat.Vecs[0].Length())
	if sortBatchByTS {
		return sortBatch(bat, len(bat.Vecs)-1, mp)
	}
	return nil
}
func updateDataBatch(
	bat *batch.Batch,
	start, end types.TS,
	blk *types.Blockid,
	specialLayout *objectio.SpecialColumnLayout,
	retainRowID bool,
	mp *mpool.MPool,
) error {
	return updateDataBatchWithSchema(
		bat, start, end, blk, specialLayout, nil, nil, retainRowID, mp,
	)
}

func updateDataBatchWithSchema(
	bat *batch.Batch,
	start, end types.TS,
	blk *types.Blockid,
	specialLayout *objectio.SpecialColumnLayout,
	columnSeqnums []uint16,
	schema *engine.CollectChangesSchema,
	retainRowID bool,
	mp *mpool.MPool,
) error {
	if specialLayout != nil {
		return updatePersistedDataBatchWithSchema(
			bat, start, end, blk, *specialLayout, columnSeqnums, schema, retainRowID, mp,
		)
	}
	if bat == nil || len(bat.Vecs) == 0 {
		return moerr.NewInternalErrorNoCtx("invalid data batch layout for collect changes")
	}
	if retainRowID {
		if err := prependRowIDVectorIfNeeded(bat, blk, mp); err != nil {
			return err
		}
	}
	rowCount := bat.RowCount()
	if err := validateChangeBatchShape(bat, rowCount, "data change batch"); err != nil {
		return err
	}
	commitPos, err := changeCommitTSPosition(bat)
	if err != nil {
		return err
	}
	rowIDPos := -1
	if len(bat.Attrs) == len(bat.Vecs) {
		for pos, attr := range bat.Attrs {
			if attr != catalog.Row_ID && attr != objectio.PhysicalAddr_Attr {
				continue
			}
			if rowIDPos >= 0 && rowIDPos != pos {
				return moerr.NewInternalErrorNoCtx("data change batch has duplicate rowid attributes")
			}
			rowIDPos = pos
		}
	}
	if len(bat.Attrs) != len(bat.Vecs) {
		if bat.Vecs[0] != nil && bat.Vecs[0].GetType().Oid == types.T_Rowid {
			rowIDPos = 0
		}
	}
	if rowIDPos >= 0 {
		if _, err = ioutil.ValidateTombstoneRowIDColumn(rowCount, bat.Vecs[rowIDPos]); err != nil {
			return err
		}
	}
	if retainRowID && rowIDPos < 0 {
		return moerr.NewInternalErrorNoCtx("data change batch is missing retained rowid column")
	}
	userColumnCount := len(bat.Vecs) - 1
	if rowIDPos >= 0 {
		userColumnCount--
	}
	if userColumnCount <= 0 {
		return moerr.NewInternalErrorNoCtx("data change batch has no user columns")
	}
	if err = applyTSFilterForBatch(bat, commitPos, nil, start, end); err != nil {
		return err
	}

	filteredVecs := make([]*vector.Vector, 0, len(bat.Vecs))
	rebuildAttrs := len(bat.Attrs) == len(bat.Vecs)
	filteredAttrs := make([]string, 0, len(bat.Attrs))
	if retainRowID {
		filteredVecs = append(filteredVecs, bat.Vecs[rowIDPos])
		if rebuildAttrs {
			filteredAttrs = append(filteredAttrs, catalog.Row_ID)
		}
	}
	for i, vec := range bat.Vecs {
		if i == rowIDPos || i == commitPos {
			continue
		}
		filteredVecs = append(filteredVecs, vec)
		if rebuildAttrs {
			filteredAttrs = append(filteredAttrs, bat.Attrs[i])
		}
	}
	filteredVecs = append(filteredVecs, bat.Vecs[commitPos])
	if rebuildAttrs {
		filteredAttrs = append(filteredAttrs, objectio.DefaultCommitTS_Attr)
	}
	if !retainRowID && rowIDPos >= 0 {
		bat.Vecs[rowIDPos].Free(mp)
	}
	bat.Vecs = filteredVecs
	if rebuildAttrs {
		bat.Attrs = filteredAttrs
	} else {
		bat.Attrs = nil
	}
	bat.SetRowCount(bat.Vecs[0].Length())
	return nil
}

// projectLoadedDataBatch converts one compact storage batch to the logical
// CollectChanges schema. columnSeqnums maps compact source positions back to
// stable physical column identities. Positions occupied by storage-only
// columns must be excluded because their object-local metadata numbers can
// collide with user sequence numbers added in a later schema version.
//
// The function validates the complete projection before publishing it. On
// error, ownership of every input vector remains with bat and ownership of
// synthesized rowID/commitTS vectors remains with the caller.
func projectLoadedDataBatch(
	bat *batch.Batch,
	columnSeqnums []uint16,
	schema *engine.CollectChangesSchema,
	excludedPositions map[int]struct{},
	rowIDVec, commitTSVec *vector.Vector,
	retainRowID bool,
	mp *mpool.MPool,
) error {
	if bat == nil || schema == nil || !schema.Valid() || len(columnSeqnums) != len(bat.Vecs) {
		return moerr.NewInternalErrorNoCtx("loaded change batch has invalid column mapping")
	}
	rowCount := bat.RowCount()
	if commitTSVec == nil || commitTSVec.Length() != rowCount ||
		(retainRowID && (rowIDVec == nil || rowIDVec.Length() != rowCount)) {
		return moerr.NewInternalErrorNoCtx("loaded change batch has invalid derived columns")
	}
	positionBySeqnum := make(map[uint16]int, len(columnSeqnums))
	for position, seqnum := range columnSeqnums {
		if _, excluded := excludedPositions[position]; excluded {
			continue
		}
		if _, duplicate := positionBySeqnum[seqnum]; duplicate {
			return moerr.NewInternalErrorNoCtxf(
				"loaded change batch has duplicate user sequence %d", seqnum,
			)
		}
		positionBySeqnum[seqnum] = position
	}
	for logicalPosition, seqnum := range schema.Seqnums {
		if sourcePosition, ok := positionBySeqnum[seqnum]; ok {
			source := bat.Vecs[sourcePosition]
			if source == nil || source.Length() != rowCount ||
				*source.GetType() != schema.Types[logicalPosition] {
				return moerr.NewInternalErrorNoCtxf(
					"loaded change column %q at sequence %d is incompatible",
					schema.Attrs[logicalPosition], seqnum,
				)
			}
		}
	}

	resultVecs := make([]*vector.Vector, 0, len(schema.Seqnums)+2)
	resultAttrs := make([]string, 0, len(schema.Seqnums)+2)
	keepSource := make([]bool, len(bat.Vecs))
	markExisting := func(target *vector.Vector) {
		for position, source := range bat.Vecs {
			if source == target {
				keepSource[position] = true
				return
			}
		}
	}
	if retainRowID {
		resultVecs = append(resultVecs, rowIDVec)
		resultAttrs = append(resultAttrs, catalog.Row_ID)
		markExisting(rowIDVec)
	}
	for logicalPosition, seqnum := range schema.Seqnums {
		if sourcePosition, ok := positionBySeqnum[seqnum]; ok {
			resultVecs = append(resultVecs, bat.Vecs[sourcePosition])
			keepSource[sourcePosition] = true
		} else {
			resultVecs = append(
				resultVecs,
				vector.NewConstNull(schema.Types[logicalPosition], rowCount, mp),
			)
		}
		resultAttrs = append(resultAttrs, schema.Attrs[logicalPosition])
	}
	resultVecs = append(resultVecs, commitTSVec)
	resultAttrs = append(resultAttrs, objectio.DefaultCommitTS_Attr)
	markExisting(commitTSVec)

	for position, vec := range bat.Vecs {
		if !keepSource[position] {
			vec.Free(mp)
		}
	}
	bat.Vecs = resultVecs
	bat.Attrs = resultAttrs
	bat.SetRowCount(rowCount)
	return nil
}

func updatePersistedDataBatch(
	bat *batch.Batch,
	start, end types.TS,
	blk *types.Blockid,
	layout objectio.SpecialColumnLayout,
	retainRowID bool,
	mp *mpool.MPool,
) error {
	return updatePersistedDataBatchWithSchema(
		bat, start, end, blk, layout, nil, nil, retainRowID, mp,
	)
}

func updatePersistedDataBatchWithSchema(
	bat *batch.Batch,
	start, end types.TS,
	blk *types.Blockid,
	layout objectio.SpecialColumnLayout,
	columnSeqnums []uint16,
	schema *engine.CollectChangesSchema,
	retainRowID bool,
	mp *mpool.MPool,
) error {
	if bat == nil {
		return moerr.NewInternalErrorNoCtx("updatePersistedDataBatch: nil batch")
	}
	commitPos, ok := layout.Resolve(objectio.SEQNUM_COMMITTS)
	if !ok || int(commitPos) >= len(bat.Vecs) || bat.Vecs[commitPos] == nil ||
		bat.Vecs[commitPos].GetType().Oid != types.T_TS {
		return moerr.NewInternalErrorNoCtx("persisted appendable object has no valid commit-ts column")
	}
	abortPos, hasAbort := layout.Resolve(objectio.SEQNUM_ABORT)
	if hasAbort && (int(abortPos) >= len(bat.Vecs) || bat.Vecs[abortPos] == nil ||
		bat.Vecs[abortPos].GetType().Oid != types.T_bool) {
		return moerr.NewInternalErrorNoCtx("persisted appendable object has an invalid abort column")
	}
	rowIDPos := uint16(objectio.InvalidSpecialColumnPosition)
	if layout.PhysicalAddr != objectio.InvalidSpecialColumnPosition {
		rowIDPos = layout.PhysicalAddr
		if int(rowIDPos) >= len(bat.Vecs) || bat.Vecs[rowIDPos] == nil ||
			bat.Vecs[rowIDPos].GetType().Oid != types.T_Rowid {
			return moerr.NewInternalErrorNoCtx("persisted appendable object has an invalid rowid column")
		}
	}
	if hasAbort && abortPos == commitPos {
		return moerr.NewInternalErrorNoCtx("persisted appendable object has overlapping special columns")
	}
	if rowIDPos != objectio.InvalidSpecialColumnPosition &&
		(rowIDPos == commitPos || (hasAbort && rowIDPos == abortPos)) {
		return moerr.NewInternalErrorNoCtx("persisted appendable object has overlapping special columns")
	}

	commitTSVec := bat.Vecs[commitPos]
	if commitTSVec.IsConstNull() {
		return moerr.NewInternalErrorNoCtx("persisted appendable object commit-ts column is null")
	}
	var abortVec *vector.Vector
	if hasAbort {
		abortVec = bat.Vecs[abortPos]
		if abortVec.IsConstNull() {
			return moerr.NewInternalErrorNoCtx("persisted appendable object abort column is null")
		}
	}
	rowCount := bat.RowCount()
	if err := validateChangeBatchShape(bat, rowCount, "persisted data batch"); err != nil {
		return err
	}
	if rowIDPos != objectio.InvalidSpecialColumnPosition {
		if _, err := ioutil.ValidateTombstoneRowIDColumn(
			rowCount, bat.Vecs[rowIDPos],
		); err != nil {
			return err
		}
	}
	userColumnCount := len(bat.Vecs) - 1
	if hasAbort {
		userColumnCount--
	}
	if rowIDPos != objectio.InvalidSpecialColumnPosition {
		userColumnCount--
	}
	if userColumnCount <= 0 {
		return moerr.NewInternalErrorNoCtx("persisted appendable object has no user columns")
	}
	commits, err := ioutil.ValidateTombstoneCommitTSColumn(rowCount, commitTSVec)
	if err != nil {
		return err
	}
	var aborts ioutil.TombstoneAbortColumn
	if abortVec != nil {
		aborts, err = ioutil.ValidateTombstoneAbortColumn(rowCount, abortVec)
		if err != nil {
			return err
		}
	}
	deletes := make([]int64, 0)
	for i := 0; i < rowCount; i++ {
		ts := commits.At(i)
		if (aborts.IsPresent() && aborts.At(i)) || ts.Equal(&txnif.UncommitTS) ||
			ts.LT(&start) || ts.GT(&end) {
			deletes = append(deletes, int64(i))
		}
	}
	if schema != nil {
		var projectedRowID *vector.Vector
		rowIDSynthesized := false
		if retainRowID {
			if rowIDPos != objectio.InvalidSpecialColumnPosition {
				projectedRowID = bat.Vecs[rowIDPos]
			} else {
				if blk == nil && rowCount > 0 {
					return moerr.NewInternalErrorNoCtx(
						"persisted appendable object cannot synthesize rowid without block id",
					)
				}
				if err = validateSyntheticRowIDCount(rowCount); err != nil {
					return err
				}
				projectedRowID = vector.NewVec(types.T_Rowid.ToType())
				for row := 0; row < rowCount; row++ {
					if err = vector.AppendFixed(
						projectedRowID, types.NewRowid(blk, uint32(row)), false, mp,
					); err != nil {
						projectedRowID.Free(mp)
						return err
					}
				}
				rowIDSynthesized = true
			}
		}
		excluded := map[int]struct{}{int(commitPos): {}}
		if hasAbort {
			excluded[int(abortPos)] = struct{}{}
		}
		if rowIDPos != objectio.InvalidSpecialColumnPosition {
			excluded[int(rowIDPos)] = struct{}{}
		}
		if err = projectLoadedDataBatch(
			bat, columnSeqnums, schema, excluded, projectedRowID, commitTSVec,
			retainRowID, mp,
		); err != nil {
			if rowIDSynthesized {
				projectedRowID.Free(mp)
			}
			return err
		}
		for _, vec := range bat.Vecs {
			vec.Shrink(deletes, true)
		}
		bat.SetRowCount(bat.Vecs[0].Length())
		return nil
	}

	rebuildAttrs := len(bat.Attrs) == len(bat.Vecs)
	filteredVecs := make([]*vector.Vector, 0, len(bat.Vecs)-1)
	filteredAttrs := make([]string, 0, len(bat.Attrs))
	var rowIDVec *vector.Vector
	rowIDKept := false
	if retainRowID && rowIDPos == objectio.InvalidSpecialColumnPosition {
		if blk == nil {
			return moerr.NewInternalErrorNoCtx("persisted appendable object cannot synthesize rowid without block id")
		}
		if err := validateSyntheticRowIDCount(rowCount); err != nil {
			return err
		}
		rowIDVec = vector.NewVec(types.T_Rowid.ToType())
		for i := 0; i < rowCount; i++ {
			if err := vector.AppendFixed(rowIDVec, types.NewRowid(blk, uint32(i)), false, mp); err != nil {
				rowIDVec.Free(mp)
				return err
			}
		}
		rowIDKept = true
	}
	for i, vec := range bat.Vecs {
		pos := uint16(i)
		switch {
		case pos == commitPos:
		case hasAbort && pos == abortPos:
			vec.Free(mp)
		case pos == rowIDPos:
			if retainRowID {
				rowIDVec = vec
				rowIDKept = true
			} else {
				vec.Free(mp)
			}
		default:
			filteredVecs = append(filteredVecs, vec)
			if rebuildAttrs {
				filteredAttrs = append(filteredAttrs, bat.Attrs[i])
			}
		}
	}
	if rowIDKept {
		filteredVecs = append([]*vector.Vector{rowIDVec}, filteredVecs...)
		if rebuildAttrs {
			filteredAttrs = append([]string{catalog.Row_ID}, filteredAttrs...)
		}
	}
	filteredVecs = append(filteredVecs, commitTSVec)
	if rebuildAttrs {
		filteredAttrs = append(filteredAttrs, objectio.DefaultCommitTS_Attr)
	}
	bat.Vecs = filteredVecs
	if rebuildAttrs {
		bat.Attrs = filteredAttrs
	} else {
		// Partial attributes are not positional metadata. Dropping storage-only
		// columns can otherwise make their length accidentally match the rebuilt
		// vector list and turn stale names into an apparently complete schema.
		bat.Attrs = nil
	}
	for _, vec := range bat.Vecs {
		vec.Shrink(deletes, true)
	}
	if len(bat.Vecs) > 0 {
		bat.SetRowCount(bat.Vecs[0].Length())
	}
	return nil
}
func updateCNTombstoneBatch(
	bat *batch.Batch,
	commitTS types.TS,
	_ *types.Blockid,
	layout objectio.SpecialColumnLayout,
	retainRowID bool,
	mp *mpool.MPool,
) error {
	if bat == nil {
		return moerr.NewInternalErrorNoCtx("updateCNTombstoneBatch: nil batch")
	}
	rowIDPos, pkPos, derivedCommitPos := -1, -1, -1
	hasCompleteAttrs := len(bat.Attrs) == len(bat.Vecs)
	if hasCompleteAttrs {
		for pos, attr := range bat.Attrs {
			switch attr {
			case catalog.Row_ID, objectio.TombstoneAttr_Rowid_Attr:
				if rowIDPos >= 0 {
					return moerr.NewInternalErrorNoCtx(
						"updateCNTombstoneBatch: duplicate semantic rowid columns",
					)
				}
				rowIDPos = pos
			case objectio.TombstoneAttr_PK_Attr:
				if pkPos >= 0 {
					return moerr.NewInternalErrorNoCtx(
						"updateCNTombstoneBatch: duplicate pk columns",
					)
				}
				pkPos = pos
			case objectio.DefaultCommitTS_Attr:
				if derivedCommitPos >= 0 {
					return moerr.NewInternalErrorNoCtx(
						"updateCNTombstoneBatch: duplicate derived commit-ts columns",
					)
				}
				derivedCommitPos = pos
			}
		}
		canonical := (!retainRowID && len(bat.Vecs) == 2 && pkPos == 0 && derivedCommitPos == 1) ||
			(retainRowID && len(bat.Vecs) == 3 && rowIDPos == 0 && pkPos == 1 && derivedCommitPos == 2)
		if canonical {
			rowCount := bat.Vecs[pkPos].Length()
			if err := validateChangeBatchShape(bat, rowCount, "canonical CN tombstone batch"); err != nil {
				return err
			}
			if retainRowID {
				if _, err := ioutil.ValidateTombstoneRowIDColumn(rowCount, bat.Vecs[rowIDPos]); err != nil {
					return err
				}
			}
			if _, err := ioutil.ValidateTombstoneCommitTSColumn(
				rowCount, bat.Vecs[derivedCommitPos],
			); err != nil {
				return err
			}
			replacement, err := vector.NewConstFixed(types.T_TS.ToType(), commitTS, rowCount, mp)
			if err != nil {
				return err
			}
			bat.Vecs[derivedCommitPos].Free(mp)
			bat.Vecs[derivedCommitPos] = replacement
			return nil
		}
	}
	if pkPos == -1 {
		if hasCompleteAttrs {
			return moerr.NewInternalErrorNoCtx(
				"updateCNTombstoneBatch: attributes do not identify the tombstone pk",
			)
		}
		if len(bat.Vecs) < len(objectio.TombstoneSeqnums_CN_Created) ||
			bat.Vecs[objectio.TombstoneAttr_Rowid_Idx] == nil ||
			bat.Vecs[objectio.TombstoneAttr_Rowid_Idx].GetType().Oid != types.T_Rowid {
			return moerr.NewInternalErrorNoCtx("updateCNTombstoneBatch: tombstone batch missing pk vector")
		}
		rowIDPos = objectio.TombstoneAttr_Rowid_Idx
		pkPos = objectio.TombstoneAttr_PK_Idx
		if len(bat.Vecs) == len(objectio.TombstoneSeqnums_CN_Created)+1 &&
			bat.Vecs[len(bat.Vecs)-1] != nil &&
			bat.Vecs[len(bat.Vecs)-1].GetType().Oid == types.T_TS {
			derivedCommitPos = len(bat.Vecs) - 1
		}
	}
	if pkPos >= len(bat.Vecs) || bat.Vecs[pkPos] == nil {
		return moerr.NewInternalErrorNoCtx("updateCNTombstoneBatch: tombstone batch missing pk vector")
	}
	if rowIDPos >= 0 && (rowIDPos >= len(bat.Vecs) || bat.Vecs[rowIDPos] == nil ||
		bat.Vecs[rowIDPos].GetType().Oid != types.T_Rowid) {
		return moerr.NewInternalErrorNoCtx("updateCNTombstoneBatch: invalid semantic rowid column")
	}
	if rowIDPos < 0 {
		return moerr.NewInternalErrorNoCtx("updateCNTombstoneBatch: semantic rowid vector is missing")
	}

	pk := bat.Vecs[pkPos]
	rowCount := pk.Length()
	if err := validateChangeBatchShape(bat, rowCount, "CN-created tombstone batch"); err != nil {
		return err
	}
	if rowIDPos >= 0 {
		if _, err := ioutil.ValidateTombstoneRowIDColumn(
			rowCount, bat.Vecs[rowIDPos],
		); err != nil {
			return err
		}
	}
	physicalPos := -1
	if layout.PhysicalAddr != objectio.InvalidSpecialColumnPosition {
		physicalPos = int(layout.PhysicalAddr)
		if physicalPos < 0 || physicalPos >= len(bat.Vecs) ||
			physicalPos == rowIDPos || physicalPos == pkPos || bat.Vecs[physicalPos] == nil {
			return moerr.NewInternalErrorNoCtx(
				"updateCNTombstoneBatch: invalid physical rowid position",
			)
		}
		if _, err := ioutil.ValidateTombstoneRowIDColumn(
			rowCount, bat.Vecs[physicalPos],
		); err != nil {
			return err
		}
	}
	if _, ok := layout.Resolve(objectio.SEQNUM_COMMITTS); ok {
		return moerr.NewInternalErrorNoCtx(
			"updateCNTombstoneBatch: CN-created tombstone unexpectedly has commit-ts metadata",
		)
	}
	if _, ok := layout.Resolve(objectio.SEQNUM_ABORT); ok {
		return moerr.NewInternalErrorNoCtx(
			"updateCNTombstoneBatch: CN-created tombstone unexpectedly has abort metadata",
		)
	}
	for pos := range bat.Vecs {
		if pos != rowIDPos && pos != pkPos && pos != physicalPos && pos != derivedCommitPos {
			return moerr.NewInternalErrorNoCtxf(
				"updateCNTombstoneBatch: unexpected column %d outside the declared layout", pos,
			)
		}
	}
	commitTSVec, err := vector.NewConstFixed(types.T_TS.ToType(), commitTS, rowCount, mp)
	if err != nil {
		return err
	}
	var rowID *vector.Vector
	if retainRowID {
		rowID = bat.Vecs[rowIDPos]
	}
	for pos, vec := range bat.Vecs {
		if pos == pkPos || (retainRowID && pos == rowIDPos) {
			continue
		}
		vec.Free(mp)
	}
	if retainRowID {
		bat.Vecs = []*vector.Vector{rowID, pk, commitTSVec}
		bat.Attrs = []string{catalog.Row_ID, objectio.TombstoneAttr_PK_Attr, objectio.DefaultCommitTS_Attr}
	} else {
		bat.Vecs = []*vector.Vector{pk, commitTSVec}
		bat.Attrs = []string{objectio.TombstoneAttr_PK_Attr, objectio.DefaultCommitTS_Attr}
	}
	bat.SetRowCount(rowCount)
	return nil
}

func updateCNDataBatch(
	bat *batch.Batch,
	commitTS types.TS,
	blk *types.Blockid,
	layout objectio.SpecialColumnLayout,
	retainRowID bool,
	mp *mpool.MPool,
) error {
	return updateCNDataBatchWithSchema(
		bat, commitTS, blk, layout, nil, nil, retainRowID, mp,
	)
}

func updateCNDataBatchWithSchema(
	bat *batch.Batch,
	commitTS types.TS,
	blk *types.Blockid,
	layout objectio.SpecialColumnLayout,
	columnSeqnums []uint16,
	schema *engine.CollectChangesSchema,
	retainRowID bool,
	mp *mpool.MPool,
) error {
	if bat == nil {
		return moerr.NewInternalErrorNoCtx("updateCNDataBatch: nil batch")
	}
	if len(bat.Vecs) == 0 {
		return moerr.NewInternalErrorNoCtx("updateCNDataBatch: data batch has no user vectors")
	}

	specialPositions := make(map[int]struct{}, 3)
	physicalPos := -1
	if layout.PhysicalAddr != objectio.InvalidSpecialColumnPosition {
		physicalPos = int(layout.PhysicalAddr)
		if physicalPos >= len(bat.Vecs) || bat.Vecs[physicalPos] == nil ||
			bat.Vecs[physicalPos].GetType().Oid != types.T_Rowid {
			return moerr.NewInternalErrorNoCtx("updateCNDataBatch: invalid physical rowid column")
		}
		specialPositions[physicalPos] = struct{}{}
	}
	if commitPos, ok := layout.Resolve(objectio.SEQNUM_COMMITTS); ok {
		pos := int(commitPos)
		if pos >= len(bat.Vecs) || bat.Vecs[pos] == nil {
			return moerr.NewInternalErrorNoCtx("updateCNDataBatch: invalid persisted commit-ts column")
		}
		specialPositions[pos] = struct{}{}
	}
	if abortPos, ok := layout.Resolve(objectio.SEQNUM_ABORT); ok {
		pos := int(abortPos)
		if pos >= len(bat.Vecs) || bat.Vecs[pos] == nil {
			return moerr.NewInternalErrorNoCtx("updateCNDataBatch: invalid persisted abort column")
		}
		specialPositions[pos] = struct{}{}
	}

	rowCount, hasUserColumn := bat.RowCount(), false
	for pos, vec := range bat.Vecs {
		if _, special := specialPositions[pos]; special {
			continue
		}
		if vec == nil {
			return moerr.NewInternalErrorNoCtxf("updateCNDataBatch: data column %d is nil", pos)
		}
		hasUserColumn = true
		break
	}
	if !hasUserColumn {
		return moerr.NewInternalErrorNoCtx("updateCNDataBatch: data batch has no user vectors")
	}
	if err := validateChangeBatchShape(bat, rowCount, "CN-created data batch"); err != nil {
		return err
	}
	if physicalPos >= 0 {
		if _, err := ioutil.ValidateTombstoneRowIDColumn(
			rowCount, bat.Vecs[physicalPos],
		); err != nil {
			return err
		}
	}
	if commitPos, ok := layout.Resolve(objectio.SEQNUM_COMMITTS); ok {
		if _, err := ioutil.ValidateTombstoneCommitTSColumn(rowCount, bat.Vecs[commitPos]); err != nil {
			return err
		}
	}
	if abortPos, ok := layout.Resolve(objectio.SEQNUM_ABORT); ok {
		if _, err := ioutil.ValidateTombstoneAbortColumn(rowCount, bat.Vecs[abortPos]); err != nil {
			return err
		}
	}

	var rowIDVec *vector.Vector
	rowIDSynthesized := false
	if retainRowID {
		if physicalPos >= 0 {
			rowIDVec = bat.Vecs[physicalPos]
		} else {
			if blk == nil && rowCount > 0 {
				return moerr.NewInternalErrorNoCtx("updateCNDataBatch: cannot synthesize rowid without block id")
			}
			if err := validateSyntheticRowIDCount(rowCount); err != nil {
				return err
			}
			rowIDVec = vector.NewVec(types.T_Rowid.ToType())
			for row := 0; row < rowCount; row++ {
				if err := vector.AppendFixed(
					rowIDVec, types.NewRowid(blk, uint32(row)), false, mp,
				); err != nil {
					rowIDVec.Free(mp)
					return err
				}
			}
			rowIDSynthesized = true
		}
	}
	commitTSVec, err := vector.NewConstFixed(types.T_TS.ToType(), commitTS, rowCount, mp)
	if err != nil {
		if rowIDSynthesized {
			rowIDVec.Free(mp)
		}
		return err
	}
	if schema != nil {
		if err = projectLoadedDataBatch(
			bat, columnSeqnums, schema, specialPositions, rowIDVec, commitTSVec,
			retainRowID, mp,
		); err != nil {
			commitTSVec.Free(mp)
			if rowIDSynthesized {
				rowIDVec.Free(mp)
			}
			return err
		}
		return nil
	}

	rebuildAttrs := len(bat.Attrs) == len(bat.Vecs)
	resultVecs := make([]*vector.Vector, 0, len(bat.Vecs)-len(specialPositions)+2)
	resultAttrs := make([]string, 0, cap(resultVecs))
	if retainRowID {
		resultVecs = append(resultVecs, rowIDVec)
		if rebuildAttrs {
			resultAttrs = append(resultAttrs, catalog.Row_ID)
		}
	}
	for pos, vec := range bat.Vecs {
		if _, special := specialPositions[pos]; special {
			if !(retainRowID && pos == physicalPos) {
				vec.Free(mp)
			}
			continue
		}
		resultVecs = append(resultVecs, vec)
		if rebuildAttrs {
			resultAttrs = append(resultAttrs, bat.Attrs[pos])
		}
	}
	resultVecs = append(resultVecs, commitTSVec)
	if rebuildAttrs {
		resultAttrs = append(resultAttrs, objectio.DefaultCommitTS_Attr)
	}
	bat.Vecs = resultVecs
	if rebuildAttrs {
		bat.Attrs = resultAttrs
	} else {
		bat.Attrs = nil
	}
	bat.SetRowCount(rowCount)
	return nil
}

// TestGetObjectsFromCheckpointEntries exposes getObjectsFromCheckpointEntries for tests in other packages.
func TestGetObjectsFromCheckpointEntries(
	ctx context.Context,
	tid uint64,
	sid string,
	start, end types.TS,
	checkpoint []*checkpoint.CheckpointEntry,
	mp *mpool.MPool,
	fs fileservice.FileService,
) (
	dataAobj, dataCNObj, tombstoneAobj, tombstoneCNObj []*objectio.ObjectEntry,
	err error,
) {
	return getObjectsFromCheckpointEntries(ctx, tid, sid, start, end, checkpoint, mp, fs, checkpointObjectSelectionRecovery)
}

// TestGetObjectsFromCheckpointRange exposes the range-aware checkpoint object
// selector for tests in other packages.
func TestGetObjectsFromCheckpointRange(
	ctx context.Context,
	tid uint64,
	sid string,
	start, end types.TS,
	checkpoint []*checkpoint.CheckpointEntry,
	mp *mpool.MPool,
	fs fileservice.FileService,
) (
	dataAobj, dataCNObj, tombstoneAobj, tombstoneCNObj []*objectio.ObjectEntry,
	err error,
) {
	return getObjectsFromCheckpointEntries(ctx, tid, sid, start, end, checkpoint, mp, fs, checkpointObjectSelectionRange)
}

type CheckpointEntryReader = checkpointEntryReader

// SetCheckpointReaderFactoryForTest overrides the checkpoint reader factory during tests.
// It returns a restore function that should be deferred by callers.
func SetCheckpointReaderFactoryForTest(factory func(uint32, objectio.Location, uint64, *mpool.MPool, fileservice.FileService) checkpointEntryReader) func() {
	old := newCKPReaderWithTableID
	newCKPReaderWithTableID = factory
	return func() {
		newCKPReaderWithTableID = old
	}
}

type checkpointEntryReader interface {
	ReadMeta(context.Context) error
	PrefetchData(string)
	ConsumeCheckpointWithTableID(context.Context, func(context.Context, fileservice.FileService, objectio.ObjectEntry, bool) error) error
}

func checkpointEntryReaderIsNil(reader checkpointEntryReader) bool {
	if reader == nil {
		return true
	}
	value := reflect.ValueOf(reader)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map,
		reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

var newCKPReaderWithTableID = func(version uint32, location objectio.Location, tableID uint64, mp *mpool.MPool, fs fileservice.FileService) checkpointEntryReader {
	return logtail.NewCKPReaderWithTableID_V2(version, location, tableID, mp, fs)
}
