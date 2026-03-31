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
	"sync"
	"time"

	goSort "sort"

	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/sort"
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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
)

type checkpointEntryReader interface {
	ReadMeta(context.Context) error
	PrefetchData(string)
	ConsumeCheckpointWithTableID(context.Context, func(context.Context, fileservice.FileService, objectio.ObjectEntry, bool) error) error
}

var newCKPReaderWithTableID = func(version uint32, location objectio.Location, tableID uint64, mp *mpool.MPool, fs fileservice.FileService) checkpointEntryReader {
	return logtail.NewCKPReaderWithTableID_V2(version, location, tableID, mp, fs)
}

const (
	JTCDCLoad tasks.JobType = 300 + iota
)

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
}

func NewRowHandle(data *batch.Batch, mp *mpool.MPool, baseHandle *baseHandle, ctx context.Context) (handle *BatchHandle) {
	handle = &BatchHandle{
		mp:         mp,
		batches:    data,
		ctx:        ctx,
		baseHandle: baseHandle,
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
	r.batches.Clean(r.mp)
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
		*bat = batch.NewWithSize(0)
		(*bat).Attrs = append((*bat).Attrs, r.batches.Attrs...)
		for _, vec := range r.batches.Vecs {
			newVec, err := vec.CloneWindow(start, end, mp)
			if err != nil {
				return err
			}
			(*bat).Vecs = append((*bat).Vecs, newVec)
		}
	} else {
		for offset := start; offset < end; offset++ {
			for i, vec := range (*bat).Vecs {
				appendFromEntry(r.batches.Vecs[i], vec, offset, mp)
			}
		}
	}
	(*bat).SetRowCount((*bat).Vecs[0].Length())
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

	cache []*batch.Batch
	TSs   []types.TS
}

func NewCNObjectHandle(isTombstone bool, objects []*objectio.ObjectEntry, fs fileservice.FileService, baseHandle *baseHandle, mp *mpool.MPool) *CNObjectHandle {
	return &CNObjectHandle{
		base:        baseHandle,
		isTombstone: isTombstone,
		objects:     objects,
		fs:          fs,
		mp:          mp,
		cache:       make([]*batch.Batch, 0),
	}
}
func (h *CNObjectHandle) prefetch(ctx context.Context) (err error) {
	t0 := time.Now()
	jobs := make([]*tasks.Job, 0)
	for i := 0; i < LoadParallism; i++ {
		if h.objectOffsetCursor >= len(h.objects) {
			break
		}
		stats := h.objects[h.objectOffsetCursor].ObjectStats
		h.TSs = append(h.TSs, h.objects[h.objectOffsetCursor].CreateTime)
		job := prefetchObjects(ctx, uint32(h.blkOffsetCursor), h.fs, &stats, h.base.changesHandle.scheduler)
		jobs = append(jobs, job)
		h.blkOffsetCursor++
		if h.blkOffsetCursor >= int(stats.BlkCnt()) {
			h.blkOffsetCursor = 0
			h.objectOffsetCursor++
		}
	}
	for _, job := range jobs {
		res := job.GetResult()
		if res.Err != nil {
			err = res.Err
			if moerr.IsMoErrCode(err, moerr.ErrFileNotFound) {
				logutil.Info("ChangesHandle-FileNotFound",
					zap.String("err", err.Error()))
			}
			h.base.changesHandle.readDuration += time.Since(t0)
			return
		}
		putJob(job)
		bat := res.Res.(*batch.Batch)
		h.cache = append(h.cache, bat)
	}
	h.base.changesHandle.readDuration += time.Since(t0)
	return
}
func (h *CNObjectHandle) isEnd() bool {
	return h.objectOffsetCursor >= len(h.objects) && len(h.cache) == 0
}
func (h *CNObjectHandle) IsEmpty() bool {
	return len(h.objects) == 0
}
func (h *CNObjectHandle) Next(ctx context.Context, bat **batch.Batch, mp *mpool.MPool) (err error) {
	if h.isEnd() {
		return moerr.GetOkExpectedEOF()
	}
	if len(h.cache) == 0 {
		err = h.prefetch(ctx)
		if err != nil {
			return
		}
	}
	data := h.cache[0]
	ts := h.TSs[0]
	h.cache = h.cache[1:]
	h.TSs = h.TSs[1:]
	t0 := time.Now()
	if h.isTombstone {
		updateCNTombstoneBatch(
			data,
			ts,
			h.mp,
		)
	} else {
		updateCNDataBatch(
			data,
			ts,
			h.mp,
		)
	}
	h.base.changesHandle.updateDuration += time.Since(t0)
	t0 = time.Now()
	if *bat == nil {
		*bat = batch.NewWithSize(0)
		(*bat).Attrs = append((*bat).Attrs, data.Attrs...)
		for _, vec := range data.Vecs {
			newVec := vector.NewVec(*vec.GetType())
			if err != nil {
				return err
			}
			(*bat).Vecs = append((*bat).Vecs, newVec)
		}
	}
	srcLen := data.Vecs[0].Length()
	sels := make([]int64, srcLen)
	for j := 0; j < srcLen; j++ {
		sels[j] = int64(j)
	}
	for i, vec := range (*bat).Vecs {
		src := data.Vecs[i]
		vec.Union(src, sels, mp)
	}
	(*bat).SetRowCount((*bat).Vecs[0].Length())
	h.base.changesHandle.copyDuration += time.Since(t0)
	return
}

func (h *CNObjectHandle) QuickNext(ctx context.Context, data **batch.Batch, mp *mpool.MPool) (err error) {
	return h.Next(ctx, data, mp)
}

func (h *CNObjectHandle) NextTS() types.TS {
	if h.isEnd() {
		return types.TS{}
	}
	if len(h.cache) == 0 {
		return h.objects[h.objectOffsetCursor].CreateTime
	}
	return h.TSs[0]
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
	p                  *baseHandle

	// blockPlans caches block-level commit-ts overlap decisions for objects.
	// It is only populated when checkpoint-range mode enables block pruning.
	blockPlans map[string]*aobjBlockPlan

	// cacheConstantCommitTS records the synthetic commit-ts for each cached
	// batch.  A non-zero entry means the batch came from an object without
	// per-row commit-ts (e.g. a TN non-appendable compacted from CN objects)
	// and must be updated with updateCNDataBatch instead of updateDataBatch.
	cacheConstantCommitTS []types.TS
}

type aobjBlockPlan struct {
	initialized      bool
	evaluable        bool
	noCommitTSColumn bool // object has no per-row commit-ts column
	shouldReadByBlks []bool
	totalBlocks      int
	prunedBlocks     int
	// nonEvaluableReasons counts why a block cannot be pruned by commit-ts
	// zonemap, for example missing metadata or unsupported tail column type.
	nonEvaluableReasons map[string]int
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
		if h.objectOffsetCursor >= len(h.objects) {
			return nil, 0, false, nil
		}
		obj = h.objects[h.objectOffsetCursor]
		blk = uint16(h.blkOffsetCursor)
		h.blkOffsetCursor++
		if h.blkOffsetCursor >= int(obj.BlkCnt()) {
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
	changes := h.p.changesHandle
	if !changes.enableCommitTSBlockPrune {
		return true, nil
	}
	// Row-commit-ts pruning is only meaningful for TN-created non-appendable
	// objects. Appendable objects are kept on the existing path.
	if obj.GetAppendable() || obj.GetCNCreated() {
		return true, nil
	}
	key := obj.ObjectShortName().ShortString()
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
	// Object has no per-row commit-ts column (e.g. TN non-appendable
	// compacted from CN objects).  Use object-level CreateTime to decide.
	if plan.noCommitTSColumn {
		ct := obj.CreateTime
		if ct.GE(&h.start) && ct.LE(&h.end) {
			return true, nil
		}
		return false, nil
	}
	if !plan.evaluable {
		if changes.strictCommitTSBlockPrune {
			return false, moerr.NewFileNotFoundNoCtx(obj.ObjectName().String())
		}
		return true, nil
	}
	if int(blk) >= len(plan.shouldReadByBlks) {
		return false, nil
	}
	return plan.shouldReadByBlks[blk], nil
}

func (h *AObjectHandle) buildBlockPlan(
	ctx context.Context,
	obj *objectio.ObjectEntry,
	plan *aobjBlockPlan,
) error {
	plan.initialized = true
	plan.evaluable = false
	plan.shouldReadByBlks = make([]bool, int(obj.BlkCnt()))
	plan.totalBlocks = int(obj.BlkCnt())
	plan.prunedBlocks = 0
	plan.nonEvaluableReasons = make(map[string]int, 4)
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
	dataMeta := meta.MustGetMeta(objectio.SchemaData)
	evaluableBlockCnt := 0
	pkf := h.p.changesHandle.pkFilter
	pkSeqnum := uint16(h.p.changesHandle.primarySeqnum)
	for i := uint16(0); i < uint16(obj.BlkCnt()); i++ {
		blk := dataMeta.GetBlockMeta(uint32(i))
		overlap, evaluable, reason, _ := blockCommitTSOverlapsRange(blk, h.start, h.end)
		if !evaluable {
			plan.nonEvaluableReasons[reason]++
			if pkf != nil && pkf.Vec != nil {
				pkZM := blk.MustGetColumn(pkSeqnum).ZoneMap()
				if pkZM.IsInited() && !pkZM.AnyIn(pkf.Vec) {
					plan.shouldReadByBlks[i] = false
					plan.prunedBlocks++
				}
			}
			continue
		}
		evaluableBlockCnt++
		plan.shouldReadByBlks[i] = overlap
		if overlap && pkf != nil && pkf.Vec != nil {
			pkZM := blk.MustGetColumn(pkSeqnum).ZoneMap()
			if pkZM.IsInited() && !pkZM.AnyIn(pkf.Vec) {
				plan.shouldReadByBlks[i] = false
				overlap = false
			}
		}
		if !overlap {
			plan.prunedBlocks++
		}
	}
	// "evaluable" here means at least one block exposes usable commit-ts zonemap.
	// If none does, strict mode can still choose the exact-scan fallback path.
	plan.evaluable = evaluableBlockCnt > 0

	// Detect objects without per-row commit-ts column (e.g. TN non-appendable
	// compacted from CN objects).  If ALL non-evaluable blocks fail with
	// "tail_column_not_ts" or "no_meta_columns", the object has no commit-ts.
	if evaluableBlockCnt == 0 {
		noTSCnt := plan.nonEvaluableReasons["tail_column_not_ts"] +
			plan.nonEvaluableReasons["no_meta_columns"]
		if noTSCnt == plan.totalBlocks {
			plan.noCommitTSColumn = true
		}
	}
	return nil
}

// blockCommitTSOverlapsRange checks whether one block's commit-ts zonemap
// intersects [start, end]. The second return value is false when the block
// does not expose a usable commit-ts zonemap.
func blockCommitTSOverlapsRange(
	blk objectio.BlockObject,
	start, end types.TS,
) (bool, bool, string, string) {
	metaColCnt := blk.GetMetaColumnCount()
	maxSeqnum := blk.GetMaxSeqnum()
	base := fmt.Sprintf("meta_col_cnt=%d max_seqnum=%d", metaColCnt, maxSeqnum)
	if metaColCnt == 0 {
		return false, false, "no_meta_columns", base
	}
	// Commit-ts is stored as the trailing hidden column when available.
	// Do not gate by max-seqnum here: merged/rewritten TN objects may expose
	// different seqnum layouts while still carrying valid commit-ts zonemap.
	commitCol := blk.ColumnMeta(metaColCnt - 1)
	base = fmt.Sprintf("%s tail_col_type=%d", base, commitCol.DataType())
	if commitCol.DataType() != uint8(types.T_TS) {
		return false, false, "tail_column_not_ts", base
	}
	zm := commitCol.ZoneMap()
	if !zm.IsInited() {
		return false, false, "zonemap_not_inited", base
	}
	if zm.GetType() != types.T_TS {
		return false, false, "zonemap_type_not_ts", fmt.Sprintf("%s zm_type=%s", base, zm.GetType().String())
	}
	minTS := types.DecodeFixed[types.TS](zm.GetMinBuf())
	maxTS := types.DecodeFixed[types.TS](zm.GetMaxBuf())
	detail := fmt.Sprintf(
		"%s zm_type=%s zm_min=%s zm_max=%s range=[%s,%s]",
		base,
		zm.GetType().String(),
		minTS.ToString(),
		maxTS.ToString(),
		start.ToString(),
		end.ToString(),
	)
	if maxTS.LT(&start) || minTS.GT(&end) {
		return false, true, "", detail
	}
	return true, true, "", detail
}

func (h *AObjectHandle) prefetch(ctx context.Context) (err error) {
	t0 := time.Now()
	type prefetchJob struct {
		job      *tasks.Job
		createTS types.TS // non-zero if object has no commit-ts column
	}
	pjobs := make([]prefetchJob, 0, LoadParallism)
	for i := 0; i < LoadParallism; i++ {
		obj, blk, ok, targetErr := h.nextPrefetchTarget(ctx)
		if targetErr != nil {
			err = targetErr
			h.p.changesHandle.readDuration += time.Since(t0)
			return
		}
		if !ok {
			break
		}
		stats := obj.ObjectStats
		job := prefetchObjects(ctx, uint32(blk), h.fs, &stats, h.p.changesHandle.scheduler)
		pj := prefetchJob{job: job}
		// Check if this object needs synthetic commit-ts.
		key := obj.ObjectShortName().ShortString()
		if plan, ok := h.blockPlans[key]; ok && plan.noCommitTSColumn {
			pj.createTS = obj.CreateTime
		}
		pjobs = append(pjobs, pj)
	}
	for _, pj := range pjobs {
		res := pj.job.GetResult()
		if res.Err != nil {
			err = res.Err
			if moerr.IsMoErrCode(err, moerr.ErrFileNotFound) {
				logutil.Info("ChangesHandle-FileNotFound",
					zap.String("err", err.Error()))
			}
			h.p.changesHandle.readDuration += time.Since(t0)
			return
		}
		putJob(pj.job)
		bat := res.Res.(*batch.Batch)
		h.cache = append(h.cache, bat)
		h.cacheConstantCommitTS = append(h.cacheConstantCommitTS, pj.createTS)
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
	return len(h.objects) == 0
}
func (h *AObjectHandle) RowCount() int {
	cnt := 0
	for _, obj := range h.objects {
		cnt += int(obj.ObjectStats.Rows())
	}
	return cnt
}
func (h *AObjectHandle) getNextAObject(ctx context.Context) (err error) {
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
		constantTS := h.cacheConstantCommitTS[0]
		h.cache = h.cache[1:]
		h.cacheConstantCommitTS = h.cacheConstantCommitTS[1:]
		t0 := time.Now()
		if h.isTombstone {
			updateTombstoneBatch(h.currentBatch, h.start, h.end, h.p.skipTS, !h.quick, h.mp)
		} else if !constantTS.IsEmpty() {
			// Data object has no per-row commit-ts.  Row-level filtering is
			// impossible — signal the caller so it can fall back to a
			// full-table-scan strategy instead of returning wrong results.
			return engine.ErrNoCommitTSColumn
		} else {
			updateDataBatch(h.currentBatch, h.start, h.end, h.mp)
		}
		h.p.changesHandle.updateDuration += time.Since(t0)
		h.batchLength = h.currentBatch.Vecs[0].Length()
		if h.batchLength > 0 {
			return
		}
	}
}
func (h *AObjectHandle) isEnd() bool {
	return h.objectOffsetCursor >= len(h.objects) && len(h.cache) == 0
}

func (h *AObjectHandle) QuickNext(ctx context.Context, data **batch.Batch, mp *mpool.MPool) (err error) {
	if h.isEnd() && h.rowOffsetCursor >= h.batchLength {
		return moerr.GetOkExpectedEOF()
	}
	err = h.next(ctx, data, mp, h.rowOffsetCursor, h.batchLength)
	if err != nil {
		return
	}
	return
}

func (h *AObjectHandle) Next(ctx context.Context, bat **batch.Batch, mp *mpool.MPool) (err error) {
	if h.isEnd() && h.rowOffsetCursor >= h.batchLength {
		return moerr.GetOkExpectedEOF()
	}
	return h.next(ctx, bat, mp, h.rowOffsetCursor, h.rowOffsetCursor+1)
}
func (h *AObjectHandle) next(ctx context.Context, bat **batch.Batch, mp *mpool.MPool, start, end int) (err error) {
	if h.isEnd() && h.rowOffsetCursor >= h.batchLength {
		return moerr.GetOkExpectedEOF()
	}
	t0 := time.Now()
	if *bat == nil {
		*bat = batch.NewWithSize(len(h.currentBatch.Vecs))
		(*bat).Attrs = append((*bat).Attrs, h.currentBatch.Attrs...)
		for i, vec := range h.currentBatch.Vecs {
			newVec, err := vec.CloneWindow(start, end, mp)
			if err != nil {
				h.p.changesHandle.copyDuration += time.Since(t0)
				return err
			}
			(*bat).Vecs[i] = newVec
		}
	} else {
		for i, vec := range (*bat).Vecs {
			for rowOffset := start; rowOffset < end; rowOffset++ {
				appendFromEntry(h.currentBatch.Vecs[i], vec, rowOffset, mp)
			}
		}
	}
	(*bat).SetRowCount((*bat).Vecs[0].Length())
	h.p.changesHandle.copyDuration += time.Since(t0)
	h.rowOffsetCursor = end
	if h.rowOffsetCursor >= h.batchLength {
		h.currentBatch.Clean(h.mp)
		h.currentBatch = nil
		h.batchLength = 0
		h.rowOffsetCursor = 0
		if !h.isEnd() {
			err = h.getNextAObject(ctx)
			if err != nil {
				return
			}
		}
	}
	return
}
func (h *AObjectHandle) NextTS() types.TS {
	if h.isEnd() && h.rowOffsetCursor >= h.batchLength {
		return types.TS{}
	}
	commitTSVec := h.currentBatch.Vecs[len(h.currentBatch.Vecs)-1]
	return vector.GetFixedAtNoTypeCheck[types.TS](commitTSVec, h.rowOffsetCursor)
}

type baseHandle struct {
	aobjHandle     *AObjectHandle
	cnObjectHandle *CNObjectHandle
	inMemoryHandle *BatchHandle

	changesHandle *ChangeHandler

	skipTS map[types.TS]struct{}
}

const (
	NextChangeHandle_AObj = iota
	NextChangeHandle_CNObj
	NextChangeHandle_InMemory

	NextChangeHandle_Tombstone
	NextChangeHandle_Data
)

func NewBaseHandler(state *PartitionState, changesHandle *ChangeHandler, start, end types.TS, mp *mpool.MPool, tombstone bool, fs fileservice.FileService, ctx context.Context) (p *baseHandle, err error) {
	p = &baseHandle{
		skipTS:        make(map[types.TS]struct{}),
		changesHandle: changesHandle,
	}
	var iter btree.IterG[objectio.ObjectEntry]
	if tombstone {
		iter = state.tombstoneObjectsNameIndex.Iter()
	} else {
		iter = state.dataObjectsNameIndex.Iter()
	}
	defer iter.Release()
	if tombstone {
		dataIter := state.dataObjectsNameIndex.Iter()
		p.fillInSkipTS(dataIter, start, end)
		dataIter.Release()
	}
	rowIter := state.rows.Iter()
	defer rowIter.Release()
	p.inMemoryHandle = p.newBatchHandleWithRowIterator(ctx, rowIter, start, end, tombstone, mp)
	aobj, cnObj, tnByCreateTS, tnCreateTSKeys := p.getObjectEntries(iter, start, end)
	if p.changesHandle.enableDeleteChainResolve {
		resolvedAObj, resolveErr := p.resolveVisibleObjectsByDeleteChain(
			ctx, start, end, aobj, tnByCreateTS, tnCreateTSKeys, tombstone, "appendable",
		)
		if resolveErr != nil {
			return nil, resolveErr
		}
		resolvedCNObj, resolveErr := p.resolveVisibleObjectsByDeleteChain(
			ctx, start, end, cnObj, tnByCreateTS, tnCreateTSKeys, tombstone, "constant-commit-ts",
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

func NewBaseHandlerWithObjEntries(
	ctx context.Context,
	changesHandle *ChangeHandler,
	start, end types.TS,
	aobj, cnObj []*objectio.ObjectEntry,
	tombstone bool,
	mp *mpool.MPool,
	fs fileservice.FileService,
) (p *baseHandle, err error) {
	p = &baseHandle{
		skipTS:        make(map[types.TS]struct{}),
		changesHandle: changesHandle,
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
	for iter.Next() {
		obj := iter.Item()
		if !obj.DeleteTime.IsEmpty() {
			ts := obj.DeleteTime
			if ts.GE(&start) && ts.LE(&end) {
				p.skipTS[obj.DeleteTime] = struct{}{}
			}
		}
	}
}

func (p *baseHandle) fillInSkipTSFromObjects(start, end types.TS, groups ...[]*objectio.ObjectEntry) {
	for _, group := range groups {
		for _, obj := range group {
			if obj == nil || obj.DeleteTime.IsEmpty() {
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
	_, typ := p.nextTS()
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
	if p.aobjHandle != nil {
		err = p.aobjHandle.QuickNext(ctx, bat, mp)
		if err == nil {
			return
		}
		if moerr.IsMoErrCode(err, moerr.OkExpectedEOF) {
			p.aobjHandle = nil
			err = nil
		}
		if err != nil {
			return
		}
	}
	if p.inMemoryHandle != nil {
		err = p.inMemoryHandle.QuickNext(bat, mp)
		if err == nil {
			return
		}
		if moerr.IsMoErrCode(err, moerr.OkExpectedEOF) {
			p.inMemoryHandle.Close()
			p.inMemoryHandle = nil
			err = nil
		}
		if err != nil {
			return
		}
	}
	err = p.cnObjectHandle.QuickNext(ctx, bat, mp)
	return
}
func (p *baseHandle) newBatchHandleWithRowIterator(ctx context.Context, iter btree.IterG[*RowEntry], start, end types.TS, tombstone bool, mp *mpool.MPool) (h *BatchHandle) {
	bat := p.getBatchesFromRowIterator(iter, start, end, tombstone, mp)
	if bat == nil {
		return nil
	}
	h = NewRowHandle(bat, mp, p, ctx)
	return
}
func (p *baseHandle) getBatchesFromRowIterator(iter btree.IterG[*RowEntry], start, end types.TS, tombstone bool, mp *mpool.MPool) (bat *batch.Batch) {
	for iter.Next() {
		entry := iter.Item()
		if checkTS(start, end, entry.Time) {
			if !entry.Deleted && !tombstone {
				fillInInsertBatch(&bat, entry, mp)
			}
			if entry.Deleted && tombstone {
				if p.skipTS != nil {
					_, ok := p.skipTS[entry.Time]
					if ok {
						continue
					}
				}
				fillInDeleteBatch(&bat, entry, mp)
			}
		}
	}
	return
}
func (p *baseHandle) getObjectEntries(
	objIter btree.IterG[objectio.ObjectEntry],
	start, end types.TS,
) (
	aobj, cnObj []*objectio.ObjectEntry,
	tnByCreateTS map[types.TS][]*objectio.ObjectEntry,
	tnCreateTSKeys []types.TS,
) {
	aobj = make([]*objectio.ObjectEntry, 0)
	cnObj = make([]*objectio.ObjectEntry, 0)
	tnByCreateTS = make(map[types.TS][]*objectio.ObjectEntry)
	tnKeySet := make(map[types.TS]struct{})
	pkf := p.changesHandle.pkFilter
	for objIter.Next() {
		entry := objIter.Item()
		entryCopy := entry
		if entry.GetAppendable() {
			if entry.CreateTime.GT(&end) {
				continue
			}
			if !entry.DeleteTime.IsEmpty() && entry.DeleteTime.LT(&start) {
				continue
			}
			// PK zonemap pruning: skip appendable objects whose sort-key range
			// does not overlap with the requested PK values.
			if pkf != nil && pkf.Vec != nil {
				zm := entry.SortKeyZoneMap()
				if zm.IsInited() && !zm.AnyIn(pkf.Vec) {
					continue
				}
			}
			aobj = append(aobj, &entryCopy)
		} else {
			if entry.ObjectStats.GetCNCreated() {
				if entry.CreateTime.LT(&start) || entry.CreateTime.GT(&end) {
					continue
				}
				if pkf != nil && pkf.Vec != nil {
					zm := entry.SortKeyZoneMap()
					if zm.IsInited() && !zm.AnyIn(pkf.Vec) {
						continue
					}
				}
				cnObj = append(cnObj, &entryCopy)
				continue
			}
			if entry.CreateTime.GT(&end) {
				continue
			}
			// PK zonemap pruning for TN non-appendable objects.
			if pkf != nil && pkf.Vec != nil {
				zm := entry.SortKeyZoneMap()
				if zm.IsInited() && !zm.AnyIn(pkf.Vec) {
					continue
				}
			}
			// Keep every TN-produced non-appendable object in the create-time index so
			// delete-chain resolution can rewrite a deleted/missing predecessor to the
			// replacement object created at the predecessor's delete timestamp.
			tnByCreateTS[entry.CreateTime] = append(tnByCreateTS[entry.CreateTime], &entryCopy)
			tnKeySet[entry.CreateTime] = struct{}{}
			// After checkpoint + GC + restart, older appendable predecessors may be gone;
			// resolveVisibleObjectsByDeleteChain sweeps for orphaned live TN objects.
		}
	}
	tnCreateTSKeys = make([]types.TS, 0, len(tnKeySet))
	for ts := range tnKeySet {
		tnCreateTSKeys = append(tnCreateTSKeys, ts)
	}
	goSort.Slice(aobj, func(i, j int) bool {
		return aobj[i].CreateTime.LT(&aobj[j].CreateTime)
	})
	goSort.Slice(cnObj, func(i, j int) bool {
		return cnObj[i].CreateTime.LT(&cnObj[j].CreateTime)
	})
	goSort.Slice(tnCreateTSKeys, func(i, j int) bool {
		return tnCreateTSKeys[i].LT(&tnCreateTSKeys[j])
	})
	return
}

func (p *baseHandle) resolveVisibleObjectsByDeleteChain(
	ctx context.Context,
	start, end types.TS,
	visible []*objectio.ObjectEntry,
	tnByCreateTS map[types.TS][]*objectio.ObjectEntry,
	tnCreateTSKeys []types.TS,
	isTombstone bool,
	kind string,
) ([]*objectio.ObjectEntry, error) {
	if len(visible) == 0 && len(tnByCreateTS) == 0 {
		return visible, nil
	}
	resolved := make([]*objectio.ObjectEntry, 0, len(visible))
	queue := make([]*objectio.ObjectEntry, 0, len(visible))
	queue = append(queue, visible...)
	visited := make(map[string]struct{}, len(visible))
	missingCnt := 0
	rewriteHopCnt := 0
	fuzzyHopCnt := 0
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		if current == nil {
			continue
		}
		name := current.ObjectShortName().ShortString()
		if _, ok := visited[name]; ok {
			continue
		}
		visited[name] = struct{}{}
		if !current.GetAppendable() && !current.DeleteTime.IsEmpty() && current.DeleteTime.LE(&end) {
			next, successorTS, exact := lookupDeleteChainSuccessor(current.DeleteTime, tnByCreateTS, tnCreateTSKeys)
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
			if !exact {
				fuzzyHopCnt++
				logutil.Info(
					"ChangesHandle-DeleteChain matched successor create-time",
					zap.String("kind", kind),
					zap.Bool("tombstone", isTombstone),
					zap.String("current", name),
					zap.String("delete-time", current.DeleteTime.ToString()),
					zap.String("successor-create-time", successorTS.ToString()),
				)
			}
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
		if current.DeleteTime.IsEmpty() {
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
		next, successorTS, exact := lookupDeleteChainSuccessor(current.DeleteTime, tnByCreateTS, tnCreateTSKeys)
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
		if !exact {
			fuzzyHopCnt++
			logutil.Info(
				"ChangesHandle-DeleteChain matched successor create-time",
				zap.String("kind", kind),
				zap.Bool("tombstone", isTombstone),
				zap.String("missing", name),
				zap.String("delete-time", current.DeleteTime.ToString()),
				zap.String("successor-create-time", successorTS.ToString()),
			)
		}
		queue = append(queue, next...)
	}
	// Sweep for orphaned TN objects whose appendable predecessors were GC'd.
	orphanCnt := 0
	for _, objs := range tnByCreateTS {
		for _, obj := range objs {
			name := obj.ObjectShortName().ShortString()
			if _, ok := visited[name]; ok {
				continue
			}
			visited[name] = struct{}{}
			if !obj.DeleteTime.IsEmpty() && obj.DeleteTime.LE(&end) {
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
			zap.Int("fuzzy-hops", fuzzyHopCnt),
		)
	}
	return resolved, nil
}

// lookupDeleteChainSuccessor returns replacement TN non-appendable objects for
// a missing visible object.
func lookupDeleteChainSuccessor(
	deleteTS types.TS,
	tnByCreateTS map[types.TS][]*objectio.ObjectEntry,
	tnCreateTSKeys []types.TS,
) (next []*objectio.ObjectEntry, successorTS types.TS, exact bool) {
	if objs := tnByCreateTS[deleteTS]; len(objs) > 0 {
		return objs, deleteTS, true
	}
	if len(tnCreateTSKeys) == 0 {
		return nil, types.TS{}, false
	}
	idx := goSort.Search(len(tnCreateTSKeys), func(i int) bool {
		return !tnCreateTSKeys[i].LT(&deleteTS)
	})
	if idx >= len(tnCreateTSKeys) {
		return nil, types.TS{}, false
	}
	successorTS = tnCreateTSKeys[idx]
	return tnByCreateTS[successorTS], successorTS, false
}

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
			name := obj.ObjectShortName().ShortString()
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
	skipDeletes     bool
	isRecoveryMode  bool // When true, Case 2.2 (insert->delete) will keep the delete for CDC restart scenarios
	tombstoneHandle *baseHandle
	dataHandle      *baseHandle
	coarseMaxRow    int
	quick           bool
	primarySeqnum   int
	scheduler       tasks.JobScheduler
	mp              *mpool.MPool

	readDuration, copyDuration    time.Duration
	updateDuration, totalDuration time.Duration
	dataLength, tombstoneLength   int
	lastPrint                     time.Time

	start, end types.TS
	fs         fileservice.FileService
	minTS      types.TS

	LogThreshold time.Duration

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
	}
	defer func() {
		if err != nil {
			if changeHandle != nil {
				_ = changeHandle.Close()
				changeHandle = nil
			}
		}
	}()
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
	changeHandle = &ChangeHandler{
		coarseMaxRow:   int(maxRow),
		skipDeletes:    skipDeletes,
		isRecoveryMode: isRecoveryMode,
		start:          start,
		end:            end,
		fs:             fs,
		minTS:          start,
		LogThreshold:   LogThreshold,
		primarySeqnum:  primarySeqnum,
		mp:             mp,
		scheduler:      tasks.NewParallelJobScheduler(LoadParallism),
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
	if err = changeHandle.dataHandle.init(ctx, changeHandle.quick, mp); err != nil {
		return
	}
	changeHandle.tombstoneHandle, err = NewBaseHandlerWithObjEntries(ctx, changeHandle, start, end, tombstoneAobj, tombstoneCNObj, true, mp, fs)
	if err != nil {
		return
	}
	if err = changeHandle.tombstoneHandle.init(ctx, changeHandle.quick, mp); err != nil {
		return
	}
	if selection == checkpointObjectSelectionRange {
		changeHandle.tombstoneHandle.fillInSkipTSFromObjects(start, end, dataAobj, dataCNObj)
	}
	return changeHandle, nil
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
	dataAobjMap := make(map[string]*objectio.ObjectEntry)
	dataCNObjMap := make(map[string]*objectio.ObjectEntry)
	tombstoneAobjMap := make(map[string]*objectio.ObjectEntry)
	tombstoneCNObjMap := make(map[string]*objectio.ObjectEntry)
	readers := make([]checkpointEntryReader, 0)
	for _, entry := range checkpoint {
		reader := newCKPReaderWithTableID(entry.GetVersion(), entry.GetLocation(), tid, mp, fs)
		readers = append(readers, reader)
		if fs != nil && sid != "" {
			_ = ioutil.Prefetch(sid, fs, entry.GetLocation())
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
				switch classifyCheckpointObject(obj, isTombstone, start, end, selection) {
				case checkpointObjectKindRowCommitTS:
					if isTombstone {
						tombstoneAobjMap[obj.ObjectShortName().ShortString()] = &obj
					} else {
						dataAobjMap[obj.ObjectShortName().ShortString()] = &obj
					}
				case checkpointObjectKindConstantCommitTS:
					if isTombstone {
						tombstoneCNObjMap[obj.ObjectShortName().ShortString()] = &obj
					} else {
						dataCNObjMap[obj.ObjectShortName().ShortString()] = &obj
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
		if obj.CreateTime.LT(&start) || obj.CreateTime.GT(&end) {
			return checkpointObjectKindIgnore
		}
		// DN-created non-appendable objects may be rewritten by flush/merge, so
		// object create time alone does not describe which rows belong to
		// CollectChanges(start, end). Keep them on the row-commit-ts path and let
		// the batch-level TS filter recover only the rows whose commit TS falls in
		// the requested interval.
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
	if state.start.GT(&start) {
		return nil, moerr.NewErrStaleReadNoCtx(state.start.ToString(), start.ToString())
	}
	changeHandle = &ChangeHandler{
		coarseMaxRow: int(maxRow),
		skipDeletes:  skipDeletes,
		// isRecoveryMode: false (default) - normal operation, Case 2.2 deletes all rows
		start:         start,
		end:           end,
		fs:            fs,
		minTS:         state.start,
		LogThreshold:  LogThreshold,
		primarySeqnum: primarySeqnum,
		mp:            mp,
		scheduler:     tasks.NewParallelJobScheduler(LoadParallism),
		pkFilter:      engine.PKFilterFromContext(ctx),
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
	if p.dataHandle.IsSmall() && p.tombstoneHandle.IsSmall() {
		p.quick = true
	}
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
	for {
		dataEnd := false
		tombstoneEnd := false
		err = p.dataHandle.QuickNext(ctx, &data, mp)
		if moerr.IsMoErrCode(err, moerr.OkExpectedEOF) {
			dataEnd = true
			err = nil
		}
		if err != nil {
			return
		}
		err = p.tombstoneHandle.QuickNext(ctx, &tombstone, mp)
		if moerr.IsMoErrCode(err, moerr.OkExpectedEOF) {
			tombstoneEnd = true
			err = nil
		}
		if err != nil {
			return
		}
		if err = filterBatch(data, tombstone, p.primarySeqnum, p.skipDeletes, p.isRecoveryMode); err != nil {
			return
		}
		if tombstoneEnd && dataEnd {
			break
		}
		if dataEnd && tombstone.RowCount() > p.coarseMaxRow {
			break
		}
		if tombstoneEnd && data.RowCount() > p.coarseMaxRow {
			break
		}
	}
	return
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
func filterBatch(data, tombstone *batch.Batch, primarySeqnum int, skipDeletes bool, isRecoveryMode bool) (err error) {
	if data == nil || tombstone == nil {
		return
	}

	type rowInfo struct {
		row      int
		ts       types.TS
		isDelete bool
	}

	// Build maps for data and tombstone batches
	rowInfoMap := make(map[any][]rowInfo)

	// Process data batch
	pkVec := data.Vecs[primarySeqnum]
	tsVec := data.Vecs[len(data.Vecs)-1]
	timestamps := vector.MustFixedColWithTypeCheck[types.TS](tsVec)
	for i := 0; i < pkVec.Length(); i++ {
		pkVal := vector.GetAny(pkVec, i, false)
		if _, ok := pkVal.([]byte); ok {
			pkVal = string(pkVal.([]byte))
		}
		rowInfoMap[pkVal] = append(rowInfoMap[pkVal], rowInfo{
			row:      i,
			ts:       timestamps[i],
			isDelete: false,
		})
	}

	// Process tombstone batch
	pkVec = tombstone.Vecs[0]
	tsVec = tombstone.Vecs[1]
	timestamps = vector.MustFixedColWithTypeCheck[types.TS](tsVec)
	for i := 0; i < pkVec.Length(); i++ {
		pkVal := vector.GetAny(pkVec, i, false)
		if _, ok := pkVal.([]byte); ok {
			pkVal = string(pkVal.([]byte))
		}
		rowInfoMap[pkVal] = append(rowInfoMap[pkVal], rowInfo{
			row:      i,
			ts:       timestamps[i],
			isDelete: true,
		})
	}

	dataRowsToDelete := make([]int64, 0)
	tombstoneRowsToDelete := make([]int64, 0)

	for _, rowInfos := range rowInfoMap {
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
			// Keep only last insert
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
		if data != nil && data.RowCount() == 0 {
			data.Clean(p.mp)
			data = nil
		}
		if tombstone != nil && tombstone.RowCount() == 0 {
			tombstone.Clean(p.mp)
			tombstone = nil
		}
	}()
	hint = engine.ChangesHandle_Tail_done
	t0 := time.Now()
	if p.quick {
		if data, tombstone, err = p.quickNext(ctx, mp); err != nil {
			return
		}
		p.totalDuration += time.Since(t0)
		if data != nil {
			p.dataLength += data.Vecs[0].Length()
		}
		if tombstone != nil {
			p.tombstoneLength += tombstone.Vecs[0].Length()
		}
		return
	}
	for {
		typ := p.decideNextHandle()
		switch typ {
		case NextChangeHandle_Data:
			err = p.dataHandle.Next(ctx, &data, mp)
			if err == nil && data.Vecs[0].Length() >= p.coarseMaxRow*2 {
				if err = filterBatch(data, tombstone, p.primarySeqnum, p.skipDeletes, p.isRecoveryMode); err != nil {
					return
				}
				if data.Vecs[0].Length() > p.coarseMaxRow {
					p.totalDuration += time.Since(t0)
					if data != nil {
						p.dataLength += data.Vecs[0].Length()
					}
					if tombstone != nil {
						p.tombstoneLength += tombstone.Vecs[0].Length()
					}
					return
				}
			}
		case NextChangeHandle_Tombstone:
			err = p.tombstoneHandle.Next(ctx, &tombstone, mp)
			if err == nil && tombstone.Vecs[0].Length() >= p.coarseMaxRow*2 {
				if err = filterBatch(data, tombstone, p.primarySeqnum, p.skipDeletes, p.isRecoveryMode); err != nil {
					return
				}
				if tombstone.Vecs[0].Length() > p.coarseMaxRow {
					p.totalDuration += time.Since(t0)
					if data != nil {
						p.dataLength += data.Vecs[0].Length()
					}
					if tombstone != nil {
						p.tombstoneLength += tombstone.Vecs[0].Length()
					}
					return
				}
			}
		}
		if moerr.IsMoErrCode(err, moerr.OkExpectedEOF) {
			err = nil
			if err = filterBatch(data, tombstone, p.primarySeqnum, p.skipDeletes, p.isRecoveryMode); err != nil {
				return
			}
			p.totalDuration += time.Since(t0)
			if data != nil {
				p.dataLength += data.Vecs[0].Length()
			}
			if tombstone != nil {
				p.tombstoneLength += tombstone.Vecs[0].Length()
			}
			return
		}
		if err != nil {
			p.totalDuration += time.Since(t0)
			if data != nil {
				p.dataLength += data.Vecs[0].Length()
			}
			if tombstone != nil {
				p.tombstoneLength += tombstone.Vecs[0].Length()
			}
			return
		}
	}
}

func applyTSFilterForBatch(bat *batch.Batch, sortIdx int, skipTS map[types.TS]struct{}, start, end types.TS) error {
	if bat == nil {
		return nil
	}
	if bat.Vecs[sortIdx].GetType().Oid != types.T_TS {
		panic(fmt.Sprintf("logic error, batch attrs %v, sort idx %d", bat.Attrs, sortIdx))
	}
	commitTSs := vector.MustFixedColWithTypeCheck[types.TS](bat.Vecs[sortIdx])
	deletes := make([]int64, 0)
	for i, ts := range commitTSs {
		if ts.LT(&start) || ts.GT(&end) {
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
	for _, vec := range bat.Vecs {
		vec.Shrink(deletes, true)
	}
	return nil
}
func sortBatch(bat *batch.Batch, sortIdx int, mp *mpool.MPool) error {
	if bat == nil {
		return nil
	}
	if bat.Vecs[sortIdx].GetType().Oid != types.T_TS {
		panic(fmt.Sprintf("logic error, batch attrs %v, sort idx %d", bat.Attrs, sortIdx))
	}
	sortedIdx := make([]int64, bat.Vecs[0].Length())
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

func newDataBatchWithBatch(src *batch.Batch) (data *batch.Batch) {
	data = batch.NewWithSize(0)
	data.Attrs = append(data.Attrs, src.Attrs[2:]...)
	for _, vec := range src.Vecs {
		if vec.GetType().Oid == types.T_Rowid || vec.GetType().Oid == types.T_TS {
			continue
		}
		newVec := vector.NewVec(*vec.GetType())
		data.Vecs = append(data.Vecs, newVec)
	}
	data.Attrs = append(data.Attrs, objectio.DefaultCommitTS_Attr)
	newVec := vector.NewVec(types.T_TS.ToType())
	data.Vecs = append(data.Vecs, newVec)
	return
}

func appendFromEntry(src, vec *vector.Vector, offset int, mp *mpool.MPool) {
	if src.IsNull(uint64(offset)) {
		vector.AppendAny(vec, nil, true, mp)
	} else {
		var val any
		switch vec.GetType().Oid {
		case types.T_bool:
			val = vector.GetFixedAtNoTypeCheck[bool](src, offset)
		case types.T_bit:
			val = vector.GetFixedAtNoTypeCheck[uint64](src, offset)
		case types.T_int8:
			val = vector.GetFixedAtNoTypeCheck[int8](src, offset)
		case types.T_int16:
			val = vector.GetFixedAtNoTypeCheck[int16](src, offset)
		case types.T_int32:
			val = vector.GetFixedAtNoTypeCheck[int32](src, offset)
		case types.T_int64:
			val = vector.GetFixedAtNoTypeCheck[int64](src, offset)
		case types.T_uint8:
			val = vector.GetFixedAtNoTypeCheck[uint8](src, offset)
		case types.T_uint16:
			val = vector.GetFixedAtNoTypeCheck[uint16](src, offset)
		case types.T_uint32:
			val = vector.GetFixedAtNoTypeCheck[uint32](src, offset)
		case types.T_uint64:
			val = vector.GetFixedAtNoTypeCheck[uint64](src, offset)
		case types.T_decimal64:
			val = vector.GetFixedAtNoTypeCheck[types.Decimal64](src, offset)
		case types.T_decimal128:
			val = vector.GetFixedAtNoTypeCheck[types.Decimal128](src, offset)
		case types.T_uuid:
			val = vector.GetFixedAtNoTypeCheck[types.Uuid](src, offset)
		case types.T_float32:
			val = vector.GetFixedAtNoTypeCheck[float32](src, offset)
		case types.T_float64:
			val = vector.GetFixedAtNoTypeCheck[float64](src, offset)
		case types.T_date:
			val = vector.GetFixedAtNoTypeCheck[types.Date](src, offset)
		case types.T_time:
			val = vector.GetFixedAtNoTypeCheck[types.Time](src, offset)
		case types.T_datetime:
			val = vector.GetFixedAtNoTypeCheck[types.Datetime](src, offset)
		case types.T_timestamp:
			val = vector.GetFixedAtNoTypeCheck[types.Timestamp](src, offset)
		case types.T_enum:
			val = vector.GetFixedAtNoTypeCheck[types.Enum](src, offset)
		case types.T_TS:
			val = vector.GetFixedAtNoTypeCheck[types.TS](src, offset)
		case types.T_Rowid:
			val = vector.GetFixedAtNoTypeCheck[types.Rowid](src, offset)
		case types.T_Blockid:
			val = vector.GetFixedAtNoTypeCheck[types.Blockid](src, offset)
		case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary, types.T_json, types.T_blob, types.T_text,
			types.T_array_float32, types.T_array_float64, types.T_datalink:
			val = src.GetBytesAt(offset)
		default:
			//return vector.ErrVecTypeNotSupport
			panic(any("No Support"))
		}
		vector.AppendAny(vec, val, false, mp)
	}

}

func fillInInsertBatch(bat **batch.Batch, entry *RowEntry, mp *mpool.MPool) {
	if *bat == nil {
		(*bat) = newDataBatchWithBatch(entry.Batch)
	}
	for i, vec := range entry.Batch.Vecs {
		if vec.GetType().Oid == types.T_Rowid || vec.GetType().Oid == types.T_TS {
			continue
		}
		appendFromEntry(vec, (*bat).Vecs[i-2], int(entry.Offset), mp)
	}
	appendFromEntry(entry.Batch.Vecs[1], (*bat).Vecs[len((*bat).Vecs)-1], int(entry.Offset), mp)

}
func fillInDeleteBatch(bat **batch.Batch, entry *RowEntry, mp *mpool.MPool) {
	pkVec := entry.Batch.Vecs[2]
	if *bat == nil {
		(*bat) = batch.NewWithSize(2)
		(*bat).SetAttributes([]string{
			objectio.TombstoneAttr_PK_Attr,
			objectio.DefaultCommitTS_Attr,
		})
		(*bat).Vecs[0] = vector.NewVec(*pkVec.GetType())
		(*bat).Vecs[1] = vector.NewVec(types.T_TS.ToType())
	}
	appendFromEntry(pkVec, (*bat).Vecs[0], int(entry.Offset), mp)
	vector.AppendFixed((*bat).Vecs[1], entry.Time, false, mp)
}

// PXU TODO
func checkTS(start, end types.TS, ts types.TS) bool {
	return ts.LE(&end) && ts.GE(&start)
}

func prefetchObjects(
	ctx context.Context,
	blockID uint32,
	fs fileservice.FileService,
	stats *objectio.ObjectStats,
	scheduler tasks.JobScheduler) (job *tasks.Job) {
	job = getJob(
		ctx,
		stats.ObjectName().String(),
		JTCDCLoad,
		func(ctx context.Context) (res *tasks.JobResult) {
			loc := stats.BlockLocation(uint16(blockID), 8192)
			bat, _, err := ioutil.LoadOneBlock(
				ctx,
				fs,
				loc,
				objectio.SchemaData,
			)
			res = &tasks.JobResult{}
			if err != nil {
				res.Err = err
			} else {
				res.Res = bat
			}
			return
		},
	)
	scheduler.Schedule(job)
	return
}

func updateTombstoneBatch(bat *batch.Batch, start, end types.TS, skipTS map[types.TS]struct{}, sort bool, mp *mpool.MPool) {
	bat.Vecs[0].Free(mp) // rowid
	//bat.Vecs[2].Free(mp) // phyaddr
	bat.Vecs = []*vector.Vector{bat.Vecs[1], bat.Vecs[2]}
	bat.Attrs = []string{
		objectio.TombstoneAttr_PK_Attr,
		objectio.DefaultCommitTS_Attr}
	applyTSFilterForBatch(bat, 1, skipTS, start, end)
	if sort {
		sortBatch(bat, 1, mp)
	}
}
func updateDataBatch(bat *batch.Batch, start, end types.TS, mp *mpool.MPool) {
	filteredVecs := make([]*vector.Vector, 0, len(bat.Vecs))
	var commitTSVec *vector.Vector
	rebuildAttrs := len(bat.Attrs) == len(bat.Vecs)
	filteredAttrs := make([]string, 0, len(bat.Attrs))
	var commitTSAttr string

	for i, vec := range bat.Vecs {
		switch vec.GetType().Oid {
		case types.T_Rowid:
			vec.Free(mp)
		case types.T_TS:
			commitTSVec = vec
			if rebuildAttrs {
				commitTSAttr = bat.Attrs[i]
			}
		default:
			filteredVecs = append(filteredVecs, vec)
			if rebuildAttrs {
				filteredAttrs = append(filteredAttrs, bat.Attrs[i])
			}
		}
	}
	if commitTSVec != nil {
		filteredVecs = append(filteredVecs, commitTSVec)
		if rebuildAttrs {
			if commitTSAttr == "" {
				commitTSAttr = objectio.DefaultCommitTS_Attr
			}
			filteredAttrs = append(filteredAttrs, commitTSAttr)
		}
	}
	bat.Vecs = filteredVecs
	if rebuildAttrs {
		bat.Attrs = filteredAttrs
	}
	applyTSFilterForBatch(bat, len(bat.Vecs)-1, nil, start, end)
}

func updateCNTombstoneBatch(bat *batch.Batch, committs types.TS, mp *mpool.MPool) {
	var pk *vector.Vector
	for _, vec := range bat.Vecs {
		if vec.GetType().Oid != types.T_Rowid {
			pk = vec
		} else {
			vec.Free(mp)
		}
	}
	commitTS, err := vector.NewConstFixed(types.T_TS.ToType(), committs, pk.Length(), mp)
	if err != nil {
		return
	}
	bat.Vecs = []*vector.Vector{pk, commitTS}
	bat.Attrs = []string{objectio.TombstoneAttr_PK_Attr, objectio.DefaultCommitTS_Attr}
}
func updateCNDataBatch(bat *batch.Batch, commitTS types.TS, mp *mpool.MPool) {
	commitTSVec, err := vector.NewConstFixed(types.T_TS.ToType(), commitTS, bat.Vecs[0].Length(), mp)
	if err != nil {
		return
	}
	bat.Vecs = append(bat.Vecs, commitTSVec)
}

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
func SetCheckpointReaderFactoryForTest(factory func(uint32, objectio.Location, uint64, *mpool.MPool, fileservice.FileService) checkpointEntryReader) func() {
	old := newCKPReaderWithTableID
	newCKPReaderWithTableID = factory
	return func() {
		newCKPReaderWithTableID = old
	}
}
