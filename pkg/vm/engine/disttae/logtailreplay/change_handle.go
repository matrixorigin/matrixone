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
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	goSort "sort"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/sort"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
)

const (
	JTCDCLoad tasks.JobType = 300 + iota
)

func objectPrefetchWidth(maxRows, maxBytes int) int {
	if maxRows > 0 || maxBytes > 0 {
		return 1
	}
	return LoadParallism
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

type replayRowHandle interface {
	init(bool, *mpool.MPool) error
	IsEmpty() bool
	Rows() int
	NextTS() types.TS
	Close()
	Next(**batch.Batch, *mpool.MPool) error
	QuickNext(**batch.Batch, *mpool.MPool) error
}

func batchesShareAppendSchema(dst, src *batch.Batch) bool {
	if dst == nil || src == nil {
		return true
	}
	if len(dst.Vecs) != len(src.Vecs) {
		return false
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

// grownCapacity mirrors the exact allocation sequence the vector growth
// path performs when a buffer at capacity prev must hold required bytes:
// repeated mpool.GrowCapacity steps from prev, each REPLACING the previous
// buffer — the replacement is allocated before the old buffer is freed, so
// the previous and new buffers coexist during the step. It returns the
// final capacity the growth settles at and the extra bytes (beyond prev,
// which a combined-live count already charges) the sequence's peak needs:
// the largest (previous + replacement) coexistence minus prev. Callers
// share mpool.GrowCapacity itself with the allocator, so admission and
// allocation can never disagree about capacity classes or rounding.
func grownCapacity(prev, required int) (final, extra int) {
	if prev < 0 {
		prev = 0
	}
	if required <= prev {
		return prev, 0
	}
	current := prev
	transient := 0
	for current < required {
		next, ok := mpool.GrowCapacity(int64(current), int64(required))
		if !ok {
			// The allocator itself rejects this growth; charge the failing
			// request so admission rejects first with the capacity class.
			return required, required
		}
		if step := current + int(next); step > transient {
			transient = step
		}
		current = int(next)
	}
	return current, transient - prev
}

// dstVecGrowth tracks one assembly-destination vector's allocator-owned
// buffer capacities across serves. Vector Shrink compaction is in place
// and the varlena area is an append log, so both capacities and the area
// high-water are monotone: they survive coalescing unchanged.
type dstVecGrowth struct {
	dataCap   int
	areaCap   int
	areaUsed  int
	bitmapCap int // bytes of externally stored null bitmap words
}

// dstGrowthModel mirrors the allocator state of one accumulating assembly
// destination batch. The model is advanced speculatively at each
// pre-growth admission gate (chargeServeGrow stages the post-serve state)
// and committed only after the serve it admitted actually succeeds, so a
// rejected or schema-stopped serve never corrupts the tracked state. When
// the destination batch itself is replaced or released, the model resets
// with it: a fresh destination is created by the serve's first append,
// which allocates from zero capacities through the same growth arithmetic.
type dstGrowthModel struct {
	vecs    []dstVecGrowth
	pending []dstVecGrowth
}

// growColSpec describes one column of an append: the fixed slot size, the
// summed out-of-line varlena payload the append copies into the area, and
// whether any appended row may carry a null (which can install externally
// stored null bitmap words on accounted destination vectors). Const
// sources expand per appended row, matching the materialization the serve
// performs.
type growColSpec struct {
	typeSize int
	varlen   bool
	payload  int
	nulls    bool
}

// growStep advances one modeled buffer capacity to hold req bytes with
// the allocator's growth arithmetic and raises peak to the growth step's
// transient when it crosses it: the previous and replacement buffers
// coexist during the reallocation, so the step's peak is prev+final.
// Serves whose requirement never exceeds the current capacity allocate
// nothing and leave the peak alone.
func growStep(capRef *int, req int, peak *int) {
	if req <= *capRef {
		return
	}
	prev := *capRef
	final, _ := grownCapacity(prev, req)
	if transient := prev + final; transient > *peak {
		*peak = transient
	}
	*capRef = final
}

// chargeColumnGrow advances one column's modeled capacities across a
// serve appending [start, end) rows of src to a destination that
// already holds dstRows rows, returning the growth sequence's transient
// peaks beyond the capacities the combined-live count already charges.
// union selects the serve's growth primitive:
//
//   - A Vector.Union serve (CN object windows, both creation and
//     append) extends every buffer ONCE for the whole selection
//     (extendWithBitmaps) and pre-grows the varlena area once
//     (pregrowVarlenaArea); a constant source materializes once per
//     appended row in the data buffer, but its varlena payload is
//     stored once (BuildVarlenaFromVarlena) and broadcast by
//     descriptor.
//   - The clone family (BatchHandle/AObjectHandle window serves)
//     appends one row at a time onto an existing destination
//     (appendFromEntry), growing every buffer with one allocator step
//     per row; a creation serve bulk-extends the data buffer
//     (PreExtend) and copies the null bitmap once
//     (copyWindowBitmapsOwned) but still grows the varlena area per
//     appended row (BuildVarlenaFromByteSlice).
//
// Charging the peak of the sequence each primitive actually runs —
// instead of the payload sum — is what lets admission reject BEFORE an
// over-budget reallocation exists; the one-shot charge for a multi-row
// row-append would settle below the real capacity (the allocator's
// amortized doubling overshoots a single round-up).
func (g *dstVecGrowth) chargeColumnGrow(dstRows int, src *vector.Vector, start, end int, union bool) (add int) {
	if src == nil || start >= end {
		return 0
	}
	t := src.GetType()
	rows := end - start
	typeSize := t.TypeSize()
	constVec := src.IsConst()
	constNull := src.IsConstNull()

	// Data buffer.
	initData := g.dataCap
	peakData := initData
	if union || dstRows == 0 {
		growStep(&g.dataCap, (dstRows+rows)*typeSize, &peakData)
	} else {
		for r := 1; r <= rows; r++ {
			growStep(&g.dataCap, (dstRows+r)*typeSize, &peakData)
		}
	}
	add += peakData - initData

	// Varlena area: an append log of out-of-line payload bytes.
	if t.IsVarlen() {
		payloadAt := func(r int) int {
			// r is an absolute src row index in [start, end).
			if constVec {
				if constNull {
					return 0
				}
				if bs := src.GetBytesAt(0); len(bs) > types.VarlenaInlineSize {
					return len(bs)
				}
				return 0
			}
			if src.IsNull(uint64(r)) {
				return 0
			}
			if bs := src.GetBytesAt(r); len(bs) > types.VarlenaInlineSize {
				return len(bs)
			}
			return 0
		}
		initArea := g.areaCap
		peakArea := initArea
		if union {
			total := 0
			if constVec {
				// A constant varlena stores its payload once; the union
				// broadcasts the varlena descriptor across the extended
				// data buffer.
				total = payloadAt(start)
			} else {
				for r := start; r < end; r++ {
					total += payloadAt(r)
				}
			}
			areaReq := g.areaUsed + total
			growStep(&g.areaCap, areaReq, &peakArea)
			g.areaUsed = areaReq
		} else {
			for r := start; r < end; r++ {
				if p := payloadAt(r); p != 0 {
					areaReq := g.areaUsed + p
					growStep(&g.areaCap, areaReq, &peakArea)
					g.areaUsed = areaReq
				}
			}
		}
		add += peakArea - initArea
	}

	// Externally stored null bitmap words (what the destination's
	// Allocated reports for installed storage).
	hasNullRows := constNull
	if !hasNullRows && !constVec && src.HasNull() {
		for r := start; r < end; r++ {
			if src.IsNull(uint64(r)) {
				hasNullRows = true
				break
			}
		}
	}
	if hasNullRows {
		initBitmap := g.bitmapCap
		peakBitmap := initBitmap
		if union || dstRows == 0 {
			growStep(&g.bitmapCap, ((dstRows+rows+63)/64)*8, &peakBitmap)
		} else {
			for r := 0; r < rows; r++ {
				if constNull || src.IsNull(uint64(start+r)) {
					growStep(&g.bitmapCap, ((dstRows+r+1+63)/64)*8, &peakBitmap)
				}
			}
		}
		add += peakBitmap - initBitmap
	}
	return add
}

// chargeGrowCols computes the bytes one append of rows entries described
// by cols allocates beyond the destination's currently modeled capacities,
// staging the post-append capacities as pending. dstRows is the
// destination's row count before the append (zero for a creation append:
// destination vectors do not exist yet and every buffer allocates from
// zero through the same growth arithmetic). The charge is the growth
// sequence's transient peak beyond the currently modeled — and externally
// counted — capacities, so an admitted append can never exceed the
// admitted envelope, and the post-serve accounting backstop always passes.
func (m *dstGrowthModel) chargeGrowCols(dstRows int, cols []growColSpec, rows int) (add int) {
	m.pending = m.pending[:0]
	for i, col := range cols {
		model := dstVecGrowth{}
		if i < len(m.vecs) {
			model = m.vecs[i]
		}
		next := model
		finalRows := dstRows + rows
		var extra int
		next.dataCap, extra = grownCapacity(model.dataCap, finalRows*col.typeSize)
		add += extra
		if col.varlen {
			areaReq := model.areaUsed + col.payload
			next.areaCap, extra = grownCapacity(model.areaCap, areaReq)
			add += extra
			next.areaUsed = areaReq
		}
		if col.nulls {
			bitmapReq := ((finalRows + 63) / 64) * 8
			next.bitmapCap, extra = grownCapacity(model.bitmapCap, bitmapReq)
			add += extra
		}
		m.pending = append(m.pending, next)
	}
	return add
}

// chargeServeGrow computes the allocation charge of one serve appending
// [start, end) rows of src to the tracked destination, staging the
// post-serve capacities as pending. union selects the growth primitive
// the serve runs: Vector.Union serves (CN object windows) extend every
// buffer once for the whole selection, while the clone family
// (BatchHandle/AObjectHandle serves) appends row by row onto an
// existing destination. See chargeColumnGrow.
func (m *dstGrowthModel) chargeServeGrow(dstRows int, src *batch.Batch, start, end int, union bool) (add int) {
	if src == nil || start >= end {
		m.pending = m.pending[:0]
		return 0
	}
	m.pending = m.pending[:0]
	for i := range src.Vecs {
		var next dstVecGrowth
		if i < len(m.vecs) {
			next = m.vecs[i]
		}
		if src.Vecs[i] != nil {
			add += next.chargeColumnGrow(dstRows, src.Vecs[i], start, end, union)
		}
		m.pending = append(m.pending, next)
	}
	return add
}

// commitPending applies the staged post-serve model after the admitted
// serve succeeded, transferring the staged slice into the tracked state.
func (m *dstGrowthModel) commitPending() {
	if len(m.pending) > 0 {
		m.vecs = m.pending
		m.pending = nil
	}
}

// dropPending discards the staged model after a serve that was stopped
// (schema boundary) or failed, keeping the tracked state untouched.
func (m *dstGrowthModel) dropPending() {
	m.pending = m.pending[:0]
}

// fork returns a value copy used to mirror a multi-leg serve walk (quick
// mode) without touching the tracked state; the serving leg commits the
// fork's charge into its own staging so the whole walk stays atomic.
func (m *dstGrowthModel) fork() dstGrowthModel {
	f := dstGrowthModel{vecs: make([]dstVecGrowth, len(m.vecs))}
	copy(f.vecs, m.vecs)
	return f
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

// prospectiveWindow reports the window the next QuickNext call will
// append to the caller's destination BEFORE any CloneWindow allocation:
// QuickNext always clones the whole remaining window. A schema boundary
// against the current destination makes the serve stop with
// OkExpectedEOB without appending, reported as eob. The allocation
// charge of appending this window is computed by the caller through the
// destination growth model, which shares capacity arithmetic with the
// allocator itself.
func (r *BatchHandle) prospectiveWindow(bat *batch.Batch) (src *batch.Batch, start, end int, eob bool) {
	if r == nil || r.isEnd() {
		return nil, 0, 0, false
	}
	if bat != nil && !batchesShareAppendSchema(bat, r.batches) {
		return nil, 0, 0, true
	}
	return r.batches, r.rowOffsetCursor, r.batchLength, false
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
		if !batchesShareAppendSchema(*bat, r.batches) {
			return moerr.GetOkExpectedEOB()
		}
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

const rangeSpillRecordHeaderSize = int64(8)

type rangeSpillRecord struct {
	offset int64
	size   int64
	rows   int
}

type rangeSpillRun struct {
	firstRecord int
	endRecord   int
}

type rangeSpillFile struct {
	file        *os.File
	records     []rangeSpillRecord
	bytes       int64
	disk        engine.ChangeRangeGrowingSpillReservation
	fileReserve engine.ChangeRangeSpillReservation
}

type legacyColumnSpill struct {
	owner *rangeSpillFile
}

func newLegacyColumnSpill(
	ctx context.Context,
	config engine.ChangeRangeSpillConfig,
) (objectio.ColumnWindowSpill, error) {
	owner, err := newRangeSpillFile(ctx, config)
	if err != nil {
		return nil, err
	}
	return &legacyColumnSpill{owner: owner}, nil
}

func (s *legacyColumnSpill) ReadAt(data []byte, offset int64) (int, error) {
	if s == nil || s.owner == nil || s.owner.file == nil {
		return 0, os.ErrClosed
	}
	return s.owner.file.ReadAt(data, offset)
}

func (s *legacyColumnSpill) Write(data []byte) (int, error) {
	if s == nil || s.owner == nil || s.owner.file == nil {
		return 0, os.ErrClosed
	}
	return s.owner.file.Write(data)
}

func (s *legacyColumnSpill) Grow(size uint64) error {
	if s == nil || s.owner == nil || s.owner.disk == nil {
		return moerr.NewInternalErrorNoCtx("legacy object column spill reservation is missing")
	}
	return s.owner.disk.Grow(size)
}

func (s *legacyColumnSpill) Close() error {
	if s == nil || s.owner == nil {
		return nil
	}
	err := s.owner.Close()
	s.owner = nil
	return err
}

func newRangeSpillFile(ctx context.Context, config engine.ChangeRangeSpillConfig) (*rangeSpillFile, error) {
	if !config.Enabled() {
		return nil, moerr.NewInternalErrorNoCtx("change range spill is unavailable")
	}
	fileReserve, err := config.ReserveFiles(1)
	if err != nil {
		return nil, err
	}
	if fileReserve == nil {
		return nil, moerr.NewInternalErrorNoCtx("change range spill file reservation is nil")
	}
	disk, err := config.ReserveDisk(0)
	if err != nil || disk == nil {
		fileReserve.Release()
		if err != nil {
			return nil, err
		}
		return nil, moerr.NewInternalErrorNoCtx("change range spill disk reservation is nil")
	}
	file, err := config.FileFactory(ctx, "table_changes_range_"+uuid.NewString())
	if err != nil || file == nil {
		fileReserve.Release()
		disk.Release()
		if file != nil {
			_ = file.Close()
		}
		if err != nil {
			return nil, err
		}
		return nil, moerr.NewInternalErrorNoCtx("change range spill file is nil")
	}
	return &rangeSpillFile{file: file, fileReserve: fileReserve, disk: disk}, nil
}

func (f *rangeSpillFile) Append(bat *batch.Batch) error {
	if f == nil || f.file == nil || bat == nil || bat.RowCount() == 0 {
		return moerr.NewInternalErrorNoCtx("invalid change range spill append")
	}
	size, err := bat.MarshalBinarySize()
	if err != nil {
		return err
	}
	recordBytes := uint64(rangeSpillRecordHeaderSize) + uint64(size)
	if f.disk == nil {
		return moerr.NewInternalErrorNoCtx("change range spill disk reservation is missing")
	}
	if err := f.disk.Grow(recordBytes); err != nil {
		return err
	}
	var header [rangeSpillRecordHeaderSize]byte
	binary.LittleEndian.PutUint64(header[:], uint64(size))
	if n, err := f.file.Write(header[:]); err != nil {
		return err
	} else if n != len(header) {
		return io.ErrShortWrite
	}
	if err := bat.MarshalBinaryTo(f.file); err != nil {
		return err
	}
	f.records = append(f.records, rangeSpillRecord{
		offset: f.bytes + rangeSpillRecordHeaderSize,
		size:   int64(size),
		rows:   bat.RowCount(),
	})
	f.bytes += int64(recordBytes)
	return nil
}

func (f *rangeSpillFile) Read(record int, mp *mpool.MPool) (*batch.Batch, error) {
	if f == nil || f.file == nil || record < 0 || record >= len(f.records) {
		return nil, moerr.NewInternalErrorNoCtx("invalid change range spill record")
	}
	metadata := f.records[record]
	maxInt := int64(^uint(0) >> 1)
	if metadata.size < 0 || metadata.size > maxInt {
		return nil, moerr.NewInternalErrorNoCtx("invalid change range spill record size")
	}
	data := make([]byte, int(metadata.size))
	if _, err := f.file.ReadAt(data, metadata.offset); err != nil {
		return nil, err
	}
	bat := batch.NewWithSize(0)
	if err := bat.UnmarshalBinaryWithAnyMp(data, mp); err != nil {
		bat.Clean(mp)
		return nil, err
	}
	if bat.RowCount() != metadata.rows {
		bat.Clean(mp)
		return nil, moerr.NewInternalErrorNoCtx("change range spill row count mismatch")
	}
	return bat, nil
}

func (f *rangeSpillFile) Close() error {
	if f == nil {
		return nil
	}
	var firstErr error
	if f.file != nil {
		firstErr = f.file.Close()
		f.file = nil
	}
	if f.disk != nil {
		f.disk.Release()
		f.disk = nil
	}
	if f.fileReserve != nil {
		f.fileReserve.Release()
		f.fileReserve = nil
	}
	return firstErr
}

type rangeSpillRunReader struct {
	file      *rangeSpillFile
	run       rangeSpillRun
	record    int
	bat       *batch.Batch
	row       int
	mp        *mpool.MPool
	exhausted bool
}

func newRangeSpillRunReader(file *rangeSpillFile, run rangeSpillRun, mp *mpool.MPool) (*rangeSpillRunReader, error) {
	r := &rangeSpillRunReader{file: file, run: run, record: run.firstRecord, mp: mp}
	if err := r.load(); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *rangeSpillRunReader) load() error {
	if r.bat != nil {
		r.bat.Clean(r.mp)
		r.bat = nil
	}
	if r.record >= r.run.endRecord {
		r.exhausted = true
		return nil
	}
	bat, err := r.file.Read(r.record, r.mp)
	if err != nil {
		return err
	}
	r.record++
	r.bat = bat
	r.row = 0
	return nil
}

func (r *rangeSpillRunReader) next() error {
	if r.exhausted {
		return nil
	}
	r.row++
	if r.row >= r.bat.RowCount() {
		return r.load()
	}
	return nil
}

func (r *rangeSpillRunReader) ts() types.TS {
	if r == nil || r.exhausted || r.bat == nil {
		return types.TS{}
	}
	return vector.GetFixedAtNoTypeCheck[types.TS](r.bat.Vecs[len(r.bat.Vecs)-1], r.row)
}

func (r *rangeSpillRunReader) close() {
	if r != nil && r.bat != nil {
		r.bat.Clean(r.mp)
		r.bat = nil
	}
}

func appendRangeSpillRow(dst **batch.Batch, src *batch.Batch, row int, mp *mpool.MPool) error {
	if *dst == nil {
		*dst = batch.NewWithSize(len(src.Vecs))
		(*dst).Attrs = append((*dst).Attrs, src.Attrs...)
		for idx := range src.Vecs {
			(*dst).Vecs[idx] = vector.NewVec(*src.Vecs[idx].GetType())
		}
	} else if !batchesShareAppendSchema(*dst, src) {
		return moerr.GetOkExpectedEOB()
	}
	for idx := range src.Vecs {
		if err := (*dst).Vecs[idx].UnionOne(src.Vecs[idx], int64(row), mp); err != nil {
			return err
		}
	}
	(*dst).SetRowCount((*dst).Vecs[0].Length())
	return nil
}

func mergeRangeSpillRuns(
	ctx context.Context,
	input *rangeSpillFile,
	runs []rangeSpillRun,
	config engine.ChangeRangeSpillConfig,
	mp *mpool.MPool,
	chunkRows, chunkBytes int,
) (*rangeSpillFile, []rangeSpillRun, error) {
	for len(runs) > 1 {
		output, err := newRangeSpillFile(ctx, config)
		if err != nil {
			_ = input.Close()
			return nil, nil, err
		}
		outputRuns := make([]rangeSpillRun, 0, (len(runs)+1)/2)
		for idx := 0; idx < len(runs); idx += 2 {
			firstRecord := len(output.records)
			var right *rangeSpillRun
			if idx+1 < len(runs) {
				right = &runs[idx+1]
			}
			if err = mergeRangeSpillRunPair(ctx, input, runs[idx], right, output, mp, chunkRows, chunkBytes); err != nil {
				_ = output.Close()
				_ = input.Close()
				return nil, nil, err
			}
			outputRuns = append(outputRuns, rangeSpillRun{firstRecord: firstRecord, endRecord: len(output.records)})
		}
		if err = input.Close(); err != nil {
			_ = output.Close()
			return nil, nil, err
		}
		input, runs = output, outputRuns
	}
	return input, runs, nil
}

func mergeRangeSpillRunPair(
	ctx context.Context,
	input *rangeSpillFile,
	leftRun rangeSpillRun,
	rightRun *rangeSpillRun,
	output *rangeSpillFile,
	mp *mpool.MPool,
	chunkRows, chunkBytes int,
) (err error) {
	left, err := newRangeSpillRunReader(input, leftRun, mp)
	if err != nil {
		return err
	}
	defer left.close()
	var right *rangeSpillRunReader
	if rightRun != nil {
		right, err = newRangeSpillRunReader(input, *rightRun, mp)
		if err != nil {
			return err
		}
		defer right.close()
	}
	var out *batch.Batch
	defer func() {
		if out != nil {
			out.Clean(mp)
		}
	}()
	flush := func() error {
		if out == nil || out.RowCount() == 0 {
			return nil
		}
		if err := output.Append(out); err != nil {
			return err
		}
		out.Clean(mp)
		out = nil
		return nil
	}
	for !left.exhausted || (right != nil && !right.exhausted) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		selected := left
		leftTS, rightTS := left.ts(), types.TS{}
		if right != nil {
			rightTS = right.ts()
		}
		if left.exhausted || (right != nil && !right.exhausted && rightTS.LT(&leftTS)) {
			selected = right
		}
		if err := appendRangeSpillRow(&out, selected.bat, selected.row, mp); err != nil {
			return err
		}
		if err := selected.next(); err != nil {
			return err
		}
		if (chunkRows > 0 && out.RowCount() >= chunkRows) ||
			(chunkBytes > 0 && out.Allocated() >= chunkBytes) {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	return flush()
}

func batchPrimaryKeyIndex(bat *batch.Batch, primarySeqnum int, tombstone bool) int {
	idx := primarySeqnum
	if tombstone {
		idx = 0
	}
	if len(bat.Vecs) > 0 && bat.Vecs[0] != nil && bat.Vecs[0].GetType().Oid == types.T_Rowid {
		idx++
	}
	return idx
}

func encodedPrimaryKeyAt(bat *batch.Batch, pkIdx, row int, packer *types.Packer) []byte {
	return readutil.EncodePrimaryKey(vector.GetAny(bat.Vecs[pkIdx], row, false), packer)
}

func sortBatchByPrimaryKeyAndTS(bat *batch.Batch, pkIdx int, mp *mpool.MPool) error {
	if bat == nil || bat.RowCount() < 2 {
		return nil
	}
	packer := types.NewPacker()
	keys := readutil.EncodePrimaryKeyVector(bat.Vecs[pkIdx], packer)
	packer.Close()
	timestamps := vector.MustFixedColWithTypeCheck[types.TS](bat.Vecs[len(bat.Vecs)-1])
	sels := make([]int64, bat.RowCount())
	for row := range sels {
		sels[row] = int64(row)
	}
	goSort.Slice(sels, func(i, j int) bool {
		left, right := int(sels[i]), int(sels[j])
		if cmp := bytes.Compare(keys[left], keys[right]); cmp != 0 {
			return cmp < 0
		}
		return timestamps[left].LT(&timestamps[right])
	})
	for _, vec := range bat.Vecs {
		if err := vec.Shuffle(sels, mp); err != nil {
			return err
		}
	}
	return nil
}

func rangeSpillRowLessByPrimaryKey(
	left *rangeSpillRunReader,
	leftPKIdx int,
	right *rangeSpillRunReader,
	rightPKIdx int,
	leftPacker, rightPacker *types.Packer,
) bool {
	leftKey := encodedPrimaryKeyAt(left.bat, leftPKIdx, left.row, leftPacker)
	rightKey := encodedPrimaryKeyAt(right.bat, rightPKIdx, right.row, rightPacker)
	if cmp := bytes.Compare(leftKey, rightKey); cmp != 0 {
		return cmp < 0
	}
	leftTS, rightTS := left.ts(), right.ts()
	return leftTS.LT(&rightTS)
}

func mergeRangeSpillRunPairByPrimaryKey(
	ctx context.Context,
	input *rangeSpillFile,
	leftRun rangeSpillRun,
	rightRun *rangeSpillRun,
	output *rangeSpillFile,
	pkIdx int,
	mp *mpool.MPool,
	chunkRows, chunkBytes int,
) (err error) {
	left, err := newRangeSpillRunReader(input, leftRun, mp)
	if err != nil {
		return err
	}
	defer left.close()
	var right *rangeSpillRunReader
	if rightRun != nil {
		right, err = newRangeSpillRunReader(input, *rightRun, mp)
		if err != nil {
			return err
		}
		defer right.close()
	}
	leftPacker, rightPacker := types.NewPacker(), types.NewPacker()
	defer leftPacker.Close()
	defer rightPacker.Close()
	var out *batch.Batch
	defer func() {
		if out != nil {
			out.Clean(mp)
		}
	}()
	flush := func() error {
		if out == nil || out.RowCount() == 0 {
			return nil
		}
		if err := output.Append(out); err != nil {
			return err
		}
		out.Clean(mp)
		out = nil
		return nil
	}
	for !left.exhausted || (right != nil && !right.exhausted) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		selected := left
		if left.exhausted || (right != nil && !right.exhausted &&
			rangeSpillRowLessByPrimaryKey(right, pkIdx, left, pkIdx, rightPacker, leftPacker)) {
			selected = right
		}
		if err := appendRangeSpillRow(&out, selected.bat, selected.row, mp); err != nil {
			return err
		}
		if err := selected.next(); err != nil {
			return err
		}
		if (chunkRows > 0 && out.RowCount() >= chunkRows) ||
			(chunkBytes > 0 && out.Allocated() >= chunkBytes) {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	return flush()
}

func mergeRangeSpillRunsByPrimaryKey(
	ctx context.Context,
	input *rangeSpillFile,
	runs []rangeSpillRun,
	config engine.ChangeRangeSpillConfig,
	pkIdx int,
	mp *mpool.MPool,
	chunkRows, chunkBytes int,
) (*rangeSpillFile, []rangeSpillRun, error) {
	for len(runs) > 1 {
		output, err := newRangeSpillFile(ctx, config)
		if err != nil {
			_ = input.Close()
			return nil, nil, err
		}
		outputRuns := make([]rangeSpillRun, 0, (len(runs)+1)/2)
		for idx := 0; idx < len(runs); idx += 2 {
			firstRecord := len(output.records)
			var right *rangeSpillRun
			if idx+1 < len(runs) {
				right = &runs[idx+1]
			}
			if err = mergeRangeSpillRunPairByPrimaryKey(
				ctx, input, runs[idx], right, output, pkIdx, mp, chunkRows, chunkBytes,
			); err != nil {
				_ = output.Close()
				_ = input.Close()
				return nil, nil, err
			}
			outputRuns = append(outputRuns, rangeSpillRun{firstRecord: firstRecord, endRecord: len(output.records)})
		}
		if err = input.Close(); err != nil {
			_ = output.Close()
			return nil, nil, err
		}
		input, runs = output, outputRuns
	}
	return input, runs, nil
}

func spillBaseHandleByPrimaryKey(
	ctx context.Context,
	source *baseHandle,
	primarySeqnum int,
	tombstone bool,
	config engine.ChangeRangeSpillConfig,
	mp *mpool.MPool,
	chunkRows, chunkBytes int,
) (file *rangeSpillFile, run *rangeSpillRun, rows int, err error) {
	var runs []rangeSpillRun
	defer func() {
		if err != nil && file != nil {
			_ = file.Close()
			file = nil
		}
	}()
	for {
		var bat *batch.Batch
		nextErr := source.QuickNext(ctx, &bat, mp)
		if bat != nil && bat.RowCount() > 0 {
			pkIdx := batchPrimaryKeyIndex(bat, primarySeqnum, tombstone)
			if err = sortBatchByPrimaryKeyAndTS(bat, pkIdx, mp); err != nil {
				bat.Clean(mp)
				return
			}
			if file == nil {
				if file, err = newRangeSpillFile(ctx, config); err != nil {
					bat.Clean(mp)
					return
				}
			}
			first := len(file.records)
			rows += bat.RowCount()
			if err = file.Append(bat); err != nil {
				bat.Clean(mp)
				return
			}
			runs = append(runs, rangeSpillRun{firstRecord: first, endRecord: len(file.records)})
		}
		if bat != nil {
			bat.Clean(mp)
		}
		if moerr.IsMoErrCode(nextErr, moerr.OkExpectedEOF) {
			break
		}
		if nextErr != nil && !moerr.IsMoErrCode(nextErr, moerr.OkExpectedEOB) {
			err = nextErr
			return
		}
	}
	if file == nil {
		return nil, nil, 0, nil
	}
	pkIdx := batchPrimaryKeyIndexFromSpill(file, primarySeqnum, tombstone, mp)
	if pkIdx < 0 {
		err = moerr.NewInternalErrorNoCtx("cannot resolve change range spill primary key")
		return
	}
	file, _, err = mergeRangeSpillRunsByPrimaryKey(
		ctx, file, runs, config, pkIdx, mp, chunkRows, chunkBytes,
	)
	if err != nil {
		return
	}
	r := rangeSpillRun{endRecord: len(file.records)}
	run = &r
	return
}

func batchPrimaryKeyIndexFromSpill(
	file *rangeSpillFile,
	primarySeqnum int,
	tombstone bool,
	mp *mpool.MPool,
) int {
	if file == nil || len(file.records) == 0 {
		return -1
	}
	bat, err := file.Read(0, mp)
	if err != nil {
		return -1
	}
	defer bat.Clean(mp)
	return batchPrimaryKeyIndex(bat, primarySeqnum, tombstone)
}

type rangeNetEffectRow struct {
	bat        *batch.Batch
	isDelete   bool
	primaryKey []byte
}

func (r *rangeNetEffectRow) clean(mp *mpool.MPool) {
	if r != nil && r.bat != nil {
		r.bat.Clean(mp)
		r.bat = nil
	}
}

func copyRangeNetEffectRow(
	reader *rangeSpillRunReader,
	pkIdx int,
	isDelete bool,
	packer *types.Packer,
	mp *mpool.MPool,
) (*rangeNetEffectRow, error) {
	row := &rangeNetEffectRow{
		isDelete:   isDelete,
		primaryKey: bytes.Clone(encodedPrimaryKeyAt(reader.bat, pkIdx, reader.row, packer)),
	}
	if err := appendRangeSpillRow(&row.bat, reader.bat, reader.row, mp); err != nil {
		row.clean(mp)
		return nil, err
	}
	return row, nil
}

func sameRangeNetEffectKey(left, right []byte) bool {
	return bytes.Equal(left, right)
}

func selectRangeNetEffectRows(
	first, last *rangeNetEffectRow,
	skipDeletes, recovery bool,
) (keepFirst, keepLast bool) {
	if first == nil || last == nil {
		return false, false
	}
	if first == last {
		return true, false
	}
	if first.isDelete {
		if last.isDelete {
			return false, true
		}
		return !skipDeletes, true
	}
	if last.isDelete {
		return false, recovery
	}
	return false, true
}

type rangeNetEffectOutput struct {
	file  *rangeSpillFile
	runs  []rangeSpillRun
	batch *batch.Batch
	rows  int
}

func (o *rangeNetEffectOutput) append(
	ctx context.Context,
	row *rangeNetEffectRow,
	config engine.ChangeRangeSpillConfig,
	mp *mpool.MPool,
	chunkRows, chunkBytes int,
) error {
	if row == nil || row.bat == nil {
		return nil
	}
	if err := appendRangeSpillRow(&o.batch, row.bat, 0, mp); err != nil {
		return err
	}
	o.rows++
	if (chunkRows > 0 && o.batch.RowCount() >= chunkRows) ||
		(chunkBytes > 0 && o.batch.Allocated() >= chunkBytes) {
		return o.flush(ctx, config, mp)
	}
	return nil
}

func (o *rangeNetEffectOutput) flush(
	ctx context.Context,
	config engine.ChangeRangeSpillConfig,
	mp *mpool.MPool,
) error {
	if o.batch == nil || o.batch.RowCount() == 0 {
		return nil
	}
	if err := sortBatch(o.batch, len(o.batch.Vecs)-1, mp); err != nil {
		return err
	}
	if o.file == nil {
		file, err := newRangeSpillFile(ctx, config)
		if err != nil {
			return err
		}
		o.file = file
	}
	first := len(o.file.records)
	if err := o.file.Append(o.batch); err != nil {
		return err
	}
	o.runs = append(o.runs, rangeSpillRun{firstRecord: first, endRecord: len(o.file.records)})
	o.batch.Clean(mp)
	o.batch = nil
	return nil
}

func (o *rangeNetEffectOutput) close(mp *mpool.MPool) {
	if o.batch != nil {
		o.batch.Clean(mp)
		o.batch = nil
	}
	if o.file != nil {
		_ = o.file.Close()
		o.file = nil
	}
}

func prepareRangeNetEffectSpill(
	ctx context.Context,
	dataSource, tombstoneSource *baseHandle,
	primaryIdx int,
	skipDeletes, recovery bool,
	config engine.ChangeRangeSpillConfig,
	mp *mpool.MPool,
	chunkRows, chunkBytes int,
) (dataHandle, tombstoneHandle replayRowHandle, err error) {
	dataFile, dataRun, _, err := spillBaseHandleByPrimaryKey(
		ctx, dataSource, primaryIdx, false, config, mp, chunkRows, chunkBytes,
	)
	if err != nil {
		return nil, nil, err
	}
	if dataFile != nil {
		defer func() {
			if dataFile != nil {
				_ = dataFile.Close()
			}
		}()
	}
	tombstoneFile, tombstoneRun, _, err := spillBaseHandleByPrimaryKey(
		ctx, tombstoneSource, primaryIdx, true, config, mp, chunkRows, chunkBytes,
	)
	if err != nil {
		return nil, nil, err
	}
	if tombstoneFile != nil {
		defer func() {
			if tombstoneFile != nil {
				_ = tombstoneFile.Close()
			}
		}()
	}
	var dataReader, tombstoneReader *rangeSpillRunReader
	if dataFile != nil {
		dataReader, err = newRangeSpillRunReader(dataFile, *dataRun, mp)
		if err != nil {
			return nil, nil, err
		}
		defer dataReader.close()
	}
	if tombstoneFile != nil {
		tombstoneReader, err = newRangeSpillRunReader(tombstoneFile, *tombstoneRun, mp)
		if err != nil {
			return nil, nil, err
		}
		defer tombstoneReader.close()
	}
	dataPKIdx, tombstonePKIdx := -1, -1
	if dataReader != nil && !dataReader.exhausted {
		dataPKIdx = batchPrimaryKeyIndex(dataReader.bat, primaryIdx, false)
	}
	if tombstoneReader != nil && !tombstoneReader.exhausted {
		tombstonePKIdx = batchPrimaryKeyIndex(tombstoneReader.bat, primaryIdx, true)
	}
	dataPacker, tombstonePacker := types.NewPacker(), types.NewPacker()
	defer dataPacker.Close()
	defer tombstonePacker.Close()
	dataOutput, tombstoneOutput := &rangeNetEffectOutput{}, &rangeNetEffectOutput{}
	defer func() {
		if err != nil {
			dataOutput.close(mp)
			tombstoneOutput.close(mp)
			if dataHandle != nil {
				dataHandle.Close()
				dataHandle = nil
			}
			if tombstoneHandle != nil {
				tombstoneHandle.Close()
				tombstoneHandle = nil
			}
		}
	}()
	var first, last *rangeNetEffectRow
	defer func() {
		first.clean(mp)
		if last != first {
			last.clean(mp)
		}
	}()
	emitGroup := func() error {
		keepFirst, keepLast := selectRangeNetEffectRows(first, last, skipDeletes, recovery)
		appendSelected := func(row *rangeNetEffectRow) error {
			if row == nil {
				return nil
			}
			if row.isDelete {
				return tombstoneOutput.append(ctx, row, config, mp, chunkRows, chunkBytes)
			}
			return dataOutput.append(ctx, row, config, mp, chunkRows, chunkBytes)
		}
		if keepFirst {
			if err := appendSelected(first); err != nil {
				return err
			}
		}
		if keepLast {
			if err := appendSelected(last); err != nil {
				return err
			}
		}
		first.clean(mp)
		if last != first {
			last.clean(mp)
		}
		first, last = nil, nil
		return nil
	}
	for (dataReader != nil && !dataReader.exhausted) ||
		(tombstoneReader != nil && !tombstoneReader.exhausted) {
		select {
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		default:
		}
		selected, pkIdx, isDelete, packer := dataReader, dataPKIdx, false, dataPacker
		if selected == nil || selected.exhausted {
			selected, pkIdx, isDelete, packer = tombstoneReader, tombstonePKIdx, true, tombstonePacker
		} else if tombstoneReader != nil && !tombstoneReader.exhausted {
			dataKey := encodedPrimaryKeyAt(dataReader.bat, dataPKIdx, dataReader.row, dataPacker)
			tombstoneKey := encodedPrimaryKeyAt(tombstoneReader.bat, tombstonePKIdx, tombstoneReader.row, tombstonePacker)
			cmp := bytes.Compare(tombstoneKey, dataKey)
			dataTS, tombstoneTS := dataReader.ts(), tombstoneReader.ts()
			if cmp < 0 || (cmp == 0 && !dataTS.LT(&tombstoneTS)) {
				selected, pkIdx, isDelete, packer = tombstoneReader, tombstonePKIdx, true, tombstonePacker
			}
		}
		row, copyErr := copyRangeNetEffectRow(selected, pkIdx, isDelete, packer, mp)
		if copyErr != nil {
			return nil, nil, copyErr
		}
		if first != nil && !sameRangeNetEffectKey(first.primaryKey, row.primaryKey) {
			if err = emitGroup(); err != nil {
				row.clean(mp)
				return nil, nil, err
			}
		}
		if first == nil {
			first, last = row, row
		} else {
			if last != first {
				last.clean(mp)
			}
			last = row
		}
		if err = selected.next(); err != nil {
			return nil, nil, err
		}
	}
	if first != nil {
		if err = emitGroup(); err != nil {
			return nil, nil, err
		}
	}
	if err = dataOutput.flush(ctx, config, mp); err != nil {
		return nil, nil, err
	}
	if err = tombstoneOutput.flush(ctx, config, mp); err != nil {
		return nil, nil, err
	}
	makeHandle := func(output *rangeNetEffectOutput) (replayRowHandle, error) {
		if output.file == nil {
			return (*BatchHandle)(nil), nil
		}
		merged, _, mergeErr := mergeRangeSpillRuns(
			ctx, output.file, output.runs, config, mp, chunkRows, chunkBytes,
		)
		output.file = nil
		if mergeErr != nil {
			return nil, mergeErr
		}
		return newSpilledBatchHandle(merged, output.rows, chunkRows, chunkBytes, mp)
	}
	if dataHandle, err = makeHandle(dataOutput); err != nil {
		return nil, nil, err
	}
	if tombstoneHandle, err = makeHandle(tombstoneOutput); err != nil {
		return nil, nil, err
	}
	return dataHandle, tombstoneHandle, nil
}

type spilledBatchHandle struct {
	file     *rangeSpillFile
	reader   *rangeSpillRunReader
	rows     int
	maxRows  int
	maxBytes int
	mp       *mpool.MPool
}

func newSpilledBatchHandle(
	file *rangeSpillFile,
	rows, maxRows, maxBytes int,
	mp *mpool.MPool,
) (*spilledBatchHandle, error) {
	if file == nil || len(file.records) == 0 {
		return nil, moerr.NewInternalErrorNoCtx("empty change range spill")
	}
	reader, err := newRangeSpillRunReader(file, rangeSpillRun{endRecord: len(file.records)}, mp)
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	if maxRows <= 0 {
		maxRows = int(objectio.BlockMaxRows)
	}
	return &spilledBatchHandle{
		file: file, reader: reader, rows: rows,
		maxRows: maxRows, maxBytes: maxBytes, mp: mp,
	}, nil
}

func (h *spilledBatchHandle) init(bool, *mpool.MPool) error { return nil }
func (h *spilledBatchHandle) IsEmpty() bool                 { return h == nil || h.rows == 0 }
func (h *spilledBatchHandle) Rows() int {
	if h == nil {
		return 0
	}
	return h.rows
}
func (h *spilledBatchHandle) NextTS() types.TS {
	if h == nil || h.reader == nil {
		return types.TS{}
	}
	return h.reader.ts()
}
func (h *spilledBatchHandle) Close() {
	if h == nil {
		return
	}
	if h.reader != nil {
		h.reader.close()
		h.reader = nil
	}
	if h.file != nil {
		_ = h.file.Close()
		h.file = nil
	}
}
func (h *spilledBatchHandle) Next(dst **batch.Batch, mp *mpool.MPool) error {
	if h == nil || h.reader == nil || h.reader.exhausted {
		return moerr.GetOkExpectedEOF()
	}
	if h.outputLimitReached(*dst) {
		return moerr.GetOkExpectedEOB()
	}
	if err := appendRangeSpillRow(dst, h.reader.bat, h.reader.row, mp); err != nil {
		return err
	}
	if err := h.reader.next(); err != nil {
		return err
	}
	if h.outputLimitReached(*dst) {
		return moerr.GetOkExpectedEOB()
	}
	return nil
}
func (h *spilledBatchHandle) QuickNext(dst **batch.Batch, mp *mpool.MPool) error {
	if h == nil || h.reader == nil || h.reader.exhausted {
		return moerr.GetOkExpectedEOF()
	}
	if h.outputLimitReached(*dst) {
		return moerr.GetOkExpectedEOB()
	}
	for !h.reader.exhausted {
		if err := appendRangeSpillRow(dst, h.reader.bat, h.reader.row, mp); err != nil {
			return err
		}
		if err := h.reader.next(); err != nil {
			return err
		}
		if h.outputLimitReached(*dst) {
			return moerr.GetOkExpectedEOB()
		}
	}
	return nil
}

func (h *spilledBatchHandle) outputLimitReached(bat *batch.Batch) bool {
	if h == nil || bat == nil {
		return false
	}
	return (h.maxRows > 0 && bat.RowCount() >= h.maxRows) ||
		(h.maxBytes > 0 && bat.Allocated() >= h.maxBytes)
}

type CNObjectHandle struct {
	isTombstone        bool
	objectOffsetCursor int
	blkOffsetCursor    int
	blockRowOffset     int
	objects            []*objectio.ObjectEntry
	fs                 fileservice.FileService
	mp                 *mpool.MPool
	base               *baseHandle

	cache    []*batch.Batch
	prepared []bool
	blks     []types.Blockid
	TSs      []types.TS
	maxRows  int
	maxBytes int
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
		maxRows:     baseHandle.changesHandle.maxInMemoryRows,
		maxBytes:    baseHandle.changesHandle.maxInMemoryBytes,
	}
}
func (h *CNObjectHandle) prefetch(ctx context.Context) (err error) {
	t0 := time.Now()
	jobs := make([]*tasks.Job, 0)
	blks := make([]types.Blockid, 0)
	prefetchWidth := objectPrefetchWidth(h.maxRows, h.maxBytes)
	bounded := h.maxRows > 0 || h.maxBytes > 0
	for i := 0; i < prefetchWidth; i++ {
		if h.objectOffsetCursor >= len(h.objects) {
			break
		}
		entry := h.objects[h.objectOffsetCursor]
		stats := entry.ObjectStats
		blk := uint16(h.blkOffsetCursor)
		rowOffset := 0
		if bounded {
			rowOffset = h.blockRowOffset
		}
		if h.base.changesHandle.deferRangeLimitToAssembly {
			// Remaining-budget admission BEFORE the read allocates: the
			// window's metadata projection must fit the current combined
			// working set or the range is refused here, before any source
			// buffer exists. Bounded prefetch width is one window, so no
			// scheduled job can precede a rejection.
			var estRows, estBytes int
			if estRows, estBytes, err = estimatePersistedWindow(
				ctx, h.fs, &stats, uint32(h.blkOffsetCursor), rowOffset,
				h.maxRows, h.maxBytes, false,
			); err != nil {
				h.base.changesHandle.readDuration += time.Since(t0)
				return
			}
			if err = h.base.changesHandle.admitSourceWindow(estRows, estBytes); err != nil {
				h.base.changesHandle.readDuration += time.Since(t0)
				return
			}
		}
		h.TSs = append(h.TSs, entry.CreateTime)
		blks = append(blks, objectio.NewBlockidWithObjectID(stats.ObjectName().ObjectId(), blk))
		job := prefetchObjects(
			ctx, uint32(h.blkOffsetCursor), rowOffset, h.fs, &stats,
			h.base.changesHandle.scheduler, h.maxRows, h.maxBytes,
			h.base.changesHandle.spillConfig, h.mp,
		)
		jobs = append(jobs, job)
		if !bounded {
			h.advanceBlock(stats.BlkCnt())
		}
	}
	for i, job := range jobs {
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
		window := res.Res.(*persistedBlockWindow)
		h.cache = append(h.cache, window.batch)
		h.prepared = append(h.prepared, false)
		h.blks = append(h.blks, blks[i])
		if bounded {
			h.blockRowOffset += window.rows
			if h.blockRowOffset >= window.totalRows {
				h.blockRowOffset = 0
				h.advanceBlock(h.objects[h.objectOffsetCursor].ObjectStats.BlkCnt())
			}
		}
	}
	h.base.changesHandle.readDuration += time.Since(t0)
	return
}

func (h *CNObjectHandle) advanceBlock(blockCount uint32) {
	h.blkOffsetCursor++
	if h.blkOffsetCursor >= int(blockCount) {
		h.blkOffsetCursor = 0
		h.objectOffsetCursor++
	}
}
func (h *CNObjectHandle) isEnd() bool {
	return h.objectOffsetCursor >= len(h.objects) && len(h.cache) == 0
}
func (h *CNObjectHandle) IsEmpty() bool {
	return len(h.objects) == 0
}

// discardServeWindow retires the head serve window completely: the
// prepared slice entries go with it and the batch's allocation is
// released, matching the lambda that previously lived inside Next.
func (h *CNObjectHandle) discardServeWindow() {
	data := h.cache[0]
	h.cache = h.cache[1:]
	if len(h.prepared) > 0 {
		h.prepared = h.prepared[1:]
	}
	if len(h.blks) > 0 {
		h.blks = h.blks[1:]
	}
	h.TSs = h.TSs[1:]
	data.Clean(h.mp)
}

// ensureServeWindow materializes and prepares the head serve window. The
// bounded prefetch READ loads the window into the cache, then
// updateCN*Batch shapes it, so the window's complete live charge is
// measurable BEFORE any assembly growth against it. Bounded reads are the
// only allocation allowed ahead of the capacity gate; CloneWindow/Union
// growth is not.
func (h *CNObjectHandle) ensureServeWindow(ctx context.Context) (err error) {
	if h.isEnd() {
		return moerr.GetOkExpectedEOF()
	}
	if len(h.cache) == 0 {
		if err = h.prefetch(ctx); err != nil {
			return
		}
	}
	if len(h.cache) == 0 {
		return moerr.GetOkExpectedEOF()
	}
	prepared := len(h.prepared) > 0 && h.prepared[0]
	if prepared {
		return nil
	}
	data := h.cache[0]
	var blk *types.Blockid
	if len(h.blks) > 0 {
		blk = &h.blks[0]
	}
	ts := h.TSs[0]
	t0 := time.Now()
	if h.isTombstone {
		err = updateCNTombstoneBatch(
			data, ts, blk, h.base.changesHandle.retainRowID, h.mp,
		)
	} else {
		err = updateCNDataBatch(
			data, ts, blk, h.base.changesHandle.retainRowID, h.mp,
		)
	}
	if err != nil {
		h.discardServeWindow()
		return err
	}
	if len(h.prepared) == 0 {
		h.prepared = append(h.prepared, true)
	} else {
		h.prepared[0] = true
	}
	h.base.changesHandle.updateDuration += time.Since(t0)
	return nil
}

// prospectiveWindow materializes and prepares the head serve window
// (bounded reads only, each admitted by the source-window budget gate
// inside prefetch) and reports it for charging: every serve unions the
// complete window in every mode. The window itself stays charged through
// assemblyLiveRowsBytes until discardServeWindow actually releases it. A
// schema boundary against the current destination makes the serve stop
// with OkExpectedEOB without appending, reported as eob (the prepared
// window stays cached for the next destination).
func (h *CNObjectHandle) prospectiveWindow(ctx context.Context, bat *batch.Batch) (src *batch.Batch, eob bool, err error) {
	if h == nil || h.IsEmpty() || h.isEnd() {
		return nil, false, nil
	}
	if err = h.ensureServeWindow(ctx); err != nil {
		if moerr.IsMoErrCode(err, moerr.OkExpectedEOF) {
			return nil, false, nil
		}
		return nil, false, err
	}
	data := h.cache[0]
	if bat != nil && !batchesShareAppendSchema(bat, data) {
		return nil, true, nil
	}
	return data, false, nil
}

func (h *CNObjectHandle) Next(ctx context.Context, bat **batch.Batch, mp *mpool.MPool) (err error) {
	if err = h.ensureServeWindow(ctx); err != nil {
		return
	}
	data := h.cache[0]
	t0 := time.Now()
	if *bat == nil {
		*bat = batch.NewWithSize(0)
		(*bat).Attrs = append((*bat).Attrs, data.Attrs...)
		for _, vec := range data.Vecs {
			newVec := vector.NewVec(*vec.GetType())
			(*bat).Vecs = append((*bat).Vecs, newVec)
		}
	} else if !batchesShareAppendSchema(*bat, data) {
		return moerr.GetOkExpectedEOB()
	}
	srcLen := data.Vecs[0].Length()
	sels := make([]int64, srcLen)
	for j := 0; j < srcLen; j++ {
		sels[j] = int64(j)
	}
	for i, vec := range (*bat).Vecs {
		src := data.Vecs[i]
		if err = vec.Union(src, sels, mp); err != nil {
			h.discardServeWindow()
			return err
		}
	}
	(*bat).SetRowCount((*bat).Vecs[0].Length())
	h.discardServeWindow()
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
	h.prepared = nil
	h.blks = nil
	h.TSs = nil
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
	specialLayouts     []objectio.SpecialColumnLayout
	seqnums            [][]uint16
	blks               []types.Blockid
	p                  *baseHandle

	// blockPlans caches block-level commit-ts overlap decisions for objects.
	// It is only populated when checkpoint-range mode enables block pruning.
	blockPlans     map[string]*aobjBlockPlan
	maxRows        int
	maxBytes       int
	pendingObject  *objectio.ObjectEntry
	pendingBlock   uint16
	blockRowOffset int
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
		maxRows:     p.changesHandle.maxInMemoryRows,
		maxBytes:    p.changesHandle.maxInMemoryBytes,
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
		if obj == nil || obj.BlkCnt() == 0 {
			h.objectOffsetCursor++
			h.blkOffsetCursor = 0
			continue
		}
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
	plan.evaluableBlocks = 0
	plan.overlapBlocks = 0
	plan.prunedBlocks = 0
	plan.nonEvaluableReasons = make(map[string]int, 4)
	plan.nonEvaluableSamples = make([]string, 0, 5)
	plan.evaluableSamples = make([]string, 0, 5)
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
	overlapBlockCnt := 0
	pkf := h.p.changesHandle.pkFilter
	pkSeqnum := uint16(h.p.changesHandle.primarySeqnum)
	for i := uint16(0); i < uint16(obj.BlkCnt()); i++ {
		blk := dataMeta.GetBlockMeta(uint32(i))
		overlap, evaluable, reason, detail := blockCommitTSOverlapsRange(blk, h.start, h.end)
		if !evaluable {
			plan.nonEvaluableReasons[reason]++
			if len(plan.nonEvaluableSamples) < 5 {
				plan.nonEvaluableSamples = append(
					plan.nonEvaluableSamples,
					fmt.Sprintf("blk=%d reason=%s %s", i, reason, detail),
				)
			}
			// Even for non-evaluable blocks, PK pruning can still skip them.
			if pkf != nil && len(pkf.Segments) > 0 {
				pkZM := blk.MustGetColumn(pkSeqnum).ZoneMap()
				if pkZM.IsInited() && !index.AnySegmentOverlaps(pkZM, pkf.Segments) {
					plan.shouldReadByBlks[i] = false
					plan.prunedBlocks++
				}
			}
			continue
		}
		evaluableBlockCnt++
		plan.shouldReadByBlks[i] = overlap
		// Apply PK pruning as a secondary filter on blocks that survived commit-TS check.
		if overlap && pkf != nil && len(pkf.Segments) > 0 {
			pkZM := blk.MustGetColumn(pkSeqnum).ZoneMap()
			if pkZM.IsInited() && !index.AnySegmentOverlaps(pkZM, pkf.Segments) {
				plan.shouldReadByBlks[i] = false
				overlap = false
			}
		}
		if overlap {
			overlapBlockCnt++
		} else {
			plan.prunedBlocks++
		}
		if len(plan.evaluableSamples) < 5 {
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
	if evaluableBlockCnt == 0 {
		fields := []zap.Field{
			zap.String("object", obj.ObjectShortName().ShortString()),
			zap.Bool("tombstone", h.isTombstone),
			zap.String("start", h.start.ToString()),
			zap.String("end", h.end.ToString()),
			zap.Int("total-blocks", plan.totalBlocks),
			zap.Any("non-evaluable-reasons", plan.nonEvaluableReasons),
			zap.Strings("non-evaluable-samples", plan.nonEvaluableSamples),
		}
		if h.p.changesHandle.debugLabel != "" {
			fields = append(fields, zap.String("debug-label", h.p.changesHandle.debugLabel))
		}
		logutil.Warn("ChangesHandle-CommitTSBlockPlan no evaluable blocks", fields...)
	} else {
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
		if h.p.changesHandle.debugLabel != "" {
			fields = append(fields, zap.String("debug-label", h.p.changesHandle.debugLabel))
		}
		logutil.Info("ChangesHandle-CommitTSBlockPlan summary", fields...)
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
	commitPos, ok := objectio.ResolveSpecialColumnLayout(blk).Resolve(objectio.SEQNUM_COMMITTS)
	if !ok {
		return false, false, "tail_column_not_ts", base
	}
	commitCol := blk.ColumnMeta(commitPos)
	base = fmt.Sprintf("%s commit_pos=%d", base, commitPos)
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

func calcPruneRate(pruned, total int) float64 {
	if total <= 0 {
		return 0
	}
	return float64(pruned) / float64(total)
}

func (h *AObjectHandle) prefetch(ctx context.Context) (err error) {
	t0 := time.Now()
	jobs := make([]*tasks.Job, 0)
	blks := make([]types.Blockid, 0)
	prefetchWidth := objectPrefetchWidth(h.maxRows, h.maxBytes)
	bounded := h.maxRows > 0 || h.maxBytes > 0
	for i := 0; i < prefetchWidth; i++ {
		var obj *objectio.ObjectEntry
		var blk uint16
		if bounded && h.pendingObject != nil {
			obj, blk = h.pendingObject, h.pendingBlock
		} else {
			var ok bool
			var targetErr error
			obj, blk, ok, targetErr = h.nextPrefetchTarget(ctx)
			if targetErr != nil {
				err = targetErr
				h.p.changesHandle.readDuration += time.Since(t0)
				return
			}
			if !ok {
				break
			}
			if bounded {
				h.pendingObject, h.pendingBlock = obj, blk
			}
		}
		stats := obj.ObjectStats
		if h.p.changesHandle.deferRangeLimitToAssembly {
			// Remaining-budget admission BEFORE the read allocates: the
			// window's metadata projection must fit the current combined
			// working set or the range is refused before any source buffer
			// exists. On rejection the resolved target stays pending, so a
			// later retry resolves the same block again.
			var estRows, estBytes int
			if estRows, estBytes, err = estimatePersistedWindow(
				ctx, h.fs, &stats, uint32(blk), h.blockRowOffset,
				h.maxRows, h.maxBytes, h.isTombstone && !h.quick,
			); err != nil {
				h.p.changesHandle.readDuration += time.Since(t0)
				return
			}
			if err = h.p.changesHandle.admitSourceWindow(estRows, estBytes); err != nil {
				h.p.changesHandle.readDuration += time.Since(t0)
				return
			}
		}
		job := prefetchObjects(
			ctx, uint32(blk), h.blockRowOffset, h.fs, &stats,
			h.p.changesHandle.scheduler, h.maxRows, h.maxBytes,
			h.p.changesHandle.spillConfig, h.mp,
		)
		jobs = append(jobs, job)
		blks = append(blks, objectio.NewBlockidWithObjectID(stats.ObjectName().ObjectId(), blk))
	}
	for i, job := range jobs {
		res := job.GetResult()
		if res.Err != nil {
			err = res.Err
			if moerr.IsMoErrCode(err, moerr.ErrFileNotFound) {
				logutil.Info("ChangesHandle-FileNotFound",
					zap.String("err", err.Error()))
			}
			h.p.changesHandle.readDuration += time.Since(t0)
			return
		}
		putJob(job)
		window := res.Res.(*persistedBlockWindow)
		h.cache = append(h.cache, window.batch)
		h.specialLayouts = append(h.specialLayouts, window.specialLayout)
		h.seqnums = append(h.seqnums, window.seqnums)
		h.blks = append(h.blks, blks[i])
		if bounded {
			h.blockRowOffset += window.rows
			if h.blockRowOffset >= window.totalRows {
				h.blockRowOffset = 0
				h.pendingObject = nil
			}
		}
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
		h.cache = h.cache[1:]
		var specialLayout *objectio.SpecialColumnLayout
		if len(h.specialLayouts) > 0 {
			specialLayout = &h.specialLayouts[0]
			h.specialLayouts = h.specialLayouts[1:]
		}
		var seqnums []uint16
		if len(h.seqnums) > 0 {
			seqnums = h.seqnums[0]
			h.seqnums = h.seqnums[1:]
		}
		var blk *types.Blockid
		if len(h.blks) > 0 {
			blk = &h.blks[0]
			h.blks = h.blks[1:]
		}
		t0 := time.Now()
		if h.isTombstone {
			if err = updateTombstoneBatch(h.currentBatch, h.start, h.end, h.p.skipTS, !h.quick, blk, specialLayout, seqnums, h.p.changesHandle.retainRowID, h.mp); err != nil {
				return err
			}
		} else {
			if err = updateDataBatch(h.currentBatch, h.start, h.end, blk, specialLayout, seqnums, h.p.changesHandle.retainRowID, h.mp); err != nil {
				return err
			}
		}
		h.p.changesHandle.updateDuration += time.Since(t0)
		h.batchLength = h.currentBatch.Vecs[0].Length()
		if h.batchLength > 0 {
			return
		}
		// The window was fully filtered by its timestamp/abort predicates. It is
		// no longer needed and must be released before fetching the next window;
		// otherwise a bounded replay retains every empty window it visits.
		h.currentBatch.Clean(h.mp)
		h.currentBatch = nil
		h.batchLength = 0
		h.rowOffsetCursor = 0
	}
}
func (h *AObjectHandle) isEnd() bool {
	return h.pendingObject == nil && h.objectOffsetCursor >= len(h.objects) && len(h.cache) == 0
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

// prospectiveWindow loads the current window if needed (bounded reads
// only, each admitted by the source-window budget gate inside prefetch)
// and reports the remaining append window of the next serve: QuickNext
// serves [start, end); Next serves the single row at start. The batch
// itself stays charged through assemblyLiveRowsBytes until the window is
// retired. A schema boundary against the current destination makes the
// serve stop with OkExpectedEOB without appending, reported as eob (the
// loaded window stays current for the next destination).
func (h *AObjectHandle) prospectiveWindow(ctx context.Context, bat *batch.Batch) (src *batch.Batch, start, end int, eob bool, err error) {
	if h == nil || (h.isEnd() && h.rowOffsetCursor >= h.batchLength) {
		return nil, 0, 0, false, nil
	}
	if h.currentBatch == nil && !h.isEnd() {
		if err = h.getNextAObject(ctx); err != nil {
			return nil, 0, 0, false, err
		}
	}
	if h.currentBatch == nil || h.rowOffsetCursor >= h.batchLength {
		return nil, 0, 0, false, nil
	}
	if bat != nil && !batchesShareAppendSchema(bat, h.currentBatch) {
		return nil, 0, 0, true, nil
	}
	return h.currentBatch, h.rowOffsetCursor, h.batchLength, false, nil
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
		if !batchesShareAppendSchema(*bat, h.currentBatch) {
			return moerr.GetOkExpectedEOB()
		}
		for i, vec := range (*bat).Vecs {
			for rowOffset := start; rowOffset < end; rowOffset++ {
				appendFromEntry(h.currentBatch.Vecs[i], vec, rowOffset, mp)
			}
		}
	}
	(*bat).SetRowCount((*bat).Vecs[0].Length())
	// Publish the (possibly just-created) destination BEFORE the tail
	// prefetch below: its admission must charge against the live
	// combined set that includes this destination.
	h.p.changesHandle.pinAssemblyDst(h.p, *bat)
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
	h.seqnums = nil
	h.blks = nil
	h.batchLength = 0
	h.rowOffsetCursor = 0
}

type baseHandle struct {
	aobjHandle     *AObjectHandle
	cnObjectHandle *CNObjectHandle
	inMemoryHandle replayRowHandle

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
	owned := p
	defer func() {
		if err != nil {
			owned.Close()
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
		p.fillInSkipTS(dataIter, start, end)
		dataIter.Release()
	}
	rowIter, rowIterKind, pkFilterApplied := p.newReplayRowsIter(state, start, end, tombstone)
	defer rowIter.Close()
	p.inMemoryHandle, err = p.newBatchHandleWithRowIterator(
		ctx, rowIter, rowIterKind, pkFilterApplied, start, end, tombstone, mp,
	)
	if err != nil {
		return nil, err
	}
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
	if p.inMemoryHandle != nil {
		// The aobject first window is prepared and retention-charged now;
		// the in-memory sort's copy transients must be admitted against
		// the combined set that includes it BEFORE sortBatch allocates
		// any replacement buffer (shuffle allocates the replacement while
		// the original vector is still held).
		if err = p.changesHandle.chargeConstructionSort(p.inMemoryHandle, quick); err != nil {
			return
		}
		err = p.inMemoryHandle.init(quick, mp)
	}
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
	inMemoryEmpty := p.inMemoryHandle == nil || p.inMemoryHandle.IsEmpty()
	return p.aobjHandle.IsEmpty() && inMemoryEmpty && p.cnObjectHandle.IsEmpty()
}

func (p *baseHandle) IsSmall() bool {
	if !p.cnObjectHandle.IsEmpty() {
		return false
	}
	count := p.aobjHandle.RowCount()
	if p.inMemoryHandle != nil {
		count += p.inMemoryHandle.Rows()
	}
	return count < SmallBatchThreshold
}
func (p *baseHandle) Close() {
	if p == nil {
		return
	}
	if p.inMemoryHandle != nil {
		p.inMemoryHandle.Close()
	}
	p.aobjHandle.Close()
	p.cnObjectHandle.Close()
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
	var inMemoryTS types.TS
	if p.inMemoryHandle != nil {
		inMemoryTS = p.inMemoryHandle.NextTS()
	}
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
		if moerr.IsMoErrCode(err, moerr.OkExpectedEOF) {
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
	if p.cnObjectHandle != nil {
		err = p.cnObjectHandle.QuickNext(ctx, bat, mp)
		if moerr.IsMoErrCode(err, moerr.OkExpectedEOF) {
			p.cnObjectHandle.Close()
			p.cnObjectHandle = nil
			err = nil
		}
		if err != nil {
			return
		}
	}
	if p.aobjHandle == nil && p.inMemoryHandle == nil && p.cnObjectHandle == nil {
		return moerr.GetOkExpectedEOF()
	}
	return nil
}

// prospectiveQuickCharge mirrors QuickNext's serve order, schema
// boundaries, and coarseMaxRow stops WITHOUT assembling anything: it
// reports the rows and bytes the next QuickNext call would append to
// bat, so an attached bound can be enforced before the CloneWindow/Union
// growth. model carries the destination's allocator state: each leg's
// append charge comes from advancing a value fork of it, and the fork's
// final state is staged as model.pending for the gate to commit after
// the admitted serve. The model never under-counts: it shares capacity
// arithmetic with the allocator itself (capacity classes, rounding, and
// transient old+replacement buffer coexistence during growth), so a
// bound checked against the projected peak cannot be crossed inside the
// serve. When bat is nil the serve creates a fresh destination, so the
// tracked state resets to zero capacities first. A bounded read may
// materialize a handle's head serve window; every retained source keeps
// its separate assemblyLiveRowsBytes charge until its actual release.
func (p *baseHandle) prospectiveQuickCharge(ctx context.Context, bat *batch.Batch, model *dstGrowthModel) (rows, bytes int, err error) {
	accRows := 0
	// schemaRef is the schema the growing destination would carry at each
	// mirror step: the existing destination's, or the first serving
	// window's when the destination has not been created yet. A window on
	// the other side of a schema boundary appends nothing and stops the
	// whole QuickNext call, so the mirror stops charging there as well.
	schemaRef := bat
	if bat != nil {
		accRows = bat.RowCount()
	} else {
		model.vecs = nil
	}
	walk := model.fork()
	// stage hands the fork's advanced state to the caller's model: what
	// the destination's capacities will be after the admitted serve.
	stage := func() {
		model.pending = walk.vecs
		walk.vecs = nil
	}
	if p.aobjHandle != nil {
		var src *batch.Batch
		var start, end int
		var eob bool
		if src, start, end, eob, err = p.aobjHandle.prospectiveWindow(ctx, schemaRef); err != nil {
			return 0, 0, err
		}
		if eob {
			stage()
			return
		}
		if src != nil && end > start {
			r := end - start
			bytes += walk.chargeServeGrow(accRows, src, start, end, false)
			walk.commitPending()
			rows += r
			accRows += r
			if schemaRef == nil {
				schemaRef = src
			}
		}
	}
	if accRows > p.changesHandle.coarseMaxRow {
		stage()
		return
	}
	if p.inMemoryHandle != nil {
		if bh, ok := p.inMemoryHandle.(*BatchHandle); ok {
			var src *batch.Batch
			var start, end int
			var eob bool
			if src, start, end, eob = bh.prospectiveWindow(schemaRef); eob {
				stage()
				return
			}
			if src != nil && end > start {
				r := end - start
				bytes += walk.chargeServeGrow(accRows, src, start, end, false)
				walk.commitPending()
				rows += r
				accRows += r
				if schemaRef == nil {
					schemaRef = src
				}
			}
		} else {
			// Non-*BatchHandle implementations (spill-backed runs) retain no
			// mpool source, matching assemblyLiveRowsBytes, which only
			// accounts the *BatchHandle shape; rows still charge.
			accRows += p.inMemoryHandle.Rows()
		}
	}
	if accRows > p.changesHandle.coarseMaxRow {
		stage()
		return
	}
	if p.cnObjectHandle != nil {
		var src *batch.Batch
		var eob bool
		if src, eob, err = p.cnObjectHandle.prospectiveWindow(ctx, schemaRef); err != nil {
			return 0, 0, err
		}
		if eob {
			stage()
			return
		}
		if src != nil {
			if r := src.RowCount(); r > 0 {
				bytes += walk.chargeServeGrow(accRows, src, 0, r, true)
				walk.commitPending()
				rows += r
			}
		}
	}
	stage()
	return
}

// prospectiveRowCharge mirrors Next: nextTS picks the single sub-handle
// that serves next, and the charge is what that serve appends — one row
// for the row handles, the whole prepared window for CN, whose serve
// unions the complete window in every mode. model carries the
// destination's allocator state and receives the staged post-serve state
// exactly as in prospectiveQuickCharge. A serve sitting on the other
// side of a schema boundary against bat appends nothing (OkExpectedEOB),
// so it charges nothing.
func (p *baseHandle) prospectiveRowCharge(ctx context.Context, bat *batch.Batch, model *dstGrowthModel) (rows, bytes int, err error) {
	dstRows := 0
	if bat != nil {
		dstRows = bat.RowCount()
	} else {
		model.vecs = nil
	}
	_, typ := p.nextTS()
	switch typ {
	case NextChangeHandle_CNObj:
		src, eob, cerr := p.cnObjectHandle.prospectiveWindow(ctx, bat)
		if eob || cerr != nil {
			return 0, 0, cerr
		}
		if src == nil {
			return 0, 0, nil
		}
		r := src.RowCount()
		if r <= 0 {
			return 0, 0, nil
		}
		bytes = model.chargeServeGrow(dstRows, src, 0, r, true)
		return r, bytes, nil
	case NextChangeHandle_AObj:
		src, start, _, eob, cerr := p.aobjHandle.prospectiveWindow(ctx, bat)
		if eob || cerr != nil {
			return 0, 0, cerr
		}
		if src == nil {
			return 0, 0, nil
		}
		bytes = model.chargeServeGrow(dstRows, src, start, start+1, false)
		return 1, bytes, nil
	case NextChangeHandle_InMemory:
		if bh, ok := p.inMemoryHandle.(*BatchHandle); ok && !bh.isEnd() {
			if bat != nil && !batchesShareAppendSchema(bat, bh.batches) {
				return 0, 0, nil
			}
			bytes = model.chargeServeGrow(dstRows, bh.batches, bh.rowOffsetCursor, bh.rowOffsetCursor+1, false)
			return 1, bytes, nil
		}
	}
	return 0, 0, nil
}

func (p *baseHandle) newBatchHandleWithRowIterator(
	ctx context.Context,
	iter RowsIter,
	iterKind string,
	pkFilterApplied bool,
	start, end types.TS,
	tombstone bool,
	mp *mpool.MPool,
) (h replayRowHandle, err error) {
	var bat *batch.Batch
	var spillFile *rangeSpillFile
	var spillRuns []rangeSpillRun
	var scanned, tsMatched, emitted int
	defer func() {
		if err != nil {
			if bat != nil {
				bat.Clean(mp)
			}
			if spillFile != nil {
				_ = spillFile.Close()
			}
		}
	}()
	flush := func() error {
		if bat == nil || bat.RowCount() == 0 {
			return nil
		}
		if !p.changesHandle.spillConfig.Enabled() {
			return moerr.NewInvalidInputNoCtx(
				"visible-state change range exceeded its memory limit and spill is unavailable",
			)
		}
		if err := sortBatch(bat, len(bat.Vecs)-1, mp); err != nil {
			return err
		}
		if spillFile == nil {
			spillFile, err = newRangeSpillFile(ctx, p.changesHandle.spillConfig)
			if err != nil {
				return err
			}
		}
		first := len(spillFile.records)
		if err := spillFile.Append(bat); err != nil {
			return err
		}
		spillRuns = append(spillRuns, rangeSpillRun{firstRecord: first, endRecord: len(spillFile.records)})
		bat.Clean(mp)
		bat = nil
		return nil
	}
	chunkRows, chunkBytes := p.changesHandle.rangeSpillChunkLimits()
	// fillRowModel tracks the growing replay buffer's allocator state so
	// each row's growth is admitted BEFORE fillIn* allocates it: a row
	// whose buffers do not fit the remaining combined budget is refused
	// with zero new allocation, and the admitted model is committed only
	// after the row actually lands. A spill flush releases the buffer, so
	// the model resets with it.
	var fillRowModel dstGrowthModel
	admitRow := func(tombstoneRow bool, entry *RowEntry) error {
		if !p.changesHandle.deferRangeLimitToAssembly {
			return nil
		}
		cols := fillRowCols(entry, tombstoneRow, p.changesHandle.retainRowID, p.changesHandle.logicalSeqnums)
		dstRows := 0
		if bat != nil {
			dstRows = bat.RowCount()
		}
		addBytes := fillRowModel.chargeGrowCols(dstRows, cols, 1)
		liveRows, liveBytes := p.changesHandle.assemblyLiveRowsBytes(nil, nil)
		if bat != nil {
			liveRows += bat.RowCount()
			liveBytes += bat.Allocated()
		}
		return p.changesHandle.checkRangeLimit(liveRows+1, liveBytes+addBytes)
	}
	for iter.Next() {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		scanned++
		entry := iter.Entry()
		if checkTS(start, end, entry.Time) {
			tsMatched++
			if !entry.Deleted && !tombstone {
				if limitErr := admitRow(false, entry); limitErr != nil {
					fillRowModel.dropPending()
					err = limitErr
					return nil, err
				}
				fillInInsertBatch(&bat, entry, p.changesHandle.retainRowID, p.changesHandle.logicalSeqnums, mp)
				bat.SetRowCount(bat.Vecs[0].Length())
				if p.changesHandle.deferRangeLimitToAssembly {
					fillRowModel.commitPending()
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
				if limitErr := admitRow(true, entry); limitErr != nil {
					fillRowModel.dropPending()
					err = limitErr
					return nil, err
				}
				fillInDeleteBatch(&bat, entry, p.changesHandle.retainRowID, mp)
				bat.SetRowCount(bat.Vecs[0].Length())
				if p.changesHandle.deferRangeLimitToAssembly {
					fillRowModel.commitPending()
				}
				emitted++
			}
			if bat != nil && p.changesHandle.deferRangeLimitToAssembly {
				// Backstop for the pre-admission above: the measured bulk
				// can never cross the admitted envelope, so this only ever
				// trips on a model regression.
				liveRows, liveBytes := p.changesHandle.assemblyLiveRowsBytes(nil, nil)
				liveRows += bat.RowCount()
				liveBytes += bat.Allocated()
				if limitErr := p.changesHandle.checkRangeLimit(liveRows, liveBytes); limitErr != nil {
					err = limitErr
					return nil, err
				}
			}
			if bat != nil && ((chunkRows > 0 && bat.RowCount() >= chunkRows) ||
				(chunkBytes > 0 && bat.Allocated() >= chunkBytes)) {
				if err = flush(); err != nil {
					return nil, err
				}
				fillRowModel.vecs = nil
				fillRowModel.pending = nil
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
	if spillFile == nil {
		if bat == nil {
			// Keep a typed nil so baseHandle can use the nil-safe BatchHandle
			// methods through replayRowHandle.
			return (*BatchHandle)(nil), nil
		}
		return NewRowHandle(bat, mp, p, ctx, tombstone), nil
	}
	if err = flush(); err != nil {
		return nil, err
	}
	spillFile, spillRuns, err = mergeRangeSpillRuns(
		ctx, spillFile, spillRuns, p.changesHandle.spillConfig, mp, chunkRows, chunkBytes,
	)
	if err != nil {
		return nil, err
	}
	h, err = newSpilledBatchHandle(spillFile, emitted, chunkRows, chunkBytes, mp)
	if err != nil {
		return nil, err
	}
	spillFile = nil
	return h, nil
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
	var pkf *engine.PKFilter
	debugLabel := ""
	if p.changesHandle != nil {
		pkf = p.changesHandle.pkFilter
		debugLabel = p.changesHandle.debugLabel
	}
	var (
		totalAppendable, prunedAppendable int
		totalCNCreated, prunedCNCreated   int
		totalTNStatic, prunedTNStatic     int
	)
	for objIter.Next() {
		entry := objIter.Item()
		entryCopy := entry
		if entry.GetAppendable() {
			totalAppendable++
			if entry.CreateTime.GT(&end) {
				continue
			}
			if !entry.DeleteTime.IsEmpty() && entry.DeleteTime.LT(&start) {
				continue
			}
			// PK zonemap pruning: skip appendable objects whose sort-key range
			// does not overlap with the requested PK values.
			if pkf != nil && len(pkf.Segments) > 0 {
				zm := entry.SortKeyZoneMap()
				if zm.IsInited() && !index.AnySegmentOverlaps(zm, pkf.Segments) {
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
				if pkf != nil && len(pkf.Segments) > 0 {
					zm := entry.SortKeyZoneMap()
					if zm.IsInited() && !index.AnySegmentOverlaps(zm, pkf.Segments) {
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
			// PK zonemap pruning for TN non-appendable objects.
			if pkf != nil && len(pkf.Segments) > 0 {
				zm := entry.SortKeyZoneMap()
				if zm.IsInited() && !index.AnySegmentOverlaps(zm, pkf.Segments) {
					prunedTNStatic++
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
			zap.Int("tn-static-pruned", prunedTNStatic),
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
		// For snapshot-state range replay, we only need terminal objects that are
		// still visible at range end. If an object has already been deleted at or
		// before end, keep following its delete-time chain instead of reading this
		// transient intermediate object.
		if !current.DeleteTime.IsEmpty() && current.DeleteTime.LE(&end) {
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
	// After checkpoint + GC + restart, no appendable seed remains in the visible
	// set, so these live TN objects are never reached by chain walking above.
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
// a missing visible object. It first tries exact delete-time -> create-time
// matching, and then falls back to the earliest TN create-time >= delete-time.
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
	// When true, Case 2.2 (insert->delete) keeps the delete instead of dropping
	// the pair: CDC restart from checkpoint, or a caller that opts in through
	// engine.WithCollectChangesKeepTrailingDeletes because its state is seeded
	// from an earlier generation than the requested range start.
	isRecoveryMode      bool
	preserveAllVersions bool
	tombstoneHandle     *baseHandle
	dataHandle          *baseHandle
	coarseMaxRow        int
	quick               bool
	primarySeqnum       int
	primaryIdx          int
	logicalSeqnums      []uint16
	scheduler           tasks.JobScheduler
	mp                  *mpool.MPool

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

	maxInMemoryRows         int
	maxInMemoryBytes        int
	spillConfig             engine.ChangeRangeSpillConfig
	spillNetEffectData      replayRowHandle
	spillNetEffectTombstone replayRowHandle

	// deferRangeLimitToAssembly marks the default-policy constructors'
	// limit enforcement: an attached bound is charged against the combined
	// live working set while the in-memory replay buffers materialize and
	// during assembly (destination batches, prefetch cache, in-memory
	// buffer), with every allocation counted completely until its owner
	// releases it, instead of driving the visible-state family's spill
	// chunk flush. The underlying stream stays byte-identical and
	// exhausting the bound surfaces the capacity class.
	deferRangeLimitToAssembly bool

	// dataGrowth/tombGrowth mirror the allocator state of the two
	// accumulating assembly destinations for the pre-growth admission
	// gates. Each gate stages the post-serve state; the serve owning the
	// stage commits it, and only a destination replacement resets it.
	dataGrowth dstGrowthModel
	tombGrowth dstGrowthModel
	// assemblyData/assemblyTombstone reference the in-flight destinations
	// while the assembly loops own them, so persisted source-read
	// admission measures the REMAINING combined budget — destinations and
	// retained sources together — rather than the raw attached bound.
	// They are nil outside assembly (construction), where the surviving
	// handles alone make up the working set.
	assemblyData      *batch.Batch
	assemblyTombstone *batch.Batch
	// debugLabel scopes temporary diagnostics to a single CollectChanges call chain.
	debugLabel string

	retainRowID bool

	LogThreshold time.Duration
}

// attachRangeLimit reads the caller-owned recovery bound with the contract
// shared by every range constructor: a row bound cannot bound variable-width
// persisted columns by itself, so rows without bytes is a caller
// misconfiguration rejected at construction. The default-policy constructors
// (in-memory partition replay and checkpoint replay) enforce an attached
// bound through construction and assembly accounting over the combined live
// working set; an unattached context leaves both fields zero, which the
// engine API defines as unbounded, preserving every pre-existing caller
// byte-identically.
func (p *ChangeHandler) attachRangeLimit(ctx context.Context) error {
	rangeLimit := engine.ChangeRangeLimitFromContext(ctx)
	if rangeLimit.MaxInMemoryRows > 0 && rangeLimit.MaxInMemoryBytes <= 0 {
		return moerr.NewInvalidInputNoCtx(
			"change range MaxInMemoryRows requires MaxInMemoryBytes to bound variable-width persisted columns",
		)
	}
	p.maxInMemoryRows = rangeLimit.MaxInMemoryRows
	p.maxInMemoryBytes = rangeLimit.MaxInMemoryBytes
	p.deferRangeLimitToAssembly = rangeLimit.Enabled()
	return nil
}

func (p *ChangeHandler) rangeSpillChunkLimits() (rows, bytes int) {
	if p.deferRangeLimitToAssembly {
		// Limit enforcement moved to assembly: no spill ownership is
		// attached on this construction path, and the chunk flush exists
		// only to slice spilled runs.
		return 0, 0
	}
	// Construction can retain one chunk for the other replay side while a
	// merge holds two inputs and one output. Quarters keep that working set
	// within the caller's combined data+tombstone memory budget.
	if p.maxInMemoryRows > 0 {
		rows = max(1, p.maxInMemoryRows/4)
	}
	if p.maxInMemoryBytes > 0 {
		bytes = max(1, p.maxInMemoryBytes/4)
	}
	return
}

// assemblyLiveRowsBytes counts the combined live working set: both
// accumulating destination batches, the persisted prefetch window caches,
// and the in-memory replay buffers. Every allocation counts COMPLETELY
// while its owner is alive: a replay handle releases its batch only when
// the handle closes, long after the read cursor has passed (and only after
// Next/QuickNext deep-copied the served window out of it), so the bound
// governs peak retained memory, not the unread remainder. The count is
// pure inspection; nothing is mutated or moved.
func (p *ChangeHandler) assemblyLiveRowsBytes(data, tombstone *batch.Batch) (rows, bytes int) {
	account := func(b *batch.Batch) {
		if b == nil {
			return
		}
		rows += b.RowCount()
		bytes += b.Allocated()
	}
	account(data)
	account(tombstone)
	for _, h := range []*baseHandle{p.dataHandle, p.tombstoneHandle} {
		if h == nil {
			continue
		}
		if h.cnObjectHandle != nil {
			for _, c := range h.cnObjectHandle.cache {
				account(c)
			}
		}
		if h.aobjHandle != nil {
			for _, c := range h.aobjHandle.cache {
				account(c)
			}
			account(h.aobjHandle.currentBatch)
		}
		if h.inMemoryHandle != nil {
			if bh, ok := h.inMemoryHandle.(*BatchHandle); ok && bh != nil {
				account(bh.batches)
			}
		}
	}
	return
}

// checkRangeLimit compares one measured {rows, bytes} working set against
// the attached bound, returning the capacity class on the first crossing.
func (p *ChangeHandler) checkRangeLimit(rows, bytes int) error {
	if (p.maxInMemoryRows > 0 && rows > p.maxInMemoryRows) ||
		(p.maxInMemoryBytes > 0 && bytes > p.maxInMemoryBytes) {
		return engine.NewChangeRangeCapacityError(fmt.Sprintf(
			"%d rows / %d bytes live against rows=%d bytes=%d",
			rows, bytes, p.maxInMemoryRows, p.maxInMemoryBytes))
	}
	return nil
}

// chargeConstructionSort admits the sort/normalization copy transient of
// one constructor initializer BEFORE sortBatch allocates anything: the
// batch's vectors are shuffled in place, each vector's replacement
// backing materializing while its original is still held (alloc-before-
// free), so the peak is the retained working set plus the LARGEST single
// vector copy. The gate measures the combined live set at the
// initializer's own point in the construction order — replay buffers,
// prepared aobject windows, destinations alike — and refuses the range
// before the first replacement buffer exists; a later per-vector peak can
// never exceed the projection because every other vector stays retained,
// not freed, during the shuffle. Quick mode skips sorting, so it skips
// the charge; nil or non-batch replay handles (spill legs) own no in-
// memory vectors and charge nothing. The check is pure inspection; the
// caller's error path owns cleanup.
func (p *ChangeHandler) chargeConstructionSort(inMemory replayRowHandle, quick bool) error {
	if !p.deferRangeLimitToAssembly || quick {
		return nil
	}
	bh, ok := inMemory.(*BatchHandle)
	if !ok || bh == nil || bh.batches == nil {
		return nil
	}
	sortTemp := 0
	for _, vec := range bh.batches.Vecs {
		if vec == nil {
			continue
		}
		// The vector's complete current allocation soundly bounds its
		// shuffle copy: the replacement holds the same rows (same length,
		// same payload), so its fresh capacity step can never exceed the
		// original's complete charged size.
		sortTemp = max(sortTemp, vec.Allocated())
	}
	if sortTemp == 0 {
		return nil
	}
	liveRows, liveBytes := p.assemblyLiveRowsBytes(nil, nil)
	return p.checkRangeLimit(liveRows, liveBytes+sortTemp)
}

// chargeConstruction enforces the deferred default-policy bound while the
// handler is still being built: retained replay buffers and prefetch
// caches are charged at their complete allocation the moment they
// materialize, so an over-budget in-memory range, or two individually
// bounded buffers coexisting, surfaces the capacity class at construction
// instead of after the whole replay already peaked. The check is pure
// inspection; the constructor's error path owns cleanup.
func (p *ChangeHandler) chargeConstruction() error {
	if !p.deferRangeLimitToAssembly {
		return nil
	}
	return p.checkRangeLimit(p.assemblyLiveRowsBytes(nil, nil))
}

// chargeAssembly enforces the attached bound at each assembly accounting
// point: every union append into the accumulating destinations and every
// batch the Next loops emit is checked against the combined live working
// set. Exceeding the bound surfaces the capacity class after cleaning the
// destinations and releasing the replay handles, so bounded peak and full
// release are the same event. Emitted rows, order, hint progression, and
// recorded commit stamps are byte-identical to the unbudgeted read; the
// capacity class only ever replaces a result batch that would have crossed
// the bound.
func (p *ChangeHandler) chargeAssembly(data, tombstone **batch.Batch, mp *mpool.MPool) (err error) {
	if !p.deferRangeLimitToAssembly {
		return nil
	}
	rows, bytes := p.assemblyLiveRowsBytes(*data, *tombstone)
	if err = p.checkRangeLimit(rows, bytes); err != nil {
		if *data != nil {
			(*data).Clean(mp)
			*data = nil
		}
		if *tombstone != nil {
			(*tombstone).Clean(mp)
			*tombstone = nil
		}
		_ = p.Close()
		return err
	}
	return nil
}

// growthModelFor returns the destination growth model tracking the data
// (dataSide=true) or tombstone assembly side: one model per accumulating
// destination batch, committed only by serves the gate admitted.
func (p *ChangeHandler) growthModelFor(dataSide bool) *dstGrowthModel {
	if dataSide {
		return &p.dataGrowth
	}
	return &p.tombGrowth
}

// releaseAssemblyDsts cleans both in-flight destinations and closes the
// handler. It is the shared rejection cleanup for every
// assembly-capacity rejection — admission gate, source-window gate, and
// accounting backstop — so bounded peak and full release are always the
// same event.
func (p *ChangeHandler) releaseAssemblyDsts(data, tombstone **batch.Batch, mp *mpool.MPool) {
	if data != nil && *data != nil {
		(*data).Clean(mp)
		*data = nil
	}
	if tombstone != nil && *tombstone != nil {
		(*tombstone).Clean(mp)
		*tombstone = nil
	}
	_ = p.Close()
}

// admitAssemblyGrowth enforces the attached bound BEFORE an assembly
// serve grows the destinations: the prospective serve charge is added to
// the combined live working set (both accumulating destinations plus
// every retained source, charged until its actual release) and checked
// ahead of the CloneWindow/Union allocation, so a rejected range never
// transiently exceeds the admitted envelope. The charge is computed from
// the destination growth model, which shares capacity arithmetic with
// the allocator itself, so an admitted projection can never under-count
// the allocation the serve performs. Rejection runs the shared release.
// The post-serve chargeAssembly calls remain as the backstop; because
// the projection never under-counts the serve, they always pass for
// admitted growth.
func (p *ChangeHandler) admitAssemblyGrowth(data, tombstone **batch.Batch, addRows, addBytes int, mp *mpool.MPool) (err error) {
	if !p.deferRangeLimitToAssembly {
		return nil
	}
	var d, t *batch.Batch
	if data != nil {
		d = *data
	}
	if tombstone != nil {
		t = *tombstone
	}
	rows, bytes := p.assemblyLiveRowsBytes(d, t)
	if err = p.checkRangeLimit(rows+addRows, bytes+addBytes); err != nil {
		p.releaseAssemblyDsts(data, tombstone, mp)
		return err
	}
	return nil
}

// admitSourceWindow enforces the attached bound BEFORE a persisted
// source read allocates its buffers: the metadata-projected window
// charge is added to the current combined live set (both destinations
// plus every retained source), so a read whose buffers cannot fit inside
// the remaining budget is refused before any buffer exists. No buffer is
// owned by the read yet on rejection, so no local cleanup is due; the
// gate that called into the read turns the capacity error into the
// shared release. assemblyData/assemblyTombstone pin the in-flight
// destinations during the gated section; outside assembly gates
// (construction reads) they are nil and only the constructed handles
// make up the working set.
func (p *ChangeHandler) admitSourceWindow(rows, bytes int) error {
	if !p.deferRangeLimitToAssembly {
		return nil
	}
	liveRows, liveBytes := p.assemblyLiveRowsBytes(p.assemblyData, p.assemblyTombstone)
	return p.checkRangeLimit(liveRows+rows, liveBytes+bytes)
}

// pinAssemblyDst publishes a destination created inside a serve into
// the assembly gate's snapshot: a source-window load trailing the serve
// (the aobj tail prefetch) must charge against the live combined set it
// actually joins, including a destination that did not exist when the
// gate opened and snapshotted the pre-serve pointers. Appends re-pin
// the same pointer, which the liveness walk re-measures anyway; outside
// gated sections the slots are nil and each serve clears them again.
func (p *ChangeHandler) pinAssemblyDst(owner *baseHandle, dst *batch.Batch) {
	if !p.deferRangeLimitToAssembly {
		return
	}
	if p.dataHandle == owner {
		p.assemblyData = dst
	} else if p.tombstoneHandle == owner {
		p.assemblyTombstone = dst
	}
}

// estimatePersistedWindow projects the PEAK the gated section reaches
// while one bounded window read materializes and prepares its batch,
// WITHOUT scheduling the read: it repeats the bounded-read job's own
// window arithmetic from the block metadata (remaining rows, chunk row
// cap, chunk byte budget) and then charges a SOUND per-column bound —
// fixed-width columns scale exactly (every row carries the constant
// width), each varlena column charges its decoded 24-byte header per
// window row plus the WHOLE column payload area (any subset of the
// block's values, in any width distribution, materializes at most that
// much area), plus a null bitmap per column per window row — with every
// buffer passed through one allocator capacity-class step from zero
// previous capacity, the way a fresh allocation behaves. Charged rows
// equal the arithmetic exactly; charged bytes never under-count,
// including a skewed window whose single wide value dominates the block
// and a payload sitting just under a size class — the two shapes integer
// payload scaling under-counted.
//
// Two further transients ride on the same charge so rejection stays
// strictly ahead of every allocation the window provokes:
//
//   - Chunk-union growth. A chunk-encoded column (Location().Alg() ==
//     compress.Lz4Chunked) materializes its window chunk part by part,
//     unioning each part into the accumulated destination: the grown
//     replacement buffer is allocated BEFORE the previous buffer is
//     freed (mpool's reallocation sequence), while the part stays alive
//     until after the union. Previous, replacement and part coexist, so
//     one union step peaks at up to three times the destination's final
//     buffer set (previous and part each fit individually inside that
//     final set: their accumulated bytes both belong to the unioned
//     total). A chunked column is therefore charged THREE times its
//     retained composition — a sound bound for any chunk count; a
//     single-chunk window pays the same charge even though it never
//     unions, which keeps the gate fail-closed without reading chunk
//     headers. Flat columns materialize in one shot and charge once.
//   - Tombstone normalization. A persisted tombstone window is sorted by
//     commit-ts right after materialization (updateTombstoneBatch ->
//     sortBatch -> Shuffle), each vector's copy coexisting with its
//     original; the peak adds the LARGEST single column copy, bounded by
//     the largest per-column retained charge. normalizeSort is set only
//     by the tombstone aobject leg in row mode; data windows and CN
//     windows are not commit-ts sorted, and quick mode sorts nothing.
//
// The extra meta load is the only added IO and is cache-satisfiable, and
// it is skipped entirely on paths without an attached policy.
func estimatePersistedWindow(
	ctx context.Context,
	fs fileservice.FileService,
	stats *objectio.ObjectStats,
	blockID uint32,
	rowOffset, maxRows, maxBytes int,
	normalizeSort bool,
) (rows, bytes int, err error) {
	loc := stats.BlockLocation(uint16(blockID), 8192)
	meta, err := objectio.FastLoadObjectMeta(ctx, &loc, false, fs)
	if err != nil {
		return 0, 0, err
	}
	dataMeta := meta.MustGetMeta(objectio.SchemaData)
	blockMeta := dataMeta.GetBlockMeta(blockID)
	var decodedBytes uint64
	for seqnum := uint16(0); seqnum < blockMeta.GetMetaColumnCount(); seqnum++ {
		decodedBytes += uint64(blockMeta.ColumnMeta(seqnum).Location().OriginSize())
	}
	totalRows := int(blockMeta.GetRows())
	windowRows := totalRows - rowOffset
	if windowRows < 0 {
		windowRows = 0
	}
	if maxRows > 0 {
		windowRows = min(windowRows, max(1, maxRows/4))
	}
	if decodedBytes > 0 && totalRows > 0 && maxBytes > 0 {
		windowByteBudget := max(1, maxBytes/4)
		windowRows = min(windowRows, max(1, int(uint64(windowByteBudget)*uint64(totalRows)/decodedBytes)))
	}
	var payload uint64
	maxColumnCharge := 0
	charge := func(bytes int64) int {
		rounded, _ := grownCapacity(0, int(bytes))
		return rounded
	}
	for seqnum := uint16(0); seqnum < blockMeta.GetMetaColumnCount(); seqnum++ {
		cm := blockMeta.ColumnMeta(seqnum)
		origin := int64(cm.Location().OriginSize())
		var columnCharge int
		if fixed := types.T(cm.DataType()).FixedLength(); fixed > 0 {
			columnCharge += charge(int64(windowRows) * int64(fixed))
		} else {
			columnCharge += charge(types.VarlenaSize * int64(windowRows))
			area := origin - types.VarlenaSize*int64(totalRows)
			if area < 0 {
				// Layout where OriginSize does not decompose into
				// headers plus area; the whole origin is the only sound
				// metadata bound.
				area = origin
			}
			columnCharge += charge(area)
		}
		columnCharge += charge(int64((windowRows + 63) / 64 * 8))
		// The largest single-column charge binds the normalization
		// sort's copy transient below; the retained composition is the
		// measure (the sort never unions, it replaces).
		maxColumnCharge = max(maxColumnCharge, columnCharge)
		if cm.Location().Alg() == compress.Lz4Chunked {
			// Previous + replacement + part coexist at one union step
			// (see the doc comment): charge three complete copies of
			// the window's retained composition for chunked columns.
			columnCharge *= 3
		}
		payload += uint64(columnCharge)
	}
	if normalizeSort {
		payload += uint64(maxColumnCharge)
	}
	if payload == 0 && decodedBytes > 0 {
		payload = 1
	}
	rows = windowRows
	bytes = int(payload) + 128
	return rows, bytes, nil
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
// It is used by snapshot-read policies that need exact range meaning. Output
// is returned in batches. Callers may opt into bounded in-memory
// materialization with engine.WithChangeRangeLimit and provide spill ownership
// with engine.WithChangeRangeSpill. Existing callers remain unbounded by
// default.
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
	return NewChangesHandlerWithPartitionStateRangeAndPrimaryIdx(
		ctx, state, start, end, skipDeletes, maxRow, primarySeqnum, primarySeqnum, nil, mp, fs,
	)
}

// NewChangesHandlerWithPartitionStateRangeAndPrimaryIdx is the range-aware
// constructor for callers that keep logical column order separate from
// persisted physical seqnums.
func NewChangesHandlerWithPartitionStateRangeAndPrimaryIdx(
	ctx context.Context,
	state *PartitionState,
	start, end types.TS,
	skipDeletes bool,
	maxRow uint32,
	primarySeqnum, primaryIdx int,
	logicalSeqnums []uint16,
	mp *mpool.MPool,
	fs fileservice.FileService,
) (changeHandle *ChangeHandler, err error) {
	rangeLimit := engine.ChangeRangeLimitFromContext(ctx)
	if rangeLimit.MaxInMemoryRows > 0 && rangeLimit.MaxInMemoryBytes <= 0 {
		return nil, moerr.NewInvalidInputNoCtx(
			"change range MaxInMemoryRows requires MaxInMemoryBytes to bound variable-width persisted columns",
		)
	}
	stateStart := state.GetStart()
	spillConfig := engine.ChangeRangeSpillFromContext(ctx)
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
		primaryIdx:               primaryIdx,
		logicalSeqnums:           append([]uint16(nil), logicalSeqnums...),
		mp:                       mp,
		scheduler:                tasks.NewParallelJobScheduler(LoadParallism),
		enableCommitTSBlockPrune: true,
		strictCommitTSBlockPrune: true,
		enableDeleteChainResolve: true,
		isRecoveryMode:           engine.CollectChangesKeepTrailingDeletesFromContext(ctx),
		pkFilter:                 engine.PKFilterFromContext(ctx),
		maxInMemoryRows:          rangeLimit.MaxInMemoryRows,
		maxInMemoryBytes:         rangeLimit.MaxInMemoryBytes,
		spillConfig:              spillConfig,
		debugLabel:               engine.CollectChangesDebugLabelFromContext(ctx),
		retainRowID:              engine.RetainRowIDFromContext(ctx),
		preserveAllVersions:      engine.CollectChangesPreserveAllVersionsFromContext(ctx),
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
	logRangeReplaySelection(
		start,
		end,
		changeHandle.dataHandle.aobjHandle.objects,
		changeHandle.dataHandle.cnObjectHandle.objects,
		changeHandle.tombstoneHandle.aobjHandle.objects,
		changeHandle.tombstoneHandle.cnObjectHandle.objects,
	)
	if rangeLimit.Enabled() {
		chunkRows, chunkBytes := changeHandle.rangeSpillChunkLimits()
		changeHandle.spillNetEffectData, changeHandle.spillNetEffectTombstone, err =
			prepareRangeNetEffectSpill(
				ctx,
				changeHandle.dataHandle,
				changeHandle.tombstoneHandle,
				primaryIdx,
				skipDeletes,
				changeHandle.isRecoveryMode,
				spillConfig,
				mp,
				chunkRows,
				chunkBytes,
			)
		if err != nil {
			return nil, err
		}
		changeHandle.quick = true
	}
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
		coarseMaxRow:        int(maxRow),
		start:               start,
		end:                 end,
		fs:                  fs,
		minTS:               start,
		skipDeletes:         skipDeletes,
		LogThreshold:        LogThreshold,
		primarySeqnum:       primarySeqnum,
		primaryIdx:          primarySeqnum,
		mp:                  mp,
		scheduler:           tasks.NewParallelJobScheduler(LoadParallism),
		isRecoveryMode:      isRecoveryMode || engine.CollectChangesKeepTrailingDeletesFromContext(ctx),
		debugLabel:          engine.CollectChangesDebugLabelFromContext(ctx),
		retainRowID:         engine.RetainRowIDFromContext(ctx),
		preserveAllVersions: engine.CollectChangesPreserveAllVersionsFromContext(ctx),
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
	if err = changeHandle.attachRangeLimit(ctx); err != nil {
		return
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
	// Same construction-time enforcement as the in-memory partition
	// replay family: the complete retained working set must fit the
	// attached budget before the drain starts; cleanup runs through the
	// deferred close above.
	if err = changeHandle.chargeConstruction(); err != nil {
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
	sumRows := func(entries []*objectio.ObjectEntry) int {
		total := 0
		for _, entry := range entries {
			if entry == nil {
				continue
			}
			total += int(entry.Rows())
		}
		return total
	}
	logutil.Info(
		"ChangesHandle-RangeReplaySelection",
		zap.String("start", start.ToString()),
		zap.String("end", end.ToString()),
		zap.Int("data-aobj-count", len(dataAobj)),
		zap.Int("data-aobj-rows", sumRows(dataAobj)),
		zap.Int("data-cnobj-count", len(dataCNObj)),
		zap.Int("data-cnobj-rows", sumRows(dataCNObj)),
		zap.Int("tombstone-aobj-count", len(tombstoneAobj)),
		zap.Int("tombstone-aobj-rows", sumRows(tombstoneAobj)),
		zap.Int("tombstone-cnobj-count", len(tombstoneCNObj)),
		zap.Int("tombstone-cnobj-rows", sumRows(tombstoneCNObj)),
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
	dataAobjMap := make(map[string]*objectio.ObjectEntry)
	dataCNObjMap := make(map[string]*objectio.ObjectEntry)
	tombstoneAobjMap := make(map[string]*objectio.ObjectEntry)
	tombstoneCNObjMap := make(map[string]*objectio.ObjectEntry)
	readers := make([]checkpointEntryReader, 0, len(checkpoint))
	for _, entry := range checkpoint {
		reader := newCKPReaderWithTableID(entry.GetVersion(), entry.GetLocation(), tid, mp, fs)
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
	if obj.GetAppendable() && obj.Rows() == 0 && obj.BlkCnt() == 0 {
		return checkpointObjectKindIgnore
	}
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
		coarseMaxRow:        int(maxRow),
		start:               start,
		end:                 end,
		fs:                  fs,
		minTS:               state.start,
		skipDeletes:         skipDeletes,
		LogThreshold:        LogThreshold,
		primarySeqnum:       primarySeqnum,
		primaryIdx:          primarySeqnum,
		mp:                  mp,
		scheduler:           tasks.NewParallelJobScheduler(LoadParallism),
		isRecoveryMode:      engine.CollectChangesKeepTrailingDeletesFromContext(ctx),
		pkFilter:            engine.PKFilterFromContext(ctx),
		debugLabel:          engine.CollectChangesDebugLabelFromContext(ctx),
		retainRowID:         engine.RetainRowIDFromContext(ctx),
		preserveAllVersions: engine.CollectChangesPreserveAllVersionsFromContext(ctx),
	}
	defer func() {
		if err != nil {
			changeHandle.scheduler.Stop()
			changeHandle = nil
		}
	}()
	if err = changeHandle.attachRangeLimit(ctx); err != nil {
		return
	}
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
		return
	}
	// The retained buffers and prefetch caches now hold their complete
	// construction-time allocations: an attached budget that cannot hold
	// this working set fails before any drain begins.
	if err = changeHandle.chargeConstruction(); err != nil {
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
	if p.spillNetEffectData != nil {
		p.spillNetEffectData.Close()
		p.spillNetEffectData = nil
	}
	if p.spillNetEffectTombstone != nil {
		p.spillNetEffectTombstone.Close()
		p.spillNetEffectTombstone = nil
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
	if p.spillNetEffectData != nil || p.spillNetEffectTombstone != nil {
		read := func(handle replayRowHandle, dst **batch.Batch) error {
			if handle == nil || handle.IsEmpty() {
				return nil
			}
			nextErr := handle.QuickNext(dst, mp)
			if moerr.IsMoErrCode(nextErr, moerr.OkExpectedEOF) ||
				moerr.IsMoErrCode(nextErr, moerr.OkExpectedEOB) {
				return nil
			}
			return nextErr
		}
		if err = read(p.spillNetEffectData, &data); err != nil {
			return
		}
		if err = read(p.spillNetEffectTombstone, &tombstone); err != nil {
			return
		}
		return
	}
	for {
		dataEnd := false
		tombstoneEnd := false
		if p.deferRangeLimitToAssembly {
			gmodel := p.growthModelFor(true)
			var addRows, addBytes int
			p.assemblyData, p.assemblyTombstone = data, tombstone
			if addRows, addBytes, err = p.dataHandle.prospectiveQuickCharge(ctx, data, gmodel); err != nil {
				gmodel.dropPending()
				p.assemblyData, p.assemblyTombstone = nil, nil
				if engine.IsChangeRangeCapacityError(err) {
					p.releaseAssemblyDsts(&data, &tombstone, mp)
				}
				return
			}
			if err = p.admitAssemblyGrowth(&data, &tombstone, addRows, addBytes, mp); err != nil {
				gmodel.dropPending()
				p.assemblyData, p.assemblyTombstone = nil, nil
				return
			}
		}
		err = p.dataHandle.QuickNext(ctx, &data, mp)
		if p.deferRangeLimitToAssembly {
			// The admitted serve ran to one of EOF/EOB/success (or a read
			// error partway): commit the staged destination state — the
			// model never over-admits a later serve after a partial one.
			p.dataGrowth.commitPending()
			p.assemblyData, p.assemblyTombstone = nil, nil
			if engine.IsChangeRangeCapacityError(err) {
				// A source-window gate fires inside the serve when the
				// trailing prefetch lands there: the rejection cleanup is
				// the same shared release the outer gates run.
				p.releaseAssemblyDsts(&data, &tombstone, mp)
			}
		}
		if moerr.IsMoErrCode(err, moerr.OkExpectedEOF) {
			dataEnd = true
			err = nil
		} else if moerr.IsMoErrCode(err, moerr.OkExpectedEOB) {
			if err = p.chargeAssembly(&data, &tombstone, mp); err != nil {
				return
			}
			if err = p.coalesceBatch(data, tombstone); err != nil {
				return
			}
			return
		}
		if err != nil {
			return
		}
		if err = p.chargeAssembly(&data, &tombstone, mp); err != nil {
			return
		}
		if p.deferRangeLimitToAssembly {
			gmodel := p.growthModelFor(false)
			var addRows, addBytes int
			p.assemblyData, p.assemblyTombstone = data, tombstone
			if addRows, addBytes, err = p.tombstoneHandle.prospectiveQuickCharge(ctx, tombstone, gmodel); err != nil {
				gmodel.dropPending()
				p.assemblyData, p.assemblyTombstone = nil, nil
				if engine.IsChangeRangeCapacityError(err) {
					p.releaseAssemblyDsts(&data, &tombstone, mp)
				}
				return
			}
			if err = p.admitAssemblyGrowth(&data, &tombstone, addRows, addBytes, mp); err != nil {
				gmodel.dropPending()
				p.assemblyData, p.assemblyTombstone = nil, nil
				return
			}
		}
		err = p.tombstoneHandle.QuickNext(ctx, &tombstone, mp)
		if p.deferRangeLimitToAssembly {
			p.tombGrowth.commitPending()
			p.assemblyData, p.assemblyTombstone = nil, nil
			if engine.IsChangeRangeCapacityError(err) {
				p.releaseAssemblyDsts(&data, &tombstone, mp)
			}
		}
		if moerr.IsMoErrCode(err, moerr.OkExpectedEOF) {
			tombstoneEnd = true
			err = nil
		} else if moerr.IsMoErrCode(err, moerr.OkExpectedEOB) {
			if err = p.chargeAssembly(&data, &tombstone, mp); err != nil {
				return
			}
			if err = p.coalesceBatch(data, tombstone); err != nil {
				return
			}
			return
		}
		if err != nil {
			return
		}
		if err = p.chargeAssembly(&data, &tombstone, mp); err != nil {
			return
		}
		if err = p.coalesceBatch(data, tombstone); err != nil {
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

// coalesceBatch applies net-effect filtering to a pair of batches unless the
// caller explicitly requests all historical versions.
func (p *ChangeHandler) coalesceBatch(data, tombstone *batch.Batch) error {
	if p.preserveAllVersions {
		return nil
	}
	return filterBatch(data, tombstone, p.primaryIdx, p.skipDeletes, p.isRecoveryMode)
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
// isRecoveryMode: When true (CDC restart from checkpoint, or a caller that opted in
// through engine.WithCollectChangesKeepTrailingDeletes), Case 2.2 (first insert, last
// delete) keeps the delete to ensure downstream consistency. When false (normal
// operation), Case 2.2 deletes all rows since the net effect is "no change" — sound
// only while the consumer's state matches the table state at the range start.
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
	dataPKIdx := primarySeqnum
	if len(data.Vecs) > 0 && data.Vecs[0] != nil && data.Vecs[0].GetType().Oid == types.T_Rowid {
		dataPKIdx++
	}
	pkVec := data.Vecs[dataPKIdx]
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
	tombstonePKIdx := 0
	tombstoneTSIdx := 1
	if len(tombstone.Vecs) > 0 && tombstone.Vecs[0] != nil && tombstone.Vecs[0].GetType().Oid == types.T_Rowid {
		tombstonePKIdx = 1
		tombstoneTSIdx = 2
	}
	pkVec = tombstone.Vecs[tombstonePKIdx]
	tsVec = tombstone.Vecs[tombstoneTSIdx]
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
					// Recovery mode (CDC restart, or an opted-in consumer whose state was
					// seeded from an earlier generation): keep the last delete. The insert
					// may already have reached the consumer — before a restart, or through
					// a cloned/copied state that predates this range — so the delete must
					// still be delivered to keep the consumer consistent.
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
			if p.deferRangeLimitToAssembly {
				gmodel := p.growthModelFor(true)
				var addRows, addBytes int
				p.assemblyData, p.assemblyTombstone = data, tombstone
				if addRows, addBytes, err = p.dataHandle.prospectiveRowCharge(ctx, data, gmodel); err != nil {
					gmodel.dropPending()
					p.assemblyData, p.assemblyTombstone = nil, nil
					if engine.IsChangeRangeCapacityError(err) {
						p.releaseAssemblyDsts(&data, &tombstone, mp)
					}
					return
				}
				if err = p.admitAssemblyGrowth(&data, &tombstone, addRows, addBytes, mp); err != nil {
					gmodel.dropPending()
					p.assemblyData, p.assemblyTombstone = nil, nil
					return
				}
			}
			err = p.dataHandle.Next(ctx, &data, mp)
			if p.deferRangeLimitToAssembly {
				p.dataGrowth.commitPending()
				p.assemblyData, p.assemblyTombstone = nil, nil
				if engine.IsChangeRangeCapacityError(err) {
					// A source-window gate fires inside the serve when
					// the trailing prefetch lands there: the rejection
					// cleanup is the shared release the outer gates run.
					p.releaseAssemblyDsts(&data, &tombstone, mp)
				}
			}
			if err == nil {
				if err = p.chargeAssembly(&data, &tombstone, mp); err != nil {
					return
				}
			}
			if err == nil && data.Vecs[0].Length() >= p.coarseMaxRow*2 {
				if err = p.coalesceBatch(data, tombstone); err != nil {
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
			if p.deferRangeLimitToAssembly {
				gmodel := p.growthModelFor(false)
				var addRows, addBytes int
				p.assemblyData, p.assemblyTombstone = data, tombstone
				if addRows, addBytes, err = p.tombstoneHandle.prospectiveRowCharge(ctx, tombstone, gmodel); err != nil {
					gmodel.dropPending()
					p.assemblyData, p.assemblyTombstone = nil, nil
					if engine.IsChangeRangeCapacityError(err) {
						p.releaseAssemblyDsts(&data, &tombstone, mp)
					}
					return
				}
				if err = p.admitAssemblyGrowth(&data, &tombstone, addRows, addBytes, mp); err != nil {
					gmodel.dropPending()
					p.assemblyData, p.assemblyTombstone = nil, nil
					return
				}
			}
			err = p.tombstoneHandle.Next(ctx, &tombstone, mp)
			if p.deferRangeLimitToAssembly {
				p.tombGrowth.commitPending()
				p.assemblyData, p.assemblyTombstone = nil, nil
				if engine.IsChangeRangeCapacityError(err) {
					p.releaseAssemblyDsts(&data, &tombstone, mp)
				}
			}
			if err == nil {
				if err = p.chargeAssembly(&data, &tombstone, mp); err != nil {
					return
				}
			}
			if err == nil && tombstone.Vecs[0].Length() >= p.coarseMaxRow*2 {
				if err = p.coalesceBatch(data, tombstone); err != nil {
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
		if moerr.IsMoErrCode(err, moerr.OkExpectedEOB) {
			err = nil
			if data != nil || tombstone != nil {
				if err = p.chargeAssembly(&data, &tombstone, mp); err != nil {
					return
				}
				if err = p.coalesceBatch(data, tombstone); err != nil {
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
			continue
		}
		if moerr.IsMoErrCode(err, moerr.OkExpectedEOF) {
			err = nil
			if err = p.chargeAssembly(&data, &tombstone, mp); err != nil {
				return
			}
			if err = p.coalesceBatch(data, tombstone); err != nil {
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

func newDataBatchWithBatch(src *batch.Batch, retainRowID bool, logicalSeqnums []uint16) (data *batch.Batch) {
	data = batch.NewWithSize(0)
	if retainRowID {
		data.Attrs = append(data.Attrs, catalog.Row_ID)
		data.Vecs = append(data.Vecs, vector.NewVec(types.T_Rowid.ToType()))
	}
	if len(logicalSeqnums) > 0 {
		for _, seqnum := range logicalSeqnums {
			idx := 2 + int(seqnum)
			if idx >= len(src.Vecs) || src.Vecs[idx] == nil {
				continue
			}
			attr := ""
			if idx < len(src.Attrs) {
				attr = src.Attrs[idx]
			}
			data.Attrs = append(data.Attrs, attr)
			data.Vecs = append(data.Vecs, vector.NewVec(*src.Vecs[idx].GetType()))
		}
	} else {
		data.Attrs = append(data.Attrs, src.Attrs[2:]...)
		for _, vec := range src.Vecs {
			if vec.GetType().Oid == types.T_Rowid || vec.GetType().Oid == types.T_TS {
				continue
			}
			data.Vecs = append(data.Vecs, vector.NewVec(*vec.GetType()))
		}
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
		case types.T_decimal256:
			val = vector.GetFixedAtNoTypeCheck[types.Decimal256](src, offset)
		case types.T_uuid:
			val = vector.GetFixedAtNoTypeCheck[types.Uuid](src, offset)
		case types.T_float32:
			val = vector.GetFixedAtNoTypeCheck[float32](src, offset)
		case types.T_float64:
			val = vector.GetFixedAtNoTypeCheck[float64](src, offset)
		case types.T_date:
			val = vector.GetFixedAtNoTypeCheck[types.Date](src, offset)
		case types.T_year:
			val = vector.GetFixedAtNoTypeCheck[types.MoYear](src, offset)
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
			types.T_array_float32, types.T_array_float64,
			types.T_array_bf16, types.T_array_float16, types.T_array_int8, types.T_array_uint8,
			types.T_datalink, types.T_geometry, types.T_geometry32:
			val = src.GetBytesAt(offset)
		default:
			//return vector.ErrVecTypeNotSupport
			panic(any("No Support"))
		}
		vector.AppendAny(vec, val, false, mp)
	}

}

func fillInInsertBatch(bat **batch.Batch, entry *RowEntry, retainRowID bool, logicalSeqnums []uint16, mp *mpool.MPool) {
	if *bat == nil {
		(*bat) = newDataBatchWithBatch(entry.Batch, retainRowID, logicalSeqnums)
	}
	dstOffset := 0
	if retainRowID {
		if err := vector.AppendFixed((*bat).Vecs[0], entry.RowID, false, mp); err != nil {
			panic(err)
		}
		dstOffset = 1
	}
	if len(logicalSeqnums) > 0 {
		dstCol := dstOffset
		for _, seqnum := range logicalSeqnums {
			idx := 2 + int(seqnum)
			if idx >= len(entry.Batch.Vecs) || entry.Batch.Vecs[idx] == nil {
				continue
			}
			appendFromEntry(entry.Batch.Vecs[idx], (*bat).Vecs[dstCol], int(entry.Offset), mp)
			dstCol++
		}
	} else {
		for i, vec := range entry.Batch.Vecs {
			if vec.GetType().Oid == types.T_Rowid || vec.GetType().Oid == types.T_TS {
				continue
			}
			appendFromEntry(vec, (*bat).Vecs[i-2+dstOffset], int(entry.Offset), mp)
		}
	}
	if err := vector.AppendFixed((*bat).Vecs[len((*bat).Vecs)-1], entry.Time, false, mp); err != nil {
		panic(err)
	}

}

// fillRowCols describes the columns ONE replay-row append writes into
// the growing replay buffer, mirroring fillInInsertBatch/fillInDeleteBatch
// column-for-column (same source-to-destination order, same skips), with
// the row's exact out-of-line varlena payload and nullness measured from
// the source entry. It exists so the construction budget gate can admit
// the row's buffer growth BEFORE fillIn* allocates it.
func fillRowCols(entry *RowEntry, tombstone, retainRowID bool, logicalSeqnums []uint16) []growColSpec {
	srcRow := func(vec *vector.Vector) (payload int, nulls bool, typeSize int, varlen bool) {
		if vec == nil {
			return 0, false, 0, false
		}
		t := vec.GetType()
		typeSize = t.TypeSize()
		varlen = t.IsVarlen()
		row := entry.Offset
		if vec.IsConst() {
			row = 0
		}
		if varlen {
			bs := vec.GetBytesAt(int(row))
			if len(bs) > types.VarlenaInlineSize {
				payload = len(bs)
			}
		}
		nulls = vec.IsNull(uint64(row))
		return
	}
	fixedSpec := func(oid types.T) growColSpec {
		return growColSpec{typeSize: oid.ToType().TypeSize()}
	}
	var cols []growColSpec
	if tombstone {
		if retainRowID {
			cols = append(cols, fixedSpec(types.T_Rowid))
		}
		pkPayload, pkNulls, pkSize, pkVarlen := srcRow(deleteEntryPKVec(entry))
		cols = append(cols, growColSpec{typeSize: pkSize, varlen: pkVarlen, payload: pkPayload, nulls: pkNulls})
		cols = append(cols, fixedSpec(types.T_TS))
		return cols
	}
	if retainRowID {
		cols = append(cols, fixedSpec(types.T_Rowid))
	}
	appendDataCol := func(vec *vector.Vector) {
		payload, nulls, typeSize, varlen := srcRow(vec)
		cols = append(cols, growColSpec{typeSize: typeSize, varlen: varlen, payload: payload, nulls: nulls})
	}
	if len(logicalSeqnums) > 0 {
		for _, seqnum := range logicalSeqnums {
			idx := 2 + int(seqnum)
			if idx >= len(entry.Batch.Vecs) || entry.Batch.Vecs[idx] == nil {
				continue
			}
			appendDataCol(entry.Batch.Vecs[idx])
		}
	} else {
		for _, vec := range entry.Batch.Vecs {
			if vec.GetType().Oid == types.T_Rowid || vec.GetType().Oid == types.T_TS {
				continue
			}
			appendDataCol(vec)
		}
	}
	cols = append(cols, fixedSpec(types.T_TS))
	return cols
}

// deleteEntryPKVec selects the delete row's primary-key source vector.
// Replayed delete entries retain only the PK vector; older in-memory
// entries and test fixtures may still carry the original logtail layout.
// The construction estimator and the delete writer must agree on this
// selection, or the admission pass indexes past the compact batch.
func deleteEntryPKVec(entry *RowEntry) *vector.Vector {
	if len(entry.Batch.Vecs) == 1 {
		return entry.Batch.Vecs[0]
	}
	return entry.Batch.Vecs[2]
}

func fillInDeleteBatch(bat **batch.Batch, entry *RowEntry, retainRowID bool, mp *mpool.MPool) {
	compact := len(entry.Batch.Vecs) == 1
	pkVec := deleteEntryPKVec(entry)
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
	pkIdx := 0
	tsIdx := 1
	if retainRowID {
		if compact {
			if err := vector.AppendFixed((*bat).Vecs[0], entry.RowID, false, mp); err != nil {
				panic(err)
			}
		} else {
			appendFromEntry(entry.Batch.Vecs[0], (*bat).Vecs[0], int(entry.Offset), mp)
		}
		pkIdx = 1
		tsIdx = 2
	}
	appendFromEntry(pkVec, (*bat).Vecs[pkIdx], int(entry.Offset), mp)
	vector.AppendFixed((*bat).Vecs[tsIdx], entry.Time, false, mp)
}

// PXU TODO
func checkTS(start, end types.TS, ts types.TS) bool {
	return ts.LE(&end) && ts.GE(&start)
}

type persistedBlockWindow struct {
	batch         *batch.Batch
	rows          int
	totalRows     int
	specialLayout objectio.SpecialColumnLayout
	seqnums       []uint16
}

type persistedColumnSeqnum struct {
	seqnum uint16
	idx    uint16
}

func orderPersistedColumnSeqnums(columns []persistedColumnSeqnum) []uint16 {
	goSort.SliceStable(columns, func(i, j int) bool {
		return columns[i].idx < columns[j].idx
	})
	seqnums := make([]uint16, len(columns))
	for i, column := range columns {
		seqnums[i] = column.seqnum
	}
	return seqnums
}

func persistedColumnSeqnums(meta objectio.BlockObject) []uint16 {
	columns := make([]persistedColumnSeqnum, 0, meta.GetMetaColumnCount())
	for seqnum := uint16(0); seqnum < meta.GetMetaColumnCount(); seqnum++ {
		if meta.ColumnMeta(seqnum).DataType() != 0 {
			columns = append(columns, persistedColumnSeqnum{
				seqnum: seqnum,
				idx:    meta.ColumnMeta(seqnum).Idx(),
			})
		}
	}
	return orderPersistedColumnSeqnums(columns)
}

func prefetchObjects(
	ctx context.Context,
	blockID uint32,
	rowOffset int,
	fs fileservice.FileService,
	stats *objectio.ObjectStats,
	scheduler tasks.JobScheduler,
	maxRows int,
	maxBytes int,
	spillConfig engine.ChangeRangeSpillConfig,
	mp *mpool.MPool,
) (job *tasks.Job) {
	job = getJob(
		ctx,
		stats.ObjectName().String(),
		JTCDCLoad,
		func(ctx context.Context) (res *tasks.JobResult) {
			loc := stats.BlockLocation(uint16(blockID), 8192)
			if maxRows > 0 || maxBytes > 0 {
				meta, err := objectio.FastLoadObjectMeta(ctx, &loc, false, fs)
				if err != nil {
					return &tasks.JobResult{Err: err}
				}
				dataMeta := meta.MustGetMeta(objectio.SchemaData)
				blockMeta := dataMeta.GetBlockMeta(blockID)
				var decodedBytes uint64
				for seqnum := uint16(0); seqnum < blockMeta.GetMetaColumnCount(); seqnum++ {
					decodedBytes += uint64(blockMeta.ColumnMeta(seqnum).Location().OriginSize())
				}
				totalRows := int(blockMeta.GetRows())
				windowRows := totalRows - rowOffset
				if maxRows > 0 {
					windowRows = min(windowRows, max(1, maxRows/4))
				}
				windowByteBudget := 0
				if decodedBytes > 0 && totalRows > 0 {
					if maxBytes > 0 {
						windowByteBudget = max(1, maxBytes/4)
						windowRows = min(windowRows, max(1, int(uint64(windowByteBudget)*uint64(totalRows)/decodedBytes)))
					}
				}
				cols := persistedColumnSeqnums(blockMeta)
				bat, err := objectio.ReadOneBlockAllColumnsWindow(
					ctx, &dataMeta, loc.Name().String(), blockID, cols,
					rowOffset, windowRows, fileservice.SkipAllCache, fs, mp,
					windowByteBudget,
					func(ctx context.Context) (objectio.ColumnWindowSpill, error) {
						return newLegacyColumnSpill(ctx, spillConfig)
					},
				)
				if err != nil {
					return &tasks.JobResult{Err: err}
				}
				return &tasks.JobResult{Res: &persistedBlockWindow{
					batch: bat, rows: windowRows, totalRows: totalRows,
					specialLayout: objectio.ResolveSpecialColumnLayout(blockMeta), seqnums: cols,
				}}
			}
			bat, _, specialLayout, err := ioutil.LoadOneBlockWithSpecialLayout(
				ctx,
				fs,
				loc,
				objectio.SchemaData,
			)
			res = &tasks.JobResult{}
			if err != nil {
				res.Err = err
			} else {
				res.Res = &persistedBlockWindow{
					batch: bat, rows: bat.RowCount(), totalRows: bat.RowCount(),
					specialLayout: specialLayout, seqnums: nil,
				}
			}
			return
		},
	)
	scheduler.Schedule(job)
	return
}

func prependRowIDVectorIfNeeded(bat *batch.Batch, blk *types.Blockid, mp *mpool.MPool) error {
	if bat == nil || blk == nil || bat.RowCount() == 0 {
		return nil
	}
	firstRowIDIdx := -1
	rowIDCnt := 0
	for i, vec := range bat.Vecs {
		if vec != nil && vec.GetType().Oid == types.T_Rowid {
			rowIDCnt++
			if firstRowIDIdx == -1 {
				firstRowIDIdx = i
			}
		}
	}
	if firstRowIDIdx >= 0 {
		if rowIDCnt == 1 && firstRowIDIdx == 0 {
			return nil
		}
		origVecs := bat.Vecs
		rebuiltVecs := make([]*vector.Vector, 0, len(origVecs)-rowIDCnt+1)
		rebuiltVecs = append(rebuiltVecs, origVecs[firstRowIDIdx])
		for i, vec := range origVecs {
			if i == firstRowIDIdx {
				continue
			}
			if vec != nil && vec.GetType().Oid == types.T_Rowid {
				vec.Free(mp)
				continue
			}
			rebuiltVecs = append(rebuiltVecs, vec)
		}
		bat.Vecs = rebuiltVecs
		if len(bat.Attrs) == len(origVecs) {
			rebuiltAttrs := make([]string, 0, len(rebuiltVecs))
			rebuiltAttrs = append(rebuiltAttrs, catalog.Row_ID)
			for i, attr := range bat.Attrs {
				if i == firstRowIDIdx {
					continue
				}
				if origVecs[i] != nil && origVecs[i].GetType().Oid == types.T_Rowid {
					continue
				}
				rebuiltAttrs = append(rebuiltAttrs, attr)
			}
			bat.Attrs = rebuiltAttrs
		}
		return nil
	}
	rowIDVec := vector.NewVec(types.T_Rowid.ToType())
	for i := 0; i < bat.RowCount(); i++ {
		if err := vector.AppendFixed(rowIDVec, types.NewRowid(blk, uint32(i)), false, mp); err != nil {
			rowIDVec.Free(mp)
			return err
		}
	}
	bat.Vecs = append([]*vector.Vector{rowIDVec}, bat.Vecs...)
	if len(bat.Attrs) == len(bat.Vecs)-1 {
		bat.Attrs = append([]string{catalog.Row_ID}, bat.Attrs...)
	}
	return nil
}

func updateTombstoneBatch(
	bat *batch.Batch,
	start, end types.TS,
	skipTS map[types.TS]struct{},
	sort bool,
	blk *types.Blockid,
	specialLayout *objectio.SpecialColumnLayout,
	seqnums []uint16,
	retainRowID bool,
	mp *mpool.MPool,
) error {
	if specialLayout != nil {
		return updatePersistedTombstoneBatch(
			bat, start, end, skipTS, sort, *specialLayout, seqnums, retainRowID, mp,
		)
	}
	if retainRowID {
		if err := prependRowIDVectorIfNeeded(bat, blk, mp); err != nil {
			return err
		}
	}
	var rowIDVec *vector.Vector
	var pkVec *vector.Vector
	var commitTSVec *vector.Vector
	for _, vec := range bat.Vecs {
		switch vec.GetType().Oid {
		case types.T_Rowid:
			if rowIDVec == nil {
				rowIDVec = vec
			} else {
				vec.Free(mp)
			}
		case types.T_TS:
			if commitTSVec == nil {
				commitTSVec = vec
			} else {
				vec.Free(mp)
			}
		default:
			if pkVec == nil {
				pkVec = vec
			} else {
				vec.Free(mp)
			}
		}
	}
	if pkVec == nil || commitTSVec == nil || (retainRowID && rowIDVec == nil) {
		return moerr.NewInternalErrorNoCtx("invalid tombstone batch layout for collect changes")
	}
	if retainRowID {
		bat.Vecs = []*vector.Vector{rowIDVec, pkVec, commitTSVec}
		bat.Attrs = []string{
			catalog.Row_ID,
			objectio.TombstoneAttr_PK_Attr,
			objectio.DefaultCommitTS_Attr,
		}
		applyTSFilterForBatch(bat, 2, skipTS, start, end)
	} else {
		if rowIDVec != nil {
			rowIDVec.Free(mp)
		}
		bat.Vecs = []*vector.Vector{pkVec, commitTSVec}
		bat.Attrs = []string{
			objectio.TombstoneAttr_PK_Attr,
			objectio.DefaultCommitTS_Attr}
		applyTSFilterForBatch(bat, 1, skipTS, start, end)
	}
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
	seqnums []uint16,
	retainRowID bool,
	mp *mpool.MPool,
) error {
	if bat == nil || len(bat.Vecs) <= int(objectio.TombstoneAttr_PK_SeqNum) {
		return moerr.NewInternalErrorNoCtx("invalid persisted tombstone batch layout for collect changes")
	}
	if seqnums != nil && len(seqnums) != len(bat.Vecs) {
		return moerr.NewInternalErrorNoCtx("persisted tombstone column sequence metadata mismatch")
	}
	physicalIndex := func(seqnum uint16) int {
		if seqnums == nil {
			return int(seqnum)
		}
		for i, physical := range seqnums {
			if physical == seqnum {
				return i
			}
		}
		return -1
	}
	commitPos, ok := layout.Resolve(objectio.SEQNUM_COMMITTS)
	commitIdx := physicalIndex(commitPos)
	if !ok || commitIdx < 0 || commitIdx >= len(bat.Vecs) || bat.Vecs[commitIdx].GetType().Oid != types.T_TS {
		return moerr.NewInternalErrorNoCtx("persisted tombstone object has no valid commit-ts column")
	}
	abortPos, hasAbort := layout.Resolve(objectio.SEQNUM_ABORT)
	abortIdx := physicalIndex(abortPos)
	if hasAbort && (abortIdx < 0 || abortIdx >= len(bat.Vecs) || bat.Vecs[abortIdx].GetType().Oid != types.T_bool) {
		return moerr.NewInternalErrorNoCtx("persisted tombstone object has an invalid abort column")
	}
	rowIDIdx := physicalIndex(objectio.TombstoneAttr_Rowid_SeqNum)
	pkIdx := physicalIndex(objectio.TombstoneAttr_PK_SeqNum)
	if rowIDIdx < 0 || pkIdx < 0 || rowIDIdx >= len(bat.Vecs) || pkIdx >= len(bat.Vecs) {
		return moerr.NewInternalErrorNoCtx("persisted tombstone object has invalid key columns")
	}
	rowIDVec := bat.Vecs[rowIDIdx]
	pkVec := bat.Vecs[pkIdx]
	commitTSVec := bat.Vecs[commitIdx]
	if rowIDVec.GetType().Oid != types.T_Rowid || commitTSVec.IsConstNull() {
		return moerr.NewInternalErrorNoCtx("invalid persisted tombstone special columns")
	}
	commits := vector.MustFixedColWithTypeCheck[types.TS](commitTSVec)
	var abortVec *vector.Vector
	var aborts []bool
	if hasAbort {
		abortVec = bat.Vecs[abortIdx]
		if abortVec.IsConstNull() {
			return moerr.NewInternalErrorNoCtx("persisted tombstone abort column is null")
		}
		aborts = vector.MustFixedColWithTypeCheck[bool](abortVec)
		if len(aborts) != len(commits) {
			return moerr.NewInternalErrorNoCtx("persisted tombstone abort column length mismatch")
		}
	}
	deletes := make([]int64, 0)
	for i, ts := range commits {
		if commitTSVec.IsNull(uint64(i)) || (aborts != nil && abortVec.IsNull(uint64(i))) {
			return moerr.NewInternalErrorNoCtx("persisted tombstone special column contains null")
		}
		_, skip := skipTS[ts]
		if (aborts != nil && aborts[i]) || ts.LT(&start) || ts.GT(&end) || skip {
			deletes = append(deletes, int64(i))
		}
	}

	for i, vec := range bat.Vecs {
		physical := uint16(i)
		if seqnums != nil {
			physical = seqnums[i]
		}
		if physical == objectio.TombstoneAttr_Rowid_SeqNum ||
			physical == objectio.TombstoneAttr_PK_SeqNum ||
			physical == commitPos {
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
	seqnums []uint16,
	retainRowID bool,
	mp *mpool.MPool,
) error {
	if specialLayout != nil {
		return updatePersistedDataBatch(bat, start, end, blk, *specialLayout, seqnums, retainRowID, mp)
	}
	if retainRowID {
		if err := prependRowIDVectorIfNeeded(bat, blk, mp); err != nil {
			return err
		}
	}
	filteredVecs := make([]*vector.Vector, 0, len(bat.Vecs))
	var commitTSVec *vector.Vector
	rebuildAttrs := len(bat.Attrs) == len(bat.Vecs)
	filteredAttrs := make([]string, 0, len(bat.Attrs))
	var commitTSAttr string

	for i, vec := range bat.Vecs {
		switch vec.GetType().Oid {
		case types.T_Rowid:
			if retainRowID {
				filteredVecs = append(filteredVecs, vec)
				if rebuildAttrs {
					filteredAttrs = append(filteredAttrs, bat.Attrs[i])
				}
			} else {
				vec.Free(mp)
			}
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
	return nil
}

func updatePersistedDataBatch(
	bat *batch.Batch,
	start, end types.TS,
	blk *types.Blockid,
	layout objectio.SpecialColumnLayout,
	seqnums []uint16,
	retainRowID bool,
	mp *mpool.MPool,
) error {
	if bat == nil {
		return moerr.NewInternalErrorNoCtx("updatePersistedDataBatch: nil batch")
	}
	if seqnums != nil && len(seqnums) != len(bat.Vecs) {
		return moerr.NewInternalErrorNoCtx("persisted appendable column sequence metadata mismatch")
	}
	physicalIndex := func(seqnum uint16) int {
		if seqnums == nil {
			return int(seqnum)
		}
		for i, physical := range seqnums {
			if physical == seqnum {
				return i
			}
		}
		return -1
	}
	commitPos, ok := layout.Resolve(objectio.SEQNUM_COMMITTS)
	commitIdx := physicalIndex(commitPos)
	if !ok || commitIdx < 0 || commitIdx >= len(bat.Vecs) || bat.Vecs[commitIdx].GetType().Oid != types.T_TS {
		return moerr.NewInternalErrorNoCtx("persisted appendable object has no valid commit-ts column")
	}
	abortPos, hasAbort := layout.Resolve(objectio.SEQNUM_ABORT)
	abortIdx := physicalIndex(abortPos)
	if hasAbort && (abortIdx < 0 || abortIdx >= len(bat.Vecs) || bat.Vecs[abortIdx].GetType().Oid != types.T_bool) {
		return moerr.NewInternalErrorNoCtx("persisted appendable object has an invalid abort column")
	}
	rowIDPos := uint16(objectio.InvalidSpecialColumnPosition)
	if layout.PhysicalAddr != objectio.InvalidSpecialColumnPosition {
		rowIDPos = layout.PhysicalAddr
		rowIDIdx := physicalIndex(rowIDPos)
		if rowIDIdx < 0 || rowIDIdx >= len(bat.Vecs) || bat.Vecs[rowIDIdx] == nil ||
			bat.Vecs[rowIDIdx].GetType().Oid != types.T_Rowid {
			return moerr.NewInternalErrorNoCtx("persisted appendable object has an invalid rowid column")
		}
	}

	commitTSVec := bat.Vecs[commitIdx]
	if commitTSVec.IsConstNull() {
		return moerr.NewInternalErrorNoCtx("persisted appendable object commit-ts column is null")
	}
	var abortVec *vector.Vector
	if hasAbort {
		abortVec = bat.Vecs[abortIdx]
		if abortVec.IsConstNull() {
			return moerr.NewInternalErrorNoCtx("persisted appendable object abort column is null")
		}
	}
	commits := vector.MustFixedColWithTypeCheck[types.TS](commitTSVec)
	var aborts []bool
	if abortVec != nil {
		aborts = vector.MustFixedColWithTypeCheck[bool](abortVec)
		if len(aborts) != len(commits) {
			return moerr.NewInternalErrorNoCtx("persisted appendable object abort column length mismatch")
		}
	}
	deletes := make([]int64, 0)
	for i, ts := range commits {
		if commitTSVec.IsNull(uint64(i)) || (aborts != nil && abortVec.IsNull(uint64(i))) {
			return moerr.NewInternalErrorNoCtx("persisted appendable object special column contains null")
		}
		if (aborts != nil && aborts[i]) || ts.LT(&start) || ts.GT(&end) {
			deletes = append(deletes, int64(i))
		}
	}

	rebuildAttrs := len(bat.Attrs) == len(bat.Vecs)
	filteredVecs := make([]*vector.Vector, 0, len(bat.Vecs)-1)
	filteredAttrs := make([]string, 0, len(bat.Attrs))
	commitTSAttr := objectio.DefaultCommitTS_Attr
	var rowIDVec *vector.Vector
	rowIDKept := false
	if retainRowID && rowIDPos == objectio.InvalidSpecialColumnPosition {
		if blk == nil {
			return moerr.NewInternalErrorNoCtx("persisted appendable object cannot synthesize rowid without block id")
		}
		rowIDVec = vector.NewVec(types.T_Rowid.ToType())
		for i := range commits {
			if err := vector.AppendFixed(rowIDVec, types.NewRowid(blk, uint32(i)), false, mp); err != nil {
				rowIDVec.Free(mp)
				return err
			}
		}
		rowIDKept = true
	}
	for i, vec := range bat.Vecs {
		pos := uint16(i)
		if seqnums != nil {
			pos = seqnums[i]
		}
		switch {
		case pos == commitPos:
			if rebuildAttrs && bat.Attrs[i] != "" {
				commitTSAttr = bat.Attrs[i]
			}
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
		filteredAttrs = append(filteredAttrs, commitTSAttr)
	}
	bat.Vecs = filteredVecs
	if rebuildAttrs {
		bat.Attrs = filteredAttrs
	}
	for _, vec := range bat.Vecs {
		vec.Shrink(deletes, true)
	}
	if len(bat.Vecs) > 0 {
		bat.SetRowCount(bat.Vecs[0].Length())
	}
	return nil
}
func updateCNTombstoneBatch(bat *batch.Batch, committs types.TS, blk *types.Blockid, retainRowID bool, mp *mpool.MPool) error {
	if bat == nil {
		return moerr.NewInternalErrorNoCtx("updateCNTombstoneBatch: nil batch")
	}
	if retainRowID {
		if err := prependRowIDVectorIfNeeded(bat, blk, mp); err != nil {
			return err
		}
	}
	var rowid *vector.Vector
	var pk *vector.Vector
	for _, vec := range bat.Vecs {
		switch vec.GetType().Oid {
		case types.T_Rowid:
			if retainRowID {
				rowid = vec
			} else {
				vec.Free(mp)
			}
		case types.T_TS:
			vec.Free(mp)
		default:
			pk = vec
		}
	}
	if pk == nil {
		return moerr.NewInternalErrorNoCtx("updateCNTombstoneBatch: tombstone batch missing pk vector")
	}
	if retainRowID && rowid == nil {
		return moerr.NewInternalErrorNoCtx("updateCNTombstoneBatch: retainRowID set but rowid vector missing")
	}
	commitTS, err := vector.NewConstFixed(types.T_TS.ToType(), committs, pk.Length(), mp)
	if err != nil {
		return err
	}
	if retainRowID {
		bat.Vecs = []*vector.Vector{rowid, pk, commitTS}
		bat.Attrs = []string{catalog.Row_ID, objectio.TombstoneAttr_PK_Attr, objectio.DefaultCommitTS_Attr}
	} else {
		bat.Vecs = []*vector.Vector{pk, commitTS}
		bat.Attrs = []string{objectio.TombstoneAttr_PK_Attr, objectio.DefaultCommitTS_Attr}
	}
	return nil
}
func updateCNDataBatch(bat *batch.Batch, commitTS types.TS, blk *types.Blockid, retainRowID bool, mp *mpool.MPool) error {
	if bat == nil {
		return moerr.NewInternalErrorNoCtx("updateCNDataBatch: nil batch")
	}
	for i, vec := range bat.Vecs {
		if vec.GetType().Oid == types.T_TS {
			vec.Free(mp)
			bat.Vecs = append(bat.Vecs[:i], bat.Vecs[i+1:]...)
			if len(bat.Attrs) == len(bat.Vecs)+1 {
				bat.Attrs = append(bat.Attrs[:i], bat.Attrs[i+1:]...)
			}
			break
		}
	}
	if retainRowID {
		if err := prependRowIDVectorIfNeeded(bat, blk, mp); err != nil {
			return err
		}
	}
	if len(bat.Vecs) == 0 {
		return moerr.NewInternalErrorNoCtx("updateCNDataBatch: data batch has no vectors after stripping commit-ts")
	}
	commitTSVec, err := vector.NewConstFixed(types.T_TS.ToType(), commitTS, bat.Vecs[0].Length(), mp)
	if err != nil {
		return err
	}
	bat.Vecs = append(bat.Vecs, commitTSVec)
	if len(bat.Attrs) == len(bat.Vecs)-1 {
		bat.Attrs = append(bat.Attrs, objectio.DefaultCommitTS_Attr)
	}
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

var newCKPReaderWithTableID = func(version uint32, location objectio.Location, tableID uint64, mp *mpool.MPool, fs fileservice.FileService) checkpointEntryReader {
	return logtail.NewCKPReaderWithTableID_V2(version, location, tableID, mp, fs)
}
