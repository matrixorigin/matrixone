// Copyright 2021 Matrix Origin
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

package ioutil

import (
	"bytes"
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/rscthrottler"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/mergeutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
)

const DefaultInMemoryStagedSize = mpool.MB * 16

// Cap each sort-key pipeline submission so backfill can produce reasonably
// sized objects without letting pending sorted batches grow without bound.
const pipelineSortKeySubmitThreshold = mpool.MB * 128

type pipelineFlushKeyType struct{}

// PipelineFlushKey is the context key used to enable pipeline flush in Sinker.
// Set the context value to true to enable pipeline flush for INSERT operations
// (e.g., ALTER TABLE ADD PRIMARY KEY).
var PipelineFlushKey = pipelineFlushKeyType{}

type SinkerOption func(*Sinker)

func WithAllMergeSorted() SinkerOption {
	return func(sinker *Sinker) {
		sinker.config.allMergeSorted = true
	}
}

func WithDedupAll() SinkerOption {
	return func(sinker *Sinker) {
		sinker.config.dedupAll = true
	}
}

func WithTailSizeCap(size int) SinkerOption {
	return func(sinker *Sinker) {
		sinker.config.tailSizeCap = size
	}
}

func WithMemorySizeThreshold(size int) SinkerOption {
	return func(sinker *Sinker) {
		sinker.staged.memorySizeThreshold = size
	}
}

func WithOffHeap() SinkerOption {
	return func(sinker *Sinker) {
		sinker.config.offHeap = true
	}
}

func WithBuffer(buffer containers.IBatchBuffer, isOwner bool) SinkerOption {
	return func(sinker *Sinker) {
		sinker.buf.isOwner = isOwner
		sinker.buf.buffers = buffer
	}
}

func WithPipelineFlush() SinkerOption {
	return func(sinker *Sinker) {
		sinker.pipe.enabled = true
	}
}

func WithBufferBytes(limit int64) SinkerOption {
	return func(sinker *Sinker) { sinker.config.bufferBytes = limit }
}

func WithRetainedRelease(fn func()) SinkerOption {
	return func(sinker *Sinker) { sinker.retainedRelease = fn }
}

func WithMemoryWorkspace(ticket *rscthrottler.WorkingSet) SinkerOption {
	return func(sinker *Sinker) { sinker.workspace = ticket }
}

type FileSinker interface {
	Sink(context.Context, *batch.Batch) error
	Sync(context.Context) (*objectio.ObjectStats, error)
	Reset()
	Close() error
}

var _ FileSinker = new(FSinkerImpl)

type FSinkerImpl struct {
	writer *BlockWriter
	arena  *objectio.WriteArena
	mp     *mpool.MPool
	fs     fileservice.FileService

	sortKeyPos      int
	isPrimaryKey    bool
	isTombstone     bool
	seqnums         []uint16
	schemaVersion   uint32
	hiddenSelection objectio.HiddenColumnSelection
	workspaceBytes  int64
	arenaBytes      int64
}

func (s *FSinkerImpl) Sink(ctx context.Context, b *batch.Batch) error {
	if s.writer == nil {
		if s.arena == nil {
			if s.workspaceBytes > 0 {
				limit := s.arenaBytes
				if limit <= 0 {
					limit = warmArenaBytes(int64(b.Size()), int64(len(b.Vecs)))
				}
				s.arena = objectio.GetArenaWithin(objectio.ArenaLarge, int(min(limit, s.workspaceBytes)))
			} else {
				s.arena = objectio.GetArena(objectio.ArenaLarge)
			}
		}
		s.arena.Reset()
		if s.isTombstone {
			s.writer = ConstructTombstoneWriterWithArena(
				s.hiddenSelection,
				s.fs,
				s.arena,
			)
		} else {
			s.writer = ConstructWriterWithArena(
				s.schemaVersion,
				s.seqnums,
				s.sortKeyPos,
				s.isPrimaryKey,
				s.isTombstone,
				s.fs,
				s.arena,
			)
		}
	}

	_, err := s.writer.WriteBatch(b)
	return err
}

func (s *FSinkerImpl) Sync(ctx context.Context) (*objectio.ObjectStats, error) {
	if _, _, err := s.writer.Sync(ctx); err != nil {
		return nil, err
	}

	var ss objectio.ObjectStats
	if s.sortKeyPos > -1 {
		ss = s.writer.GetObjectStats(objectio.WithSorted(), objectio.WithCNCreated())
	} else {
		ss = s.writer.GetObjectStats(objectio.WithCNCreated())
	}

	// s.writer.Reset
	s.writer = nil

	return &ss, nil
}

func (s *FSinkerImpl) Reset() {
	// Scratch leaves the active workspace before its lease is returned.
	_ = s.Close()
}

func (s *FSinkerImpl) Close() error {
	s.writer = nil
	if s.arena != nil {
		// Reset grows the arena if needed (adaptive sizing), then return it
		// to the free list so the pre-warmed backing array, serialBuf and
		// compressBuf are reused by the next FSinkerImpl.
		s.arena.Reset()
		objectio.PutArena(s.arena)
		s.arena = nil
		// Debounce the idle-drain timer so pools stay warm during
		// active CN pipeline execution.  When CN (and TN) are both
		// idle for the objectio default delay the pools will drain automatically.
		objectio.ScheduleArenaDrain(objectio.DefaultArenaDrainDelay)
	}
	return nil
}

// FileSinkerFactory is a factory function to create a FileSinker
type FileSinkerFactory func(*mpool.MPool, fileservice.FileService) FileSinker

// This factory is used to create a FileSinker for tombstone
// hiddenSelection: whether to write hidden tombstone
//
//	true: TN created tombstone objects
//	false: CN created tombstone objects
func NewTombstoneFSinkerImplFactory(hidden objectio.HiddenColumnSelection) FileSinkerFactory {
	return func(mp *mpool.MPool, fs fileservice.FileService) FileSinker {
		return NewTombstoneFSinkerImpl(hidden, mp, fs)
	}
}

// This factory is used to create a FileSinker for all kinds of objects
// the user should provide the seqnums, sortKeyPos, isPrimaryKey, isTombstone, schemaVersion
func NewFSinkerImplFactory(
	seqnums []uint16,
	sortKeyPos int,
	isPrimaryKey bool,
	isTombstone bool,
	schemaVersion uint32,
) FileSinkerFactory {
	return func(mp *mpool.MPool, fs fileservice.FileService) FileSinker {
		return NewFSinkerImpl(
			seqnums,
			sortKeyPos,
			isPrimaryKey,
			isTombstone,
			schemaVersion,
			mp,
			fs,
		)
	}
}

func NewTombstoneFSinkerImpl(
	hidden objectio.HiddenColumnSelection,
	mp *mpool.MPool,
	fs fileservice.FileService,
) *FSinkerImpl {
	return &FSinkerImpl{
		fs:              fs,
		mp:              mp,
		isTombstone:     true,
		hiddenSelection: hidden,
	}
}

func NewFSinkerImpl(
	seqnums []uint16,
	sortKeyPos int,
	isPrimaryKey bool,
	isTombstone bool,
	schemaVersion uint32,
	mp *mpool.MPool,
	fs fileservice.FileService,
) *FSinkerImpl {
	return &FSinkerImpl{
		fs:            fs,
		mp:            mp,
		seqnums:       seqnums,
		sortKeyPos:    sortKeyPos,
		isPrimaryKey:  isPrimaryKey,
		isTombstone:   isTombstone,
		schemaVersion: schemaVersion,
	}
}

func NewTombstoneSinker(
	hidden objectio.HiddenColumnSelection,
	pkType types.Type,
	mp *mpool.MPool,
	fs fileservice.FileService,
	opts ...SinkerOption,
) *Sinker {
	factory := NewTombstoneFSinkerImplFactory(hidden)
	attrs, attrTypes := objectio.GetTombstoneSchema(pkType, hidden)
	return NewSinker(
		objectio.TombstonePrimaryKeyIdx,
		attrs,
		attrTypes,
		factory,
		mp,
		fs,
		opts...,
	)
}

func NewSinker(
	sortKeyIdx int,
	attrs []string,
	attrTypes []types.Type,
	factory FileSinkerFactory,
	mp *mpool.MPool,
	fs fileservice.FileService,
	opts ...SinkerOption,
) *Sinker {
	sinker := &Sinker{
		schema: struct {
			attrs      []string
			attrTypes  []types.Type
			sortKeyIdx int
		}{
			attrs:      attrs,
			attrTypes:  attrTypes,
			sortKeyIdx: sortKeyIdx,
		},
		fSinker: struct {
			executor FileSinker
			factory  FileSinkerFactory
		}{
			factory: factory,
		},

		mp: mp,
		fs: fs,
	}

	for _, opt := range opts {
		opt(sinker)
	}

	sinker.fillDefaults()
	return sinker
}

type sinkerStats struct {
	Name               string
	HighWatermarkCnt   uint64
	HighWatermarkBytes uint64
	CurrentCnt         uint64
	CurrentBytes       uint64
}

func (s *sinkerStats) String() string {
	return fmt.Sprintf("%s, high cnt: %d, current cnt: %d, hight bytes: %d, current bytes: %d",
		s.Name, s.HighWatermarkCnt, s.CurrentCnt, s.HighWatermarkBytes, s.CurrentBytes)
}

func (s *sinkerStats) updateCount(n int) {
	if n > 0 {
		s.CurrentCnt += uint64(n)
	} else if n < 0 {
		s.CurrentCnt -= uint64(-n)
	}

	if s.CurrentCnt > s.HighWatermarkCnt {
		s.HighWatermarkCnt = s.CurrentCnt
	}
}

func (s *sinkerStats) updateBytes(n int) {
	if n > 0 {
		s.CurrentBytes += uint64(n)
	} else if n < 0 {
		s.CurrentBytes -= uint64(-n)
	}
	if s.CurrentBytes > s.HighWatermarkBytes {
		s.HighWatermarkBytes = s.CurrentBytes
	}
}

type Sinker struct {
	// Pipeline sources have immutable bitmap capacity after submission. The
	// counter remains live until their worker destroys the source batches.
	pipelineBitmapBytes atomic.Int64
	schema              struct {
		attrs      []string
		attrTypes  []types.Type
		sortKeyIdx int
	}
	config struct {
		allMergeSorted bool
		dedupAll       bool
		tailSizeCap    int
		offHeap        bool
		bufferBytes    int64
	}
	fSinker struct {
		executor FileSinker
		factory  FileSinkerFactory
	}
	staged struct {
		inMemStats          sinkerStats
		inMemory            []*batch.Batch
		persisted           []objectio.ObjectStats
		inMemorySize        int
		memorySizeThreshold int
	}
	result struct {
		persisted []objectio.ObjectStats
		tail      []*batch.Batch
	}

	buf struct {
		isOwner  bool
		bufStats sinkerStats
		buffers  containers.IBatchBuffer
	}

	pipe struct {
		enabled bool
		result  *pipelineResult
	}

	timing struct {
		spillCount int64 // atomic
		sortNs     int64 // atomic, nanoseconds
		sinkNs     int64 // atomic, nanoseconds (sync path only)
		syncNs     int64 // atomic, nanoseconds (sync path only)
		waitNs     int64 // atomic, nanoseconds (main goroutine blocked on pool submit)
		spillNs    int64 // atomic, nanoseconds (total wall time in trySpill)
	}

	mp              *mpool.MPool
	fs              fileservice.FileService
	retainedRelease func()
	workspace       *rscthrottler.WorkingSet
	ioMP            *mpool.MPool
}

func (sinker *Sinker) String() string {
	buf := bytes.Buffer{}
	buf.WriteString(fmt.Sprintf(
		"schema:{attrs=%v, types=%v, sortIdx=%d}; ",
		sinker.schema.attrs, sinker.schema.attrTypes, sinker.schema.sortKeyIdx))

	buf.WriteString(fmt.Sprintf(
		"config:{allMergeSorted=%v, dedupAll=%v, tailSizeCap=%d}; ",
		sinker.config.allMergeSorted, sinker.config.dedupAll,
		sinker.config.tailSizeCap))

	return buf.String()
}

func (sinker *Sinker) fillDefaults() {
	if sinker.config.bufferBytes <= 0 {
		sinker.config.bufferBytes = 64 << 20
	}
	if sinker.staged.memorySizeThreshold == 0 {
		sinker.staged.memorySizeThreshold = DefaultInMemoryStagedSize
	}

	sinker.staged.inMemStats.Name = "staged inmem stats"
	sinker.buf.bufStats.Name = "buffer stats"

	if sinker.buf.buffers == nil {
		sinker.buf.isOwner = true
		sinker.buf.buffers = containers.NewBatchFreeList(
			sinker.schema.attrs,
			sinker.schema.attrTypes,
			sinker.config.offHeap,
		)
	}
}

func (sinker *Sinker) GetResult() ([]objectio.ObjectStats, []*batch.Batch) {
	return sinker.result.persisted, sinker.result.tail
}

func (sinker *Sinker) fetchBuffer() (*batch.Batch, error) {
	x := sinker.buf.buffers.Len()
	bat := sinker.buf.buffers.FetchWithSchema(sinker.schema.attrs, sinker.schema.attrTypes)
	y := sinker.buf.buffers.Len()

	if x < y {
		sinker.buf.bufStats.updateCount(-1)
		sinker.buf.bufStats.updateBytes(-bat.Size())
	}

	return bat, nil
}

func (sinker *Sinker) AppendAllocation(data *batch.Batch) int64 {
	var tail *batch.Batch
	if n := len(sinker.staged.inMemory); n > 0 {
		tail = sinker.staged.inMemory[n-1]
	}
	return SinkerAppendAllocation(data, tail, sinker.buf.buffers, sinker.config.bufferBytes)
}

func batchBitmapAllocation(bat *batch.Batch) int64 {
	var size int64
	if bat != nil {
		for _, vec := range bat.Vecs {
			size += int64(nulls.Allocated(vec.GetNulls()) + nulls.Allocated(vec.GetGrouping()))
		}
	}
	return size
}

// RetainedBitmapBytes reports Go bitmap backing not charged by MPool. Like
// AppendAllocation, local state must be inspected on the owning goroutine (or
// while that goroutine is excluded by its owner's mutation lock). Pipeline
// sources are counted atomically without visiting worker-owned batches.
func (sinker *Sinker) RetainedBitmapBytes() int64 {
	size := sinker.pipelineBitmapBytes.Load()
	for _, bat := range sinker.staged.inMemory {
		size += batchBitmapAllocation(bat)
	}
	for _, bat := range sinker.result.tail {
		size += batchBitmapAllocation(bat)
	}
	if buffers, ok := sinker.buf.buffers.(interface{ Peek(int) *batch.Batch }); ok {
		for i := 0; i < sinker.buf.buffers.Len(); i++ {
			size += batchBitmapAllocation(buffers.Peek(i))
		}
	}
	return size
}

// WriteBorrowedToFile is the zero-retained-credit path. The caller owns a W
// lease for the whole clone/sort/encode/IO operation. No ordinary data grant
// is acquired, and the input remains borrowed until this method returns.
func (sinker *Sinker) WriteBorrowedToFile(ctx context.Context, data *batch.Batch, lease *rscthrottler.WorkspaceLease) error {
	if lease == nil {
		return moerr.NewInternalErrorNoCtx("borrowed spill requires IO workspace")
	}
	var payload int64
	schema := make([]types.Type, len(data.Vecs))
	for i, v := range data.Vecs {
		payload += v.WindowSize(0, data.RowCount())
		schema[i] = *v.GetType()
	}
	need, err := IOEnvelope([][]types.Type{schema}, sinker.config.bufferBytes, payload)
	if err != nil {
		return err
	}
	if need > lease.Capacity() {
		return &rscthrottler.RequestTooLargeError{Requested: need, Maximum: lease.Capacity()}
	}
	ioMP := mpool.MustNew("sinker-borrowed-io")
	defer mpool.DeleteMPool(ioMP)
	copy, err := data.Clone(ioMP, sinker.config.offHeap)
	if err != nil {
		return err
	}
	defer copy.Clean(ioMP)
	if sinker.schema.sortKeyIdx >= 0 {
		if err = mergeutil.SortColumnsByIndex(copy.Vecs, sinker.schema.sortKeyIdx, ioMP); err != nil {
			return err
		}
	}
	f := sinker.fSinker.factory(ioMP, sinker.fs)
	defer f.Close()
	if fs, ok := f.(*FSinkerImpl); ok {
		fs.workspaceBytes = lease.Capacity()
		fs.arenaBytes = warmArenaBytes(int64(copy.Size()), int64(len(copy.Vecs)))
	}
	for start := 0; start < copy.RowCount(); {
		end := byteWindowEnd(copy, start, objectio.BlockMaxRows, 0, sinker.config.bufferBytes)
		window, windowErr := copy.Window(start, end)
		if windowErr != nil {
			return windowErr
		}
		err = f.Sink(ctx, window)
		window.Clean(ioMP)
		if err != nil {
			return err
		}
		start = end
	}
	stats, err := f.Sync(ctx)
	if err != nil {
		return err
	}
	sinker.staged.persisted = append(sinker.staged.persisted, *stats)
	return nil
}

func (sinker *Sinker) putBackBuffer(bat *batch.Batch) {
	x := sinker.buf.buffers.Len()
	sinker.buf.buffers.Putback(bat, sinker.mp)
	y := sinker.buf.buffers.Len()

	if x > y {
		sinker.buf.bufStats.updateCount(1)
		sinker.buf.bufStats.updateBytes(bat.Size())
	}
}

func (sinker *Sinker) putBackOneInMemory(idx int) {
	bat := sinker.staged.inMemory[idx]
	sinker.staged.inMemory[idx] = nil
	sinker.staged.inMemorySize -= bat.Size()
	sinker.staged.inMemStats.updateCount(-1)
	sinker.staged.inMemStats.updateBytes(-bat.Size())
	sinker.putBackBuffer(bat)
}

func (sinker *Sinker) popStaged() *batch.Batch {
	if len(sinker.staged.inMemory) == 0 {
		return nil
	}

	ret := sinker.staged.inMemory[len(sinker.staged.inMemory)-1]
	sinker.staged.inMemory = sinker.staged.inMemory[:len(sinker.staged.inMemory)-1]
	sinker.staged.inMemorySize -= ret.Size()

	sinker.staged.inMemStats.updateCount(-1)
	sinker.staged.inMemStats.updateBytes(-ret.Size())

	return ret
}

// pushStaged take the ownership of the bat
func (sinker *Sinker) pushStaged(
	ctx context.Context, bat *batch.Batch,
) error {

	sinker.staged.inMemStats.updateCount(1)
	sinker.staged.inMemStats.updateBytes(bat.Size())

	sinker.staged.inMemory = append(sinker.staged.inMemory, bat)
	sinker.staged.inMemorySize += bat.Size()
	if sinker.staged.inMemorySize >= sinker.staged.memorySizeThreshold {
		return sinker.trySpill(ctx)
	}
	return nil
}

func (sinker *Sinker) clearInMemoryStaged() {
	sinker.staged.inMemory = sinker.staged.inMemory[:0]
	sinker.staged.inMemorySize = 0
}

func (sinker *Sinker) cleanupInMemoryStaged() {
	for i, bat := range sinker.staged.inMemory {
		if bat != nil {
			sinker.putBackBuffer(bat)
			sinker.staged.inMemory[i] = nil
		}
	}
	sinker.staged.inMemory = sinker.staged.inMemory[:0]
	sinker.staged.inMemorySize = 0
}

func (sinker *Sinker) trySortInMemoryStaged(ctx context.Context) error {
	if sinker.schema.sortKeyIdx == -1 {
		return nil
	}
	var idxBuf []int64
	var shuffleBuf []byte
	for _, bat := range sinker.staged.inMemory {
		if err := mergeutil.SortColumnsByIndexWithBuf(
			bat.Vecs,
			sinker.schema.sortKeyIdx,
			sinker.mp,
			&idxBuf,
			&shuffleBuf,
		); err != nil {
			return err
		}
	}
	return nil
}

// pipelineSubmit submits sorted batches to the global sink pool for async
// serialization + IO. Transfers batch ownership to the pool workers.
//
// The pipelineResult context is derived from the first caller's ctx. This
// assumes all Write calls within a single Sinker lifecycle share the same
// parent context (e.g., ALTER TABLE's process context). If the parent
// context is cancelled, all in-flight pipeline operations will be aborted.
func (sinker *Sinker) pipelineSubmit(ctx context.Context, data []*batch.Batch) error {
	if sinker.pipe.result == nil {
		pctx, cancel := context.WithCancel(ctx)
		sinker.pipe.result = &pipelineResult{
			ctx:    pctx,
			cancel: cancel,
		}
	}
	waitStart := time.Now()
	var bitmapBytes int64
	for _, bat := range data {
		bitmapBytes += batchBitmapAllocation(bat)
	}
	sinker.pipelineBitmapBytes.Add(bitmapBytes)
	err := GetDefaultSinkPool().Submit(&poolSinkJob{
		data:      data,
		factory:   sinker.fSinker.factory,
		mp:        sinker.mp,
		fs:        sinker.fs,
		result:    sinker.pipe.result,
		workspace: sinker.workspace,
		sourceReleased: func() {
			sinker.pipelineBitmapBytes.Add(-bitmapBytes)
			if sinker.retainedRelease != nil {
				sinker.retainedRelease()
			}
		},
	})
	atomic.AddInt64(&sinker.timing.waitNs, int64(time.Since(waitStart)))
	return err
}

// drainPipeline waits for all in-flight pipeline jobs to complete and returns
// any accumulated error.
func (sinker *Sinker) drainPipeline() error {
	if sinker.pipe.result == nil {
		return nil
	}
	sinker.pipe.result.pending.Wait()
	return sinker.pipe.result.getError()
}

func (sinker *Sinker) trySpill(ctx context.Context) error {
	// Sorted merge output stays local while W runs. A pipeline output clone
	// must not outlive W without its own data credit; use streaming IO for this
	// bounded-workspace case. The no-sort path can transfer its existing D.
	if sinker.workspace != nil && sinker.schema.sortKeyIdx < 0 && sinker.pipe.enabled {
		jobData := sinker.TakeStagedBatches()
		return sinker.pipelineSubmit(ctx, jobData)
	}
	var workspace *rscthrottler.WorkspaceLease
	if sinker.workspace != nil {
		var err error
		workspace, err = sinker.workspace.AcquireIO(ctx, sinker.workspace.IOBytes())
		if err != nil {
			return err
		}
		sinker.ioMP = mpool.MustNew("sinker-spill-io")
		defer func() {
			if sinker.fSinker.executor != nil {
				_ = sinker.fSinker.executor.Close()
				sinker.fSinker.executor = nil
			}
			mpool.DeleteMPool(sinker.ioMP)
			sinker.ioMP = nil
			workspace.Release()
		}()
	}
	spillStart := time.Now()
	defer func() {
		atomic.AddInt64(&sinker.timing.spillCount, 1)
		atomic.AddInt64(&sinker.timing.spillNs, int64(time.Since(spillStart)))
	}()

	// sort all in memory data
	sortStart := time.Now()
	if err := sinker.trySortInMemoryStaged(ctx); err != nil {
		return err
	}

	defer sinker.cleanupInMemoryStaged()

	// merge sort
	if sinker.schema.sortKeyIdx != -1 {
		if sinker.config.dedupAll {
			fSinker := sinker.getStageFileSinker()
			if fs, ok := fSinker.(*FSinkerImpl); ok && workspace != nil {
				fs.workspaceBytes = workspace.Capacity()
			}
			err := sinker.trySpillMergeSortAccumulate(ctx, fSinker)
			if err != nil {
				sinker.resetFileSinker()
				return err
			}
			atomic.AddInt64(&sinker.timing.sortNs, int64(time.Since(sortStart)))
			err = sinker.syncFileSinker(ctx, fSinker)
			sinker.resetFileSinker()
			return err
		}

		if sinker.pipe.enabled && sinker.workspace == nil {
			err := sinker.trySpillMergeSortPipeline(ctx)
			atomic.AddInt64(&sinker.timing.sortNs, int64(time.Since(sortStart)))
			return err
		}

		fSinker := sinker.getStageFileSinker()
		if fs, ok := fSinker.(*FSinkerImpl); ok && workspace != nil {
			fs.workspaceBytes = workspace.Capacity()
		}
		err := sinker.trySpillMergeSortStreaming(ctx, fSinker)
		if err != nil {
			sinker.resetFileSinker()
			return err
		}
		atomic.AddInt64(&sinker.timing.sortNs, int64(time.Since(sortStart)))
		err = sinker.syncFileSinker(ctx, fSinker)
		sinker.resetFileSinker()
		return err
	}

	atomic.AddInt64(&sinker.timing.sortNs, int64(time.Since(sortStart)))

	// no sort key: pipeline path — steal in-memory staged batches
	if sinker.pipe.enabled {
		jobData := make([]*batch.Batch, len(sinker.staged.inMemory))
		copy(jobData, sinker.staged.inMemory)
		for i := range sinker.staged.inMemory {
			sinker.staged.inMemory[i] = nil
		}
		sinker.staged.inMemory = sinker.staged.inMemory[:0]
		sinker.staged.inMemorySize = 0
		return sinker.pipelineSubmit(ctx, jobData)
	}

	// no sort key: spill directly (sync path)
	fSinker := sinker.getStageFileSinker()
	if fs, ok := fSinker.(*FSinkerImpl); ok && workspace != nil {
		fs.workspaceBytes = workspace.Capacity()
	}
	sinkStart := time.Now()
	for _, bat := range sinker.staged.inMemory {
		if err := fSinker.Sink(ctx, bat); err != nil {
			sinker.resetFileSinker()
			return err
		}
	}
	atomic.AddInt64(&sinker.timing.sinkNs, int64(time.Since(sinkStart)))
	err := sinker.syncFileSinker(ctx, fSinker)
	sinker.resetFileSinker()
	return err
}

// syncFileSinker persists the file sinker's data. On error, the caller is
// responsible for cleanup via resetFileSinker(); the executor is eventually
// closed in Sinker.Close().
func (sinker *Sinker) syncFileSinker(ctx context.Context, fSinker FileSinker) error {
	syncStart := time.Now()
	stats, err := fSinker.Sync(ctx)
	atomic.AddInt64(&sinker.timing.syncNs, int64(time.Since(syncStart)))
	if err != nil {
		return err
	}
	sinker.staged.persisted = append(sinker.staged.persisted, *stats)
	return nil
}

// trySpillMergeSortStreaming merge-sorts staged data and streams each full
// block directly to the file sinker, reusing a single buffer. This avoids
// accumulating all sorted blocks in memory and eliminates PreExtend churn.
func (sinker *Sinker) trySpillMergeSortStreaming(
	ctx context.Context, fSinker FileSinker,
) error {
	mp := sinker.mp
	var buffer *batch.Batch
	var err error
	if sinker.workspace != nil {
		mp = sinker.ioMP
		buffer = batch.NewWithSchema(sinker.config.offHeap, sinker.schema.attrs, sinker.schema.attrTypes)
		ownedBuffer := buffer
		defer ownedBuffer.Clean(mp)
	} else {
		buffer, err = sinker.fetchBuffer()
		if err != nil {
			return err
		}
	}
	streamSinker := func(data *batch.Batch) (*batch.Batch, error) {
		sinkStart := time.Now()
		if err := fSinker.Sink(ctx, data); err != nil {
			return data, err
		}
		atomic.AddInt64(&sinker.timing.sinkNs, int64(time.Since(sinkStart)))
		data.CleanOnlyData()
		return data, nil
	}
	buffer, err = mergeutil.MergeSortBatches(
		sinker.staged.inMemory,
		sinker.schema.sortKeyIdx,
		buffer,
		streamSinker,
		mp,
		sinker.putBackOneInMemory,
		sinker.config.bufferBytes,
	)
	if sinker.workspace == nil {
		sinker.putBackBuffer(buffer)
	}
	return err
}

// trySpillMergeSortAccumulate merge-sorts staged data, accumulates all sorted
// blocks, runs dedup, then writes to the file sinker.
func (sinker *Sinker) trySpillMergeSortAccumulate(
	ctx context.Context, fSinker FileSinker,
) error {
	mp := sinker.mp
	getBuffer := sinker.fetchBuffer
	returnBuffer := sinker.putBackBuffer
	if sinker.workspace != nil {
		mp = sinker.ioMP
		var owned []*batch.Batch
		getBuffer = func() (*batch.Batch, error) {
			b := batch.NewWithSchema(sinker.config.offHeap, sinker.schema.attrs, sinker.schema.attrTypes)
			owned = append(owned, b)
			return b, nil
		}
		returnBuffer = func(*batch.Batch) {}
		defer func() {
			for _, b := range owned {
				b.Clean(mp)
			}
		}()
	}

	var sorted []*batch.Batch
	innersinker := func(data *batch.Batch) (*batch.Batch, error) {
		sorted = append(sorted, data)
		return getBuffer()
	}
	defer func() {
		for i, bat := range sorted {
			returnBuffer(bat)
			sorted[i] = nil
		}
		sorted = sorted[:0]
	}()

	buffer, err := getBuffer()
	if err != nil {
		return err
	}
	buffer, err = mergeutil.MergeSortBatches(
		sinker.staged.inMemory,
		sinker.schema.sortKeyIdx,
		buffer,
		innersinker,
		mp,
		sinker.putBackOneInMemory,
		sinker.config.bufferBytes,
	)
	returnBuffer(buffer)
	if err != nil {
		return err
	}

	// dedup
	if err := containers.DedupSortedBatches(
		sinker.schema.sortKeyIdx,
		sorted,
	); err != nil {
		return err
	}

	sinkStart := time.Now()
	for _, bat := range sorted {
		if err := fSinker.Sink(ctx, bat); err != nil {
			return err
		}
	}
	atomic.AddInt64(&sinker.timing.sinkNs, int64(time.Since(sinkStart)))
	return nil
}

// trySpillMergeSortPipeline merge-sorts staged data and submits sorted blocks
// to the pipeline in bounded groups. Grouping keeps the pipeline async path from
// turning every BlockMaxRows output block into its own tiny persisted object.
func (sinker *Sinker) trySpillMergeSortPipeline(ctx context.Context) (err error) {
	submitThreshold := sinker.staged.memorySizeThreshold
	if submitThreshold <= 0 {
		submitThreshold = DefaultInMemoryStagedSize
	}
	if submitThreshold > pipelineSortKeySubmitThreshold {
		submitThreshold = pipelineSortKeySubmitThreshold
	}

	var (
		pending     []*batch.Batch
		pendingSize int
	)
	cleanupPending := func() {
		for _, bat := range pending {
			if bat != nil {
				sinker.putBackBuffer(bat)
			}
		}
		pending = nil
		pendingSize = 0
	}
	defer func() {
		if err != nil {
			cleanupPending()
		}
	}()

	submitPending := func() error {
		if len(pending) == 0 {
			return nil
		}
		jobData := pending
		pending = nil
		pendingSize = 0
		return sinker.pipelineSubmit(ctx, jobData)
	}

	streamSubmit := func(data *batch.Batch) (*batch.Batch, error) {
		pending = append(pending, data)
		pendingSize += data.Size()
		if pendingSize >= submitThreshold {
			if err := submitPending(); err != nil {
				return nil, err
			}
		}
		return sinker.fetchBuffer()
	}

	buffer, err := sinker.fetchBuffer()
	if err != nil {
		return err
	}
	buffer, err = mergeutil.MergeSortBatches(
		sinker.staged.inMemory,
		sinker.schema.sortKeyIdx,
		buffer,
		streamSubmit,
		sinker.mp,
		sinker.putBackOneInMemory,
		sinker.config.bufferBytes,
	)
	if buffer != nil {
		sinker.putBackBuffer(buffer)
	}
	if err != nil {
		return err
	}
	if err := submitPending(); err != nil {
		return err
	}
	return nil
}

func (sinker *Sinker) resetFileSinker() {
	if sinker.fSinker.executor == nil {
		return
	}
	if sinker.workspace != nil {
		_ = sinker.fSinker.executor.Close()
		sinker.fSinker.executor = nil
		return
	}
	sinker.fSinker.executor.Reset()
}
func (sinker *Sinker) getStageFileSinker() FileSinker {
	if sinker.fSinker.executor == nil {
		mp := sinker.mp
		if sinker.ioMP != nil {
			mp = sinker.ioMP
		}
		sinker.fSinker.executor = sinker.fSinker.factory(mp, sinker.fs)
		if fs, ok := sinker.fSinker.executor.(*FSinkerImpl); ok && sinker.ioMP != nil {
			fs.arenaBytes = warmArenaBytes(int64(sinker.staged.inMemorySize), int64(len(sinker.schema.attrTypes)))
		}
	}
	return sinker.fSinker.executor
}

func (sinker *Sinker) GetInMemoryData() []*batch.Batch {
	return sinker.staged.inMemory
}

func (sinker *Sinker) GetMPool() *mpool.MPool {
	return sinker.mp
}

// Write always copy the data
func (sinker *Sinker) Write(
	ctx context.Context,
	data *batch.Batch,
) (err error) {
	var curr *batch.Batch
	defer func() {
		if err != nil && curr != nil {
			sinker.putBackBuffer(curr)
		}
	}()
	offset := 0
	left := data.RowCount()
	for left > 0 {
		if curr == nil {
			curr = sinker.popStaged()
			if curr == nil {
				if curr, err = sinker.fetchBuffer(); err != nil {
					return
				}
			} else if curr.RowCount() == objectio.BlockMaxRows {
				if err = sinker.pushStaged(ctx, curr); err != nil {
					return
				}
				if curr, err = sinker.fetchBuffer(); err != nil {
					return
				}
			}
		}

		toAdd := left
		currPos := curr.RowCount()
		if currPos+toAdd > objectio.BlockMaxRows {
			toAdd = objectio.BlockMaxRows - currPos
		}
		var firstRowBytes int64
		for _, v := range data.Vecs {
			firstRowBytes += v.WindowSize(offset, offset+1)
		}
		if currPos > 0 && int64(curr.Size())+firstRowBytes > sinker.config.bufferBytes {
			if err = sinker.pushStaged(ctx, curr); err != nil {
				return
			}
			// This tail is full by bytes even though it has fewer than 8192
			// rows. Fetch a replacement now; popping the same tail on the next
			// iteration would repeat this branch forever.
			if curr, err = sinker.fetchBuffer(); err != nil {
				return
			}
			toAdd = min(left, objectio.BlockMaxRows)
		}
		end := byteWindowEnd(data, offset, toAdd, int64(curr.Size()), sinker.config.bufferBytes)
		toAdd = end - offset
		for col, vec := range curr.Vecs {
			src := data.Vecs[col]
			area := src.UnionBatchAreaBytes(offset, end)
			if err = vec.PreExtendWithArea(toAdd, int(area), sinker.mp); err != nil {
				return
			}
		}
		if err = curr.UnionWindow(data, offset, toAdd, sinker.mp); err != nil {
			return
		}
		if curr.RowCount() == objectio.BlockMaxRows || int64(curr.Size()) >= sinker.config.bufferBytes {
			if err = sinker.pushStaged(ctx, curr); err != nil {
				return
			}
			curr = nil
		}
		left -= toAdd
		offset += toAdd
	}
	if curr != nil && curr.RowCount() > 0 {
		if err = sinker.pushStaged(ctx, curr); err != nil {
			return
		}
	}
	return
}

// WriteOwned stages a full-block batch directly without copying. The caller
// transfers ownership: the sinker will clean the batch later.
// If the batch is not exactly BlockMaxRows or there is a partially-filled
// staged buffer, falls back to Write (which copies).
func (sinker *Sinker) WriteOwned(
	ctx context.Context,
	data *batch.Batch,
) (owned bool, err error) {
	if data.RowCount() != objectio.BlockMaxRows {
		return false, sinker.Write(ctx, data)
	}
	// Check if the last staged batch is partial (not full).
	last := sinker.popStaged()
	if last != nil {
		if err = sinker.pushStaged(ctx, last); err != nil {
			return false, err
		}
		if last.RowCount() < objectio.BlockMaxRows {
			// Partial staged buffer exists; must copy to fill it.
			return false, sinker.Write(ctx, data)
		}
	}
	// No partial buffer; stage the batch directly.
	return true, sinker.pushStaged(ctx, data)
}

func (sinker *Sinker) Sync(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	default:
	}
	if sinker.pipe.enabled && sinker.pipe.result != nil {
		if err := sinker.pipe.result.getError(); err != nil {
			return err
		}
	}
	if len(sinker.staged.persisted) == 0 && len(sinker.staged.inMemory) == 0 &&
		(!sinker.pipe.enabled || sinker.pipe.result == nil) {
		return nil
	}
	// spill the remaining data
	if sinker.staged.inMemorySize > 0 &&
		sinker.staged.inMemorySize >= sinker.config.tailSizeCap {
		if err := sinker.trySpill(ctx); err != nil {
			return err
		}
	} else {
		if err := sinker.trySortInMemoryStaged(ctx); err != nil {
			return err
		}
		if sinker.config.dedupAll {
			if err := containers.DedupSortedBatches(
				sinker.schema.sortKeyIdx,
				sinker.staged.inMemory,
			); err != nil {
				return err
			}
		}
		sinker.result.tail = sinker.staged.inMemory
		sinker.clearInMemoryStaged()
	}

	// drain pipeline and collect results
	if sinker.pipe.enabled && sinker.pipe.result != nil {
		if err := sinker.drainPipeline(); err != nil {
			return err
		}
		sinker.pipe.result.mu.Lock()
		sinker.staged.persisted = append(sinker.staged.persisted, sinker.pipe.result.persisted...)
		sinker.pipe.result.persisted = nil
		sinker.pipe.result.mu.Unlock()
	}

	defer func() {
		sinker.staged.persisted = sinker.staged.persisted[:0]
	}()

	spillCount := atomic.LoadInt64(&sinker.timing.spillCount)
	if spillCount > 0 {
		var poolSinkNs, poolSyncNs int64
		if sinker.pipe.result != nil {
			poolSinkNs = atomic.LoadInt64(&sinker.pipe.result.sinkNs)
			poolSyncNs = atomic.LoadInt64(&sinker.pipe.result.syncNs)
		}
		logutil.Debug("Sinker flush stats",
			zap.Bool("pipeline", sinker.pipe.enabled),
			zap.Int64("spills", spillCount),
			zap.Duration("sortTime", time.Duration(atomic.LoadInt64(&sinker.timing.sortNs))),
			zap.Duration("serializeTime", time.Duration(atomic.LoadInt64(&sinker.timing.sinkNs)+poolSinkNs)),
			zap.Duration("ioTime", time.Duration(atomic.LoadInt64(&sinker.timing.syncNs)+poolSyncNs)),
			zap.Duration("submitWaitTime", time.Duration(atomic.LoadInt64(&sinker.timing.waitNs))),
			zap.Duration("totalSpillTime", time.Duration(atomic.LoadInt64(&sinker.timing.spillNs))),
			zap.Int("objects", len(sinker.staged.persisted)),
		)
	}

	// if there is only one file, it is sorted an deduped
	if len(sinker.staged.persisted) == 1 {
		sinker.result.persisted = append(sinker.result.persisted, sinker.staged.persisted[0])
		return nil
	}

	if !sinker.config.allMergeSorted && !sinker.config.dedupAll {
		sinker.result.persisted = append(sinker.result.persisted, sinker.staged.persisted...)
		return nil
	}
	panic("not implemented")
}

func (sinker *Sinker) GetInMemoryThreshold() int {
	return sinker.staged.memorySizeThreshold
}

// SyncAndTakeResults calls Sync, then extracts and returns the accumulated
// results, resetting the sinker so it can accept more Write calls.
func (sinker *Sinker) SyncAndTakeResults(ctx context.Context) ([]objectio.ObjectStats, []*batch.Batch, error) {
	if err := sinker.Sync(ctx); err != nil {
		return nil, nil, err
	}
	persisted := sinker.result.persisted
	tail := sinker.result.tail
	sinker.result.persisted = nil
	sinker.result.tail = nil
	return persisted, tail, nil
}

// TakeStagedBatches extracts the un-synced in-memory staged batches,
// transferring ownership to the caller. The sinker's staged area is cleared.
func (sinker *Sinker) TakeStagedBatches() []*batch.Batch {
	bats := sinker.staged.inMemory
	sinker.staged.inMemory = nil
	sinker.staged.inMemorySize = 0
	return bats
}

// StagedSize returns the current size of in-memory staged data.
func (sinker *Sinker) StagedSize() int {
	return sinker.staged.inMemorySize
}

func (sinker *Sinker) Close() error {
	var drainErr error
	if sinker.pipe.enabled && sinker.pipe.result != nil {
		drainErr = sinker.drainPipeline()
	}
	sinker.cleanupInMemoryStaged()
	if sinker.buf.buffers != nil {
		if sinker.buf.isOwner {
			// it's not safe to free a shared buffer
			sinker.buf.buffers.Close(sinker.mp)
		}

		sinker.buf.buffers = nil
	}
	for i := range sinker.result.tail {
		if sinker.result.tail[i] != nil {
			sinker.result.tail[i].Clean(sinker.mp)
			sinker.result.tail[i] = nil
		}
	}
	sinker.result.tail = nil
	sinker.staged.persisted = nil
	if sinker.fSinker.executor != nil {
		sinker.fSinker.executor.Close()
		sinker.fSinker.executor = nil
	}
	sinker.fSinker.factory = nil
	sinker.mp = nil
	sinker.fs = nil
	return drainErr
}

// Reset discards all staged and result data without tearing down the sinker.
// It is safe to call after an aborted pipeline execution so that the next
// Write/Sync cycle starts from a clean state. The buf pool, file-sinker
// executor (and its arena), mpool, and fileservice references are all kept
// alive so they can be reused without reallocation.
func (sinker *Sinker) Reset() {
	// Drain any in-flight pipeline jobs before clearing state, so pool
	// workers don't race with the cleanup below. Clearing pipe.result
	// ensures the next Write/Sync cycle gets a fresh pipelineResult and
	// doesn't inherit stale errors or persisted stats.
	if sinker.pipe.result != nil {
		sinker.drainPipeline()
		sinker.pipe.result = nil
	}

	sinker.cleanupInMemoryStaged()
	sinker.staged.persisted = sinker.staged.persisted[:0]

	for i := range sinker.result.tail {
		if sinker.result.tail[i] != nil {
			sinker.result.tail[i].Clean(sinker.mp)
			sinker.result.tail[i] = nil
		}
	}
	sinker.result.tail = sinker.result.tail[:0]
	sinker.result.persisted = sinker.result.persisted[:0]

	if sinker.fSinker.executor != nil {
		sinker.fSinker.executor.Reset()
	}
}
