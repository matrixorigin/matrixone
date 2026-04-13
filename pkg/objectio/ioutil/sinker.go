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

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/mergeutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
)

const DefaultInMemoryStagedSize = mpool.MB * 16

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
}

func (s *FSinkerImpl) Sink(ctx context.Context, b *batch.Batch) error {
	if s.writer == nil {
		if s.arena == nil {
			s.arena = objectio.GetArena(objectio.ArenaLarge)
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
	if s.writer != nil {
		// s.writer.Reset
		s.writer = nil
	}
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
		// idle for arenaDrainDelay the pools will drain automatically.
		objectio.ScheduleArenaDrain()
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
	schema struct {
		attrs      []string
		attrTypes  []types.Type
		sortKeyIdx int
	}
	config struct {
		allMergeSorted bool
		dedupAll       bool
		tailSizeCap    int
		offHeap        bool
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

	mp *mpool.MPool
	fs fileservice.FileService
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

	if err := bat.PreExtend(sinker.mp, objectio.BlockMaxRows); err != nil {
		return nil, err
	}
	return bat, nil
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
func (sinker *Sinker) pipelineSubmit(ctx context.Context, data []*batch.Batch) error {
	if sinker.pipe.result == nil {
		pctx, cancel := context.WithCancel(ctx)
		sinker.pipe.result = &pipelineResult{
			ctx:    pctx,
			cancel: cancel,
		}
	}
	waitStart := time.Now()
	err := GetDefaultSinkPool().Submit(&poolSinkJob{
		data:    data,
		factory: sinker.fSinker.factory,
		mp:      sinker.mp,
		fs:      sinker.fs,
		result:  sinker.pipe.result,
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

		if sinker.pipe.enabled {
			err := sinker.trySpillMergeSortPipeline(ctx)
			atomic.AddInt64(&sinker.timing.sortNs, int64(time.Since(sortStart)))
			return err
		}

		fSinker := sinker.getStageFileSinker()
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
	for _, bat := range sinker.staged.inMemory {
		if err := fSinker.Sink(ctx, bat); err != nil {
			sinker.resetFileSinker()
			return err
		}
	}
	err := sinker.syncFileSinker(ctx, fSinker)
	sinker.resetFileSinker()
	return err
}

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
	buffer, err := sinker.fetchBuffer()
	if err != nil {
		return err
	}
	streamSinker := func(data *batch.Batch) (*batch.Batch, error) {
		if err := fSinker.Sink(ctx, data); err != nil {
			return nil, err
		}
		data.CleanOnlyData()
		return data, nil
	}
	buffer, err = mergeutil.MergeSortBatches(
		sinker.staged.inMemory,
		sinker.schema.sortKeyIdx,
		buffer,
		streamSinker,
		sinker.mp,
		sinker.putBackOneInMemory,
	)
	sinker.putBackBuffer(buffer)
	return err
}

// trySpillMergeSortAccumulate merge-sorts staged data, accumulates all sorted
// blocks, runs dedup, then writes to the file sinker.
func (sinker *Sinker) trySpillMergeSortAccumulate(
	ctx context.Context, fSinker FileSinker,
) error {
	var sorted []*batch.Batch
	innersinker := func(data *batch.Batch) (*batch.Batch, error) {
		sorted = append(sorted, data)
		return sinker.fetchBuffer()
	}
	defer func() {
		for i, bat := range sorted {
			sinker.putBackBuffer(bat)
			sorted[i] = nil
		}
		sorted = sorted[:0]
	}()

	buffer, err := sinker.fetchBuffer()
	if err != nil {
		return err
	}
	buffer, err = mergeutil.MergeSortBatches(
		sinker.staged.inMemory,
		sinker.schema.sortKeyIdx,
		buffer,
		innersinker,
		sinker.mp,
		sinker.putBackOneInMemory,
	)
	sinker.putBackBuffer(buffer)
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

	for _, bat := range sorted {
		if err := fSinker.Sink(ctx, bat); err != nil {
			return err
		}
	}
	return nil
}

// trySpillMergeSortPipeline merge-sorts staged data into accumulated batches,
// then submits them to the pipeline for async serialization + IO.
func (sinker *Sinker) trySpillMergeSortPipeline(ctx context.Context) error {
	var sorted []*batch.Batch
	innersinker := func(data *batch.Batch) (*batch.Batch, error) {
		sorted = append(sorted, data)
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
		innersinker,
		sinker.mp,
		sinker.putBackOneInMemory,
	)
	sinker.putBackBuffer(buffer)
	if err != nil {
		for _, bat := range sorted {
			sinker.putBackBuffer(bat)
		}
		return err
	}

	// submit accumulated sorted batches to pipeline (transfers ownership)
	return sinker.pipelineSubmit(ctx, sorted)
}

func (sinker *Sinker) resetFileSinker() {
	sinker.fSinker.executor.Reset()
}
func (sinker *Sinker) getStageFileSinker() FileSinker {
	if sinker.fSinker.executor == nil {
		sinker.fSinker.executor = sinker.fSinker.factory(sinker.mp, sinker.fs)
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
		if err = curr.UnionWindow(data, offset, toAdd, sinker.mp); err != nil {
			return
		}
		if curr.RowCount() == objectio.BlockMaxRows {
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
		logutil.Info("Sinker flush stats",
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
	if sinker.pipe.enabled && sinker.pipe.result != nil {
		sinker.drainPipeline()
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
	return nil
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
