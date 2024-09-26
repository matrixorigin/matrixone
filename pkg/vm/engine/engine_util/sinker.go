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

package engine_util

import (
	"context"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/mergesort"
)

const DefaultInMemoryStagedSize = mpool.MB * 16

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

func WithBufferSizeCap(size int) SinkerOption {
	return func(sinker *Sinker) {
		sinker.config.bufferSizeCap = size
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

func WithBuffer(buffer *containers.OneSchemaBatchBuffer, isOwner bool) SinkerOption {
	return func(sinker *Sinker) {
		sinker.buf.isOwner = isOwner
		sinker.buf.buffers = buffer
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
	writer *blockio.BlockWriter
	mp     *mpool.MPool
	fs     fileservice.FileService

	sortKeyPos    int
	isPrimaryKey  bool
	isTombstone   bool
	seqnums       []uint16
	schemaVersion uint32
	withHidden    bool
}

func (s *FSinkerImpl) Sink(ctx context.Context, b *batch.Batch) error {
	if s.writer == nil {
		if s.isTombstone {
			s.writer = blockio.ConstructTombstoneWriter(
				s.withHidden,
				s.fs,
			)
		} else {
			s.writer = blockio.ConstructWriter(
				s.schemaVersion,
				s.seqnums,
				s.sortKeyPos,
				s.isPrimaryKey,
				s.isTombstone,
				s.fs,
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
	// s.writer.Reset
	s.writer = nil
	return nil
}

// FileSinkerFactory is a factory function to create a FileSinker
type FileSinkerFactory func(*mpool.MPool, fileservice.FileService) FileSinker

// This factory is used to create a FileSinker for tombstone
// withHidden: whether to write hidden tombstone
//
//	true: TN created tombstone objects
//	false: CN created tombstone objects
func NewTombstoneFSinkerImplFactory(withHidden bool) FileSinkerFactory {
	return func(mp *mpool.MPool, fs fileservice.FileService) FileSinker {
		return NewTombstoneFSinkerImpl(withHidden, mp, fs)
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
	withHidden bool,
	mp *mpool.MPool,
	fs fileservice.FileService,
) *FSinkerImpl {
	return &FSinkerImpl{
		fs:          fs,
		mp:          mp,
		isTombstone: true,
		withHidden:  withHidden,
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
	withHidden bool,
	pkType types.Type,
	mp *mpool.MPool,
	fs fileservice.FileService,
	opts ...SinkerOption,
) *Sinker {
	factory := NewTombstoneFSinkerImplFactory(withHidden)
	attrs, attrTypes := objectio.GetTombstoneSchema(pkType, withHidden)
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

type stats struct {
	Name               string
	HighWatermarkCnt   uint64
	HighWatermarkBytes uint64
	CurrentCnt         uint64
	CurrentBytes       uint64
}

func (s *stats) String() string {
	return fmt.Sprintf("%s, high cnt: %d, current cnt: %d, hight bytes: %d, current bytes: %d",
		s.Name, s.HighWatermarkCnt, s.CurrentCnt, s.HighWatermarkBytes, s.CurrentBytes)
}

func (s *stats) updateCount(n int) {
	if n > 0 {
		s.CurrentCnt += uint64(n)
	} else if n < 0 {
		s.CurrentCnt -= uint64(-n)
	}

	if s.CurrentCnt > s.HighWatermarkCnt {
		s.HighWatermarkCnt = s.CurrentCnt
	}
}

func (s *stats) updateBytes(n int) {
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
		bufferSizeCap  int
		tailSizeCap    int
	}
	fSinker struct {
		executor FileSinker
		factory  FileSinkerFactory
	}
	staged struct {
		inMemStats          stats
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
		bufStats stats
		buffers  *containers.OneSchemaBatchBuffer
	}

	mp *mpool.MPool
	fs fileservice.FileService
}

func (sinker *Sinker) fillDefaults() {
	if sinker.staged.memorySizeThreshold == 0 {
		sinker.staged.memorySizeThreshold = DefaultInMemoryStagedSize
	}

	sinker.staged.inMemStats.Name = "staged inmem stats"
	sinker.buf.bufStats.Name = "buffer stats"

	if sinker.buf.buffers == nil {
		sinker.buf.isOwner = true
		sinker.buf.buffers = containers.NewOneSchemaBatchBuffer(
			sinker.config.bufferSizeCap,
			sinker.schema.attrs,
			sinker.schema.attrTypes,
		)
	}
}

func (sinker *Sinker) GetResult() ([]objectio.ObjectStats, []*batch.Batch) {
	return sinker.result.persisted, sinker.result.tail
}

func (sinker *Sinker) fetchBuffer() *batch.Batch {
	x := sinker.buf.buffers.Len()
	bat := sinker.buf.buffers.Fetch()
	y := sinker.buf.buffers.Len()

	if x < y {
		sinker.buf.bufStats.updateCount(-1)
		sinker.buf.bufStats.updateBytes(-bat.Size())
	}

	return bat
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
		sinker.putBackBuffer(bat)
		sinker.staged.inMemory[i] = nil
	}
	sinker.staged.inMemory = sinker.staged.inMemory[:0]
	sinker.staged.inMemorySize = 0
}

func (sinker *Sinker) trySortInMemoryStaged(ctx context.Context) error {
	if sinker.schema.sortKeyIdx == -1 {
		return nil
	}
	for _, bat := range sinker.staged.inMemory {
		if err := mergesort.SortColumnsByIndex(
			bat.Vecs,
			sinker.schema.sortKeyIdx,
			sinker.mp,
		); err != nil {
			return err
		}
	}
	return nil
}

func (sinker *Sinker) trySpill(ctx context.Context) error {
	// sort all in memory data
	if err := sinker.trySortInMemoryStaged(ctx); err != nil {
		return err
	}

	defer sinker.cleanupInMemoryStaged()
	var sorted []*batch.Batch
	innersinker := func(data *batch.Batch) error {
		oneSorted := sinker.fetchBuffer()
		_, err := oneSorted.AppendWithCopy(ctx, sinker.mp, data)
		if err != nil {
			sinker.putBackBuffer(oneSorted)
			return err
		}
		sorted = append(sorted, oneSorted)
		return nil
	}

	defer func() {
		for i, bat := range sorted {
			sinker.putBackBuffer(bat)
			sorted[i] = nil
		}
		sorted = sorted[:0]
	}()

	data := sinker.staged.inMemory

	// 1. merge sort
	if sinker.schema.sortKeyIdx != -1 {
		buffer := sinker.fetchBuffer() // note the lifecycle of buffer
		defer sinker.putBackBuffer(buffer)
		if err := colexec.MergeSortBatches(
			sinker.staged.inMemory,
			sinker.schema.sortKeyIdx,
			buffer,
			innersinker,
			sinker.mp,
		); err != nil {
			return err
		}
		data = sorted
	}

	// 3. dedup
	if sinker.config.dedupAll {
		if err := containers.DedupSortedBatches(
			sinker.schema.sortKeyIdx,
			data,
		); err != nil {
			return err
		}
	}

	// 4. spill
	fSinker := sinker.getStageFileSinker()
	defer sinker.resetFileSinker()
	for _, bat := range data {
		if err := fSinker.Sink(ctx, bat); err != nil {
			return err
		}
	}
	stats, err := fSinker.Sync(ctx)
	if err != nil {
		return err
	}

	sinker.staged.persisted = append(sinker.staged.persisted, *stats)
	return nil
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
				curr = sinker.fetchBuffer()
			} else if curr.RowCount() == objectio.BlockMaxRows {
				if err = sinker.pushStaged(ctx, curr); err != nil {
					return
				}
				curr = sinker.fetchBuffer()
			}
		}

		toAdd := left
		currPos := curr.RowCount()
		if currPos+toAdd > objectio.BlockMaxRows {
			toAdd = objectio.BlockMaxRows - currPos
		}
		if err = curr.Union(data, offset, toAdd, sinker.mp); err != nil {
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

func (sinker *Sinker) Sync(ctx context.Context) error {
	if len(sinker.staged.persisted) == 0 && len(sinker.staged.inMemory) == 0 {
		return nil
	}
	// spill the remaining data
	if sinker.staged.inMemorySize >= sinker.config.tailSizeCap {
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

	defer func() {
		sinker.staged.persisted = sinker.staged.persisted[:0]
	}()

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
	// TODO: merge the files and dedup
	// newPersied, err := MergeSortedFilesAndDedup(sinker.staged.persisted)
	// if err != nil {
	// 	return err
	// }
	// sinker.results = append(sinker.results, newPersied...)
	//return nil
}

func (sinker *Sinker) Close() error {
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
