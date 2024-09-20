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

func WithBuffer(buffer *containers.OneSchemaBatchBuffer) SinkerOption {
	return func(sinker *Sinker) {
		sinker.buffers = buffer
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
}

func (s *FSinkerImpl) Sink(ctx context.Context, b *batch.Batch) error {
	if s.writer == nil {
		s.writer = mergesort.GetNewWriter(
			s.fs,
			s.schemaVersion,
			s.seqnums,
			s.sortKeyPos,
			s.isPrimaryKey,
			s.isTombstone)
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
		ss = s.writer.GetObjectStats(objectio.WithSorted())
	} else {
		ss = s.writer.GetObjectStats()
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

type FileSinkerFactory func(*mpool.MPool, fileservice.FileService) FileSinker

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
		inMemory            []*batch.Batch
		persisted           []objectio.ObjectStats
		inMemorySize        int
		memorySizeThreshold int
	}
	result struct {
		persisted []objectio.ObjectStats
		tail      *batch.Batch
	}
	buffers *containers.OneSchemaBatchBuffer
	mp      *mpool.MPool
	fs      fileservice.FileService
}

func (sinker *Sinker) fillDefaults() {
	if sinker.staged.memorySizeThreshold == 0 {
		sinker.staged.memorySizeThreshold = DefaultInMemoryStagedSize
	}
	if sinker.buffers == nil {
		sinker.buffers = containers.NewOneSchemaBatchBuffer(
			sinker.config.bufferSizeCap,
			sinker.schema.attrs,
			sinker.schema.attrTypes,
		)
	}
}

func (sinker *Sinker) fetchBuffer() *batch.Batch {
	return sinker.buffers.Fetch()
}

func (sinker *Sinker) putbackBuffer(bat *batch.Batch) {
	sinker.buffers.Putback(bat, sinker.mp)
}

// stageData take the ownership of the bat
func (sinker *Sinker) stageData(
	ctx context.Context, bat *batch.Batch,
) error {
	sinker.staged.inMemory = append(sinker.staged.inMemory, bat)
	sinker.staged.inMemorySize += bat.Size()
	if sinker.staged.inMemorySize >= sinker.staged.memorySizeThreshold {
		return sinker.trySpill(ctx)
	}
	return nil
}

func (sinker *Sinker) cleanupInMemoryStaged() {
	for i, bat := range sinker.staged.inMemory {
		sinker.putbackBuffer(bat)
		sinker.staged.inMemory[i] = nil
	}
	sinker.staged.inMemory = sinker.staged.inMemory[:0]
	sinker.staged.inMemorySize = 0
}

func (sinker *Sinker) trySpill(ctx context.Context) error {
	defer sinker.cleanupInMemoryStaged()
	var sorted []*batch.Batch
	innersinker := func(data *batch.Batch) error {
		oneSorted := sinker.fetchBuffer()
		_, err := oneSorted.AppendWithCopy(ctx, sinker.mp, data)
		if err != nil {
			sinker.putbackBuffer(oneSorted)
			return err
		}
		sorted = append(sorted, oneSorted)
		return nil
	}

	defer func() {
		for i, bat := range sorted {
			sinker.putbackBuffer(bat)
			sorted[i] = nil
		}
		sorted = sorted[:0]
	}()

	data := sinker.staged.inMemory

	// 1. merge sort
	if sinker.schema.sortKeyIdx != -1 {
		buffer := sinker.fetchBuffer() // note the lifecycle of buffer
		defer sinker.putbackBuffer(buffer)
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
) error {
	buffer := sinker.fetchBuffer()
	_, err := buffer.AppendWithCopy(ctx, sinker.mp, data)
	if err != nil {
		sinker.putbackBuffer(buffer)
		return err
	}
	if sinker.schema.sortKeyIdx != -1 {
		if err := mergesort.SortColumnsByIndex(
			buffer.Vecs,
			sinker.schema.sortKeyIdx,
			sinker.mp,
		); err != nil {
			sinker.putbackBuffer(buffer)
			return err
		}
	}
	return sinker.stageData(ctx, buffer)
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
		if len(sinker.staged.inMemory) == 1 {
			sinker.result.tail = sinker.staged.inMemory[0]
		} else {
			tail := sinker.fetchBuffer()
			for _, bat := range sinker.staged.inMemory {
				if _, err := tail.AppendWithCopy(ctx, sinker.mp, bat); err != nil {
					sinker.putbackBuffer(tail)
					return err
				}
			}
			sinker.result.tail = tail
			for i, bat := range sinker.staged.inMemory {
				sinker.putbackBuffer(bat)
				sinker.staged.inMemory[i] = nil
			}
		}
		sinker.staged.inMemory = nil
		sinker.staged.inMemorySize = 0
	}
	// if there is only one file, it is sorted an deduped
	if len(sinker.staged.persisted) == 1 {
		sinker.result.persisted = append(sinker.result.persisted, sinker.staged.persisted[0])
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
	return nil
}

func (sinker *Sinker) Close() error {
	sinker.cleanupInMemoryStaged()
	if sinker.buffers != nil {
		sinker.buffers.Close(sinker.mp)
		sinker.buffers = nil
	}
	if sinker.result.tail != nil {
		sinker.result.tail.Clean(sinker.mp)
		sinker.result.tail = nil
	}
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
