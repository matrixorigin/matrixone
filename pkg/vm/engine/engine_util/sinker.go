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

var _ FileSinker = new(SinkerImpl)

type SinkerImpl struct {
	writer *blockio.BlockWriter
	mp     *mpool.MPool
	fs     fileservice.FileService

	sortKeyPos    int
	isPrimaryKey  bool
	isTombstone   bool
	seqnums       []uint16
	schemaVersion uint32
}

func (s *SinkerImpl) Sink(ctx context.Context, b *batch.Batch) error {
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

func (s *SinkerImpl) Sync(ctx context.Context) (*objectio.ObjectStats, error) {
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

func (s *SinkerImpl) Reset() {
	if s.writer != nil {
		// s.writer.Reset
		s.writer = nil
	}
}

func (s *SinkerImpl) Close() error {
	// s.writer.Reset
	s.writer = nil
	return nil
}

func NewSinkerImpl(
	fs fileservice.FileService,
	mp *mpool.MPool,
	seqnums []uint16,
	sortKeyPos int,
	isPrimaryKey bool,
	isTombstone bool,
	schemaVersion uint32) *SinkerImpl {
	return &SinkerImpl{
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
	seqnums []uint16,
	isPrimaryKey bool,
	isTombstone bool,
	schemaVersion uint32,
	mp *mpool.MPool,
	fs fileservice.FileService,
	opts ...SinkerOption,
) *Sinker {
	sinker := &Sinker{
		schema: struct {
			attrs         []string
			attrTypes     []types.Type
			seqnums       []uint16
			sortKeyIdx    int
			schemaVersion uint32
			isPrimaryKey  bool
			isTombstone   bool
		}{
			attrs:         attrs,
			attrTypes:     attrTypes,
			seqnums:       seqnums,
			sortKeyIdx:    sortKeyIdx,
			schemaVersion: schemaVersion,
			isPrimaryKey:  isPrimaryKey,
			isTombstone:   isTombstone,
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
		attrs         []string
		attrTypes     []types.Type
		seqnums       []uint16
		sortKeyIdx    int
		schemaVersion uint32
		isPrimaryKey  bool
		isTombstone   bool
	}
	config struct {
		allMergeSorted bool
		dedupAll       bool
		bufferSizeCap  int
		tailSizeCap    int
	}
	staged struct {
		inMemory            []*batch.Batch
		persisted           []objectio.ObjectStats
		inMemorySize        int
		memorySizeThreshold int
		sinker              FileSinker
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
	if sinker.staged.sinker == nil {
		sinker.staged.sinker = NewSinkerImpl(
			sinker.fs,
			sinker.mp,
			sinker.schema.seqnums,
			sinker.schema.sortKeyIdx,
			sinker.schema.isPrimaryKey,
			sinker.schema.isTombstone,
			sinker.schema.schemaVersion,
		)
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
	defer sinker.putStageFileSinker(fSinker)
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

func (sinker *Sinker) putStageFileSinker(fsinker FileSinker) {
	sinker.staged.sinker.Reset()
}
func (sinker *Sinker) getStageFileSinker() FileSinker {
	return sinker.staged.sinker
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
	sinker.staged.sinker.Close()
	sinker.staged.sinker = nil
	sinker.mp = nil
	sinker.fs = nil
	return nil
}
