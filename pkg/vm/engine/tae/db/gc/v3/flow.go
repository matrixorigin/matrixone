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

package gc

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/mergesort"
)

type FlowOption func(*InsertFlow)

func WithAllMergeSorte() FlowOption {
	return func(flow *InsertFlow) {
		flow.config.allMergeSorted = true
	}
}

func WithDedupAll() FlowOption {
	return func(flow *InsertFlow) {
		flow.config.dedupAll = true
	}
}

func WithMemorySizeThreshold(size int) FlowOption {
	return func(flow *InsertFlow) {
		flow.staged.memorySizeThreshold = size
	}
}

type FileSinker interface {
	Sink(context.Context, *batch.Batch) error
	Sync(context.Context) (*objectio.ObjectStats, error)
	Reset()
	Close() error
}

func ConsructInsertFlow(
	mp *mpool.MPool,
	fs fileservice.FileService,
	loadNextBatch func(context.Context, *batch.Batch, *mpool.MPool) (bool, error),
	opts ...FlowOption,
) *InsertFlow {
	flow := &InsertFlow{
		loadNextBatch: loadNextBatch,
		mp:            mp,
		fs:            fs,
	}
	for _, opt := range opts {
		opt(flow)
	}
	flow.fillDefaults()
	return flow
}

type InsertFlow struct {
	loadNextBatch func(context.Context, *batch.Batch, *mpool.MPool) (bool, error)
	config        struct {
		allMergeSorted bool
		dedupAll       bool
	}
	staged struct {
		inMemory            []*batch.Batch
		persisted           []*objectio.ObjectStats
		inMemorySize        int
		memorySizeThreshold int
		sinker              FileSinker
	}
	results []*objectio.ObjectStats
	buffers []*batch.Batch
	mp      *mpool.MPool
	fs      fileservice.FileService
}

func (flow *InsertFlow) fillDefaults() {
	if flow.staged.memorySizeThreshold == 0 {
		flow.staged.memorySizeThreshold = DefaultInMemoryStagedSize
	}
}

func (flow *InsertFlow) fetchBuffer() *batch.Batch {
	if len(flow.buffers) > 0 {
		bat := flow.buffers[len(flow.buffers)-1]
		flow.buffers = flow.buffers[:len(flow.buffers)-1]
		return bat
	}
	return batch.New(false, ObjectTableAttrs)
}

func (flow *InsertFlow) putbackBuffer(bat *batch.Batch) {
	bat.CleanOnlyData()
	flow.buffers = append(flow.buffers, bat)
}

// bat should be sorted by the first column
// stageData take the ownership of the bat
func (flow *InsertFlow) stageData(
	ctx context.Context, bat *batch.Batch,
) error {
	flow.staged.inMemory = append(flow.staged.inMemory, bat)
	flow.staged.inMemorySize += bat.Size()
	if flow.staged.inMemorySize >= flow.staged.memorySizeThreshold {
		return flow.trySpill(ctx)
	}
	return nil
}

func (flow *InsertFlow) cleanupInMemoryStaged() {
	for i, bat := range flow.staged.inMemory {
		flow.putbackBuffer(bat)
		flow.staged.inMemory[i] = nil
	}
	flow.staged.inMemory = flow.staged.inMemory[:0]
	flow.staged.inMemorySize = 0
}

func (flow *InsertFlow) trySpill(ctx context.Context) error {
	var sorted []*batch.Batch
	sinker := func(data *batch.Batch) error {
		oneSorted := flow.fetchBuffer()
		_, err := oneSorted.AppendWithCopy(ctx, flow.mp, data)
		if err != nil {
			flow.putbackBuffer(oneSorted)
			return err
		}
		sorted = append(sorted, oneSorted)
		return nil
	}

	defer func() {
		for i, bat := range sorted {
			flow.putbackBuffer(bat)
			sorted[i] = nil
		}
		sorted = sorted[:0]
	}()

	// 1. merge sort
	buffer := flow.fetchBuffer() // note the lifecycle of buffer
	defer flow.putbackBuffer(buffer)
	if err := colexec.MergeSortBatches(
		flow.staged.inMemory,
		0,
		buffer,
		sinker,
		flow.mp,
	); err != nil {
		return err
	}

	// 2. cleanup the in-memory data
	flow.cleanupInMemoryStaged()

	// 3. dedup
	if err := DedupSortedBatches(
		0,
		sorted,
	); err != nil {
		return err
	}

	// 4. spill
	fSinker := flow.getStageFileSinker()
	defer flow.putStageFileSinker(fSinker)
	for _, bat := range sorted {
		if err := fSinker.Sink(ctx, bat); err != nil {
			return err
		}
	}
	stats, err := fSinker.Sync(ctx)
	if err != nil {
		return err
	}

	flow.staged.persisted = append(flow.staged.persisted, &stats)
}

func (flow *InsertFlow) putStageFileSinker(sinker FileSinker) {
	flow.staged.sinker.Reset()
}
func (flow *InsertFlow) getStageFileSinker() FileSinker {
	// TODO
	// if flow.staged.sinker == nil {
	// 	flow.staged.sinker = NewFileSinker()
	// }
	return flow.staged.sinker
}

func (flow *InsertFlow) Process(ctx context.Context) error {
	for {
		buffer := flow.fetchBuffer()
		done, err := flow.loadNextBatch(ctx, buffer, flow.mp)
		if err != nil || done {
			flow.putbackBuffer(buffer)
			return err
		}
		if err = flow.processOneBatch(ctx, buffer); err != nil {
			flow.putbackBuffer(buffer)
			return err
		}
	}
	return flow.doneAllBatches(ctx)
}

// processOneBatch take the ownership of the buffer
func (flow *InsertFlow) processOneBatch(
	ctx context.Context,
	data *batch.Batch,
) error {
	if err := mergesort.SortColumnsByIndex(
		data.Vecs,
		0,
		flow.mp,
	); err != nil {
		return err
	}
	return flow.stageData(ctx, data)
}

func (flow *InsertFlow) doneAllBatches(ctx context.Context) error {
	if len(flow.staged.persisted) == 0 && len(flow.staged.inMemory) == 0 {
		return nil
	}
	// spill the remaining data
	if len(flow.staged.inMemory) > 0 {
		if err = flow.trySpill(ctx); err != nil {
			return err
		}
	}
	// if there is only one file, it is sorted an deduped
	if len(flow.staged.persisted) == 1 {
		flow.results = append(flow.results, flow.staged.persisted[0])
	}

	if !flow.config.allMergeSorted && !flow.config.dedupAll {
		flow.results = append(flow.results, flow.staged.persisted...)
		return nil
	}
	panic("not implemented")
	// TODO: merge the files and dedup
	// newPersied, err := MergeSortedFilesAndDedup(flow.staged.persisted)
	// if err != nil {
	// 	return err
	// }
	// flow.results = append(flow.results, newPersied...)
	return nil
}

func (flow *InsertFlow) Close() error {
	flow.cleanupInMemoryStaged()
	for i, bat := range flow.buffers {
		bat.Clean(flow.mp)
		flow.buffers[i] = nil
	}
	flow.buffers = nil
	flow.results = nil
	flow.staged.persisted = nil
	flow.staged.sinker.Close()
	flow.staged.sinker = nil
	flow.mp = nil
	flow.fs = nil
	return nil
}
