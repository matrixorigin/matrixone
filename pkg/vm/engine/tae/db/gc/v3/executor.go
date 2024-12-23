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

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/bloomfilter"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/engine_util"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/mergesort"
)

type GCJob = CheckpointBasedGCJob

type FilterFn func(context.Context, *bitmap.Bitmap, *batch.Batch, *mpool.MPool) error
type SourerFn func(context.Context, []string, *plan.Expr, *mpool.MPool, *batch.Batch) (bool, error)
type SinkerFn func(context.Context, *batch.Batch) error

func BuildBloomfilter(
	ctx context.Context,
	rowCount int,
	probability float64,
	columnIdx int,
	sourcer SourerFn,
	buffer containers.IBatchBuffer,
	mp *mpool.MPool,
) (bf *bloomfilter.BloomFilter, err error) {
	nbf := bloomfilter.New(int64(rowCount), probability)
	bf = &nbf
	bat := buffer.Fetch()
	defer buffer.Putback(bat, mp)
	var done bool
	for {
		bat.CleanOnlyData()
		select {
		case <-ctx.Done():
			return nil, context.Cause(ctx)
		default:
		}
		if done, err = sourcer(ctx, bat.Attrs, nil, mp, bat); err != nil {
			return
		}
		if done {
			break
		}
		bf.Add(bat.Vecs[columnIdx])
	}
	return
}

func NewGCExecutor(
	buffer *containers.OneSchemaBatchBuffer,
	isBufferOwner bool,
	cacheSize int,
	mp *mpool.MPool,
	fs fileservice.FileService,
) *GCExecutor {
	exec := &GCExecutor{
		mp: mp,
		fs: fs,
	}
	exec.buffer.isOwner = isBufferOwner
	exec.buffer.impl = buffer

	exec.config.canGCCacheSize = cacheSize
	return exec
}

type GCExecutor struct {
	buffer struct {
		isOwner bool
		impl    *containers.OneSchemaBatchBuffer
	}

	config struct {
		canGCCacheSize int
	}
	mp   *mpool.MPool
	fs   fileservice.FileService
	bm   bitmap.Bitmap
	sels []int64
}

func (exec *GCExecutor) doFilter(
	ctx context.Context,
	sourcer SourerFn,
	filter FilterFn,
	cannotGCSinker SinkerFn,
	canGCSinker SinkerFn,
) error {
	bat := exec.getBuffer()
	canGCBat := exec.getBuffer()
	defer exec.putBuffer(bat)
	defer exec.putBuffer(canGCBat)
	for {
		bat.CleanOnlyData()
		canGCBat.CleanOnlyData()
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		default:
		}
		// 1. get next batch from sourcer
		done, err := sourcer(ctx, bat.Attrs, nil, exec.mp, bat)
		if err != nil {
			return err
		}
		if done {
			break
		}

		// 2. do filter on the batch and get the bitmap
		//    bit 1 means the row can be GC'ed
		//    bit 0 means the row cannot be GC'ed
		exec.bm.Clear()
		exec.bm.TryExpandWithSize(bat.RowCount())
		err = mergesort.SortColumnsByIndex(bat.Vecs, 2, exec.mp)
		if err != nil {
			return err
		}
		if err := filter(ctx, &exec.bm, bat, exec.mp); err != nil {
			return err
		}
		// 3. sink the batch to the corresponding sinker
		exec.sels = exec.sels[:0]
		bitmap.ToArray(&exec.bm, &exec.sels)
		if err := canGCBat.Union(bat, exec.sels, exec.mp); err != nil {
			return err
		}
		bat.Shrink(exec.sels, true)
		if err := cannotGCSinker(ctx, bat); err != nil {
			return err
		}
		if err := canGCSinker(ctx, canGCBat); err != nil {
			return err
		}
	}

	return nil
}

func (exec *GCExecutor) Run(
	ctx context.Context,
	sourcer SourerFn,
	corseFilter FilterFn,
	fineFilter FilterFn,
	finalCanGCSinker SinkerFn,
) (newFiles []objectio.ObjectStats, err error) {
	cannotGCSinker := exec.getSinker(
		engine_util.WithBuffer(exec.buffer.impl, false),
	)

	canGCSinker := exec.getSinker(
		engine_util.WithBuffer(exec.buffer.impl, false),
		engine_util.WithTailSizeCap(exec.config.canGCCacheSize),
	)
	defer cannotGCSinker.Close()
	defer canGCSinker.Close()

	// 1. do coarse filter
	if err = exec.doFilter(
		ctx,
		sourcer,
		corseFilter,
		cannotGCSinker.Write,
		canGCSinker.Write,
	); err != nil {
		return
	}

	// 2. make fine sourcer
	if err = canGCSinker.Sync(ctx); err != nil {
		return
	}
	canGCObjects, canGCMemTable := canGCSinker.GetResult()
	defer func() {
		if err = DeleteObjects(ctx, exec.fs, canGCObjects); err != nil {
			//TODO: handle error
			err = nil
		}
	}()

	fineSourcer, release := MakeLoadFunc(
		ctx,
		canGCMemTable,
		canGCObjects,
		exec.fs,
		timestamp.Timestamp{},
		engine_util.WithColumns(
			ObjectTableSeqnums,
			ObjectTableTypes,
		),
	)
	if release != nil {
		defer release()
	}

	// 3. do fine filter
	if err = exec.doFilter(
		ctx,
		fineSourcer,
		fineFilter,
		cannotGCSinker.Write,
		finalCanGCSinker,
	); err != nil {
		return
	}

	if err = cannotGCSinker.Sync(ctx); err != nil {
		return
	}

	newFiles, _ = cannotGCSinker.GetResult()

	return
}

func (exec *GCExecutor) getBuffer() *batch.Batch {
	return exec.buffer.impl.Fetch()
}
func (exec *GCExecutor) putBuffer(bat *batch.Batch) {
	exec.buffer.impl.Putback(bat, exec.mp)
}

func (exec *GCExecutor) getSinker(
	opts ...engine_util.SinkerOption,
) *engine_util.Sinker {
	return engine_util.NewSinker(
		ObjectTablePrimaryKeyIdx,
		ObjectTableAttrs,
		ObjectTableTypes,
		FSinkerFactory,
		exec.mp,
		exec.fs,
		opts...,
	)
}

func (exec *GCExecutor) Close() error {
	if exec.buffer.isOwner && exec.buffer.impl != nil {
		exec.buffer.impl.Close(exec.mp)
		exec.buffer.impl = nil
	}
	exec.mp = nil
	exec.sels = nil
	exec.bm = bitmap.Bitmap{}
	return nil
}
