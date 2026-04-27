// Copyright 2026 Matrix Origin
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

package disttae

import (
	"context"
	"fmt"
	"slices"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
)

type materializedSnapshotDataSource struct {
	ctx        context.Context
	fs         fileservice.FileService
	pState     *logtailreplay.PartitionState
	currentTS  types.TS
	snapshotTS types.TS
	tombstone  engine.Tombstoner
	remote     engine.DataSource

	memPKFilter *readutil.MemPKFilter
	orderBy     []*plan.OrderBySpec

	built        bool
	mp           *mpool.MPool
	inMemBatches []*batch.Batch
	inMemCursor  int

	deletedRowsCache map[objectio.Blockid]*objectio.Bitmap
}

func newMaterializedSnapshotDataSource(
	ctx context.Context,
	fs fileservice.FileService,
	pState *logtailreplay.PartitionState,
	currentTS types.TS,
	snapshotTS types.TS,
	relData engine.RelData,
	tombstone engine.Tombstoner,
) engine.DataSource {
	var remote engine.DataSource
	if relData != nil && relData.DataCnt() > 0 {
		remote = readutil.NewRemoteDataSource(ctx, fs, snapshotTS.ToTimestamp(), relData)
	}
	return &materializedSnapshotDataSource{
		ctx:              ctx,
		fs:               fs,
		pState:           pState,
		currentTS:        currentTS,
		snapshotTS:       snapshotTS,
		tombstone:        tombstone,
		remote:           remote,
		deletedRowsCache: make(map[objectio.Blockid]*objectio.Bitmap),
	}
}

func (ds *materializedSnapshotDataSource) String() string {
	return fmt.Sprintf(
		"MaterializedSnapshotDataSource{current-ts=%s,snapshot-ts=%s,inmem-batches=%d,inmem-cursor=%d,remote=%v}",
		ds.currentTS.ToString(),
		ds.snapshotTS.ToString(),
		len(ds.inMemBatches),
		ds.inMemCursor,
		ds.remote != nil,
	)
}

func (ds *materializedSnapshotDataSource) Next(
	ctx context.Context,
	cols []string,
	colTypes []types.Type,
	seqNums []uint16,
	_ int32,
	filter any,
	mp *mpool.MPool,
	outBatch *batch.Batch,
) (*objectio.BlockInfo, engine.DataState, error) {
	if !ds.built {
		ds.built = true
		ds.mp = mp
		if memFilter, ok := filter.(*readutil.MemPKFilter); ok {
			ds.memPKFilter = memFilter
		}
		if err := ds.buildCommittedInMemBatches(ctx, cols, colTypes, seqNums, mp); err != nil {
			return nil, engine.End, err
		}
	}

	if ds.inMemCursor < len(ds.inMemBatches) {
		next := ds.inMemBatches[ds.inMemCursor]
		ds.inMemCursor++
		outBatch.CleanOnlyData()
		if _, err := outBatch.Append(ctx, mp, next); err != nil {
			return nil, engine.End, err
		}
		return nil, engine.InMem, nil
	}

	if ds.remote == nil {
		return nil, engine.End, nil
	}
	return ds.remote.Next(ctx, cols, colTypes, seqNums, 0, filter, mp, outBatch)
}

func (ds *materializedSnapshotDataSource) ApplyTombstones(
	ctx context.Context,
	bid *objectio.Blockid,
	rowsOffset []int64,
	_ engine.TombstoneApplyPolicy,
) ([]int64, error) {
	if ds.remote != nil {
		return ds.remote.ApplyTombstones(ctx, bid, rowsOffset, engine.Policy_CheckAll)
	}
	if ds.tombstone == nil || len(rowsOffset) == 0 {
		return rowsOffset, nil
	}
	slices.Sort(rowsOffset)
	left := ds.tombstone.ApplyInMemTombstones(bid, rowsOffset, nil)
	return ds.tombstone.ApplyPersistedTombstones(ctx, ds.fs, &ds.snapshotTS, bid, left, nil)
}

func (ds *materializedSnapshotDataSource) GetTombstones(
	ctx context.Context,
	bid *objectio.Blockid,
) (objectio.Bitmap, error) {
	if ds.remote != nil {
		return ds.remote.GetTombstones(ctx, bid)
	}
	bm := objectio.GetReusableBitmap()
	if ds.tombstone == nil {
		return bm, nil
	}
	ds.tombstone.ApplyInMemTombstones(bid, nil, &bm)
	if _, err := ds.tombstone.ApplyPersistedTombstones(ctx, ds.fs, &ds.snapshotTS, bid, nil, &bm); err != nil {
		bm.Release()
		return objectio.Bitmap{}, err
	}
	return bm, nil
}

func (ds *materializedSnapshotDataSource) SetOrderBy(orderby []*plan.OrderBySpec) {
	ds.orderBy = orderby
	if ds.remote != nil {
		ds.remote.SetOrderBy(orderby)
	}
}

func (ds *materializedSnapshotDataSource) GetOrderBy() []*plan.OrderBySpec {
	if ds.remote != nil {
		return ds.remote.GetOrderBy()
	}
	return ds.orderBy
}

func (ds *materializedSnapshotDataSource) SetFilterZM(zm objectio.ZoneMap) {
	if ds.remote != nil {
		ds.remote.SetFilterZM(zm)
	}
}

func (ds *materializedSnapshotDataSource) Close() {
	if ds.remote != nil {
		ds.remote.Close()
	}
	for _, bat := range ds.inMemBatches {
		if bat != nil && ds.mp != nil {
			bat.Clean(ds.mp)
		}
	}
	ds.inMemBatches = nil
	ds.inMemCursor = 0
	for bid, bm := range ds.deletedRowsCache {
		if bm != nil {
			bm.Release()
		}
		delete(ds.deletedRowsCache, bid)
	}
}

func (ds *materializedSnapshotDataSource) buildCommittedInMemBatches(
	ctx context.Context,
	attrs []string,
	colTypes []types.Type,
	seqNums []uint16,
	mp *mpool.MPool,
) error {
	if ds.pState == nil {
		return nil
	}

	var iter logtailreplay.RowsIter
	if ds.memPKFilter != nil && ds.memPKFilter.Valid() {
		iter = ds.pState.NewPrimaryKeyIter(
			ds.snapshotTS,
			ds.memPKFilter.Op(),
			ds.memPKFilter.Keys(),
		)
	} else {
		iter = ds.pState.NewRowsIter(ds.snapshotTS, nil, false)
	}
	defer iter.Close()

	out := newMaterializedInMemBatch(attrs, colTypes)
	totalRows := 0
	for iter.Next() {
		entry := iter.Entry()
		if ds.shouldSkipCommittedInMemEntry(entry) {
			continue
		}
		deleted, err := ds.isDeletedByCommittedTombstones(ctx, entry.RowID)
		if err != nil {
			out.Clean(mp)
			return err
		}
		if deleted {
			continue
		}
		if out.RowCount() >= int(objectio.BlockMaxRows) {
			ds.inMemBatches = append(ds.inMemBatches, out)
			out = newMaterializedInMemBatch(attrs, colTypes)
		}
		if err := appendCommittedInMemEntry(out, entry, seqNums, mp); err != nil {
			out.Clean(mp)
			return err
		}
		totalRows++
	}

	if out.RowCount() > 0 {
		ds.inMemBatches = append(ds.inMemBatches, out)
	} else {
		out.Clean(mp)
	}

	if ds.memPKFilter != nil && totalRows == 1 {
		ds.memPKFilter.RecordExactHit()
	}
	return nil
}

func (ds *materializedSnapshotDataSource) shouldSkipCommittedInMemEntry(
	entry *logtailreplay.RowEntry,
) bool {
	if ds.memPKFilter == nil || !ds.memPKFilter.HasBF || ds.memPKFilter.BFSeqNum == -1 {
		return false
	}
	bfColVec := entry.Batch.Vecs[2+ds.memPKFilter.BFSeqNum]
	if bfColVec.IsNull(uint64(entry.Offset)) {
		return true
	}
	return !ds.memPKFilter.FilterHint.BF.Test(
		bfColVec.GetRawBytesAt(int(entry.Offset)),
	)
}

func (ds *materializedSnapshotDataSource) isDeletedByCommittedTombstones(
	ctx context.Context,
	rowID objectio.Rowid,
) (bool, error) {
	if ds.tombstone == nil {
		return false, nil
	}
	bid := rowID.BorrowBlockID()
	mask, ok := ds.deletedRowsCache[*bid]
	if !ok {
		bm := objectio.GetReusableBitmap()
		ds.tombstone.ApplyInMemTombstones(bid, nil, &bm)
		if _, err := ds.tombstone.ApplyPersistedTombstones(
			ctx,
			ds.fs,
			&ds.snapshotTS,
			bid,
			nil,
			&bm,
		); err != nil {
			bm.Release()
			return false, err
		}
		ds.deletedRowsCache[*bid] = &bm
		mask = &bm
	}
	return mask.Contains(uint64(rowID.GetRowOffset())), nil
}

func newMaterializedInMemBatch(
	attrs []string,
	colTypes []types.Type,
) *batch.Batch {
	bat := batch.NewWithSize(len(attrs))
	bat.SetAttributes(attrs)
	for i := range attrs {
		bat.Vecs[i] = vector.NewVec(colTypes[i])
	}
	return bat
}

func appendCommittedInMemEntry(
	out *batch.Batch,
	entry *logtailreplay.RowEntry,
	seqNums []uint16,
	mp *mpool.MPool,
) error {
	for i := range out.Attrs {
		switch seqNums[i] {
		case objectio.SEQNUM_ROWID:
			if err := vector.AppendFixed(out.Vecs[i], entry.RowID, false, mp); err != nil {
				return err
			}
		case objectio.SEQNUM_COMMITTS:
			if err := vector.AppendFixed(out.Vecs[i], entry.Time, false, mp); err != nil {
				return err
			}
		default:
			idx := 2 + seqNums[i]
			if int(idx) >= len(entry.Batch.Vecs) || entry.Batch.Attrs[idx] == "" {
				if err := vector.AppendAny(out.Vecs[i], nil, true, mp); err != nil {
					return err
				}
				continue
			}
			if err := out.Vecs[i].UnionOne(entry.Batch.Vecs[idx], entry.Offset, mp); err != nil {
				return err
			}
		}
	}
	out.SetRowCount(out.Vecs[0].Length())
	return nil
}
