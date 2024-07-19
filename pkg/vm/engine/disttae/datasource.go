// Copyright 2021-2024 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package disttae

import (
	"context"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
)

type DataState uint8

const (
	InMem DataState = iota
	Persisted
	End
)

type tombstoneDataV1 struct {
	typ engine.TombstoneType
	//tombstones
	inMemTombstones []types.Rowid
	deltaLocs       []objectio.Location
}

func buildTombstoneV1() *tombstoneDataV1 {
	return &tombstoneDataV1{
		typ: engine.TombstoneV1,
	}
}

func (tomV1 *tombstoneDataV1) HasTombstones(bid types.Blockid) bool {
	return true
}

func (tomV1 *tombstoneDataV1) ApplyTombstones(rows []types.Rowid) ([]int64, error) {
	return nil, nil
}

func (tomV1 *tombstoneDataV1) Type() engine.TombstoneType {
	return tomV1.typ
}

type tombstoneDataV2 struct {
	typ engine.TombstoneType
	//tombstones
	inMemTombstones []types.Rowid
	tombstoneObjs   []objectio.ObjectStats
}

func buildTombstoneV2() *tombstoneDataV2 {
	return &tombstoneDataV2{
		typ: engine.TombstoneV2,
	}
}

func (tomV2 *tombstoneDataV2) HasTombstones(bid types.Blockid) bool {
	return true
}

func (tomV2 *tombstoneDataV2) ApplyTombstones(rows []types.Rowid) ([]int64, error) {
	return nil, nil
}

func (tomV2 *tombstoneDataV2) Type() engine.TombstoneType {
	return tomV2.typ
}

type RelDataType uint8

const (
	RelDataV0 RelDataType = iota
	RelDataV1
	RelDataV2
)

type relationDataV0 struct {
	typ     uint8
	blkList []*objectio.BlockInfo
}

type relationDataV1 struct {
	typ RelDataType
	//blkList[0] is a empty block info
	blkList []*objectio.BlockInfoInProgress

	//tombstones
	//inMemTombstones []types.Rowid
	//deltaLocs []objectio.Location
	tombstoneTyp uint8
	tombstones   engine.Tombstoner
}

func buildRelationDataV1(blkList []*objectio.BlockInfoInProgress) *relationDataV1 {
	return &relationDataV1{
		typ:     RelDataV1,
		blkList: blkList,
		//tombstoneTyp: 1,
	}
}

func (rd1 *relationDataV1) MarshalToBytes() []byte {
	return nil
}

func (rd1 *relationDataV1) AttachTombstones(tombstones engine.Tombstoner) error {
	rd1.tombstones = tombstones
	//if tombstones.Type() != rd1.tombstoneTyp {
	//	return moerr.NewInternalErrorNoCtx("tombstone type mismatch")
	//}
	return nil
}

func (rd1 *relationDataV1) ForeachDataBlk(f func(blk *objectio.BlockInfoInProgress)) {
	for i := 1; i < len(rd1.blkList); i++ {
		f(rd1.blkList[i])
	}
}

func (rd1 *relationDataV1) GetDataBlk(i int) *objectio.BlockInfoInProgress {
	return rd1.blkList[i]
}

func (rd1 *relationDataV1) AppendDataBlk(blk *objectio.BlockInfoInProgress) {
	rd1.blkList = append(rd1.blkList, blk)
}

func (rd1 *relationDataV1) BuildEmptyRelData() engine.RelData {
	return &relationDataV1{
		typ: rd1.typ,
	}
}

func (rd1 *relationDataV1) BlkCnt() int {
	return len(rd1.blkList)
}

//type relationDataV2 struct {
//	typ RelDataType
//	blkList objectio.BlockInfoInProgress
//
//	//tombstones
//	//inMemTombstones []types.Rowid
//	//tombstoneObjs   []objectio.ObjectStats
//
//	tombstoneTyp uint8
//	tombstones  Tombstoner
//}
//
//func buildRelationDataV2(blkList objectio.BlockInfoInProgress) *relationDataV2 {
//	return &relationDataV2{
//		blkList: blkList,
//		typ: RelDataV2,
//		//tombstoneTyp: 2,
//	}
//}
//
//func (rd2 *relationDataV2) MarshalToBytes() []byte {
//	return nil
//}
//
//func (rd2 *relationDataV2) AttachTombstones(tombstones Tombstoner) error {
//	rd2.tombstones = tombstones
//	//if tombstones.Type() != rd2.tombstoneTyp {
//	//	return moerr.NewInternalErrorNoCtx("tombstone type mismatch")
//	//}
//	return nil
//}

func UnmarshalRelationData(data []byte) engine.RelData {
	//typ := int(data[0])
	//switch typ {
	//case :

	//}
	return nil

}

type DataSource interface {
	Next(ctx context.Context, cols []string, types []types.Type, seqNums []uint16,
		memFilter MemPKFilterInProgress, txnOffset int, mp *mpool.MPool,
		vp engine.VectorPool, bat *batch.Batch) (*objectio.BlockInfoInProgress, DataState, error)

	HasTombstones(bid types.Blockid) bool

	// ApplyTombstones Apply tombstones into rows.
	ApplyTombstones(rows []types.Rowid) ([]int64, error)
}

type RemoteDataSource struct {
	tombstone engine.Tombstoner
	ranges    []*objectio.BlockInfoInProgress
}

func NewRemoteDataSource(
	ctx context.Context,
	mp *mpool.MPool,
	snapshotTS types.TS,
	fs fileservice.FileService,
	databaseId, tableId uint64,
	ranges []*objectio.BlockInfoInProgress,
	tombstone engine.Tombstoner,
) (source *RemoteDataSource, err error) {
	return &RemoteDataSource{
		tombstone: tombstone,
		ranges:    ranges,
	}, nil
}

func (rs *RemoteDataSource) Next(
	ctx context.Context,
	cols []string,
	types []types.Type,
	seqNums []uint16,
	memFilter MemPKFilterInProgress,
	txnOffset int,
	mp *mpool.MPool,
	vp engine.VectorPool,
	bat *batch.Batch) (*objectio.BlockInfoInProgress, DataState, error) {

	return nil, InMem, nil
}

func (rs *RemoteDataSource) HasTombstones(bid types.Blockid) bool {
	return true
}

// ApplyTombstones Apply tombstones into rows.
func (rs *RemoteDataSource) ApplyTombstones(rows []types.Rowid) ([]int64, error) {
	return nil, nil
}

// local data source

type LocalDataSource struct {
	ranges []*objectio.BlockInfoInProgress
	pState *logtailreplay.PartitionState

	prevBlockId           types.Blockid
	persistedDeletes      *nulls.Nulls
	committedInmemDeletes map[types.Rowid]struct{}
	unCommittedS3Deletes  map[types.Rowid]struct{}

	unCommittedInmemDeletes map[types.Rowid]struct{}
	unCommittedInmemInserts []*batch.Batch
	unCommittedS3DeletesBat map[types.Blockid][]*batch.Batch

	mp           *mpool.MPool
	ctx          context.Context
	fs           fileservice.FileService
	cursor       int
	snapshotTS   types.TS
	iteratePhase DataState
}

func NewLocalDataSource(
	ctx context.Context,
	mp *mpool.MPool,
	snapshotTS types.TS,
	fs fileservice.FileService,
	databaseId, tableId uint64,
	ranges []*objectio.BlockInfoInProgress,
	pState *logtailreplay.PartitionState,
	unCommittedS3DeletesBat map[types.Blockid][]*batch.Batch,
	unCommittedInmemWrites []Entry) (source *LocalDataSource, err error) {

	source = &LocalDataSource{}

	source.fs = fs
	source.ctx = ctx
	source.mp = mp

	source.ranges = ranges
	source.pState = pState
	source.snapshotTS = snapshotTS
	source.unCommittedInmemDeletes = make(map[types.Rowid]struct{})

	if err = extractAllInsertAndDeletesFromWorkspace(
		databaseId, tableId, &source.unCommittedInmemInserts,
		&source.unCommittedInmemDeletes, unCommittedInmemWrites); err != nil {
		return source, err
	}

	source.iteratePhase = InMem
	return source, nil
}

func (ls *LocalDataSource) HasTombstones(bid types.Blockid) bool {
	if ls.iteratePhase == InMem {
		return false
	}

	if len(ls.unCommittedS3DeletesBat[bid]) != 0 {
		return true
	}

	if _, _, ok := ls.pState.GetBockDeltaLoc(bid); ok {
		return true
	}

	delIter := ls.pState.NewRowsIter(ls.snapshotTS, &ls.prevBlockId, true)

	if delIter.Next() {
		return true
	}

	return false
}

// ApplyTombstones check if any deletes exist in
// 1. unCommittedInmemDeletes
// 2. unCommittedS3Deletes
// 3. committedInmemDeletes
// 4. committedPersistedTombstone
func (ls *LocalDataSource) ApplyTombstones(rows []types.Rowid) (sel []int64, err error) {
	rowIdsToOffsets := func(rowIds []types.Rowid) (ret []int64) {
		for _, row := range rows {
			_, offset := row.Decode()
			ret = append(ret, int64(offset))
		}
		return ret
	}

	blockId, _ := rows[0].Decode()

	if blockId.Compare(ls.prevBlockId) != 0 {
		ls.prevBlockId = blockId

		deltaLoc, commitTS, ok := ls.pState.GetBockDeltaLoc(blockId)
		if ok {
			ls.persistedDeletes, err = loadBlockDeletesByDeltaLoc(ls.ctx, ls.fs, blockId, deltaLoc, ls.snapshotTS, commitTS)
			if err != nil {
				return nil, err
			}
		}

		if len(ls.unCommittedS3DeletesBat[blockId]) != 0 {
			ls.unCommittedS3Deletes = make(map[types.Rowid]struct{})
			err = loadUncommittedS3Deletes(ls.ctx, ls.mp, ls.fs, &ls.unCommittedS3Deletes, ls.unCommittedS3DeletesBat[blockId])
			if err != nil {
				return nil, err
			}
		}

		committedInmemDeletes := make(map[types.Rowid]struct{})
		delIter := ls.pState.NewRowsIter(ls.snapshotTS, &ls.prevBlockId, true)
		for delIter.Next() {
			committedInmemDeletes[delIter.Entry().RowID] = struct{}{}
		}
		delIter.Close()
	}

	left := make([]types.Rowid, 0)

	for _, row := range rows {
		if ls.unCommittedS3Deletes != nil {
			if _, ok := ls.unCommittedS3Deletes[row]; ok {
				continue
			}
		}

		if _, ok := ls.unCommittedInmemDeletes[row]; ok {
			continue
		}

		if ls.persistedDeletes != nil {
			_, offset := row.Decode()
			if ls.persistedDeletes.Contains(uint64(offset)) {
				continue
			}
		}

		if ls.committedInmemDeletes != nil {
			if _, ok := ls.committedInmemDeletes[row]; ok {
				continue
			}
		}

		left = append(left, row)
	}

	return rowIdsToOffsets(left), nil
}

func (ls *LocalDataSource) Next(
	ctx context.Context, cols []string, types []types.Type, seqNums []uint16,
	memFilter MemPKFilterInProgress, txnOffset int, mp *mpool.MPool, vp engine.VectorPool,
	bat *batch.Batch) (*objectio.BlockInfoInProgress, DataState, error) {

	for {
		switch ls.iteratePhase {
		case InMem:
			err := ls.iterateInMemData(ctx, cols, types, seqNums, memFilter, bat, mp, vp)
			if bat.RowCount() == 0 && err == nil {
				ls.iteratePhase = Persisted
				continue
			}

			return nil, InMem, err

		case Persisted:
			if ls.cursor < len(ls.ranges) {
				ls.cursor++
				return ls.ranges[ls.cursor-1], Persisted, nil
			}

			ls.iteratePhase = End
			continue

		case End:
			return nil, ls.iteratePhase, nil
		}
	}
}

func (ls *LocalDataSource) iterateInMemData(
	ctx context.Context, cols []string, colTypes []types.Type,
	seqNums []uint16, memFilter MemPKFilterInProgress, bat *batch.Batch,
	mp *mpool.MPool, vp engine.VectorPool) error {

	defer func() {
		if bat.RowCount() == 0 {
			ls.iteratePhase = Persisted
		}
	}()

	if bat == nil {
		bat = batch.New(true, cols)
	}

	bat.SetRowCount(0)

	if err := ls.filterInMemUncommittedInserts(mp, bat); err != nil {
		return err
	}

	if bat.RowCount() != 0 {
		return nil
	}

	if err := ls.filterInMemCommittedInserts(colTypes, seqNums, memFilter, mp, bat); err != nil {
		return err
	}

	return nil
}

func (ls *LocalDataSource) filterInMemUncommittedInserts(mp *mpool.MPool, bat *batch.Batch) error {
	if len(ls.unCommittedInmemInserts) == 0 {
		return nil
	}

	insertsBat := ls.unCommittedInmemInserts[0]
	ls.unCommittedInmemInserts = ls.unCommittedInmemInserts[1:]

	rowIDs := vector.MustFixedCol[types.Rowid](insertsBat.Vecs[0])
	for i, vec := range bat.Vecs {
		uf := vector.GetUnionOneFunction(*vec.GetType(), mp)

		for j, k := int64(0), int64(bat.RowCount()); j < k; j++ {

			if _, ok := ls.unCommittedInmemDeletes[rowIDs[j]]; ok {
				continue
			}

			if err := uf(vec, bat.Vecs[i], j); err != nil {
				return err
			}
		}
	}

	bat.SetRowCount(insertsBat.RowCount())

	return nil
}

func (ls *LocalDataSource) filterInMemCommittedInserts(
	colTypes []types.Type, seqNums []uint16,
	memFilter MemPKFilterInProgress, mp *mpool.MPool, bat *batch.Batch) error {

	var (
		err          error
		sel          []int64
		appendedRows uint32
		insIter      logtailreplay.RowsIter
	)

	appendFunctions := make([]func(*vector.Vector, *vector.Vector, int64) error, len(bat.Attrs))
	for i := range bat.Attrs {
		appendFunctions[i] = vector.GetUnionOneFunction(colTypes[i], mp)
	}

	if memFilter.Spec.Move == nil {
		insIter = ls.pState.NewRowsIter(memFilter.TS, nil, false)
	} else {
		insIter = ls.pState.NewPrimaryKeyIter(memFilter.TS, memFilter.Spec)
	}

	defer insIter.Close()

	for insIter.Next() && appendedRows < options.DefaultBlockMaxRows {
		entry := insIter.Entry()

		sel, err = ls.ApplyTombstones([]types.Rowid{entry.RowID})
		if err != nil {
			return err
		}

		if len(sel) == 0 {
			continue
		}

		for i, name := range bat.Attrs {
			if name == catalog.Row_ID {
				if err = vector.AppendFixed(
					bat.Vecs[i],
					entry.RowID,
					false,
					mp); err != nil {
					return err
				}
			} else {
				idx := 2 /*rowid and commits*/ + seqNums[i]
				if int(idx) >= len(entry.Batch.Vecs) /*add column*/ ||
					entry.Batch.Attrs[idx] == "" /*drop column*/ {
					err = vector.AppendAny(
						bat.Vecs[i],
						nil,
						true,
						mp)
				} else {
					err = appendFunctions[i](
						bat.Vecs[i],
						entry.Batch.Vecs[int(2+seqNums[i])],
						entry.Offset,
					)
				}
				if err != nil {
					return err
				}
			}
		}
		appendedRows++
	}

	bat.SetRowCount(int(appendedRows))

	return nil
}

func loadUncommittedS3Deletes(
	ctx context.Context,
	mp *mpool.MPool,
	fs fileservice.FileService,
	dest *map[types.Rowid]struct{},
	blkDeletesBat []*batch.Batch) error {

	for _, bat := range blkDeletesBat {
		vs, area := vector.MustVarlenaRawData(bat.GetVector(0))

		for i := range vs {
			location, err := blockio.EncodeLocationFromString(vs[i].UnsafeGetString(area))
			if err != nil {
				return err
			}

			rowIdBat, release, err := blockio.LoadColumns(ctx, []uint16{0}, nil, fs, location, mp, fileservice.Policy(0))
			if err != nil {
				release()
				return err
			}

			rowIds := vector.MustFixedCol[types.Rowid](rowIdBat.GetVector(0))
			for _, rowId := range rowIds {
				(*dest)[rowId] = struct{}{}
			}

			release()
		}
	}

	return nil
}

func extractAllInsertAndDeletesFromWorkspace(
	databaseId, tableId uint64,
	destInserts *[]*batch.Batch,
	destDeletes *map[types.Rowid]struct{},
	unCommittedInmemWrites []Entry) error {

	for _, entry := range unCommittedInmemWrites {
		if entry.DatabaseId() != databaseId || entry.TableId() != tableId {
			continue
		}

		if entry.IsGeneratedByTruncate() {
			continue
		}

		if (entry.Type() == DELETE || entry.Type() == DELETE_TXN) && entry.FileName() == "" {
			vs := vector.MustFixedCol[types.Rowid](entry.Bat().GetVector(0))
			for _, v := range vs {
				(*destDeletes)[v] = struct{}{}
			}
		}
	}

	for _, entry := range unCommittedInmemWrites {
		if entry.DatabaseId() != databaseId || entry.TableId() != tableId {
			continue
		}

		if entry.IsGeneratedByTruncate() {
			continue
		}

		if entry.Type() == INSERT || entry.Type() == INSERT_TXN {
			if entry.Bat() == nil || entry.Bat().IsEmpty() || entry.Bat().Attrs[0] == catalog.BlockMeta_MetaLoc {
				continue
			}

			*destInserts = append(*destInserts, entry.Bat())
		}
	}

	return nil
}

func loadBlockDeletesByDeltaLoc(
	ctx context.Context, fs fileservice.FileService,
	blockId types.Blockid, deltaLoc objectio.ObjectLocation,
	snapshotTS, blockCommitTS types.TS) (deleteMask *nulls.Nulls, err error) {

	var (
		rows             *nulls.Nulls
		bisect           time.Duration
		release          func()
		persistedByCN    bool
		persistedDeletes *batch.Batch
	)

	location := objectio.Location(deltaLoc[:])

	if !location.IsEmpty() {
		t1 := time.Now()

		if persistedDeletes, persistedByCN, release, err = blockio.ReadBlockDelete(ctx, location, fs); err != nil {
			return nil, err
		}
		defer release()

		readCost := time.Since(t1)

		if persistedByCN {
			rows = blockio.EvalDeleteRowsByTimestampForDeletesPersistedByCN(persistedDeletes, snapshotTS, blockCommitTS)
		} else {
			t2 := time.Now()
			rows = blockio.EvalDeleteRowsByTimestamp(persistedDeletes, snapshotTS, &blockId)
			bisect = time.Since(t2)
		}

		if rows != nil {
			deleteMask = rows
		}

		readTotal := time.Since(t1)
		blockio.RecordReadDel(readTotal, readCost, bisect)
	}

	return deleteMask, nil
}
