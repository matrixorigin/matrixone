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
	"bytes"
	"context"
	"slices"
	"sort"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type tombstoneDataV1 struct {
	typ engine.TombstoneType
	//in memory tombstones
	inMemTombstones []types.Rowid

	//persisted tombstones
	// written by CN, one block maybe respond to multi deltaLocs.
	uncommittedDeltaLocs []objectio.Location

	committedDeltalocs []objectio.Location
	commitTS           []types.TS

	//For improve the performance, but don't need to marshal and unmarshal the follow fields.
	init                  bool
	blk2RowID             map[types.Blockid][]types.Rowid
	rowIDs                map[types.Rowid]struct{}
	blk2DeltaLoc          map[types.Blockid]objectio.Location
	blk2CommitTS          map[types.Blockid]types.TS
	blk2UncommitDeltaLocs map[types.Blockid][]objectio.Location
}

func buildTombstoneV1() *tombstoneDataV1 {
	return &tombstoneDataV1{
		typ: engine.TombstoneV1,
	}
}

func (tomV1 *tombstoneDataV1) initMap() {
	if !tomV1.init {
		tomV1.blk2DeltaLoc = make(map[types.Blockid]objectio.Location)
		tomV1.blk2CommitTS = make(map[types.Blockid]types.TS)
		tomV1.blk2RowID = make(map[types.Blockid][]types.Rowid)
		tomV1.blk2UncommitDeltaLocs = make(map[types.Blockid][]objectio.Location)
		tomV1.rowIDs = make(map[types.Rowid]struct{})
		for i, loc := range tomV1.committedDeltalocs {
			blkID := *objectio.BuildObjectBlockid(loc.Name(), loc.ID())
			tomV1.blk2DeltaLoc[blkID] = loc
			tomV1.blk2CommitTS[blkID] = tomV1.commitTS[i]
		}
		for _, loc := range tomV1.uncommittedDeltaLocs {
			blkID := *objectio.BuildObjectBlockid(loc.Name(), loc.ID())
			tomV1.blk2UncommitDeltaLocs[blkID] = append(tomV1.blk2UncommitDeltaLocs[blkID], loc)
		}
		for _, row := range tomV1.inMemTombstones {
			blkID, _ := row.Decode()
			tomV1.blk2RowID[blkID] = append(tomV1.blk2RowID[blkID], row)
			tomV1.rowIDs[row] = struct{}{}
		}
		tomV1.init = true
	}
}

func (tomV1 *tombstoneDataV1) IsEmpty() bool {
	if len(tomV1.inMemTombstones) == 0 &&
		len(tomV1.committedDeltalocs) == 0 &&
		len(tomV1.uncommittedDeltaLocs) == 0 {
		return true
	}
	return false
}

func (tomV1 *tombstoneDataV1) UnMarshal(buf []byte) error {
	tomV1.typ = engine.TombstoneType(types.DecodeUint8(buf))
	buf = buf[1:]

	rowIDCnt := types.DecodeUint32(buf)
	buf = buf[4:]
	for i := 0; i < int(rowIDCnt); i++ {
		rowid := types.DecodeFixed[types.Rowid](buf[:types.RowidSize])
		tomV1.inMemTombstones = append(tomV1.inMemTombstones, rowid)
		buf = buf[types.RowidSize:]
	}

	cntOfUncommit := types.DecodeUint32(buf)
	buf = buf[4:]
	for i := 0; i < int(cntOfUncommit); i++ {
		loc := objectio.Location(buf[:objectio.LocationLen])
		tomV1.uncommittedDeltaLocs = append(tomV1.uncommittedDeltaLocs, loc)
		buf = buf[objectio.LocationLen:]
	}

	cntOfCommit := types.DecodeUint32(buf)
	buf = buf[4:]
	for i := 0; i < int(cntOfCommit); i++ {
		loc := objectio.Location(buf[:objectio.LocationLen])
		tomV1.committedDeltalocs = append(tomV1.committedDeltalocs, loc)
		buf = buf[objectio.LocationLen:]
	}

	if cntOfCommit > 0 {
		cntOfCommitTS := types.DecodeUint32(buf)
		buf = buf[4:]
		for i := 0; i < int(cntOfCommitTS); i++ {
			ts := types.DecodeFixed[types.TS](buf[:types.TxnTsSize])
			tomV1.commitTS = append(tomV1.commitTS, ts)
			buf = buf[types.TxnTsSize:]
		}
	}
	return nil
}

func (tomV1 *tombstoneDataV1) MarshalWithBuf(w *bytes.Buffer) (uint32, error) {
	var size uint32
	typ := uint8(tomV1.typ)
	if _, err := w.Write(types.EncodeUint8(&typ)); err != nil {
		return 0, err
	}
	size += 1

	length := uint32(len(tomV1.inMemTombstones))
	if _, err := w.Write(types.EncodeUint32(&length)); err != nil {
		return 0, err
	}
	size += 4

	for _, row := range tomV1.inMemTombstones {
		if _, err := w.Write(types.EncodeFixed(row)); err != nil {
			return 0, err
		}
		size += types.RowidSize
	}

	length = uint32(len(tomV1.uncommittedDeltaLocs))
	if _, err := w.Write(types.EncodeUint32(&length)); err != nil {
		return 0, err
	}
	size += 4

	for _, loc := range tomV1.uncommittedDeltaLocs {
		if _, err := w.Write(types.EncodeSlice([]byte(loc))); err != nil {
			return 0, err
		}
		size += uint32(objectio.LocationLen)
	}

	length = uint32(len(tomV1.committedDeltalocs))
	if _, err := w.Write(types.EncodeUint32(&length)); err != nil {
		return 0, err
	}
	size += 4

	for _, loc := range tomV1.committedDeltalocs {
		if _, err := w.Write(types.EncodeSlice([]byte(loc))); err != nil {
			return 0, err
		}
		size += uint32(objectio.LocationLen)
	}

	if length > 0 {
		length = uint32(len(tomV1.commitTS))
		if _, err := w.Write(types.EncodeUint32(&length)); err != nil {
			return 0, err
		}
		size += 4

		for _, ts := range tomV1.commitTS {
			if _, err := w.Write(types.EncodeFixed(ts)); err != nil {
				return 0, err
			}
			size += types.TxnTsSize
		}
	}
	return size, nil

}

func (tomV1 *tombstoneDataV1) HasTombstones(bid types.Blockid) bool {
	tomV1.initMap()
	if _, ok := tomV1.blk2DeltaLoc[bid]; ok {
		return true
	}
	if _, ok := tomV1.blk2RowID[bid]; ok {
		return true
	}
	if _, ok := tomV1.blk2UncommitDeltaLocs[bid]; ok {
		return true
	}
	return false
}

func (tomV1 *tombstoneDataV1) ApplyTombstones(
	rows []types.Rowid,
	loadCommit func(
		bid types.Blockid,
		loc objectio.Location,
		committs types.TS) (*nulls.Nulls, error),
	loadUncommit func(loc objectio.Location) (*nulls.Nulls, error),
) ([]int64, error) {

	rowIdsToOffsets := func(rowIds []types.Rowid) (ret []int64) {
		for _, row := range rowIds {
			_, offset := row.Decode()
			ret = append(ret, int64(offset))
		}
		return ret
	}
	tomV1.initMap()

	left := make([]types.Rowid, 0)
	blockId, _ := rows[0].Decode()

	var (
		commitTombstones *nulls.Nulls
		err              error
	)

	uncommitTombstones := nulls.NewWithSize(0)

	if _, ok := tomV1.blk2DeltaLoc[blockId]; ok {
		commitTombstones, err = loadCommit(
			blockId,
			tomV1.blk2DeltaLoc[blockId],
			tomV1.blk2CommitTS[blockId])
		if err != nil {
			return nil, err
		}
	}

	if _, ok := tomV1.blk2UncommitDeltaLocs[blockId]; ok {
		for _, loc := range tomV1.blk2UncommitDeltaLocs[blockId] {
			tombstones, err := loadUncommit(loc)
			if err != nil {
				return nil, err
			}
			uncommitTombstones.Merge(tombstones)
		}
	}

	for _, row := range rows {
		if _, ok := tomV1.rowIDs[row]; ok {
			continue
		}
		_, offset := row.Decode()
		if commitTombstones != nil && commitTombstones.Contains(uint64(offset)) {
			continue
		}
		if uncommitTombstones.Contains(uint64(offset)) {
			continue
		}
		left = append(left, row)
	}
	return rowIdsToOffsets(left), nil
}

func (tomV1 *tombstoneDataV1) Type() engine.TombstoneType {
	return tomV1.typ
}

func (tomV1 *tombstoneDataV1) Merge(other engine.Tombstoner) error {
	if v, ok := other.(*tombstoneDataV1); ok {
		tomV1.inMemTombstones = append(tomV1.inMemTombstones, v.inMemTombstones...)
		tomV1.committedDeltalocs = append(tomV1.committedDeltalocs, v.committedDeltalocs...)
		tomV1.uncommittedDeltaLocs = append(tomV1.uncommittedDeltaLocs, v.uncommittedDeltaLocs...)
		tomV1.commitTS = append(tomV1.commitTS, v.commitTS...)
	}
	return moerr.NewInternalErrorNoCtx("tombstone type mismatch")
}

var _ engine.Tombstoner = buildTombstoneV2()

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

func (tomV2 *tombstoneDataV2) IsEmpty() bool {
	panic("implement me")
}

func (tomV2 *tombstoneDataV2) MarshalWithBuf(w *bytes.Buffer) (uint32, error) {
	panic("implement me")
}

func (tomV2 *tombstoneDataV2) UnMarshal(buf []byte) error {
	panic("implement me")
}

func (tomV2 *tombstoneDataV2) HasTombstones(bid types.Blockid) bool {
	panic("implement me")
}

func (tomV2 *tombstoneDataV2) ApplyTombstones(
	rows []types.Rowid,
	load1 func(
		bid types.Blockid,
		loc objectio.Location,
		committs types.TS) (*nulls.Nulls, error),
	load2 func(loc objectio.Location) (*nulls.Nulls, error)) ([]int64, error) {
	panic("implement me")
}

func (tomV2 *tombstoneDataV2) Type() engine.TombstoneType {
	return tomV2.typ
}

func (tomV2 *tombstoneDataV2) Merge(other engine.Tombstoner) error {
	if v, ok := other.(*tombstoneDataV2); ok {
		tomV2.inMemTombstones = append(tomV2.inMemTombstones, v.inMemTombstones...)
		tomV2.tombstoneObjs = append(tomV2.tombstoneObjs, v.tombstoneObjs...)
	}
	return moerr.NewInternalErrorNoCtx("tombstone type mismatch")
}

type RelDataType uint8

const (
	RelDataV0 RelDataType = iota
	RelDataV1
	RelDataV2
)

func UnmarshalRelationData(data []byte) (engine.RelData, error) {
	typ := RelDataType(data[0])
	switch typ {
	case RelDataV1:
		rd1 := buildRelationDataV1(nil)
		data = data[1:]

		blkCnt := types.DecodeUint32(data)
		data = data[4:]

		if blkCnt > 0 {
			blkSize := types.DecodeUint32(data)
			data = data[4:]
			for i := uint32(0); i < blkCnt; i++ {
				blk := &objectio.BlockInfoInProgress{}
				err := blk.Unmarshal(data[:blkSize])
				if err != nil {
					return nil, err
				}
				data = data[blkSize:]
				rd1.blkList = append(rd1.blkList, blk)
			}
		}

		isEmpty := types.DecodeBool(data)
		rd1.isEmpty = isEmpty
		data = data[1:]

		if !isEmpty {
			tombstoneTyp := engine.TombstoneType(types.DecodeUint8(data))
			rd1.tombstoneTyp = tombstoneTyp
			data = data[1:]

			size := types.DecodeUint32(data)
			data = data[4:]
			switch tombstoneTyp {
			case engine.TombstoneV1:
				tombstoner := buildTombstoneV1()
				if err := tombstoner.UnMarshal(data[:size]); err != nil {
					return nil, err
				}
				rd1.AttachTombstones(tombstoner)
			case engine.TombstoneV2:
				return nil, moerr.NewInternalErrorNoCtx("unsupported tombstone type")
			default:
				return nil, moerr.NewInternalErrorNoCtx("unsupported tombstone type")
			}
		}
		return rd1, nil

	default:
		return nil, moerr.NewInternalErrorNoCtx("unsupported relation data type")
	}
}

type relationDataV0 struct {
	typ     uint8
	blkList []*objectio.BlockInfo
}

type relationDataV1 struct {
	typ RelDataType
	//blkList[0] is a empty block info
	blkList []*objectio.BlockInfoInProgress

	//marshal tombstones if isEmpty is false, otherwise dont't need to marshal tombstones
	isEmpty      bool
	tombstoneTyp engine.TombstoneType
	//tombstones
	tombstones engine.Tombstoner
}

func buildRelationDataV1(blkList []*objectio.BlockInfoInProgress) *relationDataV1 {
	return &relationDataV1{
		typ:     RelDataV1,
		blkList: blkList,
		isEmpty: true,
	}
}

func (rd1 *relationDataV1) MarshalWithBuf(w *bytes.Buffer) error {
	var pos1 uint32
	var pos2 uint32
	typ := uint8(rd1.typ)
	if _, err := w.Write(types.EncodeUint8(&typ)); err != nil {
		return err
	}
	pos1 += 1
	pos2 += 1

	//len of blk list
	length := uint32(len(rd1.blkList))
	if _, err := w.Write(types.EncodeUint32(&length)); err != nil {
		return err
	}
	pos1 += 4
	pos2 += 4
	// reserve the space: 4 bytes for block info
	var sizeOfBlockInfo uint32
	if _, err := w.Write(types.EncodeUint32(&sizeOfBlockInfo)); err != nil {
		return err
	}
	pos2 += 4

	for i, blk := range rd1.blkList {
		space, err := blk.MarshalWithBuf(w)
		if err != nil {
			return err
		}
		if i == 0 {
			sizeOfBlockInfo = space
			//update the size of block info
			copy(w.Bytes()[pos1:pos1+4], types.EncodeUint32(&sizeOfBlockInfo))
		}
		pos2 += space
	}

	if _, err := w.Write(types.EncodeBool(&rd1.isEmpty)); err != nil {
		return err
	}
	pos2 += 1

	if !rd1.isEmpty {
		typ := uint8(rd1.tombstoneTyp)
		if _, err := w.Write(types.EncodeUint8(&typ)); err != nil {
			return err
		}
		pos2 += 1

		var sizeOfTombstones uint32
		// reserve the space: 4 bytes for size of tombstones.
		if _, err := w.Write(types.EncodeUint32(&sizeOfTombstones)); err != nil {
			return err
		}

		space, err := rd1.tombstones.MarshalWithBuf(w)
		if err != nil {
			return err
		}
		//update the size of tombstones.
		copy(w.Bytes()[pos2:pos2+4], types.EncodeUint32(&space))

	}
	return nil
}

func (rd1 *relationDataV1) MarshalToBytes() []byte {
	var w bytes.Buffer
	if err := rd1.MarshalWithBuf(&w); err != nil {
		return nil
	}
	buf := w.Bytes()
	return buf
}

func (rd1 *relationDataV1) AttachTombstones(tombstones engine.Tombstoner) error {
	rd1.tombstones = tombstones
	rd1.tombstoneTyp = tombstones.Type()
	rd1.isEmpty = tombstones.IsEmpty()
	return nil
}

func (rd1 *relationDataV1) GetTombstones() engine.Tombstoner {
	return rd1.tombstones
}

func (rd1 *relationDataV1) ForeachDataBlk(begin, end int, f func(blk *objectio.BlockInfoInProgress) error) error {
	for i := begin; i < end; i++ {
		err := f(rd1.blkList[i])
		if err != nil {
			return err
		}
	}
	return nil
}

func (rd1 *relationDataV1) GetDataBlk(i int) *objectio.BlockInfoInProgress {
	return rd1.blkList[i]
}

func (rd1 *relationDataV1) SetDataBlk(i int, blk *objectio.BlockInfoInProgress) {
	rd1.blkList[i] = blk
}

func (rd1 *relationDataV1) DataBlkSlice(i, j int) engine.RelData {
	return &relationDataV1{
		typ:          rd1.typ,
		blkList:      rd1.blkList[i:j],
		isEmpty:      rd1.isEmpty,
		tombstoneTyp: rd1.tombstoneTyp,
		tombstones:   rd1.tombstones,
	}
}

func (rd1 *relationDataV1) GroupByPartitionNum() map[int16]engine.RelData {
	ret := make(map[int16]engine.RelData)
	for _, blk := range rd1.blkList {
		if blk.IsMemBlk() {
			continue
		}
		partitionNum := blk.PartitionNum
		if _, ok := ret[partitionNum]; !ok {
			ret[partitionNum] = &relationDataV1{
				typ:          rd1.typ,
				isEmpty:      rd1.isEmpty,
				tombstoneTyp: rd1.tombstoneTyp,
				tombstones:   rd1.tombstones,
			}
			ret[partitionNum].AppendDataBlk(&objectio.EmptyBlockInfoInProgress)
		}
		ret[partitionNum].AppendDataBlk(blk)
	}
	return ret
}

//func (rd1 *relationDataV1) DataBlkClone(i, j int) engine.RelData {
//	var dst []*objectio.BlockInfoInProgress
//	copy(dst, rd1.blkList[i:j])
//	return &relationDataV1{
//		typ:          rd1.typ,
//		blkList:      dst,
//		tombstoneTyp: rd1.tombstoneTyp,
//		tombstones:   rd1.tombstones,
//	}
//}

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

type RemoteDataSource struct {
	ctx  context.Context
	proc *process.Process

	fs fileservice.FileService
	ts types.TS

	cursor int
	data   engine.RelData
}

func NewRemoteDataSource(
	ctx context.Context,
	proc *process.Process,
	fs fileservice.FileService,
	snapshotTS timestamp.Timestamp,
	relData engine.RelData,
) (source *RemoteDataSource) {
	return &RemoteDataSource{
		data: relData,
		ctx:  ctx,
		proc: proc,
		fs:   fs,
		ts:   types.TimestampToTS(snapshotTS),
	}
}

func (rs *RemoteDataSource) Next(
	_ context.Context,
	_ []string,
	_ []types.Type,
	_ []uint16,
	_ any,
	_ *mpool.MPool,
	_ engine.VectorPool,
	_ *batch.Batch) (*objectio.BlockInfoInProgress, engine.DataState, error) {

	if rs.cursor >= rs.data.BlkCnt() {
		return nil, engine.End, nil
	}
	rs.cursor++
	return rs.data.GetDataBlk(rs.cursor - 1), engine.Persisted, nil
}

func (rs *RemoteDataSource) Close() {

}

func (rs *RemoteDataSource) ApplyTombstonesInProgress(
	ctx context.Context,
	bid objectio.Blockid,
	rowsOffset []int32) ([]int32, error) {
	return nil, nil
}

func (rs *RemoteDataSource) GetTombstonesInProgress(
	ctx context.Context, bid objectio.Blockid) (deletedRows []int64, err error) {
	return nil, nil
}

func (rs *RemoteDataSource) HasTombstones(bid types.Blockid) bool {
	return rs.data.GetTombstones().HasTombstones(bid)
}

// ApplyTombstones Apply tombstones into rows.
// Notice that rows should come from the same block.
func (rs *RemoteDataSource) ApplyTombstones(rows []types.Rowid) ([]int64, error) {
	loadCommitted := func(
		bid types.Blockid,
		loc objectio.Location,
		committs types.TS) (*nulls.Nulls, error) {
		tombstones, err := loadBlockDeletesByDeltaLoc(
			rs.ctx,
			rs.fs,
			bid,
			loc,
			rs.ts,
			committs,
		)
		if err != nil {
			return nil, err
		}
		return tombstones, nil
	}
	loadUncommited := func(loc objectio.Location) (*nulls.Nulls, error) {
		rowIdBat, release, err := blockio.LoadTombstoneColumns(
			rs.ctx,
			[]uint16{0},
			nil,
			rs.fs,
			loc,
			rs.proc.GetMPool())
		if err != nil {
			return nil, err
		}
		defer release()

		offsets := nulls.NewWithSize(0)
		rowids := vector.MustFixedCol[types.Rowid](rowIdBat.GetVector(0))

		for _, rowid := range rowids {
			row := rowid.GetRowOffset()
			offsets.Add(uint64(row))
		}
		return offsets, nil
	}
	return rs.data.GetTombstones().ApplyTombstones(rows, loadCommitted, loadUncommited)
}

// local data source

type LocalDataSource struct {
	TableName string
	// cn unCommitted s3 flushed object will be collect during txnTable.Ranges
	ranges []*objectio.BlockInfoInProgress
	pState *logtailreplay.PartitionState

	pStateRowsInsIter logtailreplay.RowsIter

	// load deletes only once for each blk
	prevBlockId types.Blockid

	// comes from partition state tombstone
	persistedDeletes *nulls.Nulls

	// these parts may have overlap with each other
	workspaceDeletes struct {
		// in workspace writes
		// --> writes []Entry
		InWritesDeletes map[types.Rowid]struct{}
		// deletes that cn already flushed to s3
		// --> blockId_tn_delete_metaLoc_batch
		FlushedS3Deletes map[types.Blockid][]*batch.Batch
		// raw rowId not flush yet
		// --> deletedBlocks *deletedBlocks
		RawRowIdOffsetsDeletes map[types.Blockid][]int64
	}

	// current loaded deletes
	tmpDeletesMask               map[types.Rowid]struct{}
	unCommittedInmemDeletesEntry []Entry
	unCommittedInmemInsertsBats  []*batch.Batch

	mp           *mpool.MPool
	ctx          context.Context
	fs           fileservice.FileService
	cursor       int
	snapshotTS   types.TS
	iteratePhase engine.DataState
}

func NewLocalDataSource(
	ctx context.Context,
	mp *mpool.MPool,
	snapshotTS types.TS,
	fs fileservice.FileService,
	databaseId, tableId uint64,
	ranges []*objectio.BlockInfoInProgress,
	pState *logtailreplay.PartitionState,
	unCommittedRawRowIdOffsetsDeletes map[types.Blockid][]int64,
	unCommittedS3DeletesBat map[types.Blockid][]*batch.Batch,
	unCommittedInmemWrites []Entry,
	skipReadMem bool,
	tableName string) (source *LocalDataSource, err error) {

	source = &LocalDataSource{}
	source.TableName = tableName
	source.fs = fs
	source.ctx = ctx
	source.mp = mp

	if bytes.Equal(
		objectio.EncodeBlockInfoInProgress(*ranges[0]),
		objectio.EmptyBlockInfoInProgressBytes) {
		ranges = ranges[1:]
	}

	source.ranges = ranges
	source.pState = pState
	source.snapshotTS = snapshotTS

	source.workspaceDeletes.FlushedS3Deletes = unCommittedS3DeletesBat
	source.workspaceDeletes.RawRowIdOffsetsDeletes = unCommittedRawRowIdOffsetsDeletes

	if err = extractInsertAndDeletesFromWorkspace(
		databaseId, tableId, &source.unCommittedInmemInsertsBats,
		&source.unCommittedInmemDeletesEntry, unCommittedInmemWrites); err != nil {
		return source, err
	}

	source.iteratePhase = engine.InMem
	if skipReadMem {
		source.iteratePhase = engine.Persisted
	}
	return source, nil
}

func (ls *LocalDataSource) HasTombstones(bid types.Blockid) bool {
	if ls.iteratePhase == engine.InMem {
		return false
	}

	if len(ls.workspaceDeletes.FlushedS3Deletes[bid]) != 0 {
		return true
	}

	if len(ls.workspaceDeletes.RawRowIdOffsetsDeletes[bid]) != 0 {
		return true
	}

	if _, _, ok := ls.pState.GetBockDeltaLoc(bid); ok {
		return true
	}

	if len(ls.unCommittedInmemDeletesEntry) != 0 {
		return true
	}

	delIter := ls.pState.NewRowsIter(ls.snapshotTS, &bid, true)
	defer delIter.Close()

	if delIter.Next() {
		return true
	}

	return false
}

//func (ls *LocalDataSource) prepareDeletes(blockId types.Blockid) (err error) {
//	if blockId.Compare(ls.prevBlockId) != 0 {
//		ls.prevBlockId = blockId
//
//		// extract deletes from workspace
//		if ls.workspaceDeletes.InWritesDeletes == nil {
//			ls.workspaceDeletes.InWritesDeletes = make(map[types.Rowid]struct{})
//			for idx := range ls.unCommittedInmemDeletesEntry {
//				delRowIds := vector.MustFixedCol[types.Rowid](ls.unCommittedInmemDeletesEntry[idx].bat.Vecs[0])
//				for _, delRowId := range delRowIds {
//					ls.workspaceDeletes.InWritesDeletes[delRowId] = struct{}{}
//				}
//			}
//		}
//
//		slices.Sort(ls.workspaceDeletes.RawRowIdOffsetsDeletes[blockId])
//
//		deltaLoc, commitTS, ok := ls.pState.GetBockDeltaLoc(blockId)
//		if ok {
//			ls.persistedDeletes, err = loadBlockDeletesByDeltaLoc(ls.ctx, ls.fs, blockId, deltaLoc[:], ls.snapshotTS, commitTS)
//			if err != nil {
//				return err
//			}
//		}
//
//		if len(ls.workspaceDeletes.FlushedS3Deletes[blockId]) != 0 {
//			ls.tmpDeletesMask = make(map[types.Rowid]struct{})
//			err = loadUncommittedS3Deletes(ls.ctx, ls.mp, ls.fs, &ls.tmpDeletesMask, ls.workspaceDeletes.FlushedS3Deletes[blockId])
//			if err != nil {
//				return err
//			}
//		}
//
//		delIter := ls.pState.NewRowsIter(ls.snapshotTS, &ls.prevBlockId, true)
//		for delIter.Next() {
//			if ls.tmpDeletesMask == nil {
//				ls.tmpDeletesMask = make(map[types.Rowid]struct{})
//			}
//			ls.tmpDeletesMask[delIter.Entry().RowID] = struct{}{}
//		}
//		delIter.Close()
//	}
//
//	return nil
//}

func (ls *LocalDataSource) Close() {
	ls.pStateRowsInsIter.Close()
}

// ApplyTombstones check if any deletes exist in
//  1. unCommittedInmemDeletes:
//     a. workspace writes
//     b. flushed to s3
//     c. raw rowId offset deletes (not flush yet)
//  3. committedInmemDeletes
//  4. committedPersistedTombstone
func (ls *LocalDataSource) ApplyTombstones(rows []types.Rowid) (sel []int64, err error) {

	//rowIdsToOffsets := func(rowIds []types.Rowid) (ret []int64) {
	//	for _, r := range rowIds {
	//		_, offset := r.Decode()
	//		ret = append(ret, int64(offset))
	//	}
	//	return ret
	//}

	//blockId, _ := rows[0].Decode()
	//if err = ls.prepareDeletes(blockId); err != nil {
	//	return nil, err
	//}
	//
	//left := make([]int64, 0)
	//
	//for idx, row := range rows {
	//	if ls.tmpDeletesMask != nil {
	//		if _, ok := ls.tmpDeletesMask[row]; ok {
	//			continue
	//		}
	//	}
	//
	//	if ls.persistedDeletes != nil {
	//		_, offset := row.Decode()
	//		if ls.persistedDeletes.Contains(uint64(offset)) {
	//			continue
	//		}
	//	}
	//
	//	if offsets := ls.workspaceDeletes.RawRowIdOffsetsDeletes[blockId]; len(offsets) != 0 {
	//		_, o := row.Decode()
	//		_, found := sort.Find(len(offsets), func(i int) int {
	//			return int(int64(o) - offsets[i])
	//		})
	//		if found {
	//			continue
	//		}
	//	}
	//
	//	if ls.workspaceDeletes.InWritesDeletes != nil {
	//		if _, ok := ls.workspaceDeletes.InWritesDeletes[row]; ok {
	//			continue
	//		}
	//	}
	//
	//	left = append(left, int64(idx))
	//}

	//return left, nil
	return nil, nil
}

func (ls *LocalDataSource) Next(
	ctx context.Context, cols []string, types []types.Type, seqNums []uint16,
	filter any, mp *mpool.MPool, vp engine.VectorPool,
	bat *batch.Batch) (*objectio.BlockInfoInProgress, engine.DataState, error) {

	memFilter := filter.(MemPKFilterInProgress)

	if len(cols) == 0 {
		return nil, engine.End, nil
	}

	for {
		switch ls.iteratePhase {
		case engine.InMem:
			err := ls.iterateInMemData(ctx, cols, types, seqNums, memFilter, bat, mp, vp)
			if bat.RowCount() == 0 && err == nil {
				ls.iteratePhase = engine.Persisted
				continue
			}

			return nil, engine.InMem, err

		case engine.Persisted:
			if ls.cursor < len(ls.ranges) {
				ls.cursor++
				return ls.ranges[ls.cursor-1], engine.Persisted, nil
			}

			ls.iteratePhase = engine.End
			continue

		case engine.End:
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
			ls.iteratePhase = engine.Persisted
		}
	}()

	if bat == nil {
		bat = batch.New(true, cols)
	}

	bat.SetRowCount(0)

	if err := ls.filterUncommittedInMemInserts(seqNums, mp, bat); err != nil {
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

func (ls *LocalDataSource) filterUncommittedInMemInserts(seqNums []uint16, mp *mpool.MPool, bat *batch.Batch) error {
	if len(ls.unCommittedInmemInsertsBats) == 0 {
		return nil
	}

	insertsBat := ls.unCommittedInmemInsertsBats[0]
	ls.unCommittedInmemInsertsBats = ls.unCommittedInmemInsertsBats[1:]

	insRowIDs := vector.MustFixedCol[types.Rowid](insertsBat.Vecs[0])

	for i, vec := range bat.Vecs {
		uf := vector.GetUnionOneFunction(*vec.GetType(), mp)

		for j, k := int64(0), int64(insertsBat.RowCount()); j < k; j++ {
			b, o := insRowIDs[j].Decode()

			sel, err := ls.ApplyTombstonesInProgress(ls.ctx, b, []int32{int32(o)})
			if err != nil {
				return err
			}

			if len(sel) == 0 {
				continue
			}

			colIdx := int(seqNums[i])
			if colIdx != objectio.SEQNUM_ROWID {
				colIdx++
			} else {
				colIdx = 0
			}

			if err = uf(vec, insertsBat.Vecs[colIdx], j); err != nil {
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
		sel          []int32
		appendedRows uint32
	)

	appendFunctions := make([]func(*vector.Vector, *vector.Vector, int64) error, len(bat.Attrs))
	for i := range bat.Attrs {
		appendFunctions[i] = vector.GetUnionOneFunction(colTypes[i], mp)
	}

	if ls.pStateRowsInsIter == nil {
		if memFilter.Spec.Move == nil {
			ls.pStateRowsInsIter = ls.pState.NewRowsIter(memFilter.TS, nil, false)
		} else {
			ls.pStateRowsInsIter = ls.pState.NewPrimaryKeyIter(memFilter.TS, memFilter.Spec)
		}
	}

	for ls.pStateRowsInsIter.Next() && appendedRows < options.DefaultBlockMaxRows {
		entry := ls.pStateRowsInsIter.Entry()
		b, o := entry.RowID.Decode()

		sel, err = ls.ApplyTombstonesInProgress(ls.ctx, b, []int32{int32(o)})
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

func extractInsertAndDeletesFromWorkspace(
	databaseId, tableId uint64,
	destInserts *[]*batch.Batch,
	destDeletes *[]Entry,
	unCommittedInmemWrites []Entry) error {

	for _, entry := range unCommittedInmemWrites {
		if entry.DatabaseId() != databaseId || entry.TableId() != tableId {
			continue
		}

		if entry.IsGeneratedByTruncate() {
			continue
		}

		if (entry.Type() == DELETE || entry.Type() == DELETE_TXN) && entry.FileName() == "" {
			*destDeletes = append(*destDeletes, entry)
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
	blockId types.Blockid, deltaLoc objectio.Location,
	snapshotTS, blockCommitTS types.TS) (deleteMask *nulls.Nulls, err error) {

	var (
		rows             *nulls.Nulls
		bisect           time.Duration
		release          func()
		persistedByCN    bool
		persistedDeletes *batch.Batch
	)

	if !deltaLoc.IsEmpty() {
		t1 := time.Now()

		if persistedDeletes, persistedByCN, release, err = blockio.ReadBlockDelete(ctx, deltaLoc, fs); err != nil {
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

func (ls *LocalDataSource) ApplyTombstonesInProgress(
	ctx context.Context,
	bid objectio.Blockid,
	rowsOffset []int32) ([]int32, error) {

	slices.SortFunc(rowsOffset, func(a, b int32) int {
		return int(a - b)
	})

	var err error

	rowsOffset, _ = ls.applyWorkspaceEntryDeletes(bid, rowsOffset)
	rowsOffset, _, err = ls.applyWorkspaceFlushedS3Deletes(bid, rowsOffset)
	if err != nil {
		return nil, err
	}

	rowsOffset, _ = ls.applyWorkspaceRawRowIdDeletes(bid, rowsOffset)
	rowsOffset, _ = ls.applyPStateInMemDeletes(bid, rowsOffset)
	rowsOffset, _, err = ls.applyPStatePersistedDeltaLocation(bid, rowsOffset)
	if err != nil {
		return nil, err
	}

	return rowsOffset, nil
}

func (ls *LocalDataSource) GetTombstonesInProgress(
	ctx context.Context, bid objectio.Blockid) (deletedRows []int64, err error) {

	_, dels := ls.applyWorkspaceEntryDeletes(bid, nil)
	deletedRows = append(deletedRows, dels...)

	_, dels, err = ls.applyWorkspaceFlushedS3Deletes(bid, nil)
	if err != nil {
		return nil, err
	}
	deletedRows = append(deletedRows, dels...)

	_, dels = ls.applyWorkspaceRawRowIdDeletes(bid, nil)
	deletedRows = append(deletedRows, dels...)

	_, dels = ls.applyPStateInMemDeletes(bid, nil)
	deletedRows = append(deletedRows, dels...)

	_, dels, err = ls.applyPStatePersistedDeltaLocation(bid, nil)
	if err != nil {
		return nil, err
	}
	deletedRows = append(deletedRows, dels...)

	return deletedRows, nil
}

func fastApplyDeletedRows(leftRows []int32, deletedRows []int64, o uint32) ([]int32, []int64) {
	if leftRows != nil {
		if x, found := sort.Find(len(leftRows), func(i int) int {
			return int(int32(o) - leftRows[i])
		}); found {
			leftRows = append(leftRows[:x], leftRows[x+1:]...)
		}
		//if x := sort.Search(len(leftRows), func(i int) bool {
		//	return int32(o) > leftRows[i]
		//}); x != len(leftRows) && leftRows[x] == int32(o) {
		//	leftRows = append(leftRows[:x], leftRows[x+1:]...)
		//}
	} else {
		deletedRows = append(deletedRows, int64(o))
	}

	return leftRows, deletedRows
}

func (ls *LocalDataSource) applyWorkspaceEntryDeletes(
	bid objectio.Blockid, offsets []int32) (leftRows []int32, deletedRows []int64) {

	leftRows = offsets

	for idx := range ls.unCommittedInmemDeletesEntry {
		delRowIds := vector.MustFixedCol[types.Rowid](ls.unCommittedInmemDeletesEntry[idx].bat.Vecs[0])
		for _, delRowId := range delRowIds {
			b, o := delRowId.Decode()
			if bid.Compare(b) != 0 {
				continue
			}

			leftRows, deletedRows = fastApplyDeletedRows(leftRows, deletedRows, o)
			if leftRows != nil && len(leftRows) == 0 {
				break
			}
		}
	}

	return offsets, deletedRows
}

func (ls *LocalDataSource) applyWorkspaceFlushedS3Deletes(
	bid objectio.Blockid, offsets []int32) (leftRows []int32, deletedRows []int64, err error) {

	leftRows = offsets

	if len(ls.workspaceDeletes.FlushedS3Deletes[bid]) == 0 {
		return
	}

	done := false
	for _, bat := range ls.workspaceDeletes.FlushedS3Deletes[bid] {
		vs, area := vector.MustVarlenaRawData(bat.GetVector(0))
		for i := range vs {
			location, err := blockio.EncodeLocationFromString(vs[i].UnsafeGetString(area))
			if err != nil {
				return nil, nil, err
			}

			rowIdBat, release, err := blockio.LoadColumns(ls.ctx, []uint16{0}, nil, ls.fs, location, ls.mp, fileservice.Policy(0))
			if err != nil {
				release()
				return nil, nil, err
			}

			delRowIds := vector.MustFixedCol[types.Rowid](rowIdBat.GetVector(0))
			for _, delRowId := range delRowIds {
				b, o := delRowId.Decode()
				if bid.Compare(b) != 0 {
					continue
				}

				leftRows, deletedRows = fastApplyDeletedRows(leftRows, deletedRows, o)
				if leftRows != nil && len(leftRows) == 0 {
					done = true
					break
				}
			}

			release()

			if done {
				break
			}
		}
	}

	return leftRows, deletedRows, nil
}

func (ls *LocalDataSource) applyWorkspaceRawRowIdDeletes(
	bid objectio.Blockid, offsets []int32) (leftRows []int32, deletedRows []int64) {

	leftRows = offsets

	for _, o := range ls.workspaceDeletes.RawRowIdOffsetsDeletes[bid] {
		leftRows, deletedRows = fastApplyDeletedRows(leftRows, deletedRows, uint32(o))
		if leftRows != nil && len(leftRows) == 0 {
			break
		}
	}

	return leftRows, deletedRows
}

func (ls *LocalDataSource) applyPStateInMemDeletes(
	bid objectio.Blockid, offsets []int32) (leftRows []int32, deletedRows []int64) {

	leftRows = offsets
	delIter := ls.pState.NewRowsIter(ls.snapshotTS, &bid, true)
	for delIter.Next() {
		_, o := delIter.Entry().RowID.Decode()
		leftRows, deletedRows = fastApplyDeletedRows(leftRows, deletedRows, o)
		if leftRows != nil && len(leftRows) == 0 {
			break
		}
	}

	delIter.Close()

	return leftRows, deletedRows
}

func (ls *LocalDataSource) applyPStatePersistedDeltaLocation(
	bid objectio.Blockid, offsets []int32) (leftRows []int32, deletedRows []int64, err error) {

	deltaLoc, commitTS, ok := ls.pState.GetBockDeltaLoc(bid)
	if ok {
		ls.persistedDeletes, err = loadBlockDeletesByDeltaLoc(ls.ctx, ls.fs, bid, deltaLoc[:], ls.snapshotTS, commitTS)
		if err != nil {
			return nil, nil, err
		}

		if offsets != nil {
			for _, offset := range offsets {
				if ls.persistedDeletes.Contains(uint64(offset)) {
					continue
				}
				leftRows = append(leftRows, offset)
			}
		} else {
			ls.persistedDeletes.Foreach(func(u uint64) bool {
				deletedRows = append(deletedRows, int64(u))
				return true
			})
		}
	} else {
		leftRows = offsets
	}

	return leftRows, deletedRows, nil
}
