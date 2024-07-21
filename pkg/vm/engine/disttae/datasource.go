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

type DataState uint8

const (
	InMem DataState = iota
	Persisted
	End
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
		for _, row := range rows {
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
		partitionNum := blk.PartitionNum
		if _, ok := ret[partitionNum]; !ok {
			ret[partitionNum] = &relationDataV1{
				typ:          rd1.typ,
				isEmpty:      rd1.isEmpty,
				tombstoneTyp: rd1.tombstoneTyp,
				tombstones:   rd1.tombstones,
			}
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

type DataSource interface {
	Next(
		ctx context.Context,
		cols []string,
		types []types.Type,
		seqNums []uint16,
		memFilter MemPKFilterInProgress,
		mp *mpool.MPool,
		vp engine.VectorPool,
		bat *batch.Batch) (*objectio.BlockInfoInProgress, DataState, error)

	HasTombstones(bid types.Blockid) bool

	// ApplyTombstones Apply tombstones into rows.
	ApplyTombstones(rows []types.Rowid) ([]int64, error)
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
	_ MemPKFilterInProgress,
	_ *mpool.MPool,
	_ engine.VectorPool,
	_ *batch.Batch) (*objectio.BlockInfoInProgress, DataState, error) {

	if rs.cursor >= rs.data.BlkCnt() {
		return nil, End, nil
	}
	rs.cursor++
	return rs.data.GetDataBlk(rs.cursor - 1), Persisted, nil
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
	unCommittedInmemWrites []Entry,
	skipReadMem bool) (source *LocalDataSource, err error) {

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
	if skipReadMem {
		source.iteratePhase = Persisted
	}
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
// 1. unCommittedInmemDeletes, include deletes belongs to S3 uncommitted blocks written by CN.
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
			location := objectio.Location(deltaLoc[:])
			ls.persistedDeletes, err = loadBlockDeletesByDeltaLoc(ls.ctx, ls.fs, blockId, location, ls.snapshotTS, commitTS)
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
	memFilter MemPKFilterInProgress, mp *mpool.MPool, vp engine.VectorPool,
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
