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

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type tombstoneDataWithDeltaLoc struct {
	typ engine.TombstoneType
	//in memory tombstones
	inMemTombstones []types.Rowid

	//persisted tombstones
	// written by CN, one block maybe respond to multi deltaLocs.
	uncommittedDeltaLocs []objectio.Location

	committedDeltalocs []objectio.Location
	commitTS           []types.TS

	//For improve the performance, but don't need to marshal and unmarshal the follow fields.
	init      bool
	blk2RowID map[types.Blockid][]types.Rowid
	//TODO:: remove it
	rowIDs                map[types.Rowid]struct{}
	blk2DeltaLoc          map[types.Blockid]objectio.Location
	blk2CommitTS          map[types.Blockid]types.TS
	blk2UncommitDeltaLocs map[types.Blockid][]objectio.Location
}

func buildTombstoneWithDeltaLoc() *tombstoneDataWithDeltaLoc {
	return &tombstoneDataWithDeltaLoc{
		typ: engine.TombstoneWithDeltaLoc,
	}
}

func (tomb *tombstoneDataWithDeltaLoc) Init() {
	if !tomb.init {
		tomb.blk2DeltaLoc = make(map[types.Blockid]objectio.Location)
		tomb.blk2CommitTS = make(map[types.Blockid]types.TS)
		tomb.blk2RowID = make(map[types.Blockid][]types.Rowid)
		tomb.blk2UncommitDeltaLocs = make(map[types.Blockid][]objectio.Location)
		tomb.rowIDs = make(map[types.Rowid]struct{})
		for i, loc := range tomb.committedDeltalocs {
			blkID := *objectio.BuildObjectBlockid(loc.Name(), loc.ID())
			tomb.blk2DeltaLoc[blkID] = loc
			tomb.blk2CommitTS[blkID] = tomb.commitTS[i]
		}
		for _, loc := range tomb.uncommittedDeltaLocs {
			blkID := *objectio.BuildObjectBlockid(loc.Name(), loc.ID())
			tomb.blk2UncommitDeltaLocs[blkID] = append(tomb.blk2UncommitDeltaLocs[blkID], loc)
		}
		for _, row := range tomb.inMemTombstones {
			blkID, _ := row.Decode()
			tomb.blk2RowID[blkID] = append(tomb.blk2RowID[blkID], row)
			tomb.rowIDs[row] = struct{}{}
		}
		tomb.init = true
	}
}

func (tomb *tombstoneDataWithDeltaLoc) IsEmpty() bool {
	if len(tomb.inMemTombstones) == 0 &&
		len(tomb.committedDeltalocs) == 0 &&
		len(tomb.uncommittedDeltaLocs) == 0 {
		return true
	}
	return false
}

func (tomb *tombstoneDataWithDeltaLoc) UnMarshal(buf []byte) error {

	tomb.typ = engine.TombstoneType(types.DecodeUint8(buf))
	buf = buf[1:]

	rowIDCnt := types.DecodeUint32(buf)
	buf = buf[4:]
	for i := 0; i < int(rowIDCnt); i++ {
		rowid := types.DecodeFixed[types.Rowid](buf[:types.RowidSize])
		tomb.inMemTombstones = append(tomb.inMemTombstones, rowid)
		buf = buf[types.RowidSize:]
	}

	cntOfUncommit := types.DecodeUint32(buf)
	buf = buf[4:]
	for i := 0; i < int(cntOfUncommit); i++ {
		loc := objectio.Location(buf[:objectio.LocationLen])
		tomb.uncommittedDeltaLocs = append(tomb.uncommittedDeltaLocs, loc)
		buf = buf[objectio.LocationLen:]
	}

	cntOfCommit := types.DecodeUint32(buf)
	buf = buf[4:]
	for i := 0; i < int(cntOfCommit); i++ {
		loc := objectio.Location(buf[:objectio.LocationLen])
		tomb.committedDeltalocs = append(tomb.committedDeltalocs, loc)
		buf = buf[objectio.LocationLen:]
	}

	if cntOfCommit > 0 {
		cntOfCommitTS := types.DecodeUint32(buf)
		buf = buf[4:]
		for i := 0; i < int(cntOfCommitTS); i++ {
			ts := types.DecodeFixed[types.TS](buf[:types.TxnTsSize])
			tomb.commitTS = append(tomb.commitTS, ts)
			buf = buf[types.TxnTsSize:]
		}
	}
	return nil
}

func (tomb *tombstoneDataWithDeltaLoc) MarshalWithBuf(w *bytes.Buffer) (uint32, error) {
	var size uint32
	typ := uint8(tomb.typ)
	if _, err := w.Write(types.EncodeUint8(&typ)); err != nil {
		return 0, err
	}
	size += 1

	length := uint32(len(tomb.inMemTombstones))
	if _, err := w.Write(types.EncodeUint32(&length)); err != nil {
		return 0, err
	}
	size += 4

	for _, row := range tomb.inMemTombstones {
		if _, err := w.Write(types.EncodeFixed(row)); err != nil {
			return 0, err
		}
		size += types.RowidSize
	}

	length = uint32(len(tomb.uncommittedDeltaLocs))
	if _, err := w.Write(types.EncodeUint32(&length)); err != nil {
		return 0, err
	}
	size += 4

	for _, loc := range tomb.uncommittedDeltaLocs {
		if _, err := w.Write(types.EncodeSlice([]byte(loc))); err != nil {
			return 0, err
		}
		size += uint32(objectio.LocationLen)
	}

	length = uint32(len(tomb.committedDeltalocs))
	if _, err := w.Write(types.EncodeUint32(&length)); err != nil {
		return 0, err
	}
	size += 4

	for _, loc := range tomb.committedDeltalocs {
		if _, err := w.Write(types.EncodeSlice([]byte(loc))); err != nil {
			return 0, err
		}
		size += uint32(objectio.LocationLen)
	}

	if length > 0 {
		length = uint32(len(tomb.commitTS))
		if _, err := w.Write(types.EncodeUint32(&length)); err != nil {
			return 0, err
		}
		size += 4

		for _, ts := range tomb.commitTS {
			if _, err := w.Write(types.EncodeFixed(ts)); err != nil {
				return 0, err
			}
			size += types.TxnTsSize
		}
	}
	return size, nil

}

func (tomb *tombstoneDataWithDeltaLoc) ApplyInMemTombstones(
	bid types.Blockid,
	rowsOffset []int32,
	deleted *nulls.Nulls,
) (left []int32) {

	left = rowsOffset

	if rowIDs, ok := tomb.blk2RowID[bid]; ok {
		for _, row := range rowIDs {
			_, o := row.Decode()
			left = fastApplyDeletedRows(left, deleted, o)
		}
	}

	return
}

func (tomb *tombstoneDataWithDeltaLoc) ApplyPersistedTombstones(
	ctx context.Context,
	bid types.Blockid,
	rowsOffset []int32,
	mask *nulls.Nulls,
	apply func(
		ctx2 context.Context,
		loc objectio.Location,
		cts types.TS,
		rowsOffset []int32,
		left *[]int32,
		deleted *nulls.Nulls) (err error),
) (left []int32, err error) {

	if locs, ok := tomb.blk2UncommitDeltaLocs[bid]; ok {
		for _, loc := range locs {
			err = apply(ctx, loc, types.TS{}, rowsOffset, &left, mask)
			if err != nil {
				return
			}
		}
	}

	if loc, ok := tomb.blk2DeltaLoc[bid]; ok {
		cts, ok := tomb.blk2CommitTS[bid]
		if !ok {
			panic("commit ts not found")
		}
		err = apply(ctx, loc, cts, rowsOffset, &left, mask)
		if err != nil {
			return
		}
	}
	return
}

func rowIdsToOffset(rowIds []types.Rowid, wantedType any) any {
	switch wantedType.(type) {
	case int32:
		var ret []int32
		for _, rowId := range rowIds {
			_, offset := rowId.Decode()
			ret = append(ret, int32(offset))
		}
		return ret

	case uint32:
		var ret []uint32
		for _, rowId := range rowIds {
			_, offset := rowId.Decode()
			ret = append(ret, uint32(offset))
		}
		return ret

	case uint64:
		var ret []uint64
		for _, rowId := range rowIds {
			_, offset := rowId.Decode()
			ret = append(ret, uint64(offset))
		}
		return ret

	case int64:
		var ret []int64
		for _, rowId := range rowIds {
			_, offset := rowId.Decode()
			ret = append(ret, int64(offset))
		}
		return ret
	}

	return nil
}

func (tomb *tombstoneDataWithDeltaLoc) Type() engine.TombstoneType {
	return tomb.typ
}

func (tomb *tombstoneDataWithDeltaLoc) Merge(other engine.Tombstoner) error {
	if v, ok := other.(*tombstoneDataWithDeltaLoc); ok {
		tomb.inMemTombstones = append(tomb.inMemTombstones, v.inMemTombstones...)
		tomb.committedDeltalocs = append(tomb.committedDeltalocs, v.committedDeltalocs...)
		tomb.uncommittedDeltaLocs = append(tomb.uncommittedDeltaLocs, v.uncommittedDeltaLocs...)
		tomb.commitTS = append(tomb.commitTS, v.commitTS...)
	}
	return moerr.NewInternalErrorNoCtx("tombstone type mismatch")
}

func UnmarshalRelationData(data []byte) (engine.RelData, error) {
	typ := engine.RelDataType(data[0])
	switch typ {
	case engine.RelDataBlockList:
		relData := buildRelationDataV1()
		if err := relData.UnMarshal(data); err != nil {
			return nil, err
		}
		return relData, nil
	default:
		return nil, moerr.NewInternalErrorNoCtx("unsupported relation data type")
	}
}

var _ engine.RelData = new(blockListRelData)

type blockListRelData struct {
	typ engine.RelDataType
	//blkList[0] is a empty block info
	//blkList []*objectio.BlockInfoInProgress
	blklist *objectio.BlockInfoSliceInProgress

	//marshal tombstones if isEmpty is false, otherwise don't need to marshal tombstones
	isEmpty      bool
	tombstoneTyp engine.TombstoneType
	//tombstones
	tombstones engine.Tombstoner
}

func buildRelationDataV1() *blockListRelData {
	return &blockListRelData{
		typ:     engine.RelDataBlockList,
		blklist: &objectio.BlockInfoSliceInProgress{},
		isEmpty: true,
	}
}

func (relData *blockListRelData) GetShardIDList() []uint64 {
	panic("not supported")
}
func (relData *blockListRelData) GetShardID(i int) uint64 {
	panic("not supported")
}
func (relData *blockListRelData) SetShardID(i int, id uint64) {
	panic("not supported")
}
func (relData *blockListRelData) AppendShardID(id uint64) {
	panic("not supported")
}

func (relData *blockListRelData) GetBlockInfoSlice() objectio.BlockInfoSliceInProgress {
	return relData.blklist.GetAllBytes()
}

func (relData *blockListRelData) GetBlockInfo(i int) objectio.BlockInfoInProgress {
	return *relData.blklist.Get(i)
}

func (relData *blockListRelData) SetBlockInfo(i int, blk objectio.BlockInfoInProgress) {
	relData.blklist.Set(i, &blk)
}

func (relData *blockListRelData) AppendBlockInfo(blk objectio.BlockInfoInProgress) {
	relData.blklist.AppendBlockInfo(blk)
}

func (relData *blockListRelData) UnMarshal(data []byte) error {
	data = data[1:]

	sizeofblks := types.DecodeUint32(data)
	data = data[4:]

	*relData.blklist = data[:sizeofblks]
	data = data[sizeofblks:]

	isEmpty := types.DecodeBool(data)
	relData.isEmpty = isEmpty
	data = data[1:]

	if !isEmpty {
		tombstoneTyp := engine.TombstoneType(types.DecodeUint8(data))
		relData.tombstoneTyp = tombstoneTyp
		data = data[1:]

		size := types.DecodeUint32(data)
		data = data[4:]
		switch tombstoneTyp {
		case engine.TombstoneWithDeltaLoc:
			tombstoner := buildTombstoneWithDeltaLoc()
			if err := tombstoner.UnMarshal(data[:size]); err != nil {
				return err
			}
			relData.AttachTombstones(tombstoner)
		default:
			return moerr.NewInternalErrorNoCtx("unsupported tombstone type")
		}
	}

	return nil
}

func (relData *blockListRelData) MarshalWithBuf(w *bytes.Buffer) error {
	var pos2 uint32
	typ := uint8(relData.typ)
	if _, err := w.Write(types.EncodeUint8(&typ)); err != nil {
		return err
	}
	pos2 += 1

	sizeofblks := uint32(relData.blklist.Size())
	if _, err := w.Write(types.EncodeUint32(&sizeofblks)); err != nil {
		return err
	}
	pos2 += 4

	//marshal blk list
	if _, err := w.Write(*relData.blklist); err != nil {
		return err
	}
	pos2 += sizeofblks

	if _, err := w.Write(types.EncodeBool(&relData.isEmpty)); err != nil {
		return err
	}
	pos2 += 1

	if !relData.isEmpty {
		typ := uint8(relData.tombstoneTyp)
		if _, err := w.Write(types.EncodeUint8(&typ)); err != nil {
			return err
		}
		pos2 += 1

		var sizeOfTombstones uint32
		// reserve the space: 4 bytes for size of tombstones.
		if _, err := w.Write(types.EncodeUint32(&sizeOfTombstones)); err != nil {
			return err
		}

		space, err := relData.tombstones.MarshalWithBuf(w)
		if err != nil {
			return err
		}
		//update the size of tombstones.
		copy(w.Bytes()[pos2:pos2+4], types.EncodeUint32(&space))

	}
	return nil
}

func (relData *blockListRelData) GetType() engine.RelDataType {
	return relData.typ
}

func (relData *blockListRelData) MarshalToBytes() []byte {
	var w bytes.Buffer
	if err := relData.MarshalWithBuf(&w); err != nil {
		return nil
	}
	buf := w.Bytes()
	return buf
}

func (relData *blockListRelData) AttachTombstones(tombstones engine.Tombstoner) error {
	relData.tombstones = tombstones
	relData.tombstoneTyp = tombstones.Type()
	relData.isEmpty = tombstones.IsEmpty()
	return nil
}

func (relData *blockListRelData) GetTombstones() engine.Tombstoner {
	return relData.tombstones
}

func (relData *blockListRelData) DataSlice(i, j int) engine.RelData {
	blist := objectio.BlockInfoSliceInProgress(relData.blklist.Slice(i, j))
	return &blockListRelData{
		typ:          relData.typ,
		blklist:      &blist,
		isEmpty:      relData.isEmpty,
		tombstoneTyp: relData.tombstoneTyp,
		tombstones:   relData.tombstones,
	}
}

func (relData *blockListRelData) GroupByPartitionNum() map[int16]engine.RelData {
	ret := make(map[int16]engine.RelData)

	blks := relData.GetBlockInfoSlice()
	blksLen := blks.Len()
	for idx := range blksLen {
		blkInfo := blks.Get(idx)
		if blkInfo.IsMemBlk() {
			return nil
		}
		partitionNum := blkInfo.PartitionNum
		if _, ok := ret[partitionNum]; !ok {
			ret[partitionNum] = &blockListRelData{
				typ:          relData.typ,
				isEmpty:      relData.isEmpty,
				tombstoneTyp: relData.tombstoneTyp,
				tombstones:   relData.tombstones,
			}
			ret[partitionNum].AppendBlockInfo(objectio.EmptyBlockInfoInProgress)
		}
		ret[partitionNum].AppendBlockInfo(*blkInfo)
	}

	return ret
}

func (relData *blockListRelData) BuildEmptyRelData() engine.RelData {
	return &blockListRelData{
		blklist: &objectio.BlockInfoSliceInProgress{},
		typ:     relData.typ,
		isEmpty: true,
	}
}

func (relData *blockListRelData) DataCnt() int {
	return relData.blklist.Len()
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

	if rs.cursor >= rs.data.DataCnt() {
		return nil, engine.End, nil
	}
	rs.cursor++
	cur := rs.data.GetBlockInfo(rs.cursor - 1)
	return &cur, engine.Persisted, nil
}

func (rs *RemoteDataSource) Close() {

}

func (rs *RemoteDataSource) applyInMemTombstones(
	bid types.Blockid,
	rowsOffset []int32,
	deletedRows *nulls.Nulls,
) (leftRows []int32) {
	if rs.data.GetTombstones() == nil {
		return rowsOffset
	}
	return rs.data.GetTombstones().ApplyInMemTombstones(
		bid,
		rowsOffset,
		deletedRows)
}

func (rs *RemoteDataSource) applyUncommitDeltaLoc(
	ctx context.Context,
	bid types.Blockid,
	rowsOffset []int32,
	deletedRows *nulls.Nulls,
) (leftRows []int32, err error) {

	applyUncommit := func(
		ctx context.Context,
		loc objectio.Location,
		_ types.TS,
		rowsOffset []int32,
		left *[]int32,
		deleted *nulls.Nulls) (err error) {
		*left, err = applyDeletesWithinDeltaLocations(ctx, rs.fs, bid, rs.ts, types.TS{}, rowsOffset, deleted, loc)
		return err
	}

	if rs.data.GetTombstones() == nil {
		return rowsOffset, nil
	}
	return rs.data.GetTombstones().ApplyPersistedTombstones(
		ctx,
		bid,
		rowsOffset,
		deletedRows,
		applyUncommit)
}

func (rs *RemoteDataSource) applyCommittedDeltaLoc(
	ctx context.Context,
	bid types.Blockid,
	rowsOffset []int32,
	deletedRows *nulls.Nulls,
) (leftRows []int32, err error) {

	applyCommit := func(
		ctx context.Context,
		loc objectio.Location,
		cts types.TS,
		rowsOffset []int32,
		left *[]int32,
		deleted *nulls.Nulls) error {

		*left, err = applyDeletesWithinDeltaLocations(ctx, rs.fs, bid, rs.ts, cts, rowsOffset, deleted, loc[:])
		return err
	}

	if rs.data.GetTombstones() == nil {
		return rowsOffset, nil
	}
	return rs.data.GetTombstones().ApplyPersistedTombstones(
		ctx,
		bid,
		rowsOffset,
		deletedRows,
		applyCommit)

}

func (rs *RemoteDataSource) ApplyTombstonesInProgress(
	ctx context.Context,
	bid objectio.Blockid,
	rowsOffset []int32,
) ([]int32, error) {

	slices.SortFunc(rowsOffset, func(a, b int32) int {
		return int(a - b)
	})

	var err error
	var mask nulls.Nulls
	mask.InitWithSize(8192)

	rowsOffset = rs.applyInMemTombstones(bid, rowsOffset, nil)
	rowsOffset, err = rs.applyUncommitDeltaLoc(ctx, bid, rowsOffset, nil)
	if err != nil {
		return nil, err
	}
	rowsOffset, err = rs.applyCommittedDeltaLoc(ctx, bid, rowsOffset, nil)
	if err != nil {
		return nil, err
	}
	return rowsOffset, nil
}

func (rs *RemoteDataSource) GetTombstonesInProgress(
	ctx context.Context, bid objectio.Blockid,
) (mask *nulls.Nulls, err error) {

	mask = &nulls.Nulls{}
	mask.InitWithSize(8192)

	rs.applyInMemTombstones(bid, nil, mask)

	_, err = rs.applyUncommitDeltaLoc(ctx, bid, nil, mask)
	if err != nil {
		return
	}

	_, err = rs.applyCommittedDeltaLoc(ctx, bid, nil, mask)
	if err != nil {
		return
	}

	return mask, nil
}

func (rs *RemoteDataSource) SetOrderBy(orderby []*plan.OrderBySpec) {
	panic("Not Support order by")
}

func (rs *RemoteDataSource) GetOrderBy() []*plan.OrderBySpec {
	panic("Not Support order by")
}

func (rs *RemoteDataSource) SetFilterZM(zm objectio.ZoneMap) {
	panic("Not Support order by")
}

// local data source

type LocalDataSource struct {
	ranges []*objectio.BlockInfoInProgress
	pState *logtailreplay.PartitionState

	memPKFilter *MemPKFilter
	pStateRows  struct {
		insIter logtailreplay.RowsIter
	}

	table     *txnTable
	wsCursor  int
	txnOffset int

	// runtime config
	rc struct {
		batchPrefetchCursor int
		WorkspaceLocked     bool
		SkipPStateDeletes   bool
	}

	mp  *mpool.MPool
	ctx context.Context
	fs  fileservice.FileService

	rangesCursor int
	snapshotTS   types.TS
	iteratePhase engine.DataState

	//TODO:: It's so ugly, need to refactor
	//for order by
	desc     bool
	blockZMS []index.ZM
	sorted   bool // blks need to be sorted by zonemap
	OrderBy  []*plan.OrderBySpec

	filterZM objectio.ZoneMap
}

func NewLocalDataSource(
	ctx context.Context,
	table *txnTable,
	txnOffset int,
	rangesSlice objectio.BlockInfoSliceInProgress,
	skipReadMem bool,
) (source *LocalDataSource, err error) {

	source = &LocalDataSource{}
	source.fs = table.getTxn().engine.fs
	source.ctx = ctx
	source.mp = table.proc.Load().Mp()

	if rangesSlice != nil && rangesSlice.Len() > 0 {
		if bytes.Equal(
			objectio.EncodeBlockInfoInProgress(*rangesSlice.Get(0)),
			objectio.EmptyBlockInfoInProgressBytes) {
			rangesSlice = rangesSlice.Slice(1, rangesSlice.Len())
		}

		rangeLen := rangesSlice.Len()
		for i := 0; i < rangeLen; i++ {
			source.ranges = append(source.ranges, rangesSlice.Get(i))
		}
	}

	state, err := table.getPartitionState(ctx)
	if err != nil {
		return nil, err
	}

	source.table = table
	source.pState = state
	source.txnOffset = txnOffset
	source.snapshotTS = types.TimestampToTS(table.getTxn().op.SnapshotTS())

	source.iteratePhase = engine.InMem
	if skipReadMem {
		source.iteratePhase = engine.Persisted
	}

	return source, nil
}

func (ls *LocalDataSource) SetOrderBy(orderby []*plan.OrderBySpec) {
	ls.OrderBy = orderby
}

func (ls *LocalDataSource) GetOrderBy() []*plan.OrderBySpec {
	return ls.OrderBy
}

func (ls *LocalDataSource) SetFilterZM(zm objectio.ZoneMap) {
	if !ls.filterZM.IsInited() {
		ls.filterZM = zm.Clone()
		return
	}
	if ls.desc && ls.filterZM.CompareMax(zm) < 0 {
		ls.filterZM = zm.Clone()
		return
	}
	if !ls.desc && ls.filterZM.CompareMin(zm) > 0 {
		ls.filterZM = zm.Clone()
		return
	}
}

func (ls *LocalDataSource) needReadBlkByZM(i int) bool {
	zm := ls.blockZMS[i]
	if !ls.filterZM.IsInited() || !zm.IsInited() {
		return true
	}
	if ls.desc {
		return ls.filterZM.CompareMax(zm) <= 0
	} else {
		return ls.filterZM.CompareMin(zm) >= 0
	}
}

func (ls *LocalDataSource) getBlockZMs() {
	orderByCol, _ := ls.OrderBy[0].Expr.Expr.(*plan.Expr_Col)

	def := ls.table.tableDef
	orderByColIDX := int(def.Cols[int(orderByCol.Col.ColPos)].Seqnum)

	ls.blockZMS = make([]index.ZM, len(ls.ranges))
	var objDataMeta objectio.ObjectDataMeta
	var location objectio.Location
	for i := range ls.ranges {
		location = ls.ranges[i].MetaLocation()
		if !objectio.IsSameObjectLocVsMeta(location, objDataMeta) {
			objMeta, err := objectio.FastLoadObjectMeta(ls.ctx, &location, false, ls.fs)
			if err != nil {
				panic("load object meta error when ordered scan!")
			}
			objDataMeta = objMeta.MustDataMeta()
		}
		blkMeta := objDataMeta.GetBlockMeta(uint32(location.ID()))
		ls.blockZMS[i] = blkMeta.ColumnMeta(uint16(orderByColIDX)).ZoneMap()
	}
}

func (ls *LocalDataSource) sortBlockList() {
	helper := make([]*blockSortHelperInProgress, len(ls.ranges))
	for i := range ls.ranges {
		helper[i] = &blockSortHelperInProgress{}
		helper[i].blk = ls.ranges[i]
		helper[i].zm = ls.blockZMS[i]
	}
	if ls.desc {
		sort.Slice(helper, func(i, j int) bool {
			zm1 := helper[i].zm
			if !zm1.IsInited() {
				return true
			}
			zm2 := helper[j].zm
			if !zm2.IsInited() {
				return false
			}
			return zm1.CompareMax(zm2) > 0
		})
	} else {
		sort.Slice(helper, func(i, j int) bool {
			zm1 := helper[i].zm
			if !zm1.IsInited() {
				return true
			}
			zm2 := helper[j].zm
			if !zm2.IsInited() {
				return false
			}
			return zm1.CompareMin(zm2) < 0
		})
	}

	for i := range helper {
		ls.ranges[i] = helper[i].blk
		ls.blockZMS[i] = helper[i].zm
	}
}

func (ls *LocalDataSource) deleteFirstNBlocks(n int) {
	ls.ranges = ls.ranges[n:]
	if len(ls.OrderBy) > 0 {
		ls.blockZMS = ls.blockZMS[n:]
	}
}

func (ls *LocalDataSource) Close() {
	ls.pStateRows.insIter.Close()
}

func (ls *LocalDataSource) Next(
	ctx context.Context,
	cols []string,
	types []types.Type,
	seqNums []uint16,
	filter any,
	mp *mpool.MPool,
	vp engine.VectorPool,
	bat *batch.Batch,
) (*objectio.BlockInfoInProgress, engine.DataState, error) {

	if ls.memPKFilter == nil {
		ff := filter.(MemPKFilter)
		ls.memPKFilter = &ff
	}

	if len(cols) == 0 {
		return nil, engine.End, nil
	}

	// bathed prefetch block data and deletes
	ls.batchPrefetch(seqNums)

	for {
		switch ls.iteratePhase {
		case engine.InMem:
			err := ls.iterateInMemData(ctx, cols, types, seqNums, bat, mp, vp)
			if err != nil {
				return nil, engine.InMem, err
			}

			if bat.RowCount() == 0 {
				ls.iteratePhase = engine.Persisted
				continue
			}

			return nil, engine.InMem, nil

		case engine.Persisted:
			if len(ls.ranges) == 0 {
				return nil, engine.End, nil
			}

			ls.handleOrderBy()

			if len(ls.ranges) == 0 {
				return nil, engine.End, nil
			}

			blk := ls.ranges[0]
			ls.deleteFirstNBlocks(1)

			return blk, engine.Persisted, nil

		case engine.End:
			return nil, ls.iteratePhase, nil
		}
	}
}

func (ls *LocalDataSource) handleOrderBy() {
	// for ordered scan, sort blocklist by zonemap info, and then filter by zonemap
	if len(ls.OrderBy) > 0 {
		if !ls.sorted {
			ls.desc = ls.OrderBy[0].Flag&plan.OrderBySpec_DESC != 0
			ls.getBlockZMs()
			ls.sortBlockList()
			ls.sorted = true
		}
		i := 0
		for i < len(ls.ranges) {
			if ls.needReadBlkByZM(i) {
				break
			}
			i++
		}
		ls.deleteFirstNBlocks(i)

		if ls.table.tableName == "statement_info" {
			logutil.Infof("xxxx txn:%s, handle order by,delete blks:%d, rest blks:%d",
				ls.table.db.op.Txn().DebugString(),
				i,
				len(ls.ranges))
		}
	}
}

func (ls *LocalDataSource) iterateInMemData(
	ctx context.Context,
	cols []string,
	colTypes []types.Type,
	seqNums []uint16,
	bat *batch.Batch,
	mp *mpool.MPool,
	vp engine.VectorPool,
) (err error) {

	bat.SetRowCount(0)

	if err = ls.filterInMemUnCommittedInserts(seqNums, mp, bat); err != nil {
		return err
	}

	if err = ls.filterInMemCommittedInserts(colTypes, seqNums, mp, bat); err != nil {
		return err
	}

	return nil
}

func checkWorkspaceEntryType(tbl *txnTable, entry Entry) int {
	if entry.DatabaseId() != tbl.db.databaseId || entry.TableId() != tbl.tableId {
		return -1
	}

	if entry.typ == INSERT || entry.typ == INSERT_TXN {
		if entry.bat == nil || entry.bat.IsEmpty() {
			return -1
		}
		if entry.bat.Attrs[0] == catalog.BlockMeta_MetaLoc {
			return -1
		}

		return INSERT
	}

	if entry.IsGeneratedByTruncate() {
		return -1
	}

	if (entry.typ == DELETE || entry.typ == DELETE_TXN) && entry.fileName == "" {
		return DELETE
	}

	return -1
}

func (ls *LocalDataSource) filterInMemUnCommittedInserts(
	seqNums []uint16,
	mp *mpool.MPool,
	bat *batch.Batch,
) error {

	if ls.wsCursor >= ls.txnOffset {
		return nil
	}

	ls.table.getTxn().Lock()
	ls.rc.WorkspaceLocked = true
	defer func() {
		ls.table.getTxn().Unlock()
		ls.rc.WorkspaceLocked = false
	}()

	rows := 0
	writes := ls.table.getTxn().writes
	maxRows := int(options.DefaultBlockMaxRows)
	if len(writes) == 0 {
		return nil
	}

	for ; ls.wsCursor < ls.txnOffset &&
		rows+writes[ls.wsCursor].bat.RowCount() <= maxRows; ls.wsCursor++ {
		entry := ls.table.getTxn().writes[ls.wsCursor]

		if checkWorkspaceEntryType(ls.table, entry) != INSERT {
			continue
		}

		insRowIDs := vector.MustFixedCol[types.Rowid](entry.bat.Vecs[0])
		offsets := rowIdsToOffset(insRowIDs, int32(0)).([]int32)

		b, _ := insRowIDs[0].Decode()
		sels, err := ls.ApplyTombstonesInProgress(ls.ctx, b, offsets)
		if err != nil {
			return err
		}

		if len(sels) == 0 {
			continue
		}

		rows += len(sels)

		for i, destVec := range bat.Vecs {
			uf := vector.GetUnionOneFunction(*destVec.GetType(), mp)

			colIdx := int(seqNums[i])
			if colIdx != objectio.SEQNUM_ROWID {
				colIdx++
			} else {
				colIdx = 0
			}

			for j := range sels {
				if err = uf(destVec, entry.bat.Vecs[colIdx], int64(j)); err != nil {
					return err
				}
			}
		}
	}

	bat.SetRowCount(bat.Vecs[0].Length())
	return nil
}

func (ls *LocalDataSource) filterInMemCommittedInserts(
	colTypes []types.Type, seqNums []uint16, mp *mpool.MPool, bat *batch.Batch,
) error {

	// in meme committed insert only need to apply deletes that exists
	// in workspace and flushed to s3 but not commit.
	ls.rc.SkipPStateDeletes = true
	defer func() {
		ls.rc.SkipPStateDeletes = false
	}()

	if bat.RowCount() >= int(options.DefaultBlockMaxRows) {
		return nil
	}

	var (
		err          error
		sel          []int32
		appendedRows = bat.RowCount()
	)

	appendFunctions := make([]func(*vector.Vector, *vector.Vector, int64) error, len(bat.Attrs))
	for i := range bat.Attrs {
		appendFunctions[i] = vector.GetUnionOneFunction(colTypes[i], mp)
	}

	if ls.pStateRows.insIter == nil {
		if ls.memPKFilter.SpecFactory == nil {
			ls.pStateRows.insIter = ls.pState.NewRowsIter(ls.snapshotTS, nil, false)
		} else {
			ls.pStateRows.insIter = ls.pState.NewPrimaryKeyIter(
				ls.memPKFilter.TS, ls.memPKFilter.SpecFactory(ls.memPKFilter))
		}
	}

	for appendedRows < int(options.DefaultBlockMaxRows) && ls.pStateRows.insIter.Next() {
		entry := ls.pStateRows.insIter.Entry()
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

	bat.SetRowCount(bat.Vecs[0].Length())

	return nil
}

func loadBlockDeletesByDeltaLoc(
	ctx context.Context,
	fs fileservice.FileService,
	blockId types.Blockid,
	deltaLoc objectio.Location,
	snapshotTS, blockCommitTS types.TS,
) (deleteMask *nulls.Nulls, err error) {

	var (
		rows *nulls.Nulls
		//bisect           time.Duration
		release          func()
		persistedByCN    bool
		persistedDeletes *batch.Batch
	)

	if !deltaLoc.IsEmpty() {
		//t1 := time.Now()

		if persistedDeletes, persistedByCN, release, err = blockio.ReadBlockDelete(ctx, deltaLoc, fs); err != nil {
			return nil, err
		}
		defer release()

		//readCost := time.Since(t1)

		if persistedByCN {
			rows = blockio.EvalDeleteRowsByTimestampForDeletesPersistedByCN(persistedDeletes, snapshotTS, blockCommitTS)
		} else {
			//t2 := time.Now()
			rows = blockio.EvalDeleteRowsByTimestamp(persistedDeletes, snapshotTS, &blockId)
			//bisect = time.Since(t2)
		}

		if rows != nil {
			deleteMask = rows
		}

		//readTotal := time.Since(t1)
		//blockio.RecordReadDel(readTotal, readCost, bisect)
	}

	return deleteMask, nil
}

// ApplyTombstonesInProgress check if any deletes exist in
//  1. unCommittedInmemDeletes:
//     a. workspace writes
//     b. flushed to s3
//     c. raw rowId offset deletes (not flush yet)
//  3. committedInmemDeletes
//  4. committedPersistedTombstone
func (ls *LocalDataSource) ApplyTombstonesInProgress(
	ctx context.Context,
	bid objectio.Blockid,
	rowsOffset []int32,
) ([]int32, error) {

	slices.SortFunc(rowsOffset, func(a, b int32) int {
		return int(a - b)
	})

	var err error

	rowsOffset = ls.applyWorkspaceEntryDeletes(bid, rowsOffset, nil)
	rowsOffset, err = ls.applyWorkspaceFlushedS3Deletes(bid, rowsOffset, nil)
	if err != nil {
		return nil, err
	}

	rowsOffset = ls.applyWorkspaceRawRowIdDeletes(bid, rowsOffset, nil)
	rowsOffset = ls.applyPStateInMemDeletes(bid, rowsOffset, nil)
	rowsOffset, err = ls.applyPStatePersistedDeltaLocation(bid, rowsOffset, nil)
	if err != nil {
		return nil, err
	}

	return rowsOffset, nil
}

func (ls *LocalDataSource) GetTombstonesInProgress(
	ctx context.Context, bid objectio.Blockid,
) (deletedRows *nulls.Nulls, err error) {

	deletedRows = &nulls.Nulls{}
	deletedRows.InitWithSize(8192)

	ls.applyWorkspaceEntryDeletes(bid, nil, deletedRows)
	_, err = ls.applyWorkspaceFlushedS3Deletes(bid, nil, deletedRows)
	if err != nil {
		return nil, err
	}

	ls.applyWorkspaceRawRowIdDeletes(bid, nil, deletedRows)
	ls.applyPStateInMemDeletes(bid, nil, deletedRows)

	_, err = ls.applyPStatePersistedDeltaLocation(bid, nil, deletedRows)
	if err != nil {
		return nil, err
	}

	return deletedRows, nil
}

// will return the rows which applied deletes if the `leftRows` is not empty,
// or the deletes will only record into the `deleteRows` bitmap.
func fastApplyDeletedRows(
	leftRows []int32,
	deletedRows *nulls.Nulls,
	o uint32,
) []int32 {
	if len(leftRows) != 0 {
		if x, found := sort.Find(len(leftRows), func(i int) int {
			return int(int32(o) - leftRows[i])
		}); found {
			leftRows = append(leftRows[:x], leftRows[x+1:]...)
		}
	} else if deletedRows != nil {
		deletedRows.Add(uint64(o))
	}

	return leftRows
}

func (ls *LocalDataSource) applyWorkspaceEntryDeletes(
	bid objectio.Blockid,
	offsets []int32,
	deletedRows *nulls.Nulls,
) (leftRows []int32) {

	leftRows = offsets

	// may have locked in `filterInMemUnCommittedInserts`
	if !ls.rc.WorkspaceLocked {
		ls.table.getTxn().Lock()
		defer ls.table.getTxn().Unlock()
	}

	done := false
	writes := ls.table.getTxn().writes[:ls.txnOffset]

	for idx := range writes {
		if checkWorkspaceEntryType(ls.table, writes[idx]) != DELETE {
			continue
		}

		delRowIds := vector.MustFixedCol[types.Rowid](writes[idx].bat.Vecs[0])
		for _, delRowId := range delRowIds {
			b, o := delRowId.Decode()
			if bid.Compare(b) != 0 {
				continue
			}

			leftRows = fastApplyDeletedRows(leftRows, deletedRows, o)
			if leftRows != nil && len(leftRows) == 0 {
				done = true
				break
			}
		}

		if done {
			break
		}
	}

	return leftRows
}

// if blks comes from unCommitted flushed s3 deletes, the
// blkCommitTS can be zero.
func applyDeletesWithinDeltaLocations(
	ctx context.Context,
	fs fileservice.FileService,
	bid objectio.Blockid,
	snapshotTS types.TS,
	blkCommitTS types.TS,
	offsets []int32,
	deletedRows *nulls.Nulls,
	locations ...objectio.Location,
) (leftRows []int32, err error) {

	if offsets != nil {
		leftRows = make([]int32, 0, len(offsets))
	}

	var mask *nulls.Nulls

	for _, loc := range locations {
		if mask, err = loadBlockDeletesByDeltaLoc(
			ctx, fs, bid, loc[:], snapshotTS, blkCommitTS); err != nil {
			return nil, err
		}

		if offsets != nil {
			for _, offset := range offsets {
				if mask.Contains(uint64(offset)) {
					continue
				}
				leftRows = append(leftRows, offset)
			}
		} else if deletedRows != nil {
			deletedRows.Merge(mask)
		}
	}

	return leftRows, nil
}

func (ls *LocalDataSource) applyWorkspaceFlushedS3Deletes(
	bid objectio.Blockid,
	offsets []int32,
	deletedRows *nulls.Nulls,
) (leftRows []int32, err error) {

	leftRows = offsets
	var locations []objectio.Location

	// cannot hold the lock too long
	{
		s3FlushedDeletes := &ls.table.getTxn().blockId_tn_delete_metaLoc_batch
		s3FlushedDeletes.RWMutex.Lock()
		defer s3FlushedDeletes.RWMutex.Unlock()

		if len(s3FlushedDeletes.data[bid]) == 0 || ls.pState.BlockPersisted(bid) {
			return
		}

		locations = make([]objectio.Location, 0, len(s3FlushedDeletes.data[bid]))

		for _, bat := range s3FlushedDeletes.data[bid] {
			vs, area := vector.MustVarlenaRawData(bat.GetVector(0))
			for i := range vs {
				loc, err := blockio.EncodeLocationFromString(vs[i].UnsafeGetString(area))
				if err != nil {
					return nil, err
				}

				locations = append(locations, loc)
			}
		}
	}

	return applyDeletesWithinDeltaLocations(
		ls.ctx,
		ls.fs,
		bid,
		ls.snapshotTS,
		types.TS{},
		offsets,
		deletedRows,
		locations...)
}

func (ls *LocalDataSource) applyWorkspaceRawRowIdDeletes(
	bid objectio.Blockid,
	offsets []int32,
	deletedRows *nulls.Nulls,
) (leftRows []int32) {

	leftRows = offsets

	rawRowIdDeletes := ls.table.getTxn().deletedBlocks
	rawRowIdDeletes.RWMutex.RLock()
	defer rawRowIdDeletes.RWMutex.RUnlock()

	for _, o := range rawRowIdDeletes.offsets[bid] {
		leftRows = fastApplyDeletedRows(leftRows, deletedRows, uint32(o))
		if leftRows != nil && len(leftRows) == 0 {
			break
		}
	}

	return leftRows
}

func (ls *LocalDataSource) applyPStateInMemDeletes(
	bid objectio.Blockid,
	offsets []int32,
	deletedRows *nulls.Nulls,
) (leftRows []int32) {

	if ls.rc.SkipPStateDeletes {
		return offsets
	}

	var delIter logtailreplay.RowsIter

	if ls.memPKFilter == nil || ls.memPKFilter.SpecFactory == nil {
		delIter = ls.pState.NewRowsIter(ls.snapshotTS, &bid, true)
	} else {
		delIter = ls.pState.NewPrimaryKeyDelIter(
			ls.memPKFilter.TS,
			ls.memPKFilter.SpecFactory(ls.memPKFilter), bid)
	}

	leftRows = offsets

	for delIter.Next() {
		_, o := delIter.Entry().RowID.Decode()
		leftRows = fastApplyDeletedRows(leftRows, deletedRows, o)
		if leftRows != nil && len(leftRows) == 0 {
			break
		}
	}

	delIter.Close()

	return leftRows
}

func (ls *LocalDataSource) applyPStatePersistedDeltaLocation(
	bid objectio.Blockid,
	offsets []int32,
	deletedRows *nulls.Nulls,
) (leftRows []int32, err error) {

	if ls.rc.SkipPStateDeletes {
		return offsets, nil
	}

	deltaLoc, commitTS, ok := ls.pState.GetBockDeltaLoc(bid)
	if !ok {
		return offsets, nil
	}

	return applyDeletesWithinDeltaLocations(
		ls.ctx,
		ls.fs,
		bid,
		ls.snapshotTS,
		commitTS,
		offsets,
		deletedRows,
		deltaLoc[:])
}

func (ls *LocalDataSource) batchPrefetch(seqNums []uint16) {
	if ls.rc.batchPrefetchCursor >= len(ls.ranges) ||
		ls.rangesCursor < ls.rc.batchPrefetchCursor {
		return
	}

	batchSize := min(1000, len(ls.ranges)-ls.rc.batchPrefetchCursor)

	begin := ls.rc.batchPrefetchCursor
	end := ls.rc.batchPrefetchCursor + batchSize

	// prefetch blk data
	err := blockio.BlockPrefetchInProgress(
		ls.table.proc.Load().GetService(), seqNums, ls.fs,
		[][]*objectio.BlockInfoInProgress{ls.ranges[begin:end]}, true)
	if err != nil {
		logutil.Errorf("pefetch block data: %s", err.Error())
	}

	// prefetch blk delta location
	for idx := begin; idx < end; idx++ {
		if loc, _, ok := ls.pState.GetBockDeltaLoc(ls.ranges[idx].BlockID); ok {
			if err = blockio.PrefetchTombstone(
				ls.table.proc.Load().GetService(), []uint16{0, 1, 2},
				[]uint16{objectio.Location(loc[:]).ID()}, ls.fs, objectio.Location(loc[:])); err != nil {
				logutil.Errorf("prefetch block delta location: %s", err.Error())
			}
		}
	}

	ls.table.getTxn().blockId_tn_delete_metaLoc_batch.RLock()
	defer ls.table.getTxn().blockId_tn_delete_metaLoc_batch.RUnlock()

	// prefetch cn flushed but not committed deletes
	var ok bool
	var bats []*batch.Batch
	var locs []objectio.Location = make([]objectio.Location, 0)

	pkColIdx := ls.table.tableDef.Pkey.PkeyColId

	for idx := begin; idx < end; idx++ {
		if bats, ok = ls.table.getTxn().blockId_tn_delete_metaLoc_batch.data[ls.ranges[idx].BlockID]; !ok {
			continue
		}

		locs = locs[:0]
		for _, bat := range bats {
			vs, area := vector.MustVarlenaRawData(bat.GetVector(0))
			for i := range vs {
				location, err := blockio.EncodeLocationFromString(vs[i].UnsafeGetString(area))
				if err != nil {
					logutil.Errorf("prefetch cn flushed s3 deletes: %s", err.Error())
				}
				locs = append(locs, location)
			}
		}

		if len(locs) == 0 {
			continue
		}

		pref, err := blockio.BuildPrefetchParams(ls.fs, locs[0])
		if err != nil {
			logutil.Errorf("prefetch cn flushed s3 deletes: %s", err.Error())
		}

		for _, loc := range locs {
			//rowId + pk
			pref.AddBlockWithType([]uint16{0, uint16(pkColIdx)}, []uint16{loc.ID()}, uint16(objectio.SchemaTombstone))
		}

		if err = blockio.PrefetchWithMerged(ls.table.proc.Load().GetService(), pref); err != nil {
			logutil.Errorf("prefetch cn flushed s3 deletes: %s", err.Error())
		}
	}

	ls.rc.batchPrefetchCursor += batchSize
}
