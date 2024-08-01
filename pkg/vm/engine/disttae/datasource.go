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
	"fmt"
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
	//in memory tombstones
	inMemTombstones map[types.Blockid][]int32

	//persisted tombstones
	// uncommitted tombstones, written by CN, one block maybe respond to multi deltaLocs.
	blk2UncommitLoc map[types.Blockid][]objectio.Location
	//committed tombstones.
	blk2CommitLoc map[types.Blockid]logtailreplay.BlockDeltaInfo
}

func buildTombstoneWithDeltaLoc() *tombstoneDataWithDeltaLoc {
	return &tombstoneDataWithDeltaLoc{
		inMemTombstones: make(map[types.Blockid][]int32),
		blk2UncommitLoc: make(map[types.Blockid][]objectio.Location),
		blk2CommitLoc:   make(map[types.Blockid]logtailreplay.BlockDeltaInfo),
	}
}

func (tomb *tombstoneDataWithDeltaLoc) String() string {
	return tomb.StringWithPrefix("")
}

func (tomb *tombstoneDataWithDeltaLoc) StringWithPrefix(prefix string) string {
	var w bytes.Buffer
	w.WriteString(fmt.Sprintf("%sTombstone[%d]<\n", prefix, tomb.Type()))
	w.WriteString(fmt.Sprintf("\t%sInMemTombstones: \n", prefix))
	for bid, offsets := range tomb.inMemTombstones {
		w.WriteString(fmt.Sprintf("\t\t%sblk:%s, offsets:%v\n", prefix, bid.String(), offsets))
	}
	w.WriteString(fmt.Sprintf("\t%sBlk2UncommitLoc: \n", prefix))
	for bid, locs := range tomb.blk2UncommitLoc {
		w.WriteString(fmt.Sprintf("\t\t%sblk:%s, locs:%v\n", prefix, bid.String(), locs))
	}
	w.WriteString(fmt.Sprintf("\t%sBlk2CommitLoc: \n", prefix))
	for bid, loc := range tomb.blk2CommitLoc {
		w.WriteString(fmt.Sprintf("\t\t%sblk:%s, loc:%v, cts:%d\n", prefix, bid.String(), loc.Loc, loc.Cts))
	}
	w.WriteString(fmt.Sprintf("%s>\n", prefix))
	return w.String()
}

func (tomb *tombstoneDataWithDeltaLoc) HasTombstones() bool {
	if len(tomb.inMemTombstones) == 0 &&
		len(tomb.blk2UncommitLoc) == 0 &&
		len(tomb.blk2CommitLoc) == 0 {
		return false
	}
	return true
}

func (tomb *tombstoneDataWithDeltaLoc) UnmarshalBinary(buf []byte) error {
	typ := engine.TombstoneType(types.DecodeUint8(buf))
	if typ != engine.TombstoneWithDeltaLoc {
		return moerr.NewInternalErrorNoCtx("UnmarshalBinary TombstoneWithDeltaLoc with %v", typ)
	}
	buf = buf[1:]

	cnt := types.DecodeUint32(buf)
	buf = buf[4:]

	tomb.inMemTombstones = make(map[types.Blockid][]int32, int(cnt))
	for i := 0; i < int(cnt); i++ {
		bid := types.DecodeFixed[types.Blockid](buf[:types.BlockidSize])
		buf = buf[types.BlockidSize:]

		offsetLen := types.DecodeUint32(buf)
		buf = buf[4:]
		if offsetLen > 0 {
			tomb.inMemTombstones[bid] = types.DecodeSlice[int32](buf[:offsetLen])
			buf = buf[offsetLen:]
		}
	}

	cnt = types.DecodeUint32(buf)
	buf = buf[4:]
	tomb.blk2UncommitLoc = make(map[types.Blockid][]objectio.Location, int(cnt))
	for i := 0; i < int(cnt); i++ {
		bid := types.DecodeFixed[types.Blockid](buf[:types.BlockidSize])
		buf = buf[types.BlockidSize:]
		locLen := types.DecodeUint32(buf)
		buf = buf[4:]
		if locLen > 0 {
			locs := make([]objectio.Location, int(locLen)/objectio.LocationLen)
			for j := range locs {
				locs[j] = buf[:objectio.LocationLen]
				buf = buf[objectio.LocationLen:]
			}
			tomb.blk2UncommitLoc[bid] = locs
		}
	}

	cnt = types.DecodeUint32(buf)
	buf = buf[4:]
	tomb.blk2CommitLoc = make(map[types.Blockid]logtailreplay.BlockDeltaInfo, int(cnt))

	for i := 0; i < int(cnt); i++ {
		bid := types.DecodeFixed[types.Blockid](buf[:types.BlockidSize])
		buf = buf[types.BlockidSize:]

		loc := buf[:objectio.LocationLen]
		buf = buf[objectio.LocationLen:]

		cts := types.DecodeFixed[types.TS](buf[:types.TxnTsSize])
		buf = buf[types.TxnTsSize:]

		tomb.blk2CommitLoc[bid] = logtailreplay.BlockDeltaInfo{
			Cts: cts,
			Loc: loc,
		}
	}

	return nil
}

func (tomb *tombstoneDataWithDeltaLoc) MarshalBinaryWithBuffer(w *bytes.Buffer) (err error) {
	typ := uint8(tomb.Type())
	if _, err = w.Write(types.EncodeUint8(&typ)); err != nil {
		return
	}

	length := uint32(len(tomb.inMemTombstones))
	if _, err = w.Write(types.EncodeUint32(&length)); err != nil {
		return
	}

	w.Grow(int(length * types.BlockidSize))

	for bid, offsets := range tomb.inMemTombstones {
		if _, err = w.Write(bid[:]); err != nil {
			return
		}

		buf := types.EncodeSlice[int32](offsets)
		bufLen := uint32(len(buf))
		if _, err = w.Write(types.EncodeUint32(&bufLen)); err != nil {
			return
		}
		if _, err = w.Write(buf); err != nil {
			return
		}
	}

	length = uint32(len(tomb.blk2UncommitLoc))
	if _, err = w.Write(types.EncodeUint32(&length)); err != nil {
		return
	}

	w.Grow(int(length * types.BlockidSize))

	for bid, locs := range tomb.blk2UncommitLoc {
		if _, err = w.Write(bid[:]); err != nil {
			return
		}

		bufLen := uint32(len(locs) * objectio.LocationLen)
		if _, err = w.Write(types.EncodeUint32(&bufLen)); err != nil {
			return
		}
		for _, loc := range locs {
			if _, err = w.Write(loc[:]); err != nil {
				return
			}
		}
	}

	length = uint32(len(tomb.blk2CommitLoc))
	if _, err = w.Write(types.EncodeUint32(&length)); err != nil {
		return
	}
	w.Grow(int(length) * (types.BlockidSize + objectio.LocationLen + types.TxnTsSize))

	for bid, loc := range tomb.blk2CommitLoc {
		if _, err = w.Write(bid[:]); err != nil {
			return
		}

		if _, err = w.Write(loc.Loc[:]); err != nil {
			return
		}

		if _, err = w.Write(types.EncodeTxnTS(&loc.Cts)); err != nil {
			return
		}
	}

	return
}

func (tomb *tombstoneDataWithDeltaLoc) ApplyInMemTombstones(
	bid types.Blockid,
	rowsOffset []int32,
	deleted *nulls.Nulls,
) (left []int32) {
	left = rowsOffset

	if rowOffsets, ok := tomb.inMemTombstones[bid]; ok {
		for _, o := range rowOffsets {
			left = fastApplyDeletedRows(left, deleted, uint32(o))
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
		ctx context.Context,
		loc objectio.Location,
		cts types.TS,
		rowsOffset []int32,
		deleted *nulls.Nulls,
	) (left []int32, err error),
) (left []int32, err error) {

	if locs, ok := tomb.blk2UncommitLoc[bid]; ok {
		for _, loc := range locs {
			left, err = apply(ctx, loc, types.TS{}, rowsOffset, mask)
			if err != nil {
				return
			}
		}
	}

	if loc, ok := tomb.blk2CommitLoc[bid]; ok {
		left, err = apply(ctx, loc.Loc, loc.Cts, rowsOffset, mask)
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
	return engine.TombstoneWithDeltaLoc
}

func (tomb *tombstoneDataWithDeltaLoc) Merge(other engine.Tombstoner) error {
	if v, ok := other.(*tombstoneDataWithDeltaLoc); ok {
		for blkID, rows := range v.inMemTombstones {
			tomb.inMemTombstones[blkID] = append(tomb.inMemTombstones[blkID], rows...)
		}
		for blkID, locs := range v.blk2UncommitLoc {
			tomb.blk2UncommitLoc[blkID] = append(tomb.blk2UncommitLoc[blkID], locs...)
		}
		for blkID, loc := range v.blk2CommitLoc {
			tomb.blk2CommitLoc[blkID] = loc
		}
	}
	return moerr.NewInternalErrorNoCtx("tombstone type mismatch")
}

func UnmarshalTombstoneData(data []byte) (engine.Tombstoner, error) {
	typ := engine.TombstoneType(data[0])
	switch typ {
	case engine.TombstoneWithDeltaLoc:
		tomb := buildTombstoneWithDeltaLoc()
		if err := tomb.UnmarshalBinary(data); err != nil {
			return nil, err
		}
		return tomb, nil
	default:
		return nil, moerr.NewInternalErrorNoCtx("unsupported tombstone type")
	}
}

func UnmarshalRelationData(data []byte) (engine.RelData, error) {
	typ := engine.RelDataType(data[0])
	switch typ {
	case engine.RelDataBlockList:
		relData := buildBlockListRelationData()
		if err := relData.UnmarshalBinary(data); err != nil {
			return nil, err
		}
		return relData, nil
	default:
		return nil, moerr.NewInternalErrorNoCtx("unsupported relation data type")
	}
}

var _ engine.RelData = new(blockListRelData)

type blockListRelData struct {
	//blkList[0] is a empty block info
	//blkList []*objectio.BlockInfoInProgress
	blklist objectio.BlockInfoSliceInProgress

	//tombstones
	tombstones engine.Tombstoner
}

func buildBlockListRelationData() *blockListRelData {
	return &blockListRelData{
		blklist: objectio.BlockInfoSliceInProgress{},
	}
}

func (relData *blockListRelData) String() string {
	var w bytes.Buffer
	w.WriteString(fmt.Sprintf("RelData[%d]<\n", relData.GetType()))
	if relData.blklist != nil {
		w.WriteString(fmt.Sprintf("\tBlockList: %s\n", relData.blklist.String()))
	} else {
		w.WriteString("\tBlockList: nil\n")
	}
	if relData.tombstones != nil {
		w.WriteString(relData.tombstones.StringWithPrefix("\t"))
	} else {
		w.WriteString("\tTombstones: nil\n")
	}
	return w.String()
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

func (relData *blockListRelData) UnmarshalBinary(data []byte) (err error) {
	typ := engine.RelDataType(types.DecodeUint8(data))
	if typ != engine.RelDataBlockList {
		return moerr.NewInternalErrorNoCtx("UnmarshalBinary RelDataBlockList with %v", typ)
	}
	data = data[1:]

	sizeofblks := types.DecodeUint32(data)
	data = data[4:]

	relData.blklist = data[:sizeofblks]
	data = data[sizeofblks:]

	tombstoneLen := types.DecodeUint32(data)
	data = data[4:]

	if tombstoneLen == 0 {
		return
	}

	relData.tombstones, err = UnmarshalTombstoneData(data[:tombstoneLen])
	return
}

func (relData *blockListRelData) MarshalBinaryWithBuffer(w *bytes.Buffer) (err error) {
	typ := uint8(relData.GetType())
	if _, err = w.Write(types.EncodeUint8(&typ)); err != nil {
		return
	}

	sizeofblks := uint32(relData.blklist.Size())
	if _, err = w.Write(types.EncodeUint32(&sizeofblks)); err != nil {
		return
	}

	// marshal blk list
	if _, err = w.Write(relData.blklist); err != nil {
		return
	}

	// marshal tombstones
	offset := w.Len()
	tombstoneLen := uint32(0)
	if _, err = w.Write(types.EncodeUint32(&tombstoneLen)); err != nil {
		return
	}
	if relData.tombstones != nil {
		if err = relData.tombstones.MarshalBinaryWithBuffer(w); err != nil {
			return
		}
		tombstoneLen = uint32(w.Len() - offset)
		buf := w.Bytes()
		copy(buf[offset:], types.EncodeUint32(&tombstoneLen))
	}
	return
}

func (relData *blockListRelData) GetType() engine.RelDataType {
	return engine.RelDataBlockList
}

func (relData *blockListRelData) MarshalBinary() ([]byte, error) {
	var w bytes.Buffer
	if err := relData.MarshalBinaryWithBuffer(&w); err != nil {
		return nil, err
	}
	buf := w.Bytes()
	return buf, nil
}

func (relData *blockListRelData) AttachTombstones(tombstones engine.Tombstoner) error {
	relData.tombstones = tombstones
	return nil
}

func (relData *blockListRelData) GetTombstones() engine.Tombstoner {
	return relData.tombstones
}

func (relData *blockListRelData) DataSlice(i, j int) engine.RelData {
	blist := objectio.BlockInfoSliceInProgress(relData.blklist.Slice(i, j))
	return &blockListRelData{
		blklist:    blist,
		tombstones: relData.tombstones,
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
				tombstones: relData.tombstones,
			}
			ret[partitionNum].AppendBlockInfo(objectio.EmptyBlockInfoInProgress)
		}
		ret[partitionNum].AppendBlockInfo(*blkInfo)
	}

	return ret
}

func (relData *blockListRelData) BuildEmptyRelData() engine.RelData {
	return &blockListRelData{
		blklist: objectio.BlockInfoSliceInProgress{},
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
	rs.cursor = 0
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

func (rs *RemoteDataSource) applyPersistedTombstones(
	ctx context.Context,
	bid types.Blockid,
	rowsOffset []int32,
	mask *nulls.Nulls,
) (leftRows []int32, err error) {

	apply := func(
		ctx context.Context,
		loc objectio.Location,
		cts types.TS,
		rowsOffset []int32,
		deleted *nulls.Nulls) (left []int32, err error) {

		deletes, err := loadBlockDeletesByDeltaLoc(ctx, rs.fs, bid, loc, rs.ts, cts)
		if err != nil {
			return nil, err
		}

		if rowsOffset != nil {
			for _, offset := range rowsOffset {
				if deletes.Contains(uint64(offset)) {
					continue
				}
				left = append(left, offset)
			}
		} else if deleted != nil {
			deleted.Merge(deletes)
		}

		return
	}

	if rs.data.GetTombstones() == nil {
		return rowsOffset, nil
	}
	return rs.data.GetTombstones().ApplyPersistedTombstones(
		ctx,
		bid,
		rowsOffset,
		mask,
		apply)
}

func (rs *RemoteDataSource) ApplyTombstonesInProgress(
	ctx context.Context,
	bid objectio.Blockid,
	rowsOffset []int32,
) (left []int32, err error) {

	slices.SortFunc(rowsOffset, func(a, b int32) int {
		return int(a - b)
	})

	left = rs.applyInMemTombstones(bid, rowsOffset, nil)

	left, err = rs.applyPersistedTombstones(ctx, bid, left, nil)
	if err != nil {
		return
	}
	return
}

func (rs *RemoteDataSource) GetTombstonesInProgress(
	ctx context.Context, bid objectio.Blockid,
) (mask *nulls.Nulls, err error) {

	mask = &nulls.Nulls{}
	mask.InitWithSize(8192)

	rs.applyInMemTombstones(bid, nil, mask)

	_, err = rs.applyPersistedTombstones(ctx, bid, nil, mask)
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
	rangeSlice objectio.BlockInfoSliceInProgress
	pState     *logtailreplay.PartitionState

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

	filterZM    objectio.ZoneMap
	checkPolicy SkipCheckPolicy
}

type SkipCheckPolicy uint64

const (
	SkipUncommitedInMemory = 1 << iota
	SkipCommittedInMemory
	SkipUncommitedS3
	SkipCommittedS3
)

const (
	CheckAll             = 0
	CheckCommittedS3Only = SkipUncommitedInMemory | SkipCommittedInMemory | SkipUncommitedS3
)

func NewLocalDataSource(
	ctx context.Context,
	table *txnTable,
	txnOffset int,
	rangesSlice objectio.BlockInfoSliceInProgress,
	skipReadMem bool,
	policy SkipCheckPolicy,
) (source *LocalDataSource, err error) {

	source = &LocalDataSource{}
	source.fs = table.getTxn().engine.fs
	source.ctx = ctx
	source.mp = table.proc.Load().Mp()
	source.checkPolicy = policy

	if rangesSlice != nil && rangesSlice.Len() > 0 {
		if bytes.Equal(
			objectio.EncodeBlockInfoInProgress(*rangesSlice.Get(0)),
			objectio.EmptyBlockInfoInProgressBytes) {
			rangesSlice = rangesSlice.Slice(1, rangesSlice.Len())
		}

		source.rangeSlice = rangesSlice
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

func (ls *LocalDataSource) String() string {
	blks := make([]*objectio.BlockInfoInProgress, ls.rangeSlice.Len())
	for i := range blks {
		blks[i] = ls.rangeSlice.Get(i)
	}

	return fmt.Sprintf("snapshot: %s, phase: %v, txnOffset: %d, rangeCursor: %d, blk list: %v",
		ls.snapshotTS.ToString(),
		ls.iteratePhase,
		ls.txnOffset,
		ls.rangesCursor,
		blks)
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

	sliceLen := ls.rangeSlice.Len()
	ls.blockZMS = make([]index.ZM, sliceLen)
	var objDataMeta objectio.ObjectDataMeta
	var location objectio.Location
	for i := ls.rangesCursor; i < sliceLen; i++ {
		location = ls.rangeSlice.Get(i).MetaLocation()
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
	sliceLen := ls.rangeSlice.Len()
	helper := make([]*blockSortHelperInProgress, sliceLen)
	for i := range sliceLen {
		helper[i] = &blockSortHelperInProgress{}
		helper[i].blk = ls.rangeSlice.Get(i)
		helper[i].zm = ls.blockZMS[i]
	}
	ls.rangeSlice = make(objectio.BlockInfoSliceInProgress, ls.rangeSlice.Size())

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
		ls.rangeSlice.Set(i, helper[i].blk)
		//ls.ranges[i] = helper[i].blk
		ls.blockZMS[i] = helper[i].zm
	}
}

func (ls *LocalDataSource) deleteFirstNBlocks(n int) {
	ls.rangesCursor += n
	//ls.rangeSlice = ls.rangeSlice.Slice(n, ls.rangeSlice.Len())
	//ls.ranges = ls.ranges[n:]
	//if len(ls.OrderBy) > 0 {
	//	ls.blockZMS = ls.blockZMS[n:]
	//}
}

func (ls *LocalDataSource) Close() {
	if ls.pStateRows.insIter != nil {
		ls.pStateRows.insIter.Close()
		ls.pStateRows.insIter = nil
	}
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
			if ls.rangesCursor >= ls.rangeSlice.Len() {
				return nil, engine.End, nil
			}

			ls.handleOrderBy()

			if ls.rangesCursor >= ls.rangeSlice.Len() {
				return nil, engine.End, nil
			}

			blk := ls.rangeSlice.Get(ls.rangesCursor)
			ls.rangesCursor++

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
		i := ls.rangesCursor
		sliceLen := ls.rangeSlice.Len()
		for i < sliceLen {
			if ls.needReadBlkByZM(i) {
				break
			}
			i++
		}
		ls.rangesCursor = i

		if ls.table.tableName == "statement_info" {
			logutil.Infof("xxxx txn:%s, handle order by,delete blks:%d, rest blks:%d",
				ls.table.db.op.Txn().DebugString(),
				i,
				ls.rangeSlice.Len()-ls.rangesCursor)
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

	if ls.checkPolicy&SkipUncommitedInMemory == 0 {
		ls.applyWorkspaceEntryDeletes(bid, nil, deletedRows)
	}
	if ls.checkPolicy&SkipUncommitedS3 == 0 {
		_, err = ls.applyWorkspaceFlushedS3Deletes(bid, nil, deletedRows)
		if err != nil {
			return nil, err
		}
	}

	if ls.checkPolicy&SkipUncommitedInMemory == 0 {
		ls.applyWorkspaceRawRowIdDeletes(bid, nil, deletedRows)
	}

	if ls.checkPolicy&SkipCommittedInMemory == 0 {
		ls.applyPStateInMemDeletes(bid, nil, deletedRows)
	}

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
	if ls.rc.batchPrefetchCursor >= ls.rangeSlice.Len() ||
		ls.rangesCursor < ls.rc.batchPrefetchCursor {
		return
	}

	batchSize := min(1000, ls.rangeSlice.Len()-ls.rangesCursor)

	begin := ls.rangesCursor
	end := ls.rangesCursor + batchSize

	blks := make([]*objectio.BlockInfoInProgress, end-begin)
	for idx := begin; idx < end; idx++ {
		blks[idx-begin] = ls.rangeSlice.Get(idx)
	}

	// prefetch blk data
	err := blockio.BlockPrefetch(
		ls.table.proc.Load().GetService(), seqNums, ls.fs, blks, true)
	if err != nil {
		logutil.Errorf("pefetch block data: %s", err.Error())
	}

	// prefetch blk delta location
	for idx := begin; idx < end; idx++ {
		if loc, _, ok := ls.pState.GetBockDeltaLoc(ls.rangeSlice.Get(idx).BlockID); ok {
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
		if bats, ok = ls.table.getTxn().blockId_tn_delete_metaLoc_batch.data[ls.rangeSlice.Get(idx).BlockID]; !ok {
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

	ls.rc.batchPrefetchCursor = end
}
