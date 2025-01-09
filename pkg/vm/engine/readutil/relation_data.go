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

package readutil

import (
	"bytes"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

var _ engine.RelData = new(BlockListRelData)
var _ engine.RelData = new(ObjListRelData)

func UnmarshalRelationData(data []byte) (engine.RelData, error) {
	typ := engine.RelDataType(data[0])
	switch typ {
	case engine.RelDataBlockList:
		relData := new(BlockListRelData)
		if err := relData.UnmarshalBinary(data); err != nil {
			return nil, err
		}
		return relData, nil
	case engine.RelDataEmpty:
		relData := BuildEmptyRelData()
		if err := relData.UnmarshalBinary(data); err != nil {
			return nil, err
		}
		return relData, nil
	default:
		return nil, moerr.NewInternalErrorNoCtx("unsupported relation data type")
	}
}

type EmptyRelationData struct {
	tombs engine.Tombstoner
}

func BuildEmptyRelData() engine.RelData {
	return &EmptyRelationData{}
}

func (rd *EmptyRelationData) String() string {
	return fmt.Sprintf("RelData[%d]", engine.RelDataEmpty)
}

func (rd *EmptyRelationData) GetShardIDList() []uint64 {
	panic("not supported")
}

func (rd *EmptyRelationData) Split(_ int) []engine.RelData {
	panic("not supported")
}

func (rd *EmptyRelationData) GetShardID(i int) uint64 {
	panic("not supported")
}

func (rd *EmptyRelationData) SetShardID(i int, id uint64) {
	panic("not supported")
}

func (rd *EmptyRelationData) AppendShardID(id uint64) {
	panic("not supported")
}

func (rd *EmptyRelationData) GetBlockInfoSlice() objectio.BlockInfoSlice {
	panic("not supported")
}

func (rd *EmptyRelationData) GetBlockInfo(i int) objectio.BlockInfo {
	panic("not supported")
}

func (rd *EmptyRelationData) SetBlockInfo(i int, blk *objectio.BlockInfo) {
	panic("not supported")
}

func (rd *EmptyRelationData) AppendBlockInfo(blk *objectio.BlockInfo) {
	panic("not supported")
}

func (rd *EmptyRelationData) AppendBlockInfoSlice(objectio.BlockInfoSlice) {
	panic("not supported")
}

func (rd *EmptyRelationData) GetType() engine.RelDataType {
	return engine.RelDataEmpty
}

func (rd *EmptyRelationData) MarshalBinaryWithBuffer(w *bytes.Buffer) (err error) {
	typ := uint8(rd.GetType())
	if _, err = w.Write(types.EncodeUint8(&typ)); err != nil {
		return
	}

	// marshal tombstones
	offset := w.Len()
	tombstoneLen := uint32(0)
	if _, err = w.Write(types.EncodeUint32(&tombstoneLen)); err != nil {
		return
	}
	if rd.tombs != nil {
		if err = rd.tombs.MarshalBinaryWithBuffer(w); err != nil {
			return
		}
		tombstoneLen = uint32(w.Len() - offset - 4)
		buf := w.Bytes()
		copy(buf[offset:], types.EncodeUint32(&tombstoneLen))
	}
	return nil
}
func (rd *EmptyRelationData) MarshalBinary() ([]byte, error) {
	var w bytes.Buffer
	if err := rd.MarshalBinaryWithBuffer(&w); err != nil {
		return nil, err
	}
	buf := w.Bytes()
	return buf, nil
}

func (rd *EmptyRelationData) UnmarshalBinary(data []byte) (err error) {
	typ := engine.RelDataType(types.DecodeUint8(data))
	if typ != engine.RelDataEmpty {
		return moerr.NewInternalErrorNoCtxf("UnmarshalBinary empty rel data with type:%v", typ)
	}
	data = data[1:]

	tombstoneLen := types.DecodeUint32(data)
	data = data[4:]

	if tombstoneLen == 0 {
		return
	}
	rd.tombs, err = UnmarshalTombstoneData(data[:tombstoneLen])
	return
}

func (rd *EmptyRelationData) AttachTombstones(tombstones engine.Tombstoner) error {
	rd.tombs = tombstones
	return nil
}

func (rd *EmptyRelationData) GetTombstones() engine.Tombstoner {
	return rd.tombs
}

func (rd *EmptyRelationData) ForeachDataBlk(begin, end int, f func(blk any) error) error {
	panic("Not Supported")
}

func (rd *EmptyRelationData) GetDataBlk(i int) any {
	panic("Not Supported")
}

func (rd *EmptyRelationData) SetDataBlk(i int, blk any) {
	panic("Not Supported")
}

func (rd *EmptyRelationData) DataSlice(begin, end int) engine.RelData {
	panic("Not Supported")
}

func (rd *EmptyRelationData) GroupByPartitionNum() map[int16]engine.RelData {
	panic("Not Supported")
}

func (rd *EmptyRelationData) AppendDataBlk(blk any) {
	panic("Not Supported")
}

func (rd *EmptyRelationData) BuildEmptyRelData(i int) engine.RelData {
	return &EmptyRelationData{}
}

func (rd *EmptyRelationData) DataCnt() int {
	return 0
}

// emptyCnt is the number of empty blocks preserved
func NewBlockListRelationData(emptyCnt int) *BlockListRelData {
	return &BlockListRelData{
		blklist: objectio.MakeBlockInfoSlice(emptyCnt),
	}
}

func NewBlockListRelationDataOfObject(
	obj *objectio.ObjectStats, withInMemory bool,
) *BlockListRelData {
	slice := objectio.ObjectStatsToBlockInfoSlice(
		obj, withInMemory,
	)
	return &BlockListRelData{
		blklist: slice,
	}
}

type ObjListRelData struct {
	NeedFirstEmpty   bool
	expanded         bool
	TotalBlocks      uint32
	Objlist          []objectio.ObjectStats
	Rsp              *engine.RangesShuffleParam
	blocklistRelData BlockListRelData
}

func (or *ObjListRelData) expand() {
	if !or.expanded {
		or.expanded = true
		or.blocklistRelData.blklist = objectio.MultiObjectStatsToBlockInfoSlice(or.Objlist, or.NeedFirstEmpty)
	}
}

func (or *ObjListRelData) AppendObj(obj *objectio.ObjectStats) {
	or.Objlist = append(or.Objlist, *obj)
	or.TotalBlocks += obj.BlkCnt()
}

func (or *ObjListRelData) GetType() engine.RelDataType {
	return engine.RelDataObjList
}

func (or *ObjListRelData) String() string {
	return "ObjListRelData"
}

func (or *ObjListRelData) GetShardIDList() []uint64 {
	panic("not supported")
}
func (or *ObjListRelData) GetShardID(i int) uint64 {
	panic("not supported")
}
func (or *ObjListRelData) SetShardID(i int, id uint64) {
	panic("not supported")
}
func (or *ObjListRelData) AppendShardID(id uint64) {
	panic("not supported")
}

func (or *ObjListRelData) Split(cpunum int) []engine.RelData {
	rsp := or.Rsp
	if len(or.Objlist) < cpunum || or.TotalBlocks < 64 || rsp == nil || !rsp.Node.Stats.HashmapStats.Shuffle || rsp.Node.Stats.HashmapStats.ShuffleType != plan.ShuffleType_Range {
		//dont need to range shuffle, just split average
		or.expand()
		return or.blocklistRelData.Split(cpunum)
	}
	//split by range shuffle
	result := make([]engine.RelData, cpunum)
	for i := range result {
		result[i] = or.blocklistRelData.BuildEmptyRelData(int(or.TotalBlocks) / cpunum)
	}
	if or.NeedFirstEmpty {
		result[0].AppendBlockInfo(&objectio.EmptyBlockInfo)
	}
	for i := range or.Objlist {
		shuffleIDX := int(plan2.CalcRangeShuffleIDXForObj(rsp, &or.Objlist[i], int(rsp.CNCNT)*cpunum)) % cpunum
		blks := objectio.ObjectStatsToBlockInfoSlice(&or.Objlist[i], false)
		result[shuffleIDX].AppendBlockInfoSlice(blks)
	}
	//make result average
	for {
		maxCnt := result[0].DataCnt()
		minCnt := result[0].DataCnt()
		maxIdx := 0
		minIdx := 0
		for i := range result {
			if result[i].DataCnt() > maxCnt {
				maxCnt = result[i].DataCnt()
				maxIdx = i
			}
			if result[i].DataCnt() < minCnt {
				minCnt = result[i].DataCnt()
				minIdx = i
			}
		}
		if maxCnt < minCnt*2 {
			break
		}

		diff := (maxCnt-minCnt)/3 + 1
		cut_from := result[maxIdx].DataCnt() - diff
		result[minIdx].AppendBlockInfoSlice(result[maxIdx].DataSlice(cut_from, maxCnt).GetBlockInfoSlice())
		result[maxIdx] = result[maxIdx].DataSlice(0, cut_from)
	}
	//check total block cnt
	totalBlocks := 0
	for i := range result {
		totalBlocks += result[i].DataCnt()
	}
	if totalBlocks != int(or.TotalBlocks) {
		panic("wrong blocks cnt after objlist reldata split!")
	}
	return result
}

func (or *ObjListRelData) GetBlockInfoSlice() objectio.BlockInfoSlice {
	or.expand()
	return or.blocklistRelData.GetBlockInfoSlice()
}

func (or *ObjListRelData) BuildEmptyRelData(i int) engine.RelData {
	return or.blocklistRelData.BuildEmptyRelData(i)
}

func (or *ObjListRelData) GetBlockInfo(i int) objectio.BlockInfo {
	panic("not supported")
}

func (or *ObjListRelData) SetBlockInfo(i int, blk *objectio.BlockInfo) {
	panic("not supported")
}

func (or *ObjListRelData) SetBlockList(slice objectio.BlockInfoSlice) {
	or.expand()
	or.blocklistRelData.SetBlockList(slice)
}

func (or *ObjListRelData) AppendBlockInfo(blk *objectio.BlockInfo) {
	panic("not supported")
}

func (or *ObjListRelData) AppendBlockInfoSlice(slice objectio.BlockInfoSlice) {
	or.expand()
	or.blocklistRelData.AppendBlockInfoSlice(slice)
}

func (or *ObjListRelData) UnmarshalBinary(buf []byte) error {
	or.expanded = true
	return or.blocklistRelData.UnmarshalBinary(buf)
}

func (or *ObjListRelData) MarshalBinary() ([]byte, error) {
	or.expand()
	return or.blocklistRelData.MarshalBinary()
}

func (or *ObjListRelData) AttachTombstones(tombstones engine.Tombstoner) error {
	or.blocklistRelData.tombstones = tombstones
	return nil
}

func (or *ObjListRelData) GetTombstones() engine.Tombstoner {
	return or.blocklistRelData.tombstones
}

func (or *ObjListRelData) DataSlice(i, j int) engine.RelData {
	or.expand()
	return or.blocklistRelData.DataSlice(i, j)
}

func (or *ObjListRelData) GroupByPartitionNum() map[int16]engine.RelData {
	or.expand()
	return or.blocklistRelData.GroupByPartitionNum()
}

func (or *ObjListRelData) DataCnt() int {
	return int(or.TotalBlocks)
}

type BlockListRelData struct {
	// blkList[0] is a empty block info
	blklist objectio.BlockInfoSlice

	// tombstones
	tombstones engine.Tombstoner
}

func (relData *BlockListRelData) String() string {
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

func (relData *BlockListRelData) GetShardIDList() []uint64 {
	panic("not supported")
}
func (relData *BlockListRelData) GetShardID(i int) uint64 {
	panic("not supported")
}
func (relData *BlockListRelData) SetShardID(i int, id uint64) {
	panic("not supported")
}
func (relData *BlockListRelData) AppendShardID(id uint64) {
	panic("not supported")
}

func (relData *BlockListRelData) Split(i int) []engine.RelData {
	blkCnt := relData.DataCnt()
	mod := blkCnt % i
	divide := blkCnt / i
	current := 0
	shards := make([]engine.RelData, i)
	for j := 0; j < i; j++ {
		if j < mod {
			shards[j] = relData.DataSlice(current, current+divide+1)
			current = current + divide + 1
		} else {
			shards[j] = relData.DataSlice(current, current+divide)
			current = current + divide
		}
	}
	return shards
}

func (relData *BlockListRelData) GetBlockInfoSlice() objectio.BlockInfoSlice {
	return relData.blklist.GetAllBytes()
}

func (relData *BlockListRelData) BuildEmptyRelData(i int) engine.RelData {
	l := make([]byte, 0, objectio.BlockInfoSize*i)
	return &BlockListRelData{
		blklist: l,
	}
}

func (relData *BlockListRelData) GetBlockInfo(i int) objectio.BlockInfo {
	return *relData.blklist.Get(i)
}

func (relData *BlockListRelData) SetBlockInfo(i int, blk *objectio.BlockInfo) {
	relData.blklist.Set(i, blk)
}

func (relData *BlockListRelData) SetBlockList(slice objectio.BlockInfoSlice) {
	relData.blklist = slice
}

func (relData *BlockListRelData) AppendBlockInfo(blk *objectio.BlockInfo) {
	relData.blklist.AppendBlockInfo(blk)
}

func (relData *BlockListRelData) AppendBlockInfoSlice(slice objectio.BlockInfoSlice) {
	relData.blklist = append(relData.blklist, slice...)
}

func (relData *BlockListRelData) UnmarshalBinary(data []byte) (err error) {
	typ := engine.RelDataType(types.DecodeUint8(data))
	if typ != engine.RelDataBlockList {
		return moerr.NewInternalErrorNoCtxf("UnmarshalBinary RelDataBlockList with %v", typ)
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

func (relData *BlockListRelData) MarshalBinaryWithBuffer(w *bytes.Buffer) (err error) {
	typ := uint8(relData.GetType())
	if _, err = w.Write(types.EncodeUint8(&typ)); err != nil {
		return
	}

	// marshal blk list
	sizeofblks := uint32(relData.blklist.Size())
	if _, err = w.Write(types.EncodeUint32(&sizeofblks)); err != nil {
		return
	}

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
		tombstoneLen = uint32(w.Len() - offset - 4)
		buf := w.Bytes()
		copy(buf[offset:], types.EncodeUint32(&tombstoneLen))
	}
	return
}

func (relData *BlockListRelData) GetType() engine.RelDataType {
	return engine.RelDataBlockList
}

func (relData *BlockListRelData) MarshalBinary() ([]byte, error) {
	var w bytes.Buffer
	if err := relData.MarshalBinaryWithBuffer(&w); err != nil {
		return nil, err
	}
	buf := w.Bytes()
	return buf, nil
}

func (relData *BlockListRelData) AttachTombstones(tombstones engine.Tombstoner) error {
	relData.tombstones = tombstones
	return nil
}

func (relData *BlockListRelData) GetTombstones() engine.Tombstoner {
	return relData.tombstones
}

func (relData *BlockListRelData) DataSlice(i, j int) engine.RelData {
	blist := objectio.BlockInfoSlice(relData.blklist.Slice(i, j))
	return &BlockListRelData{
		blklist:    blist,
		tombstones: relData.tombstones,
	}
}

func (relData *BlockListRelData) GroupByPartitionNum() map[int16]engine.RelData {
	ret := make(map[int16]engine.RelData)

	blks := relData.GetBlockInfoSlice()
	blksLen := blks.Len()
	for idx := range blksLen {
		blkInfo := blks.Get(idx)
		if blkInfo.IsMemBlk() {
			continue
		}
		partitionNum := blkInfo.PartitionNum
		if _, ok := ret[partitionNum]; !ok {
			ret[partitionNum] = &BlockListRelData{
				tombstones: relData.tombstones,
			}
			ret[partitionNum].AppendBlockInfo(&objectio.EmptyBlockInfo)
		}
		ret[partitionNum].AppendBlockInfo(blkInfo)
	}

	return ret
}

func (relData *BlockListRelData) DataCnt() int {
	return relData.blklist.Len()
}
