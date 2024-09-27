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

package engine_util

import (
	"bytes"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

var _ engine.RelData = new(BlockListRelData)

func UnmarshalRelationData(data []byte) (engine.RelData, error) {
	typ := engine.RelDataType(data[0])
	switch typ {
	case engine.RelDataBlockList:
		relData := new(BlockListRelData)
		if err := relData.UnmarshalBinary(data); err != nil {
			return nil, err
		}
		return relData, nil
	default:
		return nil, moerr.NewInternalErrorNoCtx("unsupported relation data type")
	}
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

func (relData *BlockListRelData) GetBlockInfoSlice() objectio.BlockInfoSlice {
	return relData.blklist.GetAllBytes()
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

func (relData *BlockListRelData) BuildEmptyRelData() engine.RelData {
	return &BlockListRelData{
		blklist: objectio.BlockInfoSlice{},
	}
}

func (relData *BlockListRelData) DataCnt() int {
	return relData.blklist.Len()
}
