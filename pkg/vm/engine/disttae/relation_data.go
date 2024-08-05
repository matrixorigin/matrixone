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
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

var _ engine.RelData = new(blockListRelData)

func UnmarshalRelationData(data []byte) (engine.RelData, error) {
	typ := engine.RelDataType(data[0])
	switch typ {
	case engine.RelDataBlockList:
		relData := new(blockListRelData)
		if err := relData.UnmarshalBinary(data); err != nil {
			return nil, err
		}
		return relData, nil
	default:
		return nil, moerr.NewInternalErrorNoCtx("unsupported relation data type")
	}
}

func NewEmptyBlockListRelationData() *blockListRelData {
	return &blockListRelData{
		blklist: objectio.BlockInfoSlice{},
	}
}

type blockListRelData struct {
	// blkList[0] is a empty block info
	blklist objectio.BlockInfoSlice

	// tombstones
	tombstones engine.Tombstoner
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

func (relData *blockListRelData) GetBlockInfoSlice() objectio.BlockInfoSlice {
	return relData.blklist.GetAllBytes()
}

func (relData *blockListRelData) GetBlockInfo(i int) objectio.BlockInfo {
	return *relData.blklist.Get(i)
}

func (relData *blockListRelData) SetBlockInfo(i int, blk objectio.BlockInfo) {
	relData.blklist.Set(i, &blk)
}

func (relData *blockListRelData) AppendBlockInfo(blk objectio.BlockInfo) {
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
	blist := objectio.BlockInfoSlice(relData.blklist.Slice(i, j))
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
			ret[partitionNum].AppendBlockInfo(objectio.EmptyBlockInfo)
		}
		ret[partitionNum].AppendBlockInfo(*blkInfo)
	}

	return ret
}

func (relData *blockListRelData) BuildEmptyRelData() engine.RelData {
	return &blockListRelData{
		blklist: objectio.BlockInfoSlice{},
	}
}

func (relData *blockListRelData) DataCnt() int {
	return relData.blklist.Len()
}
