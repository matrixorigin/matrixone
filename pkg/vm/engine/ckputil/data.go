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

package ckputil

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
)

// used to scan data with columns: [table object attrs, `phy_addr`]
var DataScan_TableIDAtrrs = append(TableObjectsAttrs, objectio.PhysicalAddr_Attr)
var DataScan_TableIDTypes = append(TableObjectsTypes, objectio.RowidType)
var DataScan_TableIDSeqnums = append(TableObjectsSeqnums, objectio.SEQNUM_ROWID)

func MakeDataScanTableIDBatch() *batch.Batch {
	return batch.NewWithSchema(
		true,
		DataScan_TableIDAtrrs,
		DataScan_TableIDTypes,
	)
}

var DataScan_ObjectEntryAttrs = []string{
	TableObjectsAttr_Table,
	TableObjectsAttr_ID,
	TableObjectsAttr_CreateTS,
	TableObjectsAttr_DeleteTS,
	// objectio.PhysicalAddr_Attr,
}

var DataScan_ObjectEntryTypes = []types.Type{
	TableObjectsTypes[TableObjectsAttr_Table_Idx],
	TableObjectsTypes[TableObjectsAttr_ID_Idx],
	TableObjectsTypes[TableObjectsAttr_CreateTS_Idx],
	TableObjectsTypes[TableObjectsAttr_DeleteTS_Idx],
	// objectio.RowidType,
}

var DataScan_ObjectEntrySeqnums = []uint16{
	TableObjectsAttr_Table_Idx,
	TableObjectsAttr_ID_Idx,
	TableObjectsAttr_CreateTS_Idx,
	TableObjectsAttr_DeleteTS_Idx,
	// objectio.SEQNUM_ROWID,
}

func MakeDataScanTableEntryBatch() *batch.Batch {
	return batch.NewWithSchema(
		true,
		DataScan_ObjectEntryAttrs,
		DataScan_ObjectEntryTypes,
	)
}

func NewDataReader(
	ctx context.Context,
	fs fileservice.FileService,
	obj objectio.ObjectStats,
	opts ...readutil.ReaderOption,
) engine.Reader {
	return readutil.SimpleObjectReader(
		ctx,
		fs,
		&obj,
		timestamp.Timestamp{},
		opts...,
	)
}

type ObjectIter struct {
	ctx    context.Context
	ranges []TableRange
	index  struct {
		rangeIdx int
		blockIdx uint16
		offset   uint32
	}
	data    containers.Vectors
	release func()

	mp *mpool.MPool
	fs fileservice.FileService
}

func NewObjectIter(
	ctx context.Context,
	ranges []TableRange,
	mp *mpool.MPool,
	fs fileservice.FileService,
) (iter ObjectIter) {
	iter.ctx = ctx
	iter.ranges = ranges
	iter.index.rangeIdx = -1
	iter.mp = mp
	iter.fs = fs
	iter.data = containers.NewVectors(len(DataScan_ObjectEntryAttrs))
	return
}

func (iter *ObjectIter) Next() (bool, error) {
	if iter.index.rangeIdx >= len(iter.ranges) {
		return false, nil
	}
	needLoad := false
	if iter.index.rangeIdx == -1 {
		iter.index.rangeIdx = 0
		iter.index.blockIdx = iter.ranges[iter.index.rangeIdx].Start.GetBlockOffset()
		iter.index.offset = iter.ranges[iter.index.rangeIdx].Start.GetRowOffset()
		needLoad = true
	} else {
		iter.index.offset++
		if iter.index.offset >= objectio.BlockMaxRows {
			iter.index.blockIdx++
			iter.index.offset = 0
			needLoad = true
		}
		if iter.index.blockIdx == iter.ranges[iter.index.rangeIdx].End.GetBlockOffset() {
			// blockIdx == end
			if iter.index.offset > iter.ranges[iter.index.rangeIdx].End.GetRowOffset() {
				iter.index.rangeIdx++
				if iter.index.rangeIdx >= len(iter.ranges) {
					return false, nil
				}

				// move to the start of the new range
				iter.index.blockIdx = iter.ranges[iter.index.rangeIdx].Start.GetBlockOffset()
				iter.index.offset = iter.ranges[iter.index.rangeIdx].Start.GetRowOffset()
				needLoad = true
			}
		} else if iter.index.blockIdx > iter.ranges[iter.index.rangeIdx].End.GetBlockOffset() {
			// blockIdx > end

			// move to next range
			iter.index.rangeIdx++

			// check if reach the end, if so, return false
			if iter.index.rangeIdx >= len(iter.ranges) {
				return false, nil
			}

			// move to the start of the new range
			iter.index.blockIdx = iter.ranges[iter.index.rangeIdx].Start.GetBlockOffset()
			iter.index.offset = iter.ranges[iter.index.rangeIdx].Start.GetRowOffset()
			needLoad = true
		}
	}
	if needLoad {
		if iter.release != nil {
			iter.release()
			iter.release = nil
		}
		iter.data.Free(iter.mp)
		var err error
		loc := iter.ranges[iter.index.rangeIdx].ObjectStats.ObjectLocation().Clone()
		loc.SetID(iter.index.blockIdx)
		if _, iter.release, err = ioutil.LoadColumnsData(
			iter.ctx,
			DataScan_ObjectEntrySeqnums,
			DataScan_ObjectEntryTypes,
			iter.fs,
			loc,
			iter.data,
			iter.mp,
			0,
		); err != nil {
			return false, err
		}
		// {
		// 	vec := iter.data[0]
		// 	logutil.Infof("debugxx %s", common.MoVectorToString(&vec, 2))
		// 	vec = iter.data[1]
		// 	logutil.Infof("debugxx %s", common.MoVectorToString(&vec, 2))
		// 	vec = iter.data[2]
		// 	logutil.Infof("debugxx %s", common.MoVectorToString(&vec, 2))
		// 	vec = iter.data[3]
		// 	logutil.Infof("debugxx %s", common.MoVectorToString(&vec, 3))
		// }
	}
	return true, nil
}

func (iter *ObjectIter) Entry() (ret objectio.ObjectEntry) {
	ret.CreateTime = vector.GetFixedAtNoTypeCheck[types.TS](
		&iter.data[2], int(iter.index.offset),
	)
	ret.DeleteTime = vector.GetFixedAtNoTypeCheck[types.TS](
		&iter.data[3], int(iter.index.offset),
	)
	ret.ObjectStats.UnMarshal(iter.data[1].GetBytesAt(int(iter.index.offset)))
	return
}

func (iter *ObjectIter) Reset() {
	iter.ctx = nil
	iter.ranges = nil
	iter.index.rangeIdx = -1
	iter.index.blockIdx = 0
	iter.index.offset = 0
	iter.data.Free(iter.mp)
	if iter.release != nil {
		iter.release()
		iter.release = nil
	}
	iter.mp = nil
	iter.fs = nil
}

func (iter *ObjectIter) Close() {
	iter.Reset()
}
