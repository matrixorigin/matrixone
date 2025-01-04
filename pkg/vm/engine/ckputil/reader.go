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

var TableEntryAttrs2 = []string{
	TableObjectsAttr_Table,
	TableObjectsAttr_ID,
	TableObjectsAttr_CreateTS,
	TableObjectsAttr_DeleteTS,
	objectio.PhysicalAddr_Attr,
}

var TableEntryTypes2 = []types.Type{
	TableObjectsTypes[TableObjectsAttr_Table_Idx],
	TableObjectsTypes[TableObjectsAttr_ID_Idx],
	TableObjectsTypes[TableObjectsAttr_CreateTS_Idx],
	TableObjectsTypes[TableObjectsAttr_DeleteTS_Idx],
	objectio.RowidType,
}

var TableEntrySeqnums2 = []uint16{
	TableObjectsAttr_Table_Idx,
	TableObjectsAttr_ID_Idx,
	TableObjectsAttr_CreateTS_Idx,
	TableObjectsAttr_DeleteTS_Idx,
	objectio.SEQNUM_ROWID,
}

var ScanTableIDAtrrs = []string{
	TableObjectsAttr_Table,
	objectio.PhysicalAddr_Attr,
}
var ScanTableIDTypes = []types.Type{
	TableObjectsTypes[TableObjectsAttr_Table_Idx],
	objectio.RowidType,
}
var ScanTableIDSeqnums = []uint16{
	TableObjectsAttr_Table_Idx,
	objectio.SEQNUM_ROWID,
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

// the schema of the table entry
// 0: table id
// 1: start rowid
// 2: end rowid
// 3: location
func (r *TableRange) AppendTo(bat *batch.Batch, mp *mpool.MPool) (err error) {
	if err = vector.AppendFixed[uint64](
		bat.Vecs[0], r.TableID, false, mp,
	); err != nil {
		return
	}
	if err = vector.AppendFixed[types.Rowid](
		bat.Vecs[1], r.Start, false, mp,
	); err != nil {
		return
	}
	if err = vector.AppendFixed[types.Rowid](
		bat.Vecs[2], r.End, false, mp,
	); err != nil {
		return
	}
	if err = vector.AppendBytes(bat.Vecs[3], r.Location, false, mp); err != nil {
		return
	}
	bat.AddRowCount(1)
	return
}

func (r *TableRange) IsEmpty() bool {
	return r.Start == types.Rowid{}
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
		}
		if iter.index.blockIdx > iter.ranges[iter.index.rangeIdx].End.GetBlockOffset() {
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
		if _, iter.release, err = ioutil.LoadColumnsData(
			iter.ctx,
			ObjectEntrySeqnums,
			ObjectEntryTypes,
			iter.fs,
			iter.ranges[iter.index.rangeIdx].Location[:],
			iter.data,
			iter.mp,
			0,
		); err != nil {
			return false, err
		}
	}
	return true, nil
}

func (iter *ObjectIter) Entry() (ret objectio.ObjectEntry) {
	ret.CreateTime = vector.GetFixedAtNoTypeCheck[types.TS](
		&iter.data[1], int(iter.index.offset),
	)
	ret.DeleteTime = vector.GetFixedAtNoTypeCheck[types.TS](
		&iter.data[2], int(iter.index.offset),
	)
	ret.ObjectStats.UnMarshal(iter.data[0].GetBytesAt(int(iter.index.offset)))
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
