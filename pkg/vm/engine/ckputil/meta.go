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
	"bytes"
	"context"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/mergeutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
)

// MetaSchema
// ['table_id', 'object_type', 'start_row', 'end_row', 'location']
// [uint64, int8, row id, row id, string]
// `table_id` is the id of the table
// `object_type` is the type of the object [Data|Tombstone]
// `start_row` is the start rowid of the table in the object
// `end_row` is the end rowid of the table in the object (same object as `start_row`)
// `location` is the location of the object
var MetaSchema_TableRange_Seqnums = []uint16{0, 1, 2, 3, 4}
var MetaSchema_TableRange_Attrs = []string{
	TableObjectsAttr_Table,
	"object_type",
	"start_row",
	"end_row",
	"location",
}
var MetaSchema_TableRange_Types = []types.Type{
	TableObjectsTypes[TableObjectsAttr_Table_Idx],
	types.T_int8.ToType(),
	objectio.RowidType,
	objectio.RowidType,
	types.T_char.ToType(),
}

func TableRangesString(r []TableRange) string {
	var buf bytes.Buffer
	for i, rng := range r {
		buf.WriteString(fmt.Sprintf("%d: %s\n", i, rng.String()))
	}
	return buf.String()
}

func TableRangesRows(r []TableRange) int {
	rows := 0
	for _, rng := range r {
		rows += rng.Rows()
	}
	return rows
}

type TableRange struct {
	TableID    uint64
	ObjectType int8
	Start      types.Rowid
	End        types.Rowid
	Location   objectio.Location
}

func (r *TableRange) String() string {
	objType := ""
	switch r.ObjectType {
	case ObjectType_Data:
		objType = "DATA"
	case ObjectType_Tombstone:
		objType = "TOMBSTONE"
	default:
		panic(fmt.Sprintf("invalid object type %d", r.ObjectType))
	}
	return fmt.Sprintf(
		"Range<%d-%v:[%d-%d,%d-%d]:%s>",
		r.TableID,
		objType,
		r.Start.GetBlockOffset(),
		r.Start.GetRowOffset(),
		r.End.GetBlockOffset(),
		r.End.GetRowOffset(),
		r.Location.String(),
	)
}

func (r *TableRange) Rows() int {
	if r.Start.GetBlockOffset() == r.End.GetBlockOffset() {
		return int(r.End.GetRowOffset()-r.Start.GetRowOffset()) + 1
	}
	startBlock := r.Start.GetBlockOffset()
	endBlock := r.End.GetBlockOffset()

	return int(objectio.BlockMaxRows) - int(r.Start.GetRowOffset()) +
		int(r.End.GetRowOffset()) + 1 +
		int(endBlock-startBlock-1)*int(objectio.BlockMaxRows)
}

// the schema of the table entry
// 0: table id
// 1: object type
// 2: start rowid
// 3: end rowid
// 4: location
func (r *TableRange) AppendTo(bat *batch.Batch, mp *mpool.MPool) (err error) {
	if err = vector.AppendFixed[uint64](
		bat.Vecs[0], r.TableID, false, mp,
	); err != nil {
		return
	}
	if err = vector.AppendFixed[int8](
		bat.Vecs[1], r.ObjectType, false, mp,
	); err != nil {
		return
	}
	if err = vector.AppendFixed[types.Rowid](
		bat.Vecs[2], r.Start, false, mp,
	); err != nil {
		return
	}
	if err = vector.AppendFixed[types.Rowid](
		bat.Vecs[3], r.End, false, mp,
	); err != nil {
		return
	}
	if err = vector.AppendBytes(bat.Vecs[4], r.Location, false, mp); err != nil {
		return
	}
	bat.AddRowCount(1)
	return
}

func (r *TableRange) IsEmpty() bool {
	return r.Start == types.Rowid{}
}

func MakeTableRangeBatch() *batch.Batch {
	return batch.NewWithSchema(
		true,
		MetaSchema_TableRange_Attrs,
		MetaSchema_TableRange_Types,
	)
}

// data should be sorted by table id and object type
// the schema of the table entry
func ExportToTableRanges(
	data *batch.Batch,
	tableId uint64,
	objectType int8,
) (ranges []TableRange) {
	tableIds := vector.MustFixedColNoTypeCheck[uint64](data.Vecs[0])
	objectTypes := vector.MustFixedColNoTypeCheck[int8](data.Vecs[1])
	start := vector.OrderedFindFirstIndexInSortedSlice(tableId, tableIds)
	if start == -1 {
		return
	}
	for i := start; i < len(tableIds); i++ {
		if objectTypes[i] == objectType {
			break
		}
		start++
	}
	startRows := vector.MustFixedColNoTypeCheck[types.Rowid](data.Vecs[2])
	endRows := vector.MustFixedColNoTypeCheck[types.Rowid](data.Vecs[3])
	for i, rows := start, data.RowCount(); i < rows; i++ {
		if tableIds[i] != tableId || objectTypes[i] != objectType {
			break
		}
		ranges = append(ranges, TableRange{
			TableID:    tableId,
			ObjectType: objectTypes[i],
			Start:      startRows[i],
			End:        endRows[i],
			Location:   data.Vecs[4].GetBytesAt(i),
		})
	}
	return
}

func CollectTableRanges(
	ctx context.Context,
	objs []objectio.ObjectStats,
	data *batch.Batch,
	mp *mpool.MPool,
	fs fileservice.FileService,
) (err error) {
	if len(objs) == 0 {
		return
	}
	tmpBat := MakeDataScanTableIDBatch()
	defer tmpBat.Clean(mp)
	for _, obj := range objs {
		if err = CollectTableRangesFromFile(
			ctx,
			obj,
			tmpBat,
			data,
			mp,
			fs,
		); err != nil {
			return
		}
	}
	err = mergeutil.SortColumnsByIndex(
		data.Vecs,
		0,
		mp,
	)
	return
}

// the data in the obj must be sorted by the table id and object type
func CollectTableRangesFromFile(
	ctx context.Context,
	obj objectio.ObjectStats,
	tmpBat *batch.Batch,
	data *batch.Batch,
	mp *mpool.MPool,
	fs fileservice.FileService,
) (err error) {
	reader := NewDataReader(
		ctx,
		fs,
		obj,
		readutil.WithColumns(
			DataScan_TableIDSeqnums,
			DataScan_TableIDTypes,
		),
	)
	var (
		end         bool
		activeRange TableRange
	)
	for {
		tmpBat.CleanOnlyData()
		if end, err = reader.Read(
			ctx, tmpBat.Attrs, nil, mp, tmpBat,
		); err != nil {
			return
		}
		if end {
			break
		}
		tableIds := vector.MustFixedColNoTypeCheck[uint64](tmpBat.Vecs[0])
		objectTypes := vector.MustFixedColNoTypeCheck[int8](tmpBat.Vecs[1])
		rowids := vector.MustFixedColNoTypeCheck[types.Rowid](tmpBat.Vecs[2])
		for i, rows := 0, tmpBat.RowCount(); i < rows; i++ {
			if activeRange.TableID != tableIds[i] || activeRange.ObjectType != objectTypes[i] {
				if activeRange.IsEmpty() {
					// first table id, object type
					activeRange.TableID = tableIds[i]
					activeRange.ObjectType = objectTypes[i]
					activeRange.Start = rowids[i]
					activeRange.Location = obj.ObjectLocation()
				} else {
					// different table id, object type
					// 1. save the active range to data
					if err = activeRange.AppendTo(data, mp); err != nil {
						return
					}

					// 2. reset the active range
					activeRange.TableID = tableIds[i]
					activeRange.ObjectType = objectTypes[i]
					activeRange.Start = rowids[i]
					activeRange.Location = obj.ObjectLocation()
				}
			}
			activeRange.End = rowids[i]
		}
		if !activeRange.IsEmpty() {
			if err = activeRange.AppendTo(data, mp); err != nil {
				return
			}
		}
	}
	return
}
