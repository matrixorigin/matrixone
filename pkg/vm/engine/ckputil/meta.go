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
	"github.com/matrixorigin/matrixone/pkg/objectio/mergeutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
)

// MetaSchema
// ['table_id', 'start_row', 'end_row', 'location']
// [uint64, uint64, uint64, string]
// `table_id` is the id of the table
// `start_row` is the start rowid of the table in the object
// `end_row` is the end rowid of the table in the object (same object as `start_row`)
// `location` is the location of the object
var MetaSchema_TableRange_Seqnums = []uint16{0, 1, 2, 3}
var MetaSchema_TableRange_Attrs = []string{
	TableObjectsAttr_Table,
	"start_row",
	"end_row",
	"location",
}
var MetaSchema_TableRange_Types = []types.Type{
	TableObjectsTypes[TableObjectsAttr_Table_Idx],
	objectio.RowidType,
	objectio.RowidType,
	types.T_char.ToType(),
}

type TableRange struct {
	TableID  uint64
	Start    types.Rowid
	End      types.Rowid
	Location objectio.Location
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

func MakeTableRangeBatch() *batch.Batch {
	return batch.NewWithSchema(
		true,
		MetaSchema_TableRange_Attrs,
		MetaSchema_TableRange_Types,
	)
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

// the data in the obj must be sorted by the table id
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
		rowids := vector.MustFixedColNoTypeCheck[types.Rowid](tmpBat.Vecs[1])
		for i, rows := 0, tmpBat.RowCount(); i < rows; i++ {
			if activeRange.TableID != tableIds[i] {
				if activeRange.IsEmpty() {
					// first table id
					activeRange.TableID = tableIds[i]
					activeRange.Start = rowids[i]
					activeRange.Location = obj.ObjectLocation()
				} else {
					// different table id
					// 1. save the active range to data
					if err = activeRange.AppendTo(data, mp); err != nil {
						return
					}

					// 2. reset the active range
					activeRange.TableID = tableIds[i]
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
