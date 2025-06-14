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
	"encoding/json"
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
var MetaSchema_TableRange_Seqnums = MetaSeqnums
var MetaSchema_TableRange_Attrs = MetaAttrs
var MetaSchema_TableRange_Types = MetaTypes

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
	TableID     uint64
	ObjectType  int8
	Start       types.Rowid
	End         types.Rowid
	ObjectStats objectio.ObjectStats
}

func (r *TableRange) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]any{
		"table_id":     r.TableID,
		"object_type":  r.ObjectType,
		"start":        fmt.Sprintf("%d-%d", r.Start.GetBlockOffset(), r.Start.GetRowOffset()),
		"end":          fmt.Sprintf("%d-%d", r.End.GetBlockOffset(), r.End.GetRowOffset()),
		"object_stats": r.ObjectStats.String(),
	})
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
		r.ObjectStats.String(),
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
	if err = vector.AppendBytes(bat.Vecs[4], r.ObjectStats[:], false, mp); err != nil {
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
func ExportToTableRangesByFilter(
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
			TableID:     tableId,
			ObjectType:  objectTypes[i],
			Start:       startRows[i],
			End:         endRows[i],
			ObjectStats: objectio.ObjectStats(data.Vecs[4].GetBytesAt(i)),
		})
	}
	return
}

func ExportToTableRanges(
	data *batch.Batch,
) (ranges []TableRange) {
	tableIds := vector.MustFixedColNoTypeCheck[uint64](data.Vecs[0])
	objectTypes := vector.MustFixedColNoTypeCheck[int8](data.Vecs[1])
	startRows := vector.MustFixedColNoTypeCheck[types.Rowid](data.Vecs[2])
	endRows := vector.MustFixedColNoTypeCheck[types.Rowid](data.Vecs[3])
	for i, rows := 0, data.RowCount(); i < rows; i++ {
		ranges = append(ranges, TableRange{
			TableID:     tableIds[i],
			ObjectType:  objectTypes[i],
			Start:       startRows[i],
			End:         endRows[i],
			ObjectStats: objectio.ObjectStats(data.Vecs[4].GetBytesAt(i)),
		})
	}
	return
}

func ScanObjectStats(
	data *batch.Batch,
) (objs []objectio.ObjectStats) {
	if data == nil || data.RowCount() == 0 {
		return nil
	}
	objectsMap := make(map[string]objectio.ObjectStats)
	objectStatsVec := data.Vecs[MetaAttr_ObjectStats_Idx]
	for i := 0; i < data.RowCount(); i++ {
		stats := objectio.ObjectStats(objectStatsVec.GetBytesAt(i))
		objectsMap[stats.String()] = stats
	}
	objs = make([]objectio.ObjectStats, 0, len(objectsMap))
	for _, stats := range objectsMap {
		objs = append(objs, stats)
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
	for _, obj := range objs {
		if err = CollectTableRangesFromFile(
			ctx,
			obj,
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

func ForEachFile(
	ctx context.Context,
	obj objectio.ObjectStats,
	forEachRow func(
		accout uint32,
		dbid, tid uint64,
		objectType int8,
		objectStats objectio.ObjectStats,
		start, end types.TS,
		rowID types.Rowid,
	) error,
	postEachRow func() error,
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
		end bool
	)
	tmpBat := batch.NewWithSchema(
		true, DataScan_TableIDAtrrs, DataScan_TableIDTypes,
	)
	defer tmpBat.Clean(mp)
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
		accouts := vector.MustFixedColNoTypeCheck[uint32](tmpBat.Vecs[0])
		dbids := vector.MustFixedColNoTypeCheck[uint64](tmpBat.Vecs[1])
		tableIds := vector.MustFixedColNoTypeCheck[uint64](tmpBat.Vecs[2])
		objectTypes := vector.MustFixedColNoTypeCheck[int8](tmpBat.Vecs[3])
		objectStatsVec := tmpBat.Vecs[4]
		createTSs := vector.MustFixedColNoTypeCheck[types.TS](tmpBat.Vecs[5])
		deleteTSs := vector.MustFixedColNoTypeCheck[types.TS](tmpBat.Vecs[6])
		rowids := vector.MustFixedColNoTypeCheck[types.Rowid](tmpBat.Vecs[8])
		for i, rows := 0, tmpBat.RowCount(); i < rows; i++ {
			if err = forEachRow(
				accouts[i],
				dbids[i],
				tableIds[i],
				objectTypes[i],
				objectio.ObjectStats(objectStatsVec.GetBytesAt(i)),
				createTSs[i],
				deleteTSs[i],
				rowids[i],
			); err != nil {
				return
			}
		}
	}
	if err = postEachRow(); err != nil {
		return
	}
	return
}

// the data in the obj must be sorted by the table id and object type
func CollectTableRangesFromFile(
	ctx context.Context,
	obj objectio.ObjectStats,
	data *batch.Batch,
	mp *mpool.MPool,
	fs fileservice.FileService,
) (err error) {
	var (
		activeRange TableRange
	)
	if err = ForEachFile(
		ctx,
		obj,
		func(
			accout uint32,
			dbid, tid uint64,
			objectType int8,
			objectStats objectio.ObjectStats,
			start, end types.TS,
			rowID types.Rowid) error {

			if activeRange.TableID != tid || activeRange.ObjectType != objectType {
				if activeRange.IsEmpty() {
					// first table id, object type
					activeRange.TableID = tid
					activeRange.ObjectType = objectType
					activeRange.Start = rowID
					activeRange.ObjectStats = obj
				} else {
					// different table id, object type
					// 1. save the active range to data
					if err = activeRange.AppendTo(data, mp); err != nil {
						return err
					}

					// 2. reset the active range
					activeRange.TableID = tid
					activeRange.ObjectType = objectType
					activeRange.Start = rowID
					activeRange.ObjectStats = obj
				}
			}
			activeRange.End = rowID
			return nil
		},
		func() error {
			if !activeRange.IsEmpty() {
				if err = activeRange.AppendTo(data, mp); err != nil {
					return err
				}
			}
			return nil
		},
		mp,
		fs,
	); err != nil {
		return
	}
	return
}
