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

package blockio

import (
	"context"
	"time"

	"github.com/RoaringBitmap/roaring"
	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
)

// BlockRead read block data from storage and apply deletes according given timestamp. Caller make sure metaloc is not empty
func BlockRead(
	ctx context.Context,
	info *pkgcatalog.BlockInfo,
	columns []string,
	colIdxs []uint16,
	colTypes []types.Type,
	colNulls []bool,
	tableDef *plan.TableDef,
	ts timestamp.Timestamp,
	fs fileservice.FileService,
	pool *mpool.MPool) (*batch.Batch, error) {

	// read
	columnBatch, err := BlockReadInner(
		ctx, info, len(tableDef.Cols), /*including rowid*/
		columns, colIdxs, colTypes, colNulls,
		types.TimestampToTS(ts), fs, pool,
	)
	if err != nil {
		return nil, err
	}

	bat := batch.NewWithSize(len(columns))
	bat.Attrs = columns
	for i, vec := range columnBatch.Vecs {
		// If the vector uses mpool to allocate memory internally,
		// it needs to be free here
		if vec.Allocated() > 0 {
			bat.Vecs[i] = containers.CopyToMoVec(vec)
		} else {
			bat.Vecs[i] = containers.UnmarshalToMoVec(vec)
		}
		vec.Close()
	}
	bat.SetZs(bat.Vecs[0].Length(), pool)

	return bat, nil
}

func BlockReadInner(
	ctx context.Context,
	info *pkgcatalog.BlockInfo,
	schemaColCnt int,
	colNames []string,
	colIdxs []uint16,
	colTyps []types.Type,
	colNulls []bool,
	ts types.TS,
	fs fileservice.FileService,
	pool *mpool.MPool) (*containers.Batch, error) {
	columnBatch, err := readColumnBatchByMetaloc(
		ctx, info, ts, schemaColCnt,
		colNames, colIdxs, colTyps, colNulls,
		fs, pool,
	)
	if err != nil {
		return nil, err
	}
	if info.DeltaLoc != "" {
		deleteBatch, err := readDeleteBatchByDeltaloc(ctx, info.DeltaLoc, fs)
		if err != nil {
			return nil, err
		}
		recordDeletes(columnBatch, deleteBatch, ts)
		logutil.Infof(
			"blockread %d read delete %d: base %s filter out %v\n",
			info.BlockID, deleteBatch.Length(), ts.ToString(), columnBatch.DeleteCnt())
		deleteBatch.Close()
	}
	// remove rows from columns
	if columnBatch.Deletes != nil {
		for _, col := range columnBatch.Vecs {
			col.Compact(columnBatch.Deletes)
		}
	}
	return columnBatch, nil
}

func readColumnBatchByMetaloc(
	ctx context.Context,
	info *pkgcatalog.BlockInfo,
	ts types.TS,
	schemaColCnt int,
	colNames []string,
	colIdxs []uint16,
	colTyps []types.Type,
	colNulls []bool,
	fs fileservice.FileService,
	pool *mpool.MPool) (*containers.Batch, error) {
	var bat *containers.Batch
	var err error
	_, extent, rows := DecodeMetaLoc(info.MetaLoc)
	idxsWithouRowid := make([]uint16, 0, len(colIdxs))
	var rowidData containers.Vector
	// sift rowid column
	for i, typ := range colTyps {
		if typ.Oid == types.T_Rowid {
			// generate rowid data
			prefix := model.EncodeBlockKeyPrefix(info.SegmentID, info.BlockID)
			rowidData, err = model.PreparePhyAddrDataWithPool(
				types.T_Rowid.ToType(),
				prefix,
				0,
				rows,
				pool,
			)
			if err != nil {
				return nil, err
			}
			defer func() {
				if err != nil {
					rowidData.Close()
				}
			}()
		} else {
			idxsWithouRowid = append(idxsWithouRowid, colIdxs[i])
		}
	}

	bat = containers.NewBatch()
	defer func() {
		if err != nil {
			bat.Close()
		}
	}()

	// only read rowid column on non appendable block, return early
	if len(idxsWithouRowid) == 0 && !info.EntryState {
		for _, name := range colNames {
			bat.AddVector(name, rowidData)
		}
		return bat, nil
	}

	if info.EntryState { // appendable block should be filtered by committs
		idxsWithouRowid = append(idxsWithouRowid, uint16(schemaColCnt))   // committs
		idxsWithouRowid = append(idxsWithouRowid, uint16(schemaColCnt+1)) // aborted
	}

	reader, err := NewBlockReader(fs, info.MetaLoc)
	if err != nil {
		return nil, err
	}

	ioResult, err := reader.LoadColumns(ctx, idxsWithouRowid, []uint32{extent.Id()}, nil)
	if err != nil {
		return nil, err
	}

	for i, typ := range colTyps {
		if typ.Oid == types.T_Rowid {
			bat.AddVector(colNames[i], rowidData)
		} else {
			bat.AddVector(colNames[i], containers.NewVectorWithSharedMemory(ioResult[0].Vecs[i], colNulls[i]))
		}
	}
	lenVecs := len(ioResult[0].Vecs)
	// generate filter map
	if info.EntryState {
		t0 := time.Now()
		v1 := ioResult[0].Vecs[lenVecs-2]
		commits := containers.NewVectorWithSharedMemory(v1, false)
		defer commits.Close()
		v2 := ioResult[0].Vecs[lenVecs-1]
		abort := containers.NewVectorWithSharedMemory(v2, false)
		defer abort.Close()
		for i := 0; i < commits.Length(); i++ {
			if abort.Get(i).(bool) || commits.Get(i).(types.TS).Greater(ts) {
				if bat.Deletes == nil {
					bat.Deletes = roaring.NewBitmap()
				}
				bat.Deletes.Add(uint32(i))
			}
		}
		logutil.Infof(
			"blockread %d scan filter cost %v: base %s filter out %v\n ",
			info.BlockID, time.Since(t0), ts.ToString(), bat.DeleteCnt())
	}

	return bat, nil
}

func readDeleteBatchByDeltaloc(ctx context.Context, deltaloc string, fs fileservice.FileService) (*containers.Batch, error) {
	bat := containers.NewBatch()
	colNames := []string{catalog.PhyAddrColumnName, catalog.AttrCommitTs, catalog.AttrAborted}
	//colTypes := []types.Type{types.T_Rowid.ToType(), types.T_TS.ToType(), types.T_bool.ToType()}

	_, extent, _ := DecodeMetaLoc(deltaloc)
	reader, err := NewBlockReader(fs, deltaloc)
	if err != nil {
		return nil, err
	}

	ioResult, err := reader.LoadColumns(ctx, []uint16{0, 1, 2}, []uint32{extent.Id()}, nil)
	if err != nil {
		return nil, err
	}
	for i, entry := range ioResult[0].Vecs {
		bat.AddVector(colNames[i], containers.NewVectorWithSharedMemory(entry, false))
	}
	return bat, nil
}

func recordDeletes(columnBatch *containers.Batch, deleteBatch *containers.Batch, ts types.TS) {
	if deleteBatch == nil {
		return
	}

	// record visible delete rows
	for i := 0; i < deleteBatch.Length(); i++ {
		abort := deleteBatch.GetVectorByName(catalog.AttrAborted).Get(i).(bool)
		if abort {
			continue
		}
		commitTS := deleteBatch.GetVectorByName(catalog.AttrCommitTs).Get(i).(types.TS)
		if commitTS.Greater(ts) {
			continue
		}
		rowid := deleteBatch.GetVectorByName(catalog.PhyAddrColumnName).Get(i).(types.Rowid)
		_, _, row := model.DecodePhyAddrKey(rowid)
		if columnBatch.Deletes == nil {
			columnBatch.Deletes = roaring.NewBitmap()
		}
		columnBatch.Deletes.Add(row)
	}
}
