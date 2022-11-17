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

	"github.com/RoaringBitmap/roaring"
	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
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
		ctx, info,
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
	colNames []string,
	colIdxs []uint16,
	colTyps []types.Type,
	colNulls []bool,
	ts types.TS,
	fs fileservice.FileService,
	pool *mpool.MPool) (*containers.Batch, error) {
	columnBatch, err := readColumnBatchByMetaloc(
		ctx, info.BlockID, info.SegmentID,
		info.MetaLoc,
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
		applyDeletes(columnBatch, deleteBatch, ts)
		deleteBatch.Close()
	}
	return columnBatch, nil
}

func readColumnBatchByMetaloc(
	ctx context.Context,
	blkid uint64,
	segid uint64,
	metaloc string,
	colNames []string,
	colIdxs []uint16,
	colTyps []types.Type,
	colNulls []bool,
	fs fileservice.FileService,
	pool *mpool.MPool) (bat *containers.Batch, err error) {
	name, extent, rows := DecodeMetaLoc(metaloc)
	idxsWithouRowid := make([]uint16, 0, len(colIdxs))
	var rowidData containers.Vector
	// sift rowid column
	for i, typ := range colTyps {
		if typ.Oid == types.T_Rowid {
			// generate rowid data
			prefix := model.EncodeBlockKeyPrefix(segid, blkid)
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

	// only read rowid column, return early
	if len(idxsWithouRowid) == 0 {
		for _, name := range colNames {
			bat.AddVector(name, rowidData)
		}
		return bat, nil
	}

	// raed s3
	reader, err := objectio.NewObjectReader(name, fs)
	if err != nil {
		return nil, err
	}

	ioResult, err := reader.Read(ctx, extent, idxsWithouRowid, nil)
	if err != nil {
		return nil, err
	}

	entry := ioResult.Entries
	for i, typ := range colTyps {
		if typ.Oid == types.T_Rowid {
			bat.AddVector(colNames[i], rowidData)
		} else {
			vec := vector.New(colTyps[i])
			data := make([]byte, len(entry[0].Object.([]byte)))
			copy(data, entry[0].Object.([]byte))
			err := vec.Read(data)
			if err != nil {
				bat.Close()
				return nil, err
			}
			bat.AddVector(colNames[i], containers.NewVectorWithSharedMemory(vec, colNulls[i]))
			entry = entry[1:]
		}
	}

	return bat, nil
}

func readDeleteBatchByDeltaloc(ctx context.Context, deltaloc string, fs fileservice.FileService) (*containers.Batch, error) {
	bat := containers.NewBatch()
	colNames := []string{catalog.PhyAddrColumnName, catalog.AttrCommitTs, catalog.AttrAborted}
	colTypes := []types.Type{types.T_Rowid.ToType(), types.T_TS.ToType(), types.T_bool.ToType()}

	name, extent, _ := DecodeMetaLoc(deltaloc)
	reader, err := objectio.NewObjectReader(name, fs)
	if err != nil {
		return nil, err
	}
	ioResult, err := reader.Read(ctx, extent, []uint16{0, 1, 2}, nil)
	if err != nil {
		return nil, err
	}
	for i, entry := range ioResult.Entries {
		vec := vector.New(colTypes[i])
		data := make([]byte, len(entry.Object.([]byte)))
		copy(data, entry.Object.([]byte))
		err := vec.Read(data)
		if err != nil {
			return nil, err
		}
		bat.AddVector(colNames[i], containers.NewVectorWithSharedMemory(vec, false))
	}
	return bat, nil
}

func applyDeletes(columnBatch *containers.Batch, deleteBatch *containers.Batch, ts types.TS) {
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

	// remove rows from columns
	if columnBatch.Deletes != nil {
		for _, col := range columnBatch.Vecs {
			col.Compact(columnBatch.Deletes)
		}
	}
}
