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
	"fmt"
	"time"

	"github.com/RoaringBitmap/roaring"
	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
)

// BlockRead read block data from storage and apply deletes according given timestamp. Caller make sure metaloc is not empty
func BlockRead(
	ctx context.Context,
	info *pkgcatalog.BlockInfo,
	colIdxes []uint16,
	colTypes []types.Type,
	ts timestamp.Timestamp,
	fs fileservice.FileService,
	pool *mpool.MPool) (*batch.Batch, error) {

	// read
	columnBatch, err := BlockReadInner(
		ctx, info, colIdxes, colTypes,
		types.TimestampToTS(ts), fs, pool,
	)
	if err != nil {
		return nil, err
	}

	bat := batch.NewWithSize(len(colIdxes))
	for i, vec := range columnBatch.Vecs {
		if columnBatch.Deletes != nil {
			// already copied in BlockReadInner
			bat.Vecs[i] = containers.UnmarshalToMoVec(vec)
		} else {
			// FIXME: CN may modify the vector, so it must be copied here now
			bat.Vecs[i] = containers.CopyToMoVec(vec)
		}
		vec.Close()
	}
	bat.SetZs(bat.Vecs[0].Length(), pool)

	return bat, nil
}

func BlockReadInner(
	ctx context.Context,
	info *pkgcatalog.BlockInfo,
	colIdxes []uint16,
	colTypes []types.Type,
	ts types.TS,
	fs fileservice.FileService,
	pool *mpool.MPool) (*containers.Batch, error) {
	columnBatch, err := readBlockData(ctx, colIdxes, colTypes, info, ts,
		fs, pool)
	if err != nil {
		return nil, err
	}
	if info.DeltaLoc != "" {
		deleteBatch, err := readBlockDelete(ctx, info.DeltaLoc, fs)
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
		for i, col := range columnBatch.Vecs {
			columnBatch.Vecs[i] = col.CloneWindow(0, col.Length(), nil)
			columnBatch.Vecs[i].Compact(columnBatch.Deletes)
			col.Close()
		}
	}
	return columnBatch, nil
}

func getRowsIdIndex(colIndexes []uint16, colTypes []types.Type) (bool, uint16, []uint16) {
	found := false
	idx := 0
	for i, typ := range colTypes {
		if typ.Oid == types.T_Rowid {
			idx = i
			found = true
			break
		}
	}
	if !found {
		return found, uint16(idx), colIndexes
	}
	idxes := make([]uint16, 0)
	for i := range colIndexes {
		if i == idx {
			continue
		}
		idxes = append(idxes, colIndexes[i])
	}
	return found, uint16(idx), idxes
}

func readBlockData(ctx context.Context, colIndexes []uint16,
	colTypes []types.Type, info *pkgcatalog.BlockInfo, ts types.TS,
	fs fileservice.FileService, m *mpool.MPool) (*containers.Batch, error) {
	ok, _, idxes := getRowsIdIndex(colIndexes, colTypes)
	_, id, extent, rows, err := DecodeLocation(info.MetaLoc)
	if err != nil {
		return nil, err
	}
	reader, err := NewObjectReader(fs, info.MetaLoc)
	if err != nil {
		return nil, err
	}
	var rowIdVec containers.Vector
	bat := containers.NewBatch()
	defer func() {
		if err != nil {
			bat.Close()
		}
	}()
	if ok {
		// generate rowIdVec
		prefix := model.EncodeBlockKeyPrefix(info.SegmentID, info.BlockID)
		rowIdVec, err = model.PreparePhyAddrDataWithPool(
			types.T_Rowid.ToType(),
			prefix,
			0,
			rows,
			m,
		)
		if err != nil {
			return nil, err
		}
		defer func() {
			if err != nil {
				rowIdVec.Close()
			}
		}()
	}

	loadBlock := func(idxes []uint16) ([]*batch.Batch, error) {
		if len(idxes) == 0 && ok {
			// only read rowid column on non appendable block, return early
			bat.AddVector(catalog.AttrRowID, rowIdVec)
			return nil, nil
		}
		bats, err := reader.LoadColumns(ctx, idxes, []uint32{id}, m)
		if err != nil {
			return nil, err
		}
		entry := bats[0].Vecs
		for i, typ := range colTypes {
			if typ.Oid == types.T_Rowid {
				bat.AddVector(fmt.Sprintf("%d", i), rowIdVec)
			} else {
				bat.AddVector(fmt.Sprintf("%d", i),
					containers.NewVectorWithSharedMemory(entry[0], true))
				entry = entry[1:]
			}
		}
		return bats, nil
	}

	loadAppendBlock := func() error {
		// appendable block should be filtered by committs
		blocks, err := reader.(*BlockReader).reader.ReadMeta(ctx, []objectio.Extent{extent}, m, LoadZoneMapFunc)
		if err != nil {
			return err
		}

		colCount := blocks[0].GetColumnCount()
		idxes = append(idxes, colCount-2) // committs
		idxes = append(idxes, colCount-1) // aborted
		bats, err := loadBlock(idxes)
		if err != nil {
			return err
		}
		lenVecs := len(bats[0].Vecs)
		t0 := time.Now()
		v1 := bats[0].Vecs[lenVecs-2]
		commits := containers.NewVectorWithSharedMemory(v1, false)
		defer commits.Close()
		v2 := bats[0].Vecs[lenVecs-1]
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
		return nil
	}

	if info.EntryState {
		err = loadAppendBlock()
	} else {
		_, err = loadBlock(idxes)
	}

	if err != nil {
		return nil, err
	}
	return bat, nil
}

func readBlockDelete(ctx context.Context, deltaloc string, fs fileservice.FileService) (*containers.Batch, error) {
	bat := containers.NewBatch()
	colNames := []string{catalog.PhyAddrColumnName, catalog.AttrCommitTs, catalog.AttrAborted}
	_, id, _, _, err := DecodeLocation(deltaloc)
	if err != nil {
		return nil, err
	}
	reader, err := NewObjectReader(fs, deltaloc)
	if err != nil {
		return nil, err
	}

	ioResult, err := reader.LoadColumns(ctx, []uint16{0, 1, 2}, []uint32{id}, nil)
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
