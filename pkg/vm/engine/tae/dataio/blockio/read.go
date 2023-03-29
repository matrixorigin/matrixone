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
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/vector"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
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

	columnBatch.SetZs(columnBatch.Vecs[0].Length(), pool)

	return columnBatch, nil
}

func BlockReadInner(
	ctx context.Context,
	info *pkgcatalog.BlockInfo,
	colIdxes []uint16,
	colTypes []types.Type,
	ts types.TS,
	fs fileservice.FileService,
	pool *mpool.MPool) (*batch.Batch, error) {
	var deleteRows []int64
	columnBatch, deleteRows, err := readBlockData(ctx, colIdxes, colTypes, info, ts,
		fs, pool)
	if err != nil {
		return nil, err
	}
	if info.DeltaLoc != "" {
		deleteBatch, err := readBlockDelete(ctx, info.DeltaLoc, fs)
		if err != nil {
			return nil, err
		}
		deleteRows = recordDeletes(deleteBatch, ts)
		logutil.Infof(
			"blockread %s read delete %d: base %s filter out %v\n",
			info.BlockID.String(), deleteBatch.Length(), ts.ToString(), len(deleteRows))
	}
	// remove rows from columns
	for i, col := range columnBatch.Vecs {
		// Fixme: Due to # 8684, we are not able to use mpool yet
		// Fixme: replace with cnVec.Dup(nil) when it implemented.
		columnBatch.Vecs[i], err = col.CloneWindow(0, col.Length(), nil)
		if err != nil {
			return nil, err
		}
		if col.GetType().Oid == types.T_Rowid {
			// rowid need free
			col.Free(pool)
		}
		if len(deleteRows) > 0 {
			columnBatch.Vecs[i].Shrink(deleteRows, true)
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

func preparePhyAddrData(typ types.Type, prefix []byte, startRow, length uint32, pool *mpool.MPool) (col *vector.Vector, err error) {
	col = vector.NewVec(typ)
	for i := uint32(0); i < length; i++ {
		rowid := model.EncodePhyAddrKeyWithPrefix(prefix, startRow+i)
		vector.AppendFixed(col, rowid, false, pool)
	}
	return
}

func readBlockData(ctx context.Context, colIndexes []uint16,
	colTypes []types.Type, info *pkgcatalog.BlockInfo, ts types.TS,
	fs fileservice.FileService, m *mpool.MPool) (*batch.Batch, []int64, error) {
	deleteRows := make([]int64, 0)
	ok, _, idxes := getRowsIdIndex(colIndexes, colTypes)
	_, id, extent, rows, err := DecodeLocation(info.MetaLoc)
	if err != nil {
		return nil, deleteRows, err
	}
	reader, err := NewObjectReader(fs, info.MetaLoc)
	if err != nil {
		return nil, deleteRows, err
	}
	var rowIdVec *vector.Vector
	var bat *batch.Batch
	if ok {
		// generate rowIdVec
		prefix := info.BlockID[:]
		rowIdVec, err = preparePhyAddrData(
			types.T_Rowid.ToType(),
			prefix,
			0,
			rows,
			m,
		)
		if err != nil {
			return nil, deleteRows, err
		}
		defer func() {
			if err != nil {
				rowIdVec.Free(m)
			}
		}()
	}

	loadBlock := func(idxes []uint16) ([]*batch.Batch, error) {
		if len(idxes) == 0 && ok {
			// only read rowid column on non appendable block, return early
			bat = batch.NewWithSize(0)
			bat.Vecs = append(bat.Vecs, rowIdVec)
			return nil, nil
		}
		bats, err := reader.LoadColumns(ctx, idxes, []uint32{id}, nil)
		if err != nil {
			return nil, err
		}
		entry := bats[0].Vecs
		bat = batch.NewWithSize(0)
		for _, typ := range colTypes {
			if typ.Oid == types.T_Rowid {
				bat.Vecs = append(bat.Vecs, rowIdVec)
				continue
			}
			bat.Vecs = append(bat.Vecs, entry[0])
			entry = entry[1:]
		}
		return bats, nil
	}

	loadAppendBlock := func() error {
		// appendable block should be filtered by committs
		meta, err := reader.(*BlockReader).reader.ReadMeta(ctx, []objectio.Extent{extent}, m, LoadZoneMapFunc)
		if err != nil {
			return err
		}

		colCount := meta.BlkMetas[0].GetColumnCount()
		idxes = append(idxes, colCount-2) // committs
		idxes = append(idxes, colCount-1) // aborted
		bats, err := loadBlock(idxes)
		if err != nil {
			return err
		}
		lenVecs := len(bats[0].Vecs)
		t0 := time.Now()
		commits := bats[0].Vecs[lenVecs-2]
		abort := bats[0].Vecs[lenVecs-1]
		for i := 0; i < commits.Length(); i++ {
			if vector.GetFixedAt[bool](abort, i) || vector.GetFixedAt[types.TS](commits, i).Greater(ts) {
				deleteRows = append(deleteRows, int64(i))
			}
		}
		logutil.Infof(
			"blockread %s scan filter cost %v: base %s filter out %v\n ",
			info.BlockID.String(), time.Since(t0), ts.ToString(), len(deleteRows))
		return nil
	}

	if info.EntryState {
		err = loadAppendBlock()
	} else {
		_, err = loadBlock(idxes)
	}

	if err != nil {
		return nil, deleteRows, err
	}

	return bat, deleteRows, nil
}

func readBlockDelete(ctx context.Context, deltaloc string, fs fileservice.FileService) (*batch.Batch, error) {
	_, id, _, _, err := DecodeLocation(deltaloc)
	if err != nil {
		return nil, err
	}
	reader, err := NewObjectReader(fs, deltaloc)
	if err != nil {
		return nil, err
	}

	bat, err := reader.LoadColumns(ctx, []uint16{0, 1, 2}, []uint32{id}, nil)
	if err != nil {
		return nil, err
	}
	return bat[0], nil
}

func recordDeletes(deleteBatch *batch.Batch, ts types.TS) []int64 {
	if deleteBatch == nil {
		return nil
	}
	// record visible delete rows
	deleteRows := nulls.NewWithSize(0)
	for i := 0; i < deleteBatch.Vecs[0].Length(); i++ {
		if vector.GetFixedAt[bool](deleteBatch.Vecs[2], i) {
			continue
		}
		if vector.GetFixedAt[types.TS](deleteBatch.Vecs[1], i).Greater(ts) {
			continue
		}
		rowid := vector.GetFixedAt[types.Rowid](deleteBatch.Vecs[0], i)
		_, _, row := model.DecodePhyAddrKey(rowid)
		nulls.Add(deleteRows, uint64(row))
	}
	var rows []int64
	itr := deleteRows.Np.Iterator()
	for itr.HasNext() {
		r := itr.Next()
		rows = append(rows, int64(r))
	}
	return rows
}
