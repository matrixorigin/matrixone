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

	"github.com/matrixorigin/matrixone/pkg/container/nulls"

	"github.com/matrixorigin/matrixone/pkg/container/vector"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
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
	mp *mpool.MPool, vp engine.VectorPool) (*batch.Batch, error) {

	// read
	columnBatch, err := BlockReadInner(
		ctx, info, colIdxes, colTypes,
		types.TimestampToTS(ts), fs, mp, vp,
	)
	if err != nil {
		return nil, err
	}

	columnBatch.SetZs(columnBatch.Vecs[0].Length(), mp)

	return columnBatch, nil
}

func mergeDeleteRows(d1, d2 []int64) []int64 {
	if len(d1) == 0 {
		return d2
	} else if len(d2) == 0 {
		return d1
	}
	ret := make([]int64, 0, len(d1)+len(d2))
	i, j := 0, 0
	n1, n2 := len(d1), len(d2)
	for i < n1 || j < n2 {
		if i == n1 {
			ret = append(ret, d2[j:]...)
			break
		}
		if j == n2 {
			ret = append(ret, d1[i:]...)
			break
		}
		if d1[i] == d2[j] {
			j++
		} else if d1[i] < d2[j] {
			ret = append(ret, d1[i])
			i++
		} else {
			ret = append(ret, d2[j])
			j++
		}
	}
	return ret
}

func BlockReadInner(
	ctx context.Context,
	info *pkgcatalog.BlockInfo,
	colIdxes []uint16,
	colTypes []types.Type,
	ts types.TS,
	fs fileservice.FileService,
	mp *mpool.MPool, vp engine.VectorPool) (*batch.Batch, error) {
	var deleteRows []int64
	columnBatch, deleteRows, err := readBlockData(ctx, colIdxes, colTypes, info, ts,
		fs, mp)
	if err != nil {
		return nil, err
	}
	if !info.DeltaLocation().IsEmpty() {
		deleteBatch, err := readBlockDelete(ctx, info.DeltaLocation(), fs)
		if err != nil {
			return nil, err
		}
		deleteRows = mergeDeleteRows(deleteRows, recordDeletes(deleteBatch, ts))
		logutil.Debugf(
			"blockread %s read delete %d: base %s filter out %v\n",
			info.BlockID.String(), deleteBatch.Length(), ts.ToString(), len(deleteRows))
	}
	rbat := batch.NewWithSize(len(columnBatch.Vecs))
	for i, col := range columnBatch.Vecs {
		typ := *col.GetType()
		if vp == nil {
			rbat.Vecs[i] = vector.NewVec(typ)
		} else {
			rbat.Vecs[i] = vp.GetVector(typ)
		}
		if err := vector.GetUnionAllFunction(typ, mp)(rbat.Vecs[i], col); err != nil {
			return nil, err
		}
		if col.GetType().Oid == types.T_Rowid {
			// rowid need free
			col.Free(mp)
		}
		if len(deleteRows) > 0 {
			rbat.Vecs[i].Shrink(deleteRows, true)
		}
	}
	return rbat, nil
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
	id := info.MetaLocation().ID()
	reader, err := NewObjectReader(fs, info.MetaLocation())
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
			info.MetaLocation().Rows(),
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

	loadBlock := func(idxes []uint16) (*batch.Batch, error) {
		if len(idxes) == 0 && ok {
			// only read rowid column on non appendable block, return early
			bat = batch.NewWithSize(0)
			bat.Vecs = append(bat.Vecs, rowIdVec)
			return nil, nil
		}
		bats, err := reader.LoadColumns(ctx, idxes, id, nil)
		if err != nil {
			return nil, err
		}
		entry := bats.Vecs
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
		meta, err := reader.reader.ReadMeta(ctx, m)
		if err != nil {
			return err
		}
		block := meta.GetBlockMeta(0)
		colCount := block.GetColumnCount()
		idxes = append(idxes, colCount-2) // committs
		idxes = append(idxes, colCount-1) // aborted
		bats, err := loadBlock(idxes)
		if err != nil {
			return err
		}
		lenVecs := len(bats.Vecs)
		t0 := time.Now()
		commits := bats.Vecs[lenVecs-2]
		abort := bats.Vecs[lenVecs-1]
		for i := 0; i < commits.Length(); i++ {
			if vector.GetFixedAt[bool](abort, i) || vector.GetFixedAt[types.TS](commits, i).Greater(ts) {
				deleteRows = append(deleteRows, int64(i))
			}
		}
		logutil.Debugf(
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

func readBlockDelete(ctx context.Context, deltaloc objectio.Location, fs fileservice.FileService) (*batch.Batch, error) {
	reader, err := NewObjectReader(fs, deltaloc)
	if err != nil {
		return nil, err
	}

	bat, err := reader.LoadColumns(ctx, []uint16{0, 1, 2}, deltaloc.ID(), nil)
	if err != nil {
		return nil, err
	}
	return bat, nil
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

// BlockPrefetch is the interface for cn to call read ahead
// columns  Which columns should be taken for columns
// service  fileservice
// infos [s3object name][block]
func BlockPrefetch(idxes []uint16, service fileservice.FileService, infos [][]*pkgcatalog.BlockInfo) error {
	// Generate prefetch task
	for i := range infos {
		// build reader
		pref, err := BuildPrefetchParams(service, infos[i][0].MetaLocation())
		if err != nil {
			return err
		}
		for _, info := range infos[i] {
			pref.AddBlock(idxes, []uint16{info.MetaLocation().ID()})
			if !info.DeltaLocation().IsEmpty() {
				// Need to read all delete
				err = Prefetch([]uint16{0, 1, 2}, []uint16{info.DeltaLocation().ID()}, service, info.DeltaLocation())
				if err != nil {
					return err
				}
			}
		}
		err = pipeline.Prefetch(pref)
		if err != nil {
			return err
		}
	}
	return nil
}
