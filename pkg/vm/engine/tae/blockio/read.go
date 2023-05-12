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
	"go.uber.org/zap"

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
	seqnums []uint16,
	colTypes []types.Type,
	ts timestamp.Timestamp,
	fs fileservice.FileService,
	mp *mpool.MPool, vp engine.VectorPool) (*batch.Batch, error) {
	if logutil.GetSkip1Logger().Core().Enabled(zap.InfoLevel) {
		logutil.Infof("read block %s, seqnums %v, typs %v", info.BlockID.String(), seqnums, colTypes)
	}
	columnBatch, err := BlockReadInner(
		ctx, info, seqnums, colTypes,
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
	seqnums []uint16,
	colTypes []types.Type,
	ts types.TS,
	fs fileservice.FileService,
	mp *mpool.MPool,
	vp engine.VectorPool,
) (result *batch.Batch, err error) {
	var (
		rowid       *vector.Vector
		deletedRows []int64
		loaded      *batch.Batch
	)

	// read block data from storage
	if loaded, rowid, deletedRows, err = readBlockData(
		ctx, seqnums, colTypes, info, ts, fs, mp,
	); err != nil {
		return
	}

	// read deletes from storage if needed
	if !info.DeltaLocation().IsEmpty() {
		var deletes *batch.Batch
		if deletes, err = readBlockDelete(ctx, info.DeltaLocation(), fs); err != nil {
			return
		}

		deletedRows = mergeDeleteRows(deletedRows, evalDeleteRowsByTimestamp(deletes, ts))
		if logutil.GetSkip1Logger().Core().Enabled(zap.DebugLevel) {
			logutil.Debugf(
				"blockread %s read delete %d: base %s filter out %v\n",
				info.BlockID.String(), deletes.Length(), ts.ToString(), len(deletedRows))
		}
	}

	result = batch.NewWithSize(len(loaded.Vecs))
	for i, col := range loaded.Vecs {
		typ := *col.GetType()
		if vp == nil {
			result.Vecs[i] = vector.NewVec(typ)
		} else {
			result.Vecs[i] = vp.GetVector(typ)
		}
		if err = vector.GetUnionAllFunction(typ, mp)(result.Vecs[i], col); err != nil {
			break
		}
		// shrink the vector by deleted rows
		if len(deletedRows) > 0 {
			result.Vecs[i].Shrink(deletedRows, true)
		}
	}

	if rowid != nil {
		rowid.Free(mp)
	}

	if err != nil {
		for _, col := range result.Vecs {
			if col != nil {
				col.Free(mp)
			}
		}
	}
	return
}

func getRowsIdIndex(colIndexes []uint16, colTypes []types.Type) (bool, []uint16, []types.Type) {
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
		return found, colIndexes, colTypes
	}
	idxes := make([]uint16, 0, len(colTypes)-1)
	typs := make([]types.Type, 0, len(colTypes)-1)
	idxes = append(idxes, colIndexes[:idx]...)
	idxes = append(idxes, colIndexes[idx+1:]...)
	typs = append(typs, colTypes[:idx]...)
	typs = append(typs, colTypes[idx+1:]...)
	return found, idxes, typs
}

func preparePhyAddrData(typ types.Type, prefix []byte, startRow, length uint32, pool *mpool.MPool) (col *vector.Vector, err error) {
	col = vector.NewVec(typ)
	col.PreExtend(int(length), pool)
	for i := uint32(0); i < length; i++ {
		rowid := model.EncodePhyAddrKeyWithPrefix(prefix, startRow+i)
		vector.AppendFixed(col, rowid, false, pool)
	}
	return
}

func readBlockData(
	ctx context.Context,
	colIndexes []uint16,
	colTypes []types.Type,
	info *pkgcatalog.BlockInfo,
	ts types.TS,
	fs fileservice.FileService,
	m *mpool.MPool,
) (bat *batch.Batch, rowid *vector.Vector, deletedRows []int64, err error) {

	hasRowId, idxes, typs := getRowsIdIndex(colIndexes, colTypes)
	if hasRowId {
		// generate rowid
		if rowid, err = preparePhyAddrData(
			types.T_Rowid.ToType(),
			info.BlockID[:],
			0,
			info.MetaLocation().Rows(),
			m,
		); err != nil {
			return
		}
		defer func() {
			if err != nil {
				rowid.Free(m)
				rowid = nil
			}
		}()
	}

	readColumns := func(cols []uint16) (result *batch.Batch, loaded *batch.Batch, err error) {
		if len(cols) == 0 && hasRowId {
			// only read rowid column on non appendable block, return early
			result = batch.NewWithSize(1)
			result.Vecs[0] = rowid
			return
		}

		if loaded, err = LoadColumns(ctx, cols, typs, fs, info.MetaLocation(), m); err != nil {
			return
		}

		colPos := 0
		result = batch.NewWithSize(len(colTypes))
		for i, typ := range colTypes {
			if typ.Oid == types.T_Rowid {
				result.Vecs[i] = rowid
			} else {
				result.Vecs[i] = loaded.Vecs[colPos]
				colPos++
			}
		}
		return
	}

	readABlkColumns := func(cols []uint16) (result *batch.Batch, deletedRows []int64, err error) {
		var loaded *batch.Batch
		// appendable block should be filtered by committs
		cols = append(cols, objectio.SEQNUM_COMMITTS, objectio.SEQNUM_ABORT) // committs, aborted

		// no need to add typs, the two columns won't be generated
		if result, loaded, err = readColumns(cols); err != nil {
			return
		}

		t0 := time.Now()
		aborts := vector.MustFixedCol[bool](loaded.Vecs[len(loaded.Vecs)-1])
		commits := vector.MustFixedCol[types.TS](loaded.Vecs[len(loaded.Vecs)-2])
		for i := 0; i < len(commits); i++ {
			if aborts[i] || commits[i].Greater(ts) {
				deletedRows = append(deletedRows, int64(i))
			}
		}
		logutil.Debugf(
			"blockread %s scan filter cost %v: base %s filter out %v\n ",
			info.BlockID.String(), time.Since(t0), ts.ToString(), len(deletedRows))
		return
	}

	if info.EntryState {
		bat, deletedRows, err = readABlkColumns(idxes)
	} else {
		bat, _, err = readColumns(idxes)
	}

	return
}

func readBlockDelete(ctx context.Context, deltaloc objectio.Location, fs fileservice.FileService) (*batch.Batch, error) {
	bat, err := LoadColumns(ctx, []uint16{0, 1, 2}, nil, fs, deltaloc, nil)
	if err != nil {
		return nil, err
	}
	return bat, nil
}

func evalDeleteRowsByTimestamp(deletes *batch.Batch, ts types.TS) (rows []int64) {
	if deletes == nil {
		return nil
	}
	// record visible delete rows
	deletedRows := nulls.NewWithSize(0)
	rowids := vector.MustFixedCol[types.Rowid](deletes.Vecs[0])
	tss := vector.MustFixedCol[types.TS](deletes.Vecs[1])
	aborts := vector.MustFixedCol[bool](deletes.Vecs[2])

	for i, rowid := range rowids {
		if aborts[i] || tss[i].Greater(ts) {
			continue
		}
		_, row := model.DecodePhyAddrKey(&rowid)
		nulls.Add(deletedRows, uint64(row))
	}

	if !deletedRows.Any() {
		return
	}

	rows = make([]int64, 0, nulls.Length(deletedRows))

	it := deletedRows.Np.Iterator()
	for it.HasNext() {
		row := it.Next()
		rows = append(rows, int64(row))
	}
	return
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
