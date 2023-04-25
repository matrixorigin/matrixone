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
	"sort"
	"time"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"

	"github.com/matrixorigin/matrixone/pkg/container/vector"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
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
	mp *mpool.MPool, vp engine.VectorPool,
	pkidxInColIdxs int, deleteRows []int64,
	ufs []func(*vector.Vector, *vector.Vector, int64) error,
	searchFunc func(*vector.Vector) int) (*batch.Batch, error) {

	// read
	return BlockReadInner(
		ctx, info, colIdxes, colTypes,
		types.TimestampToTS(ts), fs, mp, vp,
		pkidxInColIdxs, deleteRows, ufs,
		searchFunc,
	)
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
	mp *mpool.MPool, vp engine.VectorPool,
	pkidxInColIdxs int, deleteRows []int64,
	ufs []func(*vector.Vector, *vector.Vector, int64) error,
	searchFunc func(*vector.Vector) int) (*batch.Batch, error) {
	if !info.DeltaLocation().IsEmpty() {
		deleteBatch, err := readBlockDelete(ctx, info.DeltaLocation(), fs)
		if err != nil {
			return nil, err
		}
		if deleteBatch != nil {
			for i := 0; i < deleteBatch.Vecs[0].Length(); i++ {
				if vector.GetFixedAt[bool](deleteBatch.Vecs[2], i) {
					continue
				}
				if vector.GetFixedAt[types.TS](deleteBatch.Vecs[1], i).Greater(ts) {
					continue
				}
				rowid := vector.GetFixedAt[types.Rowid](deleteBatch.Vecs[0], i)
				row := types.DecodeUint32(rowid[types.BlockidSize:])
				deleteRows = append(deleteRows, int64(row))
			}
			sort.Slice(deleteRows, func(i, j int) bool { return deleteRows[i] < deleteRows[j] })
		}
		logutil.Infof(
			"blockread %s read delete %d: base %s filter out %v\n",
			info.BlockID.String(), deleteBatch.Length(), ts.ToString(), len(deleteRows))
	}
	return readBlockData(ctx, colIdxes, colTypes, info, ts,
		fs, mp, vp, pkidxInColIdxs, searchFunc, deleteRows, ufs)
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
	fs fileservice.FileService, m *mpool.MPool, vp engine.VectorPool,
	pkidxInColIdxs int, searchFunc func(*vector.Vector) int,
	deleteRows []int64, ufs []func(*vector.Vector, *vector.Vector, int64) error) (*batch.Batch, error) {
	id := info.MetaLocation().ID()
	ok, _, idxes := getRowsIdIndex(colIndexes, colTypes)
	reader, err := NewObjectReader(fs, info.MetaLocation())
	if err != nil {
		return nil, err
	}
	prefix := info.BlockID[:]
	loadBlock := func(idxes []uint16) (*batch.Batch, error) {
		if len(idxes) == 0 && ok {
			var rowid types.Rowid

			rbat := batch.NewWithSize(1)
			rbat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
			for j := uint32(0); j < uint32(info.MetaLocation().Rows()); j++ {
				copy(rowid[:], prefix)
				copy(rowid[types.BlockidSize:], types.EncodeUint32(&j))
				if err := vector.AppendFixed(rbat.Vecs[0], rowid, false, m); err != nil {
					rbat.Clean(m)
					return nil, err
				}
			}
			if len(deleteRows) > 0 {
				rbat.Vecs[0].Shrink(deleteRows, true)
			}
			return rbat, nil
		}
		bat, err := reader.LoadColumns(ctx, idxes, id, nil)
		if err != nil {
			return nil, err
		}
		rbat := batch.NewWithSize(len(bat.Vecs))
		for i, vec := range bat.Vecs {
			if vp == nil {
				rbat.Vecs[i] = vector.NewVec(*vec.GetType())
			} else {
				rbat.Vecs[i] = vp.GetVector(*vec.GetType())
			}
		}
		if searchFunc != nil {
			vec := bat.GetVector(int32(pkidxInColIdxs))
			sel := int64(searchFunc(vec))
			if sel >= 0 && sel < int64(vec.Length()) {
				i := sort.Search(len(deleteRows), func(i int) bool { return sel <= deleteRows[0] })
				if i < len(deleteRows) && deleteRows[i] == sel {
					return rbat, nil
				}
				for i, vec := range bat.Vecs {
					if vec.GetType().Oid == types.T_Rowid {
						if err := vector.AppendFixed(rbat.Vecs[i],
							model.EncodePhyAddrKeyWithPrefix(prefix, uint32(sel)), false, m); err != nil {
							rbat.Clean(m)
							return nil, err
						}
					} else {
						if err := ufs[i](rbat.Vecs[i], vec, sel); err != nil {
							rbat.Clean(m)
							return nil, err
						}
					}
				}
				rbat.SetZs(1, m)
				return rbat, nil
			}
			return rbat, nil
		}
		for i, vec := range bat.Vecs {
			typ := *vec.GetType()
			if typ.Oid == types.T_Rowid {
				var rowid types.Rowid

				for j := uint32(0); j < uint32(bat.Vecs[0].Length()); j++ {
					copy(rowid[:], prefix)
					copy(rowid[types.BlockidSize:], types.EncodeUint32(&j))
					if err := vector.AppendFixed(rbat.Vecs[i], rowid, false, m); err != nil {
						rbat.Clean(m)
						return nil, err
					}
				}
			} else {
				if err := vector.GetUnionFunction(typ, m)(rbat.Vecs[i], vec); err != nil {
					rbat.Clean(m)
					return nil, err
				}
			}
			if len(deleteRows) > 0 {
				rbat.Vecs[i].Shrink(deleteRows, true)
			}
		}
		rbat.SetZs(rbat.Vecs[0].Length(), m)
		return rbat, nil
	}
	loadAppendBlock := func() (*batch.Batch, error) {
		// appendable block should be filtered by committs
		meta, err := reader.reader.ReadMeta(ctx, m)
		if err != nil {
			return nil, err
		}
		block := meta.GetBlockMeta(0)
		colCount := block.GetColumnCount()
		idxes = append(idxes, colCount-2) // committs
		idxes = append(idxes, colCount-1) // aborted
		rbat, err := loadBlock(idxes)
		if err != nil {
			return nil, err
		}
		lenVecs := len(rbat.Vecs)
		t0 := time.Now()
		commits := rbat.Vecs[lenVecs-2]
		abort := rbat.Vecs[lenVecs-1]
		for i := 0; i < commits.Length(); i++ {
			if vector.GetFixedAt[bool](abort, i) || vector.GetFixedAt[types.TS](commits, i).Greater(ts) {
				deleteRows = append(deleteRows, int64(i))
			}
		}
		logutil.Infof(
			"blockread %s scan filter cost %v: base %s filter out %v\n ",
			info.BlockID.String(), time.Since(t0), ts.ToString(), len(deleteRows))
		for i := range rbat.Vecs {
			if len(deleteRows) > 0 {
				rbat.Vecs[i].Shrink(deleteRows, true)
			}
		}
		m.PutSels(rbat.Zs)
		rbat.SetZs(rbat.Vecs[0].Length(), m)
		return rbat, nil
	}
	if info.EntryState {
		return loadAppendBlock()
	} else {
		return loadBlock(idxes)
	}
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
func BlockPrefetch(
	columns []string,
	tableDef *plan.TableDef,
	service fileservice.FileService,
	infos [][]*pkgcatalog.BlockInfo) error {
	idxes := make([]uint16, len(columns))

	// Generate index for columns
	for i, column := range columns {
		if column != pkgcatalog.Row_ID {
			if colIdx, ok := tableDef.Name2ColIndex[column]; ok {
				idxes[i] = uint16(colIdx)
			} else {
				idxes[i] = uint16(len(tableDef.Name2ColIndex))
			}
		}
	}
	return PrefetchInner(idxes, service, infos)
}

func PrefetchInner(idxes []uint16, service fileservice.FileService, infos [][]*pkgcatalog.BlockInfo) error {
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
