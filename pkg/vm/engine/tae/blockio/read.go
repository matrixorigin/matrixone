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

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
)

func removeIf[T any](data []T, pred func(t T) bool) []T {
	// from plan.RemoveIf
	if len(data) == 0 {
		return data
	}
	res := 0
	for i := 0; i < len(data); i++ {
		if !pred(data[i]) {
			if res != i {
				data[res] = data[i]
			}
			res++
		}
	}
	return data[:res]
}

// ReadDataByFilter only read block data from storage by filter, don't apply deletes.
// Right now, it cannot support filter by physical address column.
// len(columns) == len(colTypes) == 1
func ReadDataByFilter(
	ctx context.Context,
	tableName string,
	info *objectio.BlockInfo,
	ds engine.DataSource,
	columns []uint16,
	colTypes []types.Type,
	ts types.TS,
	searchFunc objectio.ReadFilterSearchFuncType,
	cacheVectors containers.Vectors,
	mp *mpool.MPool,
	fs fileservice.FileService,
) (sels []int64, err error) {
	// PXU TODO: temporary solution, need to be refactored
	// cannot filter by physical address column now
	deleteMask, release, err := readBlockData(
		ctx,
		columns,
		colTypes,
		-1,
		info,
		ts,
		fileservice.Policy(0),
		cacheVectors,
		mp,
		fs,
	)
	if err != nil {
		return
	}
	defer release()
	defer deleteMask.Release()

	sels = searchFunc(&cacheVectors[0])
	if !deleteMask.IsEmpty() {
		sels = removeIf(sels, func(i int64) bool {
			return deleteMask.Contains(uint64(i))
		})
	}
	if len(sels) == 0 {
		return
	}
	sels, err = ds.ApplyTombstones(ctx, &info.BlockID, sels, engine.Policy_CheckAll)
	return
}

// BlockDataReadNoCopy only read block data from storage, don't apply deletes.
func BlockDataReadNoCopy(
	ctx context.Context,
	info *objectio.BlockInfo,
	ds engine.DataSource,
	columns []uint16,
	colTypes []types.Type,
	ts types.TS,
	policy fileservice.Policy,
	mp *mpool.MPool,
	fs fileservice.FileService,
) (*batch.Batch, *nulls.Bitmap, func(), error) {
	if logutil.GetSkip1Logger().Core().Enabled(zap.DebugLevel) {
		logutil.Debugf("read block %s, columns %v, types %v", info.BlockID.String(), columns, colTypes)
	}

	var (
		deleteMask objectio.Bitmap
		release    func()
		err        error
	)

	defer func() {
		if err != nil {
			if release != nil {
				release()
			}
		}
	}()

	cacheVectors := containers.NewVectors(len(columns) + 1)

	phyAddrColumnPos := -1
	for i := range columns {
		if columns[i] == objectio.SEQNUM_ROWID {
			phyAddrColumnPos = i
			break
		}
	}

	// read block data from storage specified by meta location
	if deleteMask, release, err = readBlockData(
		ctx, columns, colTypes, phyAddrColumnPos, info, ts, policy, cacheVectors, mp, fs,
	); err != nil {
		return nil, nil, nil, err
	}
	defer deleteMask.Release()

	tombstones, err := ds.GetTombstones(ctx, &info.BlockID)
	if err != nil {
		release()
		return nil, nil, nil, err
	}

	// merge deletes from tombstones
	if !deleteMask.IsValid() {
		deleteMask = tombstones
	} else {
		deleteMask.Or(tombstones)
		tombstones.Release()
	}
	outputBat := batch.NewWithSize(len(columns))

	loadedColumnPos := 0
	for outputColPos := range columns {
		if outputColPos != phyAddrColumnPos {
			outputBat.Vecs[outputColPos] = &cacheVectors[loadedColumnPos]
			loadedColumnPos++
		} else {
			outputBat.Vecs[outputColPos] = vector.NewVec(objectio.RowidType)
			if err = buildRowidColumn(
				info, outputBat.Vecs[phyAddrColumnPos], nil, mp,
			); err != nil {
				release()
				return nil, nil, nil, err
			}
			release = func() {
				release()
				outputBat.Vecs[phyAddrColumnPos].Free(mp)
			}
		}
	}
	outputBat.SetRowCount(outputBat.Vecs[0].Length())

	// FIXME: w-zr
	var retMask *nulls.Bitmap

	if !deleteMask.IsEmpty() {
		retMask = &nulls.Bitmap{}
		retMask.OrBitmap(deleteMask.Bitmap())
	}
	return outputBat, retMask, release, nil
}

// BlockDataRead only read block data from storage, don't apply deletes.
func BlockDataRead(
	ctx context.Context,
	info *objectio.BlockInfo,
	ds engine.DataSource,
	columns []uint16,
	colTypes []types.Type,
	phyAddrColumnPos int,
	ts timestamp.Timestamp,
	filterSeqnums []uint16,
	filterColTypes []types.Type,
	filter objectio.BlockReadFilter,
	policy fileservice.Policy,
	tableName string,
	bat *batch.Batch,
	cacheVectors containers.Vectors,
	mp *mpool.MPool,
	fs fileservice.FileService,
) error {
	if logutil.GetSkip1Logger().Core().Enabled(zap.DebugLevel) {
		logutil.Debugf("read block %s, columns %v, types %v", info.BlockID.String(), columns, colTypes)
	}

	snapshotTS := types.TimestampToTS(ts)

	var (
		sels []int64
		err  error
	)

	searchFunc := filter.DecideSearchFunc(info.IsSorted())

	if searchFunc != nil {
		if sels, err = ReadDataByFilter(
			ctx,
			tableName,
			info,
			ds,
			filterSeqnums,
			filterColTypes,
			snapshotTS,
			searchFunc,
			cacheVectors,
			mp,
			fs,
		); err != nil {
			return err
		}
		v2.TaskSelReadFilterTotal.Inc()
		if len(sels) == 0 {
			v2.TaskSelReadFilterHit.Inc()
		}

		if len(sels) == 0 {
			return nil
		}
	}

	err = BlockDataReadInner(
		ctx,
		info,
		ds,
		columns,
		colTypes,
		phyAddrColumnPos,
		snapshotTS,
		sels,
		policy,
		bat,
		cacheVectors,
		mp,
		fs,
	)
	if err != nil {
		return err
	}

	bat.SetRowCount(bat.Vecs[0].Length())
	return nil
}

func BlockCompactionRead(
	ctx context.Context,
	location objectio.Location,
	deletes []int64,
	seqnums []uint16,
	colTypes []types.Type,
	fs fileservice.FileService,
	mp *mpool.MPool,
) (*batch.Batch, error) {
	cacheVectors := containers.NewVectors(len(seqnums))

	release, err := ioutil.LoadColumns(
		ctx, seqnums, colTypes, fs, location, cacheVectors, mp, fileservice.Policy(0),
	)
	if err != nil {
		return nil, err
	}
	defer release()
	if len(deletes) == 0 {
		result := batch.NewWithSize(len(seqnums))
		for i := range cacheVectors {
			result.Vecs[i] = &cacheVectors[i]
		}
		result.SetRowCount(result.Vecs[0].Length())
		return result, nil
	}
	result := batch.NewWithSize(len(seqnums))
	for i, col := range cacheVectors {
		typ := *col.GetType()
		result.Vecs[i] = vector.NewVec(typ)
		if err = vector.GetUnionAllFunction(typ, mp)(result.Vecs[i], &col); err != nil {
			break
		}
		result.Vecs[i].Shrink(deletes, true)
	}

	if err != nil {
		for _, col := range result.Vecs {
			if col != nil {
				col.Free(mp)
			}
		}
		return nil, err
	}
	result.SetRowCount(result.Vecs[0].Length())
	return result, nil
}

func windowCNBatch(bat *batch.Batch, start, end uint64) error {
	var err error
	for i, vec := range bat.Vecs {
		bat.Vecs[i], err = vec.Window(int(start), int(end))
		if err != nil {
			return err
		}
	}
	return nil
}

func BlockDataReadBackup(
	ctx context.Context,
	info *objectio.BlockInfo,
	ds engine.DataSource,
	idxes []uint16,
	ts types.TS,
	fs fileservice.FileService,
) (loaded *batch.Batch, sortKey uint16, err error) {
	if len(idxes) == 0 {
		loaded, sortKey, err = ioutil.LoadOneBlock(ctx, fs, info.MetaLocation(), objectio.SchemaData)
	} else {
		loaded, sortKey, err = ioutil.LoadOneBlockWithIndex(ctx, fs, idxes, info.MetaLocation(), objectio.SchemaData)
	}
	// read block data from storage specified by meta location
	if err != nil {
		return
	}
	if !ts.IsEmpty() {
		commitTs := types.TS{}
		for v := 0; v < loaded.Vecs[0].Length(); v++ {
			err = commitTs.Unmarshal(loaded.Vecs[len(loaded.Vecs)-1].GetRawBytesAt(v))
			if err != nil {
				return
			}
			if commitTs.GT(&ts) {
				err = windowCNBatch(loaded, 0, uint64(v))
				if err != nil {
					return
				}
				logutil.Info("[BlockDataReadBackup]",
					zap.String("commitTs", commitTs.ToString()),
					zap.String("ts", ts.ToString()),
					zap.String("location", info.MetaLocation().String()),
					zap.Int("rows", v))
				break
			}
		}
	}
	tombstones, err := ds.GetTombstones(ctx, &info.BlockID)
	if err != nil {
		return
	}
	defer tombstones.Release()
	rows := tombstones.ToI64Array()
	if len(rows) > 0 {
		logutil.Info("[BlockDataReadBackup Shrink]", zap.String("location", info.MetaLocation().String()), zap.Int("rows", len(rows)))
		loaded.Shrink(rows, true)
	}
	return
}

// BlockDataReadInner only read data,don't apply deletes.
func BlockDataReadInner(
	ctx context.Context,
	info *objectio.BlockInfo,
	ds engine.DataSource,
	columns []uint16,
	colTypes []types.Type,
	phyAddrColumnPos int,
	ts types.TS,
	selectRows []int64, // if selectRows is not empty, it was already filtered by filter
	policy fileservice.Policy,
	outputBat *batch.Batch,
	cacheVectors containers.Vectors,
	mp *mpool.MPool,
	fs fileservice.FileService,
) (err error) {
	var (
		deletedRows []int64
		deleteMask  objectio.Bitmap
		release     func()
	)

	// read block data from storage specified by meta location
	if deleteMask, release, err = readBlockData(
		ctx,
		columns,
		colTypes,
		phyAddrColumnPos,
		info,
		ts,
		policy,
		cacheVectors,
		mp,
		fs,
	); err != nil {
		return
	}
	defer release()
	defer deleteMask.Release()

	// len(selectRows) > 0 means it was already filtered by pk filter
	if len(selectRows) > 0 {
		// phyAddrColumnPos >= 0 means one of the columns is the physical address column
		// The physical address column should be generated by the blockid and rowid
		if phyAddrColumnPos >= 0 {
			if err = buildRowidColumn(
				info, outputBat.Vecs[phyAddrColumnPos], selectRows, mp,
			); err != nil {
				return
			}
		}

		// cacheVectors contains all the loaded columns from the storage, which
		// doesn't contain the physical address column. And the physical address column
		// is already filled into the outputBat.Vecs[phyAddrColumnPos]
		loadedColumnPos := 0
		for outputColPos := range columns {
			if outputColPos == phyAddrColumnPos {
				continue
			}
			if err = outputBat.Vecs[outputColPos].PreExtendWithArea(
				len(selectRows), 0, mp,
			); err != nil {
				break
			}
			if err = outputBat.Vecs[outputColPos].Union(
				&cacheVectors[loadedColumnPos], selectRows, mp,
			); err != nil {
				break
			}
			loadedColumnPos++
		}
		return
	}

	tombstones, err := ds.GetTombstones(ctx, &info.BlockID)
	if err != nil {
		return
	}

	// merge deletes from tombstones
	if !deleteMask.IsValid() {
		deleteMask = tombstones
	} else {
		deleteMask.Or(tombstones)
		tombstones.Release()
	}

	// Note: it always goes here if no filter or the block is not sorted

	// transform delete mask to deleted rows
	// TODO: avoid this transformation
	if !deleteMask.IsEmpty() {
		deletedRows = deleteMask.ToI64Array()
	}

	// build rowid column if needed
	if phyAddrColumnPos >= 0 {
		if err = buildRowidColumn(
			info, outputBat.Vecs[phyAddrColumnPos], nil, mp,
		); err != nil {
			return
		}
	}

	loadedColumnPos := 0
	for outputColPos := range columns {
		if outputColPos != phyAddrColumnPos {
			loadedCol := &cacheVectors[loadedColumnPos]
			if err = outputBat.Vecs[outputColPos].UnionBatch(
				loadedCol,
				0,
				loadedCol.Length(),
				nil,
				mp,
			); err != nil {
				break
			}
			loadedColumnPos++
		}
		if len(deletedRows) > 0 {
			outputBat.Vecs[outputColPos].Shrink(deletedRows, true)
		}
	}
	return
}

func excludePhyAddrColumn(
	colIndexes []uint16, colTypes []types.Type, phyAddrColumnPos int,
) ([]uint16, []types.Type) {
	if phyAddrColumnPos < 0 {
		return colIndexes, colTypes
	}
	idxes := make([]uint16, 0, len(colTypes)-1)
	typs := make([]types.Type, 0, len(colTypes)-1)
	idxes = append(idxes, colIndexes[:phyAddrColumnPos]...)
	idxes = append(idxes, colIndexes[phyAddrColumnPos+1:]...)
	typs = append(typs, colTypes[:phyAddrColumnPos]...)
	typs = append(typs, colTypes[phyAddrColumnPos+1:]...)
	return idxes, typs
}

func buildRowidColumn(
	info *objectio.BlockInfo,
	vec *vector.Vector,
	sels []int64,
	m *mpool.MPool,
) (err error) {
	if len(sels) == 0 {
		err = objectio.ConstructRowidColumnTo(
			vec,
			&info.BlockID,
			0,
			info.MetaLocation().Rows(),
			m,
		)
	} else {
		err = objectio.ConstructRowidColumnToWithSels(
			vec,
			&info.BlockID,
			sels,
			m,
		)
	}
	return
}

// This func load columns from storage of specified column indexes
// No memory copy, the loaded data is directly stored in the cacheVectors
// if `phyAddrColumnPos` >= 0, it means one of the columns is the physical address column,
// which is not loaded from storage, but generated by the blockid and rowid. We should exclude it.
// `release` is a function to release the pinned memory cache
// Example 1:
// colIndexes = [0, 1, 2, 3], phyAddrColumnPos = 2
// 1) exclude the physical address column => [0, 1, 3]
// 2) load columns [0, 1, 3] from storage into cacheVectors[0, 1, 2]
// Example 2:
// colIndexes = [0, 1, 2, 3], phyAddrColumnPos = -1
// load columns [0, 1, 2, 3] from storage into cacheVectors[0, 1, 2, 3]
func readBlockData(
	ctx context.Context,
	colIndexes []uint16,
	colTypes []types.Type,
	phyAddrColumnPos int,
	info *objectio.BlockInfo,
	ts types.TS,
	policy fileservice.Policy,
	cacheVectors containers.Vectors,
	m *mpool.MPool,
	fs fileservice.FileService,
) (
	deleteMask objectio.Bitmap,
	release func(),
	err error,
) {
	cacheVectors.Free(m)

	idxes, typs := excludePhyAddrColumn(colIndexes, colTypes, phyAddrColumnPos)

	readColumns := func(
		cols []uint16,
		cacheVectors2 containers.Vectors,
	) (err2 error) {
		if len(cols) == 0 && phyAddrColumnPos >= 0 {
			// only read rowid column on non appendable block, return early
			release = func() {}
			return
		}

		release, err2 = ioutil.LoadColumns(
			ctx, cols, typs, fs, info.MetaLocation(), cacheVectors2, m, policy,
		)
		return
	}

	readABlkColumns := func(
		cols []uint16,
		cacheVectors2 containers.Vectors,
	) (
		deletes objectio.Bitmap,
		err2 error,
	) {
		// appendable block should be filtered by committs
		//cols = append(cols, objectio.SEQNUM_COMMITTS, objectio.SEQNUM_ABORT) // committs, aborted
		cols = append(cols, objectio.SEQNUM_COMMITTS) // committs, aborted

		// no need to add typs, the two columns won't be generated
		if err2 = readColumns(
			cols, cacheVectors2,
		); err2 != nil {
			return
		}

		deletes = objectio.GetReusableBitmap()

		t0 := time.Now()
		//aborts := vector.MustFixedColWithTypeCheck[bool](loaded.Vecs[len(loaded.Vecs)-1])
		commits := vector.MustFixedColWithTypeCheck[types.TS](&cacheVectors2[len(cols)-1])
		for i := 0; i < len(commits); i++ {
			if commits[i].GT(&ts) {
				deletes.Add(uint64(i))
			}
		}
		logutil.Debugf(
			"blockread %s scan filter cost %v: base %s filter out %v\n ",
			info.BlockID.String(),
			time.Since(t0),
			ts.ToString(),
			deletes.Count(),
		)
		return
	}

	if info.IsAppendable() {
		deleteMask, err = readABlkColumns(idxes, cacheVectors)
	} else {
		err = readColumns(idxes, cacheVectors)
	}

	return
}
