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

package ioutil

import (
	"context"
	"math"
	"sort"

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
)

func ListTSRangeFiles(
	ctx context.Context,
	dir string,
	fs fileservice.FileService,
) (files []TSRangeFile, err error) {
	var (
		entries []fileservice.DirEntry
	)
	if entries, err = fileservice.SortedList(
		fs.List(ctx, dir),
	); err != nil {
		return
	}
	for _, entry := range entries {
		if !entry.IsDir {
			if file := DecodeTSRangeFile(entry.Name); file.IsValid() {
				files = append(files, file)
			}
		}
	}
	return
}

func ListTSRangeFilesInGCDir(
	ctx context.Context,
	fs fileservice.FileService,
) (files []TSRangeFile, err error) {
	var (
		entries []fileservice.DirEntry
	)
	if entries, err = fileservice.SortedList(
		fs.List(ctx, GetGCDir()),
	); err != nil {
		return
	}
	for _, entry := range entries {
		if !entry.IsDir {
			if file := DecodeTSRangeFile(entry.Name); file.IsValid() {
				files = append(files, file)
			}
		}
	}
	return
}

func IsRowDeleted(
	ctx context.Context,
	ts *types.TS,
	row *types.Rowid,
	getTombstoneFileFn func() (*objectio.ObjectStats, error),
	fs fileservice.FileService,
) (bool, error) {
	var isDeleted bool
	loadedBlkCnt := 0
	onBlockSelectedFn := func(tombstoneObject *objectio.ObjectStats, pos int) (bool, error) {
		if isDeleted {
			return false, nil
		}
		var err error
		var location objectio.ObjectLocation
		tombstoneObject.BlockLocationTo(uint16(pos), objectio.BlockMaxRows, location[:])
		deleted, err := IsRowDeletedByLocation(
			ctx, ts, row, location[:], fs, tombstoneObject.GetCNCreated(),
		)
		if err != nil {
			return false, err
		}
		loadedBlkCnt++
		// if deleted, stop searching
		if deleted {
			isDeleted = true
			return false, nil
		}
		return true, nil
	}

	tombstoneObjectCnt, skipObjectCnt, totalBlkCnt, err := CheckTombstoneFile(
		ctx, row[:], getTombstoneFileFn, onBlockSelectedFn, fs,
	)
	if err != nil {
		return false, err
	}

	v2.TxnReaderEachBLKLoadedTombstoneHistogram.Observe(float64(loadedBlkCnt))
	v2.TxnReaderScannedTotalTombstoneHistogram.Observe(float64(tombstoneObjectCnt))
	if tombstoneObjectCnt > 0 {
		v2.TxnReaderTombstoneZMSelectivityHistogram.Observe(float64(skipObjectCnt) / float64(tombstoneObjectCnt))
	}
	if totalBlkCnt > 0 {
		v2.TxnReaderTombstoneBLSelectivityHistogram.Observe(float64(loadedBlkCnt) / float64(totalBlkCnt))
	}

	return isDeleted, nil
}

func GetTombstonesByBlockId(
	ctx context.Context,
	ts *types.TS,
	blockId *objectio.Blockid,
	getTombstoneFileFn func() (*objectio.ObjectStats, error),
	deletedMask *objectio.Bitmap,
	fs fileservice.FileService,
) (err error) {
	loadedBlkCnt := 0
	onBlockSelectedFn := func(tombstoneObject *objectio.ObjectStats, pos int) (bool, error) {
		var (
			err2     error
			mask     objectio.Bitmap
			location objectio.ObjectLocation
		)
		tombstoneObject.BlockLocationTo(uint16(pos), objectio.BlockMaxRows, location[:])
		if mask, err2 = FillBlockDeleteMask(
			ctx, ts, blockId, location[:], fs, tombstoneObject.GetCNCreated(),
		); err2 != nil {
			return false, err2
		} else {
			deletedMask.Or(mask)
		}
		loadedBlkCnt++
		mask.Release()
		return true, nil
	}

	var (
		tombstoneObjectCnt int
		skipObjectCnt      int
		totalBlkCnt        int
	)

	if tombstoneObjectCnt, skipObjectCnt, totalBlkCnt, err = CheckTombstoneFile(
		ctx, blockId[:], getTombstoneFileFn, onBlockSelectedFn, fs,
	); err != nil {
		return
	}

	if loadedBlkCnt > 0 {
		v2.TxnReaderEachBLKLoadedTombstoneHistogram.Observe(float64(loadedBlkCnt))
	}

	if tombstoneObjectCnt > 0 {
		v2.TxnReaderScannedTotalTombstoneHistogram.Observe(float64(tombstoneObjectCnt))
	}

	if tombstoneObjectCnt > 0 && skipObjectCnt > 0 {
		v2.TxnReaderTombstoneZMSelectivityHistogram.Observe(float64(skipObjectCnt) / float64(tombstoneObjectCnt))
	}
	if totalBlkCnt > 0 && loadedBlkCnt > 0 {
		v2.TxnReaderTombstoneBLSelectivityHistogram.Observe(float64(loadedBlkCnt) / float64(totalBlkCnt))
	}

	return
}

/*
func FindTombstonesOfBlock(
	ctx context.Context,
	blockId objectio.Blockid,
	tombstoneObjects []objectio.ObjectStats,
	fs fileservice.FileService,
) (sels bitmap.Bitmap, err error) {
	return findTombstoneOfXXX(ctx, blockId[:], tombstoneObjects, fs)
}
*/

func FindTombstonesOfObject(
	ctx context.Context,
	objectId *objectio.ObjectId,
	tombstoneObjects []objectio.ObjectStats,
	fs fileservice.FileService,
) (sels bitmap.Bitmap, err error) {
	return findTombstoneOfXXX(ctx, objectId[:], tombstoneObjects, fs)
}

func findTombstoneOfXXX(
	ctx context.Context,
	pattern []byte,
	tombstoneObjects []objectio.ObjectStats,
	fs fileservice.FileService,
) (sels bitmap.Bitmap, err error) {
	sels.InitWithSize(int64(len(tombstoneObjects)))
	var curr int
	getTombstoneFile := func() (*objectio.ObjectStats, error) {
		if curr >= len(tombstoneObjects) {
			return nil, nil
		}
		i := curr
		curr++
		return &tombstoneObjects[i], nil
	}
	onBlockSelectedFn := func(tombstoneObject *objectio.ObjectStats, pos int) (bool, error) {
		sels.Add(uint64(curr - 1))
		return false, nil
	}
	_, _, _, err = CheckTombstoneFile(
		ctx, pattern, getTombstoneFile, onBlockSelectedFn, fs,
	)
	return
}

func CheckTombstoneFile(
	ctx context.Context,
	prefixPattern []byte,
	getTombstoneFileFn func() (*objectio.ObjectStats, error),
	onBlockSelectedFn func(*objectio.ObjectStats, int) (bool, error),
	fs fileservice.FileService,
) (
	tombstoneObjectCnt int,
	skipObjectCnt int,
	totalBlkCnt int,
	err error,
) {
	if getTombstoneFileFn == nil {
		return
	}
	var tombstoneObject *objectio.ObjectStats
	for tombstoneObject, err = getTombstoneFileFn(); err == nil && tombstoneObject != nil; tombstoneObject, err = getTombstoneFileFn() {
		tombstoneObjectCnt++
		tombstoneZM := tombstoneObject.SortKeyZoneMap()
		if !tombstoneZM.RowidPrefixEq(prefixPattern) {
			skipObjectCnt++
			continue
		}
		var objMeta objectio.ObjectMeta
		location := tombstoneObject.ObjectLocation()

		if objMeta, err = objectio.FastLoadObjectMeta(
			ctx, &location, false, fs,
		); err != nil {
			return
		}
		dataMeta := objMeta.MustDataMeta()

		blkCnt := int(dataMeta.BlockCount())
		totalBlkCnt += blkCnt

		startIdx := sort.Search(blkCnt, func(i int) bool {
			return dataMeta.GetBlockMeta(uint32(i)).MustGetColumn(0).ZoneMap().AnyGEByValue(prefixPattern)
		})

		for pos := startIdx; pos < blkCnt; pos++ {
			blkMeta := dataMeta.GetBlockMeta(uint32(pos))
			columnZonemap := blkMeta.MustGetColumn(0).ZoneMap()
			// block id is the prefixPattern of the rowid and zonemap is min-max of rowid
			// !PrefixEq means there is no rowid of this block in this zonemap, so skip
			if columnZonemap.RowidPrefixEq(prefixPattern) {
				var goOn bool
				if goOn, err = onBlockSelectedFn(tombstoneObject, pos); err != nil || !goOn {
					break
				}
			} else if columnZonemap.RowidPrefixGT(prefixPattern) {
				// all zone maps are sorted by the rowid
				// if the block id is less than the prefixPattern of the min rowid, skip the rest blocks
				break
			}
		}
	}
	return
}

// CoarseFilterTombstoneObject It is used to filter out tombstone objects that do not contain any deleted data objects.
// This is a coarse filter using ZM, so false positives may occur
func CoarseFilterTombstoneObject(
	ctx context.Context,
	nextDeletedDataObject func() *objectio.ObjectId,
	tombstoneObjects []objectio.ObjectStats,
	fs fileservice.FileService,
) (filtered []objectio.ObjectStats, err error) {
	var bm, b bitmap.Bitmap
	bm.InitWithSize(int64(len(tombstoneObjects)))
	var objid *objectio.ObjectId
	for objid = nextDeletedDataObject(); objid != nil; objid = nextDeletedDataObject() {
		b, err = FindTombstonesOfObject(ctx, objid, tombstoneObjects, fs)
		if err != nil {
			return
		}
		bm.Or(&b)
	}
	filtered = make([]objectio.ObjectStats, 0, bm.Count())
	itr := bm.Iterator()
	for itr.HasNext() {
		filtered = append(filtered, tombstoneObjects[itr.Next()])
	}

	return
}

func IsRowDeletedByLocation(
	ctx context.Context,
	snapshotTS *types.TS,
	row *objectio.Rowid,
	location objectio.Location,
	fs fileservice.FileService,
	createdByCN bool,
) (deleted bool, err error) {
	var hidden objectio.HiddenColumnSelection
	if !createdByCN {
		hidden = hidden | objectio.HiddenColumnSelection_CommitTS
	}

	attrs := objectio.GetTombstoneAttrs(hidden)
	data := containers.NewVectors(len(attrs))
	_, release, err := ReadDeletes(ctx, location, fs, createdByCN, data)
	if err != nil {
		return
	}
	defer release()
	if data.Rows() == 0 {
		return
	}
	rowids := vector.MustFixedColNoTypeCheck[types.Rowid](&data[0])
	idx := sort.Search(len(rowids), func(i int) bool {
		return rowids[i].GE(row)
	})
	if createdByCN {
		deleted = (idx < len(rowids)) && (rowids[idx].EQ(row))
	} else {
		tss := vector.MustFixedColNoTypeCheck[types.TS](&data[1])
		for i := idx; i < len(rowids); i++ {
			if !rowids[i].EQ(row) {
				break
			}
			if tss[i].LE(snapshotTS) {
				deleted = true
				break
			}
		}
	}
	return
}

func FillBlockDeleteMask(
	ctx context.Context,
	snapshotTS *types.TS,
	blockId *types.Blockid,
	location objectio.Location,
	fs fileservice.FileService,
	createdByCN bool,
) (deleteMask objectio.Bitmap, err error) {
	if location.IsEmpty() {
		return
	}

	var (
		release func()
		meta    objectio.ObjectDataMeta
		hidden  objectio.HiddenColumnSelection
	)

	if !createdByCN {
		hidden = hidden | objectio.HiddenColumnSelection_CommitTS
	}

	attrs := objectio.GetTombstoneAttrs(hidden)
	persistedDeletes := containers.NewVectors(len(attrs))

	if meta, release, err = ReadDeletes(
		ctx, location, fs, createdByCN, persistedDeletes,
	); err != nil {
		return
	}
	defer release()

	if createdByCN {
		deleteMask = EvalDeleteMaskFromCNCreatedTombstones(blockId, &persistedDeletes[0])
	} else {
		deleteMask = EvalDeleteMaskFromDNCreatedTombstones(
			&persistedDeletes[0],
			&persistedDeletes[1],
			meta.GetBlockMeta(uint32(location.ID())),
			snapshotTS,
			blockId,
		)
	}

	return
}

func ReadDeletes(
	ctx context.Context,
	deltaLoc objectio.Location,
	fs fileservice.FileService,
	isPersistedByCN bool,
	cacheVectors containers.Vectors,
) (objectio.ObjectDataMeta, func(), error) {

	var cols []uint16
	var typs []types.Type

	if isPersistedByCN {
		cols = []uint16{objectio.TombstoneAttr_Rowid_SeqNum}
		typs = []types.Type{objectio.RowidType}
	} else {
		cols = []uint16{objectio.TombstoneAttr_Rowid_SeqNum, objectio.TombstoneAttr_CommitTs_SeqNum}
		typs = []types.Type{objectio.RowidType, objectio.TSType}
	}
	return LoadTombstoneColumns(
		ctx, cols, typs, fs, deltaLoc, cacheVectors, nil, fileservice.Policy(0),
	)
}

func EvalDeleteMaskFromDNCreatedTombstones(
	deletedRows *vector.Vector,
	commitTSVec *vector.Vector,
	meta objectio.BlockObject,
	ts *types.TS,
	blockid *types.Blockid,
) (rows objectio.Bitmap) {
	if deletedRows == nil {
		return
	}
	rowids := vector.MustFixedColWithTypeCheck[types.Rowid](deletedRows)
	start, end := FindStartEndOfBlockFromSortedRowids(rowids, blockid)
	if start >= end {
		return
	}

	noTSCheck := false
	if end-start > 10 {
		// fast path is true if the maxTS is less than the snapshotTS
		// this means that all the rows between start and end are visible
		idx := objectio.GetTombstoneCommitTSAttrIdx(meta.GetMetaColumnCount())
		noTSCheck = meta.MustGetColumn(idx).ZoneMap().FastLEValue(ts[:], 0)
	}
	rows = objectio.GetReusableBitmap()
	if noTSCheck {
		for i := end - 1; i >= start; i-- {
			row := rowids[i].GetRowOffset()
			rows.Add(uint64(row))
		}
	} else {
		tss := vector.MustFixedColWithTypeCheck[types.TS](commitTSVec)
		for i := end - 1; i >= start; i-- {
			if tss[i].GT(ts) {
				continue
			}
			row := rowids[i].GetRowOffset()
			rows.Add(uint64(row))
		}
	}

	return
}

func EvalDeleteMaskFromCNCreatedTombstones(
	bid *types.Blockid,
	deletedRows *vector.Vector,
) (rows objectio.Bitmap) {
	if deletedRows == nil {
		return
	}
	rowids := vector.MustFixedColWithTypeCheck[types.Rowid](deletedRows)

	start, end := FindStartEndOfBlockFromSortedRowids(rowids, bid)
	if start < end {
		rows = objectio.GetReusableBitmap()
	}
	for i := end - 1; i >= start; i-- {
		row := rowids[i].GetRowOffset()
		rows.Add(uint64(row))
	}
	return
}

func FindStartEndOfBlockFromSortedRowids(rowids []types.Rowid, id *types.Blockid) (start int, end int) {
	lowRowid := objectio.NewRowid(id, 0)
	highRowid := objectio.NewRowid(id, math.MaxUint32)
	i, j := 0, len(rowids)
	for i < j {
		m := (i + j) / 2
		// first value >= lowRowid
		if !rowids[m].LT(lowRowid) {
			j = m
		} else {
			i = m + 1
		}
	}
	start = i

	i, j = 0, len(rowids)
	for i < j {
		m := (i + j) / 2
		// first value > highRowid
		if highRowid.LT(&rowids[m]) {
			j = m
		} else {
			i = m + 1
		}
	}
	end = i
	return
}
